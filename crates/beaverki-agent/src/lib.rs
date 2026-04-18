use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use beaverki_automation as automation;
use beaverki_core::{MemoryKind, MemoryScope, ShellRisk, TaskState, ToolInvocationStatus};
use beaverki_db::{
    Database, LuaToolRow, MemoryRow, NewApproval, NewLuaTool, NewLuaToolReview, NewSchedule,
    NewScript, NewScriptReview, NewTask, TaskRow, UpdateLuaTool, UpdateScript,
};
use beaverki_memory::{
    MemoryStore, RetrievalScope, SemanticMemoryRecord, SemanticMemoryWriteResult,
};
use beaverki_models::{ConversationItem, ModelProvider};
use beaverki_policy::{
    can_request_automation_approval, can_request_shell_approval, can_spawn_subagents,
    can_write_household_memory, visible_memory_scopes,
};
use beaverki_tools::{
    ToolContext, ToolDefinition, ToolError, ToolOutput, ToolRegistry,
    validate_json_schema_contract, validate_json_value_against_schema,
    validate_openai_tool_definitions,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::{info, warn};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub enum AgentMemoryMode {
    ScopedRetrieval,
    TaskSliceOnly,
}

#[derive(Debug, Clone)]
pub struct AgentRequest {
    pub owner_user_id: String,
    pub initiating_identity_id: String,
    pub primary_agent_id: String,
    pub assigned_agent_id: String,
    pub role_ids: Vec<String>,
    pub objective: String,
    pub scope: MemoryScope,
    pub kind: String,
    pub parent_task_id: Option<String>,
    pub task_context: Option<String>,
    pub visible_scopes: Vec<MemoryScope>,
    pub memory_mode: AgentMemoryMode,
    pub approved_shell_commands: Vec<String>,
    pub approved_automation_actions: Vec<ApprovedAutomationAction>,
}

#[derive(Debug, Clone)]
pub struct AgentResult {
    pub task: TaskRow,
}

pub struct PrimaryAgentRunner {
    db: Database,
    memory: MemoryStore,
    provider: Arc<dyn ModelProvider>,
    tools: ToolRegistry,
    base_tool_context: ToolContext,
    max_steps: usize,
}

enum ToolFailureDisposition {
    Paused(Box<AgentResult>),
    Continue(Value),
    Terminal(Box<AgentResult>),
}

#[derive(Debug, Clone)]
pub struct ApprovedAutomationAction {
    pub action_type: String,
    pub target_ref: String,
}

#[derive(Debug, Clone)]
struct RegisteredLuaTool {
    tool_id: String,
    description: String,
    source_text: String,
    input_schema: Value,
    output_schema: Value,
    capability_profile: Value,
    source: RegisteredLuaToolSource,
}

impl RegisteredLuaTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.tool_id.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
        }
    }

    fn source_kind(&self) -> &'static str {
        match self.source {
            RegisteredLuaToolSource::Database { .. } => "database",
            RegisteredLuaToolSource::Filesystem { .. } => "filesystem",
        }
    }

    fn source_ref(&self) -> String {
        match &self.source {
            RegisteredLuaToolSource::Database { owner_user_id } => owner_user_id.clone(),
            RegisteredLuaToolSource::Filesystem { manifest_path } => {
                manifest_path.display().to_string()
            }
        }
    }
}

#[derive(Debug, Clone)]
enum RegisteredLuaToolSource {
    Database { owner_user_id: String },
    Filesystem { manifest_path: PathBuf },
}

#[derive(Debug, Deserialize, Default)]
struct SkillManifest {
    #[serde(default)]
    tools: Vec<FilesystemLuaToolManifest>,
}

#[derive(Debug, Deserialize)]
struct FilesystemLuaToolManifest {
    tool_id: String,
    kind: String,
    description: String,
    #[serde(default)]
    source_text: Option<String>,
    #[serde(default)]
    source_path: Option<String>,
    input_schema: ManifestJsonSource,
    output_schema: ManifestJsonSource,
    #[serde(default)]
    capability_profile: Option<Value>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ManifestJsonSource {
    Inline(Value),
    RelativePath(String),
}

impl PrimaryAgentRunner {
    pub fn new(
        db: Database,
        memory: MemoryStore,
        provider: Arc<dyn ModelProvider>,
        tools: ToolRegistry,
        tool_context: ToolContext,
        max_steps: usize,
    ) -> Self {
        Self {
            db,
            memory,
            provider,
            tools,
            base_tool_context: tool_context,
            max_steps,
        }
    }

    pub async fn run_task(&self, request: AgentRequest) -> Result<AgentResult> {
        let task = self
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &request.owner_user_id,
                initiating_identity_id: &request.initiating_identity_id,
                primary_agent_id: &request.primary_agent_id,
                assigned_agent_id: &request.assigned_agent_id,
                parent_task_id: request.parent_task_id.as_deref(),
                session_id: None,
                kind: &request.kind,
                objective: &request.objective,
                context_summary: request.task_context.as_deref(),
                scope: request.scope,
                wake_at: None,
            })
            .await?;

        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "task_created",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "kind": request.kind,
                    "parent_task_id": request.parent_task_id,
                }),
            )
            .await?;

        self.execute_task(task, request).await
    }

    pub async fn resume_task(&self, task: TaskRow, request: AgentRequest) -> Result<AgentResult> {
        self.execute_task(task, request).await
    }

    async fn execute_task(&self, task: TaskRow, request: AgentRequest) -> Result<AgentResult> {
        self.db.clear_task_result(&task.task_id).await?;
        self.db
            .update_task_state(&task.task_id, TaskState::Running)
            .await?;

        let lifecycle_event = if matches!(
            task.state.parse::<TaskState>(),
            Ok(TaskState::WaitingApproval)
        ) {
            "task_resumed"
        } else {
            "task_started"
        };
        self.db
            .append_task_event(
                &task.task_id,
                lifecycle_event,
                "agent",
                &request.assigned_agent_id,
                json!({
                    "objective": request.objective,
                    "kind": request.kind,
                    "parent_task_id": request.parent_task_id,
                }),
            )
            .await?;

        let mut memories = Vec::new();
        if matches!(request.memory_mode, AgentMemoryMode::ScopedRetrieval) {
            let retrieval_scope = RetrievalScope {
                owner_user_id: Some(request.owner_user_id.clone()),
                visible_scopes: request.visible_scopes.clone(),
                limit: 8,
            };
            memories = self
                .memory
                .retrieve_for_agent_context(&retrieval_scope)
                .await?;
            self.db
                .record_audit_event(
                    "agent",
                    &request.assigned_agent_id,
                    "memory_scope_resolved",
                    json!({
                        "task_id": task.task_id,
                        "visible_scopes": retrieval_scope
                            .visible_scopes
                            .iter()
                            .map(|scope| scope.as_str())
                            .collect::<Vec<_>>(),
                        "owner_user_id": request.owner_user_id,
                        "memory_count": memories.len(),
                    }),
                )
                .await?;
        } else {
            self.db
                .record_audit_event(
                    "agent",
                    &request.assigned_agent_id,
                    "memory_scope_resolved",
                    json!({
                        "task_id": task.task_id,
                        "visible_scopes": [],
                        "owner_user_id": request.owner_user_id,
                        "memory_count": 0,
                        "mode": "task_slice_only",
                    }),
                )
                .await?;
        }

        let mut conversation = vec![ConversationItem::UserText(build_user_prompt(
            &request.objective,
            &request.task_context,
            &memories,
            &request.approved_shell_commands,
            &request.approved_automation_actions,
        ))];
        let instructions = system_prompt(
            &request.kind,
            &request.role_ids,
            &self.base_tool_context.allowed_roots,
        );
        let mut tool_context = self.base_tool_context.clone();
        tool_context.approved_shell_commands = request.approved_shell_commands.clone();

        for step in 0..self.max_steps {
            let (model_role, model_name) = if step == 0 {
                ("planner", self.provider.model_names().planner.as_str())
            } else {
                ("executor", self.provider.model_names().executor.as_str())
            };
            let registered_lua_tools = self
                .load_registered_lua_tools(&request.owner_user_id, &tool_context)
                .await?;
            let available_tools =
                tool_definitions_with_registered_lua_tools(&self.tools, &registered_lua_tools);
            let response = self
                .provider
                .generate_turn(
                    model_role,
                    model_name,
                    &instructions,
                    &conversation,
                    &available_tools,
                )
                .await?;

            info!(
                task_id = %task.task_id,
                model_role = %model_role,
                model_name = %model_name,
                input_tokens = ?response.usage.as_ref().and_then(|usage| usage.input_tokens),
                output_tokens = ?response.usage.as_ref().and_then(|usage| usage.output_tokens),
                "model turn finished"
            );

            for item in &response.output_items {
                conversation.push(ConversationItem::AssistantOutput(item.clone()));
            }

            self.db
                .append_task_event(
                    &task.task_id,
                    "model_turn_completed",
                    "agent",
                    &request.assigned_agent_id,
                    json!({
                        "step": step,
                        "model_role": model_role,
                        "model_name": model_name,
                        "tool_call_count": response.tool_calls.len(),
                        "input_tokens": response.usage.as_ref().and_then(|usage| usage.input_tokens),
                        "output_tokens": response.usage.as_ref().and_then(|usage| usage.output_tokens),
                        "output_text": response.output_text,
                    }),
                )
                .await?;

            if response.tool_calls.is_empty() {
                let answer = if response.output_text.trim().is_empty() {
                    "Task completed without a textual response.".to_owned()
                } else {
                    response.output_text
                };
                self.db.complete_task(&task.task_id, &answer).await?;
                if matches!(request.memory_mode, AgentMemoryMode::ScopedRetrieval) {
                    self.memory
                        .record_summary_memory(
                            &request.owner_user_id,
                            request.scope,
                            &task.task_id,
                            &request.objective,
                            &answer,
                        )
                        .await?;
                }
                self.db
                    .append_task_event(
                        &task.task_id,
                        "task_completed",
                        "agent",
                        &request.assigned_agent_id,
                        json!({ "result_text": answer }),
                    )
                    .await?;
                let completed = self
                    .db
                    .fetch_task_for_owner(&request.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("task disappeared after completion"))?;
                return Ok(AgentResult { task: completed });
            }

            for tool_call in response.tool_calls {
                if tool_call.name == "agent_spawn_subagent" {
                    let output = self
                        .handle_subagent_call(&task, &request, tool_call.arguments.clone())
                        .await?;
                    conversation.push(ConversationItem::FunctionCallOutput {
                        call_id: tool_call.call_id,
                        output: output.payload,
                    });
                    continue;
                }

                let invocation_id = self
                    .db
                    .start_tool_invocation(
                        &task.task_id,
                        &request.assigned_agent_id,
                        &tool_call.name,
                        tool_call.arguments.clone(),
                    )
                    .await?;

                let invocation_result = self
                    .invoke_tool_call(
                        &task,
                        &request,
                        &tool_call.name,
                        tool_call.arguments.clone(),
                        &tool_context,
                    )
                    .await;

                match invocation_result {
                    Ok(output) => {
                        self.db
                            .finish_tool_invocation(
                                &invocation_id,
                                ToolInvocationStatus::Completed,
                                output.payload.clone(),
                            )
                            .await?;
                        self.db
                            .append_task_event(
                                &task.task_id,
                                "tool_invocation_completed",
                                "tool",
                                &tool_call.name,
                                json!({
                                    "invocation_id": invocation_id,
                                    "response": output.payload,
                                }),
                            )
                            .await?;
                        conversation.push(ConversationItem::FunctionCallOutput {
                            call_id: tool_call.call_id,
                            output: output.payload,
                        });
                    }
                    Err(error) => match self
                        .maybe_pause_for_approval(
                            &task,
                            &request,
                            &tool_call.name,
                            &invocation_id,
                            &tool_call.call_id,
                            error,
                        )
                        .await?
                    {
                        ToolFailureDisposition::Paused(paused_result) => {
                            return Ok(*paused_result);
                        }
                        ToolFailureDisposition::Continue(response_json) => {
                            conversation.push(ConversationItem::FunctionCallOutput {
                                call_id: tool_call.call_id,
                                output: response_json,
                            });
                        }
                        ToolFailureDisposition::Terminal(result) => {
                            return Ok(*result);
                        }
                    },
                }
            }
        }

        self.db
            .fail_task(
                &task.task_id,
                "Agent exceeded the maximum number of execution steps.",
            )
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "task_failed",
                "agent",
                &request.assigned_agent_id,
                json!({ "reason": "max_steps_exceeded" }),
            )
            .await?;
        let failed = self
            .db
            .fetch_task_for_owner(&request.owner_user_id, &task.task_id)
            .await?
            .ok_or_else(|| anyhow!("task disappeared after failure"))?;
        Ok(AgentResult { task: failed })
    }

    async fn maybe_pause_for_approval(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        tool_name: &str,
        invocation_id: &str,
        call_id: &str,
        error: ToolError,
    ) -> Result<ToolFailureDisposition> {
        let response_json = error.as_json();
        let status = match &error {
            ToolError::Denied { .. } => ToolInvocationStatus::Denied,
            ToolError::Failed(_) => ToolInvocationStatus::Failed,
        };
        self.db
            .finish_tool_invocation(invocation_id, status, response_json.clone())
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "tool_invocation_failed",
                "tool",
                tool_name,
                json!({
                    "invocation_id": invocation_id,
                    "response": response_json,
                    "call_id": call_id,
                }),
            )
            .await?;

        if tool_name == "shell_exec"
            && let ToolError::Denied { detail, .. } = &error
            && let Some(command) = detail.get("command").and_then(Value::as_str)
            && let Some(risk) = parse_shell_risk(detail.get("risk").and_then(Value::as_str))
        {
            let review = self
                .review_shell_command(
                    &task.task_id,
                    &request.owner_user_id,
                    &task.objective,
                    command,
                    risk,
                )
                .await?;
            self.db
                .append_task_event(
                    &task.task_id,
                    "safety_review_completed",
                    "safety_agent",
                    "agent_safety_builtin",
                    json!({
                        "tool_name": tool_name,
                        "command": command,
                        "risk": review.risk_level,
                        "verdict": review.verdict,
                        "summary": review.summary,
                    }),
                )
                .await?;
            self.db
                .record_audit_event(
                    "safety_agent",
                    "agent_safety_builtin",
                    "shell_command_reviewed",
                    json!({
                        "task_id": task.task_id,
                        "owner_user_id": request.owner_user_id,
                        "command": command,
                        "risk": review.risk_level,
                        "verdict": review.verdict,
                        "summary": review.summary,
                    }),
                )
                .await?;

            if !review.approved() {
                let message = format!(
                    "Safety review rejected shell command '{}': {}",
                    command, review.summary
                );
                self.db.fail_task(&task.task_id, &message).await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "task_failed",
                        "safety_agent",
                        "agent_safety_builtin",
                        json!({
                            "reason": message,
                            "command": command,
                            "risk": review.risk_level,
                        }),
                    )
                    .await?;
                let failed = self
                    .db
                    .fetch_task_for_owner(&request.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("task disappeared after safety rejection"))?;
                return Ok(ToolFailureDisposition::Terminal(Box::new(AgentResult {
                    task: failed,
                })));
            }

            if can_request_shell_approval(&request.role_ids, risk) {
                let rationale = format!(
                    "Task '{}' requested shell command '{}', classified as {} risk. Safety review approved it with summary: {}",
                    task.objective,
                    command,
                    risk.as_str(),
                    review.summary
                );
                let action_summary = format!("Run shell command '{command}'");
                let approval = self
                    .db
                    .create_approval(NewApproval {
                        task_id: &task.task_id,
                        action_type: "shell_command",
                        target_ref: Some(command),
                        requested_by_agent_id: &request.assigned_agent_id,
                        requested_from_user_id: &request.owner_user_id,
                        rationale_text: &rationale,
                        risk_level: Some(risk.as_str()),
                        action_summary: Some(&action_summary),
                        requester_display_name: Some(&request.assigned_agent_id),
                        target_details: Some(command),
                    })
                    .await?;
                let message = format!(
                    "Approval required to run shell command: {} (risk: {}). Approval ID: {}",
                    command,
                    risk.as_str(),
                    approval.approval_id
                );
                self.db
                    .set_task_waiting_approval(&task.task_id, &message)
                    .await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "approval_requested",
                        "agent",
                        &request.assigned_agent_id,
                        json!({
                            "approval_id": approval.approval_id,
                            "action_type": approval.action_type,
                            "target_ref": approval.target_ref,
                        }),
                    )
                    .await?;
                self.db
                    .record_audit_event(
                        "agent",
                        &request.assigned_agent_id,
                        "approval_requested",
                        json!({
                            "task_id": task.task_id,
                            "approval_id": approval.approval_id,
                            "requested_from_user_id": approval.requested_from_user_id,
                            "command": command,
                        }),
                    )
                    .await?;
                let waiting = self
                    .db
                    .fetch_task_for_owner(&request.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("task disappeared after approval pause"))?;
                return Ok(ToolFailureDisposition::Paused(Box::new(AgentResult {
                    task: waiting,
                })));
            }

            let reason = format!(
                "User roles [{}] are not allowed to request approval for {}-risk shell commands. Command: {}",
                roles_label(&request.role_ids),
                risk.as_str(),
                command
            );
            return self
                .fail_task_terminal(
                    task,
                    request,
                    &reason,
                    json!({
                        "tool_name": tool_name,
                        "command": command,
                        "risk": risk.as_str(),
                        "approval_eligible": false,
                    }),
                )
                .await;
        }

        if matches!(
            tool_name,
            "lua_script_activate" | "lua_script_schedule" | "lua_tool_activate"
        ) && let ToolError::Denied { detail, .. } = &error
            && let Some(action_type) = detail.get("action_type").and_then(Value::as_str)
            && let Some(target_ref) = detail.get("target_ref").and_then(Value::as_str)
            && let Some(rationale) = detail.get("rationale").and_then(Value::as_str)
        {
            if can_request_automation_approval(&request.role_ids) {
                let action_summary = format!("Approve {} for {}", action_type, target_ref);
                let approval = self
                    .db
                    .create_approval(NewApproval {
                        task_id: &task.task_id,
                        action_type,
                        target_ref: Some(target_ref),
                        requested_by_agent_id: &request.assigned_agent_id,
                        requested_from_user_id: &request.owner_user_id,
                        rationale_text: rationale,
                        risk_level: Some("high"),
                        action_summary: Some(&action_summary),
                        requester_display_name: Some(&request.assigned_agent_id),
                        target_details: Some(target_ref),
                    })
                    .await?;
                let message = format!(
                    "Approval required for {}: {}. Approval ID: {}",
                    action_type, target_ref, approval.approval_id
                );
                self.db
                    .set_task_waiting_approval(&task.task_id, &message)
                    .await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "approval_requested",
                        "agent",
                        &request.assigned_agent_id,
                        json!({
                            "approval_id": approval.approval_id,
                            "action_type": approval.action_type,
                            "target_ref": approval.target_ref,
                        }),
                    )
                    .await?;
                self.db
                    .record_audit_event(
                        "agent",
                        &request.assigned_agent_id,
                        "approval_requested",
                        json!({
                            "task_id": task.task_id,
                            "approval_id": approval.approval_id,
                            "requested_from_user_id": approval.requested_from_user_id,
                            "action_type": action_type,
                            "target_ref": target_ref,
                        }),
                    )
                    .await?;
                let waiting = self
                    .db
                    .fetch_task_for_owner(&request.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("task disappeared after automation approval pause"))?;
                return Ok(ToolFailureDisposition::Paused(Box::new(AgentResult {
                    task: waiting,
                })));
            }

            let reason = format!(
                "User roles [{}] are not allowed to request automation approval for {} '{}'.",
                roles_label(&request.role_ids),
                action_type,
                target_ref
            );
            return self
                .fail_task_terminal(
                    task,
                    request,
                    &reason,
                    json!({
                        "tool_name": tool_name,
                        "action_type": action_type,
                        "target_ref": target_ref,
                        "approval_eligible": false,
                    }),
                )
                .await;
        }

        Ok(ToolFailureDisposition::Continue(response_json))
    }

    async fn fail_task_terminal(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        reason: &str,
        detail: Value,
    ) -> Result<ToolFailureDisposition> {
        self.db.fail_task(&task.task_id, reason).await?;
        self.db
            .append_task_event(
                &task.task_id,
                "task_failed",
                "agent",
                &request.assigned_agent_id,
                json!({
                    "reason": reason,
                    "detail": detail,
                }),
            )
            .await?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "task_failed",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "reason": reason,
                    "detail": detail,
                }),
            )
            .await?;
        let failed = self
            .db
            .fetch_task_for_owner(&request.owner_user_id, &task.task_id)
            .await?
            .ok_or_else(|| anyhow!("task disappeared after failure"))?;
        Ok(ToolFailureDisposition::Terminal(Box::new(AgentResult {
            task: failed,
        })))
    }

    async fn invoke_tool_call(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        tool_name: &str,
        arguments: Value,
        tool_context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        match tool_name {
            "memory_read" => self.handle_memory_read(task, request, arguments).await,
            "memory_write" => self.handle_memory_write(task, request, arguments).await,
            "memory_remember" => self.handle_memory_remember(task, request, arguments).await,
            "memory_forget" => self.handle_memory_forget(task, request, arguments).await,
            "lua_script_list"
            | "lua_script_get"
            | "lua_script_write"
            | "lua_script_activate"
            | "lua_script_run"
            | "lua_script_schedule"
            | "lua_tool_list"
            | "lua_tool_get"
            | "lua_tool_write"
            | "lua_tool_activate" => {
                self.handle_lua_tool_call(task, request, tool_name, arguments, tool_context)
                    .await
            }
            _ => {
                if let Some(lua_tool) = self
                    .find_registered_lua_tool(&request.owner_user_id, tool_name, tool_context)
                    .await
                    .map_err(ToolError::Failed)?
                {
                    self.handle_registered_lua_tool_call(
                        task,
                        request,
                        &lua_tool,
                        arguments,
                        tool_context,
                    )
                    .await
                } else {
                    self.tools.invoke(tool_name, arguments, tool_context).await
                }
            }
        }
    }

    async fn handle_lua_tool_call(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        tool_name: &str,
        arguments: Value,
        tool_context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        match tool_name {
            "lua_script_list" => self.handle_lua_script_list(request).await,
            "lua_script_get" => self.handle_lua_script_get(request, arguments).await,
            "lua_script_write" => self.handle_lua_script_write(task, request, arguments).await,
            "lua_script_activate" => {
                self.handle_lua_script_activate(task, request, arguments)
                    .await
            }
            "lua_script_run" => {
                self.handle_lua_script_run(task, request, arguments, tool_context)
                    .await
            }
            "lua_script_schedule" => {
                self.handle_lua_script_schedule(task, request, arguments)
                    .await
            }
            "lua_tool_list" => self.handle_lua_tool_list(request).await,
            "lua_tool_get" => self.handle_lua_tool_get(request, arguments).await,
            "lua_tool_write" => self.handle_lua_tool_write(task, request, arguments).await,
            "lua_tool_activate" => {
                self.handle_lua_tool_activate(task, request, arguments)
                    .await
            }
            _ => Err(ToolError::Failed(anyhow!("unknown Lua tool: {tool_name}"))),
        }
    }

    async fn handle_memory_remember(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        self.handle_semantic_memory_write(task, request, arguments, "memory_remember")
            .await
    }

    async fn handle_memory_write(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        self.handle_semantic_memory_write(task, request, arguments, "memory_write")
            .await
    }

    async fn handle_memory_read(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let explicit_scopes = explicit_memory_tool_scopes(request);
        let scope_filter = required_string_arg(&arguments, "scope", "memory_read")?;
        let scopes = match scope_filter {
            "visible" => request.visible_scopes.clone(),
            "private" => vec![MemoryScope::Private],
            "household" => vec![MemoryScope::Household],
            other => {
                return Err(ToolError::Failed(anyhow!(
                    "memory_read only supports visible, private, or household scope filters, got '{other}'"
                )));
            }
        };

        if scopes.is_empty() {
            return Err(ToolError::Denied {
                message: "memory reads are not available in this task context".to_owned(),
                detail: json!({
                    "scope": scope_filter,
                }),
            });
        }

        if scope_filter != "visible" && scopes.iter().any(|scope| !explicit_scopes.contains(scope))
        {
            return Err(ToolError::Denied {
                message: "requested memory scope is not accessible to the current user".to_owned(),
                detail: json!({
                    "scope": scope_filter,
                }),
            });
        }

        let subject_type = optional_string_arg(&arguments, "subject_type");
        let subject_key = optional_string_arg(&arguments, "subject_key");
        let limit = arguments
            .get("limit")
            .and_then(Value::as_i64)
            .map(|value| value.clamp(1, 10))
            .unwrap_or(5);

        let memories = self
            .db
            .query_memories(
                Some(&request.owner_user_id),
                &scopes,
                Some(MemoryKind::Semantic),
                subject_type,
                subject_key,
                false,
                false,
                limit,
            )
            .await
            .map_err(ToolError::Failed)?;

        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "semantic_memory_read",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "scope_filter": scope_filter,
                    "scopes": scopes.iter().map(|scope| scope.as_str()).collect::<Vec<_>>(),
                    "subject_type": subject_type,
                    "subject_key": subject_key,
                    "limit": limit,
                    "memory_ids": memories.iter().map(|memory| memory.memory_id.clone()).collect::<Vec<_>>(),
                }),
            )
            .await
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "status": "ok",
                "scope_filter": scope_filter,
                "subject_type": subject_type,
                "subject_key": subject_key,
                "match_count": memories.len(),
                "memories": memories.iter().map(memory_row_to_json).collect::<Vec<_>>(),
            }),
        })
    }

    async fn handle_semantic_memory_write(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
        tool_name: &str,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let explicit_scopes = explicit_memory_tool_scopes(request);
        let scope = required_string_arg(&arguments, "scope", tool_name)?
            .parse::<MemoryScope>()
            .map_err(ToolError::Failed)?;
        if !matches!(scope, MemoryScope::Private | MemoryScope::Household) {
            return Err(ToolError::Failed(anyhow!(
                "{tool_name} only supports private or household scope"
            )));
        }
        if !explicit_scopes.contains(&scope) {
            self.db
                .record_audit_event(
                    "agent",
                    &request.assigned_agent_id,
                    "semantic_memory_write_denied",
                    json!({
                        "task_id": task.task_id,
                        "owner_user_id": request.owner_user_id,
                        "requested_scope": scope.as_str(),
                        "subject_type": arguments.get("subject_type").and_then(Value::as_str),
                        "subject_key": arguments.get("subject_key").and_then(Value::as_str),
                        "reason": "scope_not_accessible",
                    }),
                )
                .await
                .map_err(ToolError::Failed)?;
            return Err(ToolError::Denied {
                message: "requested memory scope is not accessible to the current user".to_owned(),
                detail: json!({
                    "scope": scope.as_str(),
                    "subject_key": arguments.get("subject_key").and_then(Value::as_str),
                }),
            });
        }
        let subject_type = required_string_arg(&arguments, "subject_type", tool_name)?;
        let subject_key = required_string_arg(&arguments, "subject_key", tool_name)?;
        let content_text = required_string_arg(&arguments, "content_text", tool_name)?;
        let source_type = required_string_arg(&arguments, "source_type", tool_name)?;
        let source_summary = required_string_arg(&arguments, "source_summary", tool_name)?;
        let source_ref = arguments
            .get("source_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());

        if matches!(scope, MemoryScope::Household) && !can_write_household_memory(&request.role_ids)
        {
            self.db
                .record_audit_event(
                    "agent",
                    &request.assigned_agent_id,
                    "semantic_memory_write_denied",
                    json!({
                        "task_id": task.task_id,
                        "owner_user_id": request.owner_user_id,
                        "requested_scope": scope.as_str(),
                        "subject_type": subject_type,
                        "subject_key": subject_key,
                        "reason": "household_scope_not_allowed",
                    }),
                )
                .await
                .map_err(ToolError::Failed)?;
            return Err(ToolError::Denied {
                message: "household semantic memory writes are not allowed for this user"
                    .to_owned(),
                detail: json!({
                    "scope": scope.as_str(),
                    "subject_key": subject_key,
                }),
            });
        }

        let owner_user_id = if matches!(scope, MemoryScope::Household) {
            None
        } else {
            Some(request.owner_user_id.as_str())
        };
        let write_result = self
            .memory
            .remember_semantic_memory(SemanticMemoryRecord {
                owner_user_id,
                scope,
                subject_type,
                subject_key,
                content_text,
                sensitivity: "normal",
                source_type,
                source_ref,
                source_summary,
                task_id: Some(&task.task_id),
            })
            .await
            .map_err(ToolError::Failed)?;

        let payload = semantic_memory_write_payload(
            &write_result,
            scope,
            subject_type,
            subject_key,
            content_text,
            source_type,
            source_ref,
        );
        let event_detail = payload.clone();

        let (event_type, audit_payload) = match &write_result {
            SemanticMemoryWriteResult::Created { memory_id } => (
                "semantic_memory_created",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "memory_id": memory_id,
                    "scope": scope.as_str(),
                    "subject_type": subject_type,
                    "subject_key": subject_key,
                    "source_type": source_type,
                    "source_ref": source_ref,
                }),
            ),
            SemanticMemoryWriteResult::Deduplicated { memory_id } => (
                "semantic_memory_deduplicated",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "memory_id": memory_id,
                    "scope": scope.as_str(),
                    "subject_type": subject_type,
                    "subject_key": subject_key,
                    "source_type": source_type,
                    "source_ref": source_ref,
                }),
            ),
            SemanticMemoryWriteResult::Corrected {
                previous_memory_id,
                memory_id,
            } => (
                "semantic_memory_corrected",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "previous_memory_id": previous_memory_id,
                    "memory_id": memory_id,
                    "scope": scope.as_str(),
                    "subject_type": subject_type,
                    "subject_key": subject_key,
                    "source_type": source_type,
                    "source_ref": source_ref,
                }),
            ),
        };
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                event_type,
                audit_payload,
            )
            .await
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: event_detail,
        })
    }

    async fn handle_memory_forget(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let explicit_scopes = explicit_memory_tool_scopes(request);
        let memory_id = required_string_arg(&arguments, "memory_id", "memory_forget")?;
        let reason = required_string_arg(&arguments, "reason", "memory_forget")?;
        let memory = self
            .db
            .fetch_memory(memory_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("memory '{memory_id}' not found")))?;
        let memory_scope = memory
            .scope
            .parse::<MemoryScope>()
            .map_err(ToolError::Failed)?;

        if !explicit_scopes.contains(&memory_scope) {
            return Err(ToolError::Denied {
                message: "memory is not accessible to the current user".to_owned(),
                detail: json!({ "memory_id": memory_id }),
            });
        }
        match memory.owner_user_id.as_deref() {
            Some(owner_user_id) if owner_user_id == request.owner_user_id => {}
            None => {}
            _ => {
                return Err(ToolError::Denied {
                    message: "memory is not accessible to the current user".to_owned(),
                    detail: json!({ "memory_id": memory_id }),
                });
            }
        }
        if matches!(memory_scope, MemoryScope::Household)
            && !can_write_household_memory(&request.role_ids)
        {
            self.db
                .record_audit_event(
                    "agent",
                    &request.assigned_agent_id,
                    "memory_forget_denied",
                    json!({
                        "task_id": task.task_id,
                        "owner_user_id": request.owner_user_id,
                        "memory_id": memory_id,
                        "reason": "household_scope_not_allowed",
                    }),
                )
                .await
                .map_err(ToolError::Failed)?;
            return Err(ToolError::Denied {
                message: "household memory forget is not allowed for this user".to_owned(),
                detail: json!({ "memory_id": memory_id }),
            });
        }
        if memory.forgotten_at.is_some() {
            return Ok(ToolOutput {
                payload: json!({
                    "status": "already_forgotten",
                    "memory_id": memory_id,
                }),
            });
        }

        self.db
            .forget_memory(memory_id, reason)
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "memory_forgotten",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                    "memory_id": memory_id,
                    "scope": memory.scope,
                    "memory_kind": memory.memory_kind,
                    "subject_type": memory.subject_type,
                    "subject_key": memory.subject_key,
                    "reason": reason,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "status": "forgotten",
                "memory_id": memory_id,
                "reason": reason,
            }),
        })
    }

    async fn handle_lua_script_list(
        &self,
        request: &AgentRequest,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let scripts = self
            .db
            .list_scripts_for_owner(&request.owner_user_id)
            .await
            .map_err(ToolError::Failed)?;
        let scripts = scripts
            .into_iter()
            .map(|script| {
                json!({
                    "script_id": script.script_id,
                    "kind": script.kind,
                    "status": script.status,
                    "safety_status": script.safety_status,
                    "safety_summary": script.safety_summary,
                    "created_from_task_id": script.created_from_task_id,
                    "updated_at": script.updated_at,
                })
            })
            .collect::<Vec<_>>();

        Ok(ToolOutput {
            payload: json!({
                "scripts": scripts,
            }),
        })
    }

    async fn handle_lua_script_get(
        &self,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let script_id = required_string_arg(&arguments, "script_id", "lua_script_get")?;
        let script = self
            .db
            .fetch_script_for_owner(&request.owner_user_id, script_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("script '{script_id}' not found")))?;
        let capability_profile = serde_json::from_str::<Value>(&script.capability_profile_json)
            .map_err(|error| ToolError::Failed(anyhow!("invalid capability profile: {error}")))?;

        Ok(ToolOutput {
            payload: json!({
                "script_id": script.script_id,
                "kind": script.kind,
                "status": script.status,
                "source_text": script.source_text,
                "capability_profile": capability_profile,
                "created_from_task_id": script.created_from_task_id,
                "safety_status": script.safety_status,
                "safety_summary": script.safety_summary,
                "updated_at": script.updated_at,
            }),
        })
    }

    async fn handle_lua_script_write(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let script_id = arguments
            .get("script_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let source_text = arguments
            .get("source_text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("lua_script_write requires source_text")))?;
        let intended_behavior_summary = arguments
            .get("intended_behavior_summary")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ToolError::Failed(anyhow!(
                    "lua_script_write requires intended_behavior_summary"
                ))
            })?;
        let capability_profile = arguments
            .get("capability_profile")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let mut prior_status = None::<String>;

        let script = if let Some(script_id) = script_id.as_deref() {
            if let Some(existing_script) = self
                .db
                .fetch_script_for_owner(&request.owner_user_id, script_id)
                .await
                .map_err(ToolError::Failed)?
            {
                prior_status = Some(existing_script.status);
                self.db
                    .update_script_contents(UpdateScript {
                        script_id,
                        owner_user_id: &request.owner_user_id,
                        source_text,
                        capability_profile_json: capability_profile.clone(),
                        created_from_task_id: Some(&task.task_id),
                        status: "draft",
                        safety_status: "pending",
                        safety_summary: Some("Awaiting safety review."),
                    })
                    .await
                    .map_err(ToolError::Failed)?;
                self.db
                    .fetch_script_for_owner(&request.owner_user_id, script_id)
                    .await
                    .map_err(ToolError::Failed)?
                    .ok_or_else(|| {
                        ToolError::Failed(anyhow!("script '{script_id}' missing after update"))
                    })?
            } else {
                self.db
                    .create_script(NewScript {
                        script_id: Some(script_id),
                        owner_user_id: &request.owner_user_id,
                        kind: "lua",
                        status: "draft",
                        source_text,
                        capability_profile_json: capability_profile.clone(),
                        created_from_task_id: Some(&task.task_id),
                        safety_status: "pending",
                        safety_summary: Some("Awaiting safety review."),
                    })
                    .await
                    .map_err(ToolError::Failed)?
            }
        } else {
            self.db
                .create_script(NewScript {
                    script_id: None,
                    owner_user_id: &request.owner_user_id,
                    kind: "lua",
                    status: "draft",
                    source_text,
                    capability_profile_json: capability_profile.clone(),
                    created_from_task_id: Some(&task.task_id),
                    safety_status: "pending",
                    safety_summary: Some("Awaiting safety review."),
                })
                .await
                .map_err(ToolError::Failed)?
        };

        let review = automation::review_lua_script(
            &self.provider,
            &script.script_id,
            Some(&task.task_id),
            &request.owner_user_id,
            source_text,
            &capability_profile,
            intended_behavior_summary,
        )
        .await
        .map_err(ToolError::Failed)?;
        let approved_status = if prior_status.as_deref() == Some("active") {
            "active"
        } else {
            "draft"
        };
        let application =
            automation::apply_script_review(&review, approved_status, prior_status.as_deref());

        self.db
            .update_script_safety(
                &script.script_id,
                &application.safety_status,
                &review.summary,
                Some(&application.resulting_status),
            )
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .create_script_review(NewScriptReview {
                script_id: &script.script_id,
                reviewer_agent_id: automation::SAFETY_AGENT_ID,
                review_type: "lua_script",
                verdict: &review.verdict,
                risk_level: &review.risk_level,
                findings_json: review.as_findings_json(),
                summary_text: &review.summary,
                reviewed_artifact_text: source_text,
            })
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_script_written",
                json!({
                    "task_id": task.task_id,
                    "script_id": script.script_id,
                    "prior_status": prior_status,
                    "reactivated_after_rewrite": application.reactivated_after_rewrite,
                    "safety_status": application.safety_status,
                    "verdict": review.verdict,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;

        let updated = self
            .db
            .fetch_script_for_owner(&request.owner_user_id, &script.script_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("script missing after review")))?;

        Ok(ToolOutput {
            payload: json!({
                "script_id": updated.script_id,
                "status": updated.status,
                "safety_status": updated.safety_status,
                "safety_summary": updated.safety_summary,
                "reactivated_after_rewrite": application.reactivated_after_rewrite,
                "review_verdict": review.verdict,
                "risk_level": review.risk_level,
                "findings": review.findings,
                "required_changes": review.required_changes,
            }),
        })
    }

    async fn handle_lua_script_activate(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let script_id = required_string_arg(&arguments, "script_id", "lua_script_activate")?;
        let script = self
            .db
            .fetch_script_for_owner(&request.owner_user_id, script_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("script '{script_id}' not found")))?;
        if script.safety_status != "approved" {
            return Err(ToolError::Failed(anyhow!(
                "script '{}' must pass safety review before activation",
                script.script_id
            )));
        }
        if script.status == "active" {
            return Ok(ToolOutput {
                payload: json!({
                    "script_id": script.script_id,
                    "status": script.status,
                }),
            });
        }

        if !self.has_approved_action(request, "lua_script_activate", &script.script_id) {
            return Err(ToolError::Denied {
                message: "Lua script activation requires approval".to_owned(),
                detail: json!({
                    "action_type": "lua_script_activate",
                    "target_ref": script.script_id,
                    "rationale": format!(
                        "Task '{}' wants to activate Lua script '{}'.",
                        task.objective, script.script_id
                    ),
                }),
            });
        }

        self.db
            .update_script_status(&script.script_id, "active")
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_script_activated",
                json!({
                    "task_id": task.task_id,
                    "script_id": script.script_id,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;
        Ok(ToolOutput {
            payload: json!({
                "script_id": script.script_id,
                "status": "active",
            }),
        })
    }

    async fn handle_lua_script_run(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
        tool_context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let script_id = required_string_arg(&arguments, "script_id", "lua_script_run")?;
        let script = self
            .db
            .fetch_script_for_owner(&request.owner_user_id, script_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("script '{script_id}' not found")))?;
        if script.safety_status != "approved" || script.status != "active" {
            return Err(ToolError::Failed(anyhow!(
                "script '{}' must be active and safety-approved before it can run",
                script.script_id
            )));
        }
        let capability_profile: Value = serde_json::from_str(&script.capability_profile_json)
            .map_err(|error| ToolError::Failed(anyhow!("invalid capability profile: {error}")))?;
        let execution = automation::execute_lua_script(automation::LuaExecutionInput {
            db: self.db.clone(),
            owner_user_id: request.owner_user_id.clone(),
            task_id: task.task_id.clone(),
            script_id: script.script_id.clone(),
            source_text: script.source_text.clone(),
            input_json: None,
            capability_profile,
            working_dir: tool_context.working_dir.clone(),
            allowed_roots: tool_context.allowed_roots.clone(),
            browser_interactive_launcher: tool_context.browser_interactive_launcher.clone(),
            browser_headless_program: tool_context.browser_headless_program.clone(),
            browser_headless_args: tool_context.browser_headless_args.clone(),
        })
        .await;
        let execution = match execution {
            Ok(execution) => execution,
            Err(error) => {
                if let Some(policy_error) = error.downcast_ref::<automation::LuaToolPolicyDenied>()
                {
                    self.db
                        .append_task_event(
                            &task.task_id,
                            "lua_tool_denied",
                            "tool",
                            "lua",
                            json!({
                                "script_id": script.script_id,
                                "tool_name": policy_error.tool_name,
                                "detail": policy_error.detail,
                                "message": policy_error.message,
                            }),
                        )
                        .await
                        .map_err(ToolError::Failed)?;
                    self.db
                        .record_audit_event(
                            "agent",
                            &request.assigned_agent_id,
                            "lua_script_tool_denied",
                            json!({
                                "task_id": task.task_id,
                                "script_id": script.script_id,
                                "tool_name": policy_error.tool_name,
                                "detail": policy_error.detail,
                                "message": policy_error.message,
                            }),
                        )
                        .await
                        .map_err(ToolError::Failed)?;
                }
                return Err(ToolError::Failed(error));
            }
        };

        for log_line in &execution.logs {
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_log",
                    "tool",
                    "lua",
                    json!({ "message": log_line }),
                )
                .await
                .map_err(ToolError::Failed)?;
        }
        for notification in &execution.notifications {
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_notify_user",
                    "tool",
                    "lua",
                    json!({ "message": notification }),
                )
                .await
                .map_err(ToolError::Failed)?;
        }
        if let Some(wake_at) = &execution.deferred_until {
            let task_context = json!({ "script_id": script.script_id }).to_string();
            self.db
                .create_task_with_params(NewTask {
                    owner_user_id: &request.owner_user_id,
                    initiating_identity_id: &request.initiating_identity_id,
                    primary_agent_id: &request.primary_agent_id,
                    assigned_agent_id: &request.assigned_agent_id,
                    parent_task_id: Some(&task.task_id),
                    session_id: task.session_id.as_deref(),
                    kind: "lua_script",
                    objective: &format!("Run Lua script {}", script.script_id),
                    context_summary: Some(&task_context),
                    scope: request.scope,
                    wake_at: Some(wake_at),
                })
                .await
                .map_err(ToolError::Failed)?;
        }
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_script_run_inline",
                json!({
                    "task_id": task.task_id,
                    "script_id": script.script_id,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "script_id": script.script_id,
                "result_text": execution.result_text,
                "deferred_until": execution.deferred_until,
                "notifications": execution.notifications,
                "logs": execution.logs,
            }),
        })
    }

    async fn handle_lua_script_schedule(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let script_id = required_string_arg(&arguments, "script_id", "lua_script_schedule")?;
        let schedule_id = required_string_arg(&arguments, "schedule_id", "lua_script_schedule")?;
        let cron_expr = required_string_arg(&arguments, "cron_expr", "lua_script_schedule")?;
        let enabled = arguments
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let script = self
            .db
            .fetch_script_for_owner(&request.owner_user_id, script_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("script '{script_id}' not found")))?;
        if script.safety_status != "approved" || script.status != "active" {
            return Err(ToolError::Failed(anyhow!(
                "script '{}' must be active and safety-approved before it can be scheduled",
                script.script_id
            )));
        }
        let target_ref = format!(
            "schedule_id={schedule_id};script_id={script_id};cron={cron_expr};enabled={enabled}"
        );
        if !self.has_approved_action(request, "lua_script_schedule", &target_ref) {
            return Err(ToolError::Denied {
                message: "Lua script scheduling requires approval".to_owned(),
                detail: json!({
                    "action_type": "lua_script_schedule",
                    "target_ref": target_ref,
                    "rationale": format!(
                        "Task '{}' wants to schedule Lua script '{}' with cron '{}'.",
                        task.objective, script.script_id, cron_expr
                    ),
                }),
            });
        }

        let next_run_at = automation::next_run_after(cron_expr, &beaverki_core::now_rfc3339())
            .map_err(ToolError::Failed)?;
        let schedule = self
            .db
            .upsert_schedule(NewSchedule {
                schedule_id: Some(schedule_id),
                owner_user_id: &request.owner_user_id,
                target_type: "lua_script",
                target_id: &script.script_id,
                cron_expr,
                enabled,
                next_run_at: &next_run_at,
            })
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_script_scheduled",
                json!({
                    "task_id": task.task_id,
                    "script_id": script.script_id,
                    "schedule_id": schedule.schedule_id,
                    "cron_expr": schedule.cron_expr,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;
        Ok(ToolOutput {
            payload: json!({
                "schedule_id": schedule.schedule_id,
                "script_id": schedule.target_id,
                "cron_expr": schedule.cron_expr,
                "enabled": schedule.enabled != 0,
                "next_run_at": schedule.next_run_at,
            }),
        })
    }

    async fn handle_lua_tool_list(
        &self,
        request: &AgentRequest,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let tools = self
            .db
            .list_lua_tools_for_owner(&request.owner_user_id)
            .await
            .map_err(ToolError::Failed)?;
        let tools = tools
            .into_iter()
            .map(|tool| {
                json!({
                    "tool_id": tool.tool_id,
                    "status": tool.status,
                    "safety_status": tool.safety_status,
                    "safety_summary": tool.safety_summary,
                    "created_from_task_id": tool.created_from_task_id,
                    "updated_at": tool.updated_at,
                })
            })
            .collect::<Vec<_>>();

        Ok(ToolOutput {
            payload: json!({ "tools": tools }),
        })
    }

    async fn handle_lua_tool_get(
        &self,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let tool_id = required_string_arg(&arguments, "tool_id", "lua_tool_get")?;
        let tool = self
            .db
            .fetch_lua_tool_for_owner(&request.owner_user_id, tool_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("Lua tool '{tool_id}' not found")))?;
        let input_schema =
            parse_json_field(&tool.input_schema_json, "input schema").map_err(ToolError::Failed)?;
        let output_schema = parse_json_field(&tool.output_schema_json, "output schema")
            .map_err(ToolError::Failed)?;
        let capability_profile =
            parse_json_field(&tool.capability_profile_json, "capability profile")
                .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "tool_id": tool.tool_id,
                "description": tool.description,
                "source_text": tool.source_text,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "capability_profile": capability_profile,
                "status": tool.status,
                "created_from_task_id": tool.created_from_task_id,
                "safety_status": tool.safety_status,
                "safety_summary": tool.safety_summary,
                "updated_at": tool.updated_at,
            }),
        })
    }

    async fn handle_lua_tool_write(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let tool_id = arguments
            .get("tool_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| ToolError::Failed(anyhow!("lua_tool_write requires tool_id")))?;
        let description = required_string_arg(&arguments, "description", "lua_tool_write")?;
        let source_text = required_string_arg(&arguments, "source_text", "lua_tool_write")?;
        let intended_behavior_summary =
            required_string_arg(&arguments, "intended_behavior_summary", "lua_tool_write")?;
        let input_schema = required_json_arg(&arguments, "input_schema", "lua_tool_write")?;
        let output_schema = required_json_arg(&arguments, "output_schema", "lua_tool_write")?;
        let capability_profile =
            normalize_lua_capability_profile(arguments.get("capability_profile").cloned())
                .map_err(ToolError::Failed)?;

        self.validate_lua_tool_definition(
            &tool_id,
            description,
            &input_schema,
            &output_schema,
            &capability_profile,
            &self.base_tool_context,
        )
        .await
        .map_err(ToolError::Failed)?;

        let mut prior_status = None::<String>;
        let tool = if let Some(existing_tool) = self
            .db
            .fetch_lua_tool_for_owner(&request.owner_user_id, &tool_id)
            .await
            .map_err(ToolError::Failed)?
        {
            prior_status = Some(existing_tool.status);
            self.db
                .update_lua_tool_contents(UpdateLuaTool {
                    tool_id: &tool_id,
                    owner_user_id: &request.owner_user_id,
                    description,
                    source_text,
                    input_schema_json: input_schema.clone(),
                    output_schema_json: output_schema.clone(),
                    capability_profile_json: capability_profile.clone(),
                    created_from_task_id: Some(&task.task_id),
                    status: "draft",
                    safety_status: "pending",
                    safety_summary: Some("Awaiting safety review."),
                })
                .await
                .map_err(ToolError::Failed)?;
            self.db
                .fetch_lua_tool_for_owner(&request.owner_user_id, &tool_id)
                .await
                .map_err(ToolError::Failed)?
                .ok_or_else(|| {
                    ToolError::Failed(anyhow!("Lua tool '{tool_id}' missing after update"))
                })?
        } else {
            self.db
                .create_lua_tool(NewLuaTool {
                    tool_id: Some(&tool_id),
                    owner_user_id: &request.owner_user_id,
                    description,
                    source_text,
                    input_schema_json: input_schema.clone(),
                    output_schema_json: output_schema.clone(),
                    capability_profile_json: capability_profile.clone(),
                    status: "draft",
                    created_from_task_id: Some(&task.task_id),
                    safety_status: "pending",
                    safety_summary: Some("Awaiting safety review."),
                })
                .await
                .map_err(ToolError::Failed)?
        };

        let review = automation::review_lua_tool(
            &self.provider,
            &tool.tool_id,
            Some(&task.task_id),
            &request.owner_user_id,
            description,
            source_text,
            &input_schema,
            &output_schema,
            &capability_profile,
            intended_behavior_summary,
        )
        .await
        .map_err(ToolError::Failed)?;
        let approved_status = if prior_status.as_deref() == Some("active") {
            "active"
        } else {
            "draft"
        };
        let application =
            automation::apply_script_review(&review, approved_status, prior_status.as_deref());
        let reviewed_artifact_text = build_lua_tool_review_artifact(
            description,
            source_text,
            &input_schema,
            &output_schema,
            &capability_profile,
        )
        .map_err(ToolError::Failed)?;

        self.db
            .update_lua_tool_safety(
                &tool.tool_id,
                &application.safety_status,
                &review.summary,
                Some(&application.resulting_status),
            )
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .create_lua_tool_review(NewLuaToolReview {
                tool_id: &tool.tool_id,
                reviewer_agent_id: automation::SAFETY_AGENT_ID,
                review_type: "lua_tool",
                verdict: &review.verdict,
                risk_level: &review.risk_level,
                findings_json: review.as_findings_json(),
                summary_text: &review.summary,
                reviewed_artifact_text: &reviewed_artifact_text,
            })
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_tool_written",
                json!({
                    "task_id": task.task_id,
                    "tool_id": tool.tool_id,
                    "prior_status": prior_status,
                    "reactivated_after_rewrite": application.reactivated_after_rewrite,
                    "safety_status": application.safety_status,
                    "verdict": review.verdict,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;

        let updated = self
            .db
            .fetch_lua_tool_for_owner(&request.owner_user_id, &tool.tool_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("Lua tool missing after review")))?;

        Ok(ToolOutput {
            payload: json!({
                "tool_id": updated.tool_id,
                "status": updated.status,
                "safety_status": updated.safety_status,
                "safety_summary": updated.safety_summary,
                "reactivated_after_rewrite": application.reactivated_after_rewrite,
                "review_verdict": review.verdict,
                "risk_level": review.risk_level,
                "findings": review.findings,
                "required_changes": review.required_changes,
            }),
        })
    }

    async fn handle_lua_tool_activate(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        arguments: Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let tool_id = required_string_arg(&arguments, "tool_id", "lua_tool_activate")?;
        let tool = self
            .db
            .fetch_lua_tool_for_owner(&request.owner_user_id, tool_id)
            .await
            .map_err(ToolError::Failed)?
            .ok_or_else(|| ToolError::Failed(anyhow!("Lua tool '{tool_id}' not found")))?;
        if tool.safety_status != "approved" {
            return Err(ToolError::Failed(anyhow!(
                "Lua tool '{}' must pass safety review before activation",
                tool.tool_id
            )));
        }
        if tool.status == "active" {
            return Ok(ToolOutput {
                payload: json!({
                    "tool_id": tool.tool_id,
                    "status": tool.status,
                }),
            });
        }

        if !self.has_approved_action(request, "lua_tool_activate", &tool.tool_id) {
            return Err(ToolError::Denied {
                message: "Lua tool activation requires approval".to_owned(),
                detail: json!({
                    "action_type": "lua_tool_activate",
                    "target_ref": tool.tool_id,
                    "rationale": format!(
                        "Task '{}' wants to activate Lua tool '{}'.",
                        task.objective, tool.tool_id
                    ),
                }),
            });
        }

        self.db
            .update_lua_tool_status(&tool.tool_id, "active")
            .await
            .map_err(ToolError::Failed)?;
        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_tool_activated",
                json!({
                    "task_id": task.task_id,
                    "tool_id": tool.tool_id,
                }),
            )
            .await
            .map_err(ToolError::Failed)?;
        Ok(ToolOutput {
            payload: json!({
                "tool_id": tool.tool_id,
                "status": "active",
            }),
        })
    }

    async fn find_registered_lua_tool(
        &self,
        owner_user_id: &str,
        tool_name: &str,
        tool_context: &ToolContext,
    ) -> Result<Option<RegisteredLuaTool>> {
        Ok(self
            .load_registered_lua_tools(owner_user_id, tool_context)
            .await?
            .into_iter()
            .find(|tool| tool.tool_id == tool_name))
    }

    async fn load_registered_lua_tools(
        &self,
        owner_user_id: &str,
        tool_context: &ToolContext,
    ) -> Result<Vec<RegisteredLuaTool>> {
        let reserved_names = reserved_tool_names(&self.tools)
            .into_iter()
            .collect::<HashSet<_>>();
        let mut seen = HashSet::new();
        let mut loaded = Vec::new();

        for row in self
            .db
            .list_active_lua_tools_for_owner(owner_user_id)
            .await?
        {
            match self.database_lua_tool_from_row(row) {
                Ok(tool) if reserved_names.contains(&tool.tool_id) => {
                    warn!(tool_id = %tool.tool_id, "skipping database Lua tool because the name is reserved");
                }
                Ok(tool) if !seen.insert(tool.tool_id.clone()) => {
                    warn!(tool_id = %tool.tool_id, "skipping duplicate database Lua tool definition");
                }
                Ok(tool) => loaded.push(tool),
                Err(error) => {
                    warn!(owner_user_id = %owner_user_id, error = %error, "skipping invalid database Lua tool")
                }
            }
        }

        for tool in load_filesystem_lua_tools(tool_context)? {
            if reserved_names.contains(&tool.tool_id) {
                warn!(tool_id = %tool.tool_id, "skipping filesystem Lua tool because the name is reserved");
                continue;
            }
            if !seen.insert(tool.tool_id.clone()) {
                warn!(tool_id = %tool.tool_id, "skipping duplicate filesystem Lua tool definition");
                continue;
            }
            loaded.push(tool);
        }

        loaded.sort_by(|left, right| left.tool_id.cmp(&right.tool_id));
        Ok(loaded)
    }

    fn database_lua_tool_from_row(&self, row: LuaToolRow) -> Result<RegisteredLuaTool> {
        let input_schema = parse_json_field(&row.input_schema_json, "input schema")?;
        let output_schema = parse_json_field(&row.output_schema_json, "output schema")?;
        let capability_profile = normalize_lua_capability_profile(Some(parse_json_field(
            &row.capability_profile_json,
            "capability profile",
        )?))?;
        validate_openai_tool_definitions(&[ToolDefinition {
            name: row.tool_id.clone(),
            description: row.description.clone(),
            input_schema: input_schema.clone(),
        }])?;
        validate_json_schema_contract(&output_schema)?;

        Ok(RegisteredLuaTool {
            tool_id: row.tool_id,
            description: row.description,
            source_text: row.source_text,
            input_schema,
            output_schema,
            capability_profile,
            source: RegisteredLuaToolSource::Database {
                owner_user_id: row.owner_user_id,
            },
        })
    }

    async fn validate_lua_tool_definition(
        &self,
        tool_id: &str,
        description: &str,
        input_schema: &Value,
        output_schema: &Value,
        capability_profile: &Value,
        tool_context: &ToolContext,
    ) -> Result<()> {
        if description.trim().is_empty() {
            return Err(anyhow!("Lua tool '{}' requires a description", tool_id));
        }
        if reserved_tool_names(&self.tools)
            .iter()
            .any(|reserved| reserved == tool_id)
        {
            return Err(anyhow!(
                "Lua tool '{}' conflicts with a reserved tool name",
                tool_id
            ));
        }
        if load_filesystem_lua_tools(tool_context)?
            .into_iter()
            .any(|tool| tool.tool_id == tool_id)
        {
            return Err(anyhow!(
                "Lua tool '{}' conflicts with a filesystem-packaged tool",
                tool_id
            ));
        }
        validate_openai_tool_definitions(&[ToolDefinition {
            name: tool_id.to_owned(),
            description: description.to_owned(),
            input_schema: input_schema.clone(),
        }])?;
        validate_json_schema_contract(input_schema)?;
        validate_json_schema_contract(output_schema)?;
        let _ = capability_profile;
        Ok(())
    }

    async fn handle_registered_lua_tool_call(
        &self,
        task: &TaskRow,
        request: &AgentRequest,
        lua_tool: &RegisteredLuaTool,
        arguments: Value,
        tool_context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        validate_json_value_against_schema(&arguments, &lua_tool.input_schema).map_err(
            |error| {
                ToolError::Failed(anyhow!(
                    "Lua tool '{}' rejected invalid input: {}",
                    lua_tool.tool_id,
                    error
                ))
            },
        )?;

        let execution = automation::execute_lua_script(automation::LuaExecutionInput {
            db: self.db.clone(),
            owner_user_id: request.owner_user_id.clone(),
            task_id: task.task_id.clone(),
            script_id: lua_tool.tool_id.clone(),
            source_text: lua_tool.source_text.clone(),
            input_json: Some(arguments.clone()),
            capability_profile: lua_tool.capability_profile.clone(),
            working_dir: tool_context.working_dir.clone(),
            allowed_roots: tool_context.allowed_roots.clone(),
            browser_interactive_launcher: tool_context.browser_interactive_launcher.clone(),
            browser_headless_program: tool_context.browser_headless_program.clone(),
            browser_headless_args: tool_context.browser_headless_args.clone(),
        })
        .await;
        let execution = match execution {
            Ok(execution) => execution,
            Err(error) => {
                if let Some(policy_error) = error.downcast_ref::<automation::LuaToolPolicyDenied>()
                {
                    self.db
                        .append_task_event(
                            &task.task_id,
                            "lua_tool_denied",
                            "tool",
                            &lua_tool.tool_id,
                            json!({
                                "tool_id": lua_tool.tool_id,
                                "tool_source": lua_tool.source_kind(),
                                "tool_source_ref": lua_tool.source_ref(),
                                "tool_name": policy_error.tool_name,
                                "detail": policy_error.detail,
                                "message": policy_error.message,
                            }),
                        )
                        .await
                        .map_err(ToolError::Failed)?;
                    self.db
                        .record_audit_event(
                            "agent",
                            &request.assigned_agent_id,
                            "lua_defined_tool_denied",
                            json!({
                                "task_id": task.task_id,
                                "tool_id": lua_tool.tool_id,
                                "tool_source": lua_tool.source_kind(),
                                "tool_source_ref": lua_tool.source_ref(),
                                "blocked_tool_name": policy_error.tool_name,
                                "detail": policy_error.detail,
                            }),
                        )
                        .await
                        .map_err(ToolError::Failed)?;
                }
                return Err(ToolError::Failed(error));
            }
        };

        if execution.deferred_until.is_some() {
            return Err(ToolError::Failed(anyhow!(
                "Lua tool '{}' may not defer tasks",
                lua_tool.tool_id
            )));
        }
        validate_json_value_against_schema(&execution.result_json, &lua_tool.output_schema)
            .map_err(|error| {
                ToolError::Failed(anyhow!(
                    "Lua tool '{}' returned invalid output: {}",
                    lua_tool.tool_id,
                    error
                ))
            })?;

        for log_line in &execution.logs {
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_log",
                    "tool",
                    &lua_tool.tool_id,
                    json!({ "message": log_line }),
                )
                .await
                .map_err(ToolError::Failed)?;
        }
        for notification in &execution.notifications {
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_notify_user",
                    "tool",
                    &lua_tool.tool_id,
                    json!({ "message": notification }),
                )
                .await
                .map_err(ToolError::Failed)?;
        }

        self.db
            .record_audit_event(
                "agent",
                &request.assigned_agent_id,
                "lua_tool_executed",
                json!({
                    "task_id": task.task_id,
                    "tool_id": lua_tool.tool_id,
                    "tool_source": lua_tool.source_kind(),
                    "tool_source_ref": lua_tool.source_ref(),
                }),
            )
            .await
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: execution.result_json,
        })
    }

    fn has_approved_action(
        &self,
        request: &AgentRequest,
        action_type: &str,
        target_ref: &str,
    ) -> bool {
        request
            .approved_automation_actions
            .iter()
            .any(|action| action.action_type == action_type && action.target_ref == target_ref)
    }

    async fn review_shell_command(
        &self,
        task_id: &str,
        owner_user_id: &str,
        objective: &str,
        command: &str,
        risk: ShellRisk,
    ) -> Result<automation::SafetyReviewOutcome> {
        let request = json!({
            "review_type": "shell_command",
            "task_id": task_id,
            "owner_user_id": owner_user_id,
            "objective": objective,
            "command": command,
            "risk": risk.as_str(),
        });
        let response = self
            .provider
            .generate_turn(
                "safety_review",
                self.provider.model_names().safety_review.as_str(),
                SHELL_REVIEW_INSTRUCTIONS,
                &[ConversationItem::UserText(request.to_string())],
                &[],
            )
            .await?;
        let output_text = response.output_text.trim();
        if output_text.is_empty() {
            return Err(anyhow!("safety review returned an empty response"));
        }
        serde_json::from_str(output_text)
            .with_context(|| format!("failed to parse safety review JSON: {output_text}"))
    }

    async fn handle_subagent_call(
        &self,
        parent_task: &TaskRow,
        parent_request: &AgentRequest,
        arguments: Value,
    ) -> Result<ToolOutput> {
        if !can_spawn_subagents(&parent_request.role_ids) {
            return Ok(ToolOutput {
                payload: json!({
                    "error": {
                        "kind": "denied",
                        "message": "sub-agent creation denied by role policy",
                    }
                }),
            });
        }

        let objective = arguments
            .get("objective")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("agent_spawn_subagent requires objective"))?;
        let task_slice = arguments
            .get("task_slice")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);

        let subagent = self
            .db
            .create_subagent(
                &parent_request.owner_user_id,
                &parent_request.assigned_agent_id,
                &format!("Sub-agent for {}", parent_request.owner_user_id),
                "bounded_subagent",
            )
            .await?;

        let child_task = self
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &parent_request.owner_user_id,
                initiating_identity_id: &parent_request.initiating_identity_id,
                primary_agent_id: &parent_request.primary_agent_id,
                assigned_agent_id: &subagent.agent_id,
                parent_task_id: Some(&parent_task.task_id),
                session_id: parent_task.session_id.as_deref(),
                kind: "subagent",
                objective,
                context_summary: task_slice.as_deref(),
                scope: parent_request.scope,
                wake_at: None,
            })
            .await?;

        self.db
            .append_task_event(
                &parent_task.task_id,
                "subagent_spawned",
                "agent",
                &parent_request.assigned_agent_id,
                json!({
                    "child_task_id": child_task.task_id,
                    "child_agent_id": subagent.agent_id,
                }),
            )
            .await?;
        self.db
            .record_audit_event(
                "agent",
                &parent_request.assigned_agent_id,
                "subagent_spawned",
                json!({
                    "parent_task_id": parent_task.task_id,
                    "child_task_id": child_task.task_id,
                    "child_agent_id": subagent.agent_id,
                }),
            )
            .await?;

        let child_request = AgentRequest {
            owner_user_id: parent_request.owner_user_id.clone(),
            initiating_identity_id: parent_request.initiating_identity_id.clone(),
            primary_agent_id: parent_request.primary_agent_id.clone(),
            assigned_agent_id: subagent.agent_id.clone(),
            role_ids: parent_request.role_ids.clone(),
            objective: objective.to_owned(),
            scope: parent_request.scope,
            kind: "subagent".to_owned(),
            parent_task_id: Some(parent_task.task_id.clone()),
            task_context: task_slice.clone(),
            visible_scopes: Vec::new(),
            memory_mode: AgentMemoryMode::TaskSliceOnly,
            approved_shell_commands: Vec::new(),
            approved_automation_actions: Vec::new(),
        };
        let result = Box::pin(self.resume_task(child_task.clone(), child_request)).await?;

        Ok(ToolOutput {
            payload: json!({
                "child_task_id": child_task.task_id,
                "child_agent_id": subagent.agent_id,
                "child_state": result.task.state,
                "result_text": result.task.result_text,
            }),
        })
    }
}

fn parse_json_field(raw: &str, label: &str) -> Result<Value> {
    serde_json::from_str(raw).with_context(|| format!("failed to parse {label}"))
}

fn build_lua_tool_review_artifact(
    description: &str,
    source_text: &str,
    input_schema: &Value,
    output_schema: &Value,
    capability_profile: &Value,
) -> Result<String> {
    serde_json::to_string_pretty(&json!({
        "description": description,
        "source_text": source_text,
        "input_schema": input_schema,
        "output_schema": output_schema,
        "capability_profile": capability_profile,
    }))
    .context("failed to serialize Lua tool review artifact")
}

fn normalize_lua_capability_profile(value: Option<Value>) -> Result<Value> {
    let value = value.unwrap_or_else(|| json!({}));
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("Lua capability profile must be an object"))?;
    for key in object.keys() {
        if !matches!(key.as_str(), "allowed_tools" | "allowed_roots") {
            return Err(anyhow!(
                "Lua capability profile contains unsupported key '{}'",
                key
            ));
        }
    }
    let allowed_tools = match object.get("allowed_tools") {
        Some(Value::Array(items)) => Value::Array(
            items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(|text| Value::String(text.to_owned()))
                        .ok_or_else(|| anyhow!("allowed_tools entries must be strings"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Some(Value::Null) | None => Value::Null,
        Some(_) => return Err(anyhow!("allowed_tools must be an array or null")),
    };
    let allowed_roots = match object.get("allowed_roots") {
        Some(Value::Array(items)) => Value::Array(
            items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(|text| Value::String(text.to_owned()))
                        .ok_or_else(|| anyhow!("allowed_roots entries must be strings"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Some(Value::Null) | None => Value::Null,
        Some(_) => return Err(anyhow!("allowed_roots must be an array or null")),
    };
    Ok(json!({
        "allowed_tools": allowed_tools,
        "allowed_roots": allowed_roots,
    }))
}

fn reserved_tool_names(tools: &ToolRegistry) -> Vec<String> {
    tool_definitions(tools)
        .into_iter()
        .map(|definition| definition.name)
        .collect()
}

fn load_filesystem_lua_tools(tool_context: &ToolContext) -> Result<Vec<RegisteredLuaTool>> {
    let skills_root = tool_context.working_dir.join("skills");
    if !skills_root.exists() {
        return Ok(Vec::new());
    }

    let mut loaded = Vec::new();
    for entry in WalkDir::new(&skills_root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
    {
        let Some(file_name) = entry.path().file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !matches!(file_name, "skill.yaml" | "skill.yml") {
            continue;
        }

        let manifest_path = entry.path().to_path_buf();
        let manifest_text = match fs::read_to_string(&manifest_path) {
            Ok(text) => text,
            Err(error) => {
                warn!(path = %manifest_path.display(), error = %error, "skipping unreadable skill manifest");
                continue;
            }
        };
        let manifest = match serde_yaml::from_str::<SkillManifest>(&manifest_text) {
            Ok(manifest) => manifest,
            Err(error) => {
                warn!(path = %manifest_path.display(), error = %error, "skipping invalid skill manifest");
                continue;
            }
        };

        for tool_manifest in manifest.tools {
            match load_filesystem_lua_tool(&manifest_path, tool_manifest) {
                Ok(Some(tool)) => loaded.push(tool),
                Ok(None) => {}
                Err(error) => {
                    warn!(path = %manifest_path.display(), error = %error, "skipping invalid packaged Lua tool")
                }
            }
        }
    }

    Ok(loaded)
}

fn load_filesystem_lua_tool(
    manifest_path: &Path,
    tool_manifest: FilesystemLuaToolManifest,
) -> Result<Option<RegisteredLuaTool>> {
    if tool_manifest.kind != "lua" {
        return Ok(None);
    }
    if tool_manifest.enabled == Some(false) {
        return Ok(None);
    }
    if let Some(status) = tool_manifest.status.as_deref()
        && status != "active"
    {
        return Ok(None);
    }

    let base_dir = manifest_path.parent().ok_or_else(|| {
        anyhow!(
            "manifest '{}' has no parent directory",
            manifest_path.display()
        )
    })?;
    let source_text = resolve_manifest_text(
        base_dir,
        &tool_manifest.source_text,
        &tool_manifest.source_path,
    )?;
    let input_schema = resolve_manifest_json(base_dir, &tool_manifest.input_schema)?;
    let output_schema = resolve_manifest_json(base_dir, &tool_manifest.output_schema)?;
    let capability_profile =
        normalize_lua_capability_profile(tool_manifest.capability_profile.clone())?;
    validate_openai_tool_definitions(&[ToolDefinition {
        name: tool_manifest.tool_id.clone(),
        description: tool_manifest.description.clone(),
        input_schema: input_schema.clone(),
    }])?;
    validate_json_schema_contract(&output_schema)?;

    Ok(Some(RegisteredLuaTool {
        tool_id: tool_manifest.tool_id,
        description: tool_manifest.description,
        source_text,
        input_schema,
        output_schema,
        capability_profile,
        source: RegisteredLuaToolSource::Filesystem {
            manifest_path: manifest_path.to_path_buf(),
        },
    }))
}

fn resolve_manifest_text(
    base_dir: &Path,
    inline: &Option<String>,
    path: &Option<String>,
) -> Result<String> {
    match (inline, path) {
        (Some(text), None) => Ok(text.clone()),
        (None, Some(path)) => fs::read_to_string(base_dir.join(path))
            .with_context(|| format!("failed to read '{}'", base_dir.join(path).display())),
        (Some(_), Some(_)) => Err(anyhow!(
            "filesystem Lua tool must set either source_text or source_path, not both"
        )),
        (None, None) => Err(anyhow!(
            "filesystem Lua tool must set source_text or source_path"
        )),
    }
}

fn resolve_manifest_json(base_dir: &Path, source: &ManifestJsonSource) -> Result<Value> {
    match source {
        ManifestJsonSource::Inline(value) => Ok(value.clone()),
        ManifestJsonSource::RelativePath(path) => {
            let raw = fs::read_to_string(base_dir.join(path))
                .with_context(|| format!("failed to read '{}'", base_dir.join(path).display()))?;
            serde_json::from_str(&raw).with_context(|| {
                format!(
                    "failed to parse JSON from '{}'",
                    base_dir.join(path).display()
                )
            })
        }
    }
}

fn tool_definitions_with_registered_lua_tools(
    tools: &ToolRegistry,
    registered_lua_tools: &[RegisteredLuaTool],
) -> Vec<ToolDefinition> {
    let mut definitions = tool_definitions(tools);
    definitions.extend(
        registered_lua_tools
            .iter()
            .map(RegisteredLuaTool::definition),
    );
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    definitions
}

fn tool_definitions(tools: &ToolRegistry) -> Vec<ToolDefinition> {
    let mut definitions = tools.definitions();
    definitions.push(ToolDefinition {
        name: "memory_read".to_owned(),
        description: "Read active scoped semantic memory entries before updating or relying on existing remembered state. Use this to inspect mutable keyed state such as shopping lists, inventories, checklists, or running household notes, and to confirm what is already stored under a subject key.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["visible", "private", "household"]
                },
                "subject_type": { "type": ["string", "null"] },
                "subject_key": { "type": ["string", "null"] },
                "limit": { "type": "integer" }
            },
            "required": ["scope", "subject_type", "subject_key", "limit"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "memory_write".to_owned(),
        description: "Create or replace a scoped semantic memory entry under a stable subject key. Use this for mutable durable state such as shopping lists, inventories, checklists, and shared notes that need an explicit canonical latest value. Reuse the same `subject_key` to update the stored value.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["private", "household"]
                },
                "subject_type": { "type": "string" },
                "subject_key": { "type": "string" },
                "content_text": { "type": "string" },
                "source_type": { "type": "string" },
                "source_summary": { "type": "string" },
                "source_ref": { "type": ["string", "null"] }
            },
            "required": ["scope", "subject_type", "subject_key", "content_text", "source_type", "source_summary", "source_ref"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "memory_forget".to_owned(),
        description: "Deactivate a previously stored memory entry so it no longer participates in future retrieval. Use this when an earlier memory is wrong, unsafe, stale, or clearly should not continue influencing the agent. Prefer forgetting the specific incorrect memory row, then store the corrected fact separately when needed.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "memory_id": { "type": "string" },
                "reason": { "type": "string" }
            },
            "required": ["memory_id", "reason"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "memory_remember".to_owned(),
        description: "Persist a durable semantic memory when the user or household has stated a stable fact worth remembering. Use `private` by default. Use `household` only for genuinely shared facts and only when the current user is allowed to write household memory. Reuse the same `subject_key` to correct or confirm an existing fact instead of creating duplicates.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["private", "household"]
                },
                "subject_type": {
                    "type": "string",
                    "enum": ["fact", "identity", "preference"]
                },
                "subject_key": { "type": "string" },
                "content_text": { "type": "string" },
                "source_type": { "type": "string" },
                "source_summary": { "type": "string" },
                "source_ref": { "type": ["string", "null"] }
            },
            "required": ["scope", "subject_type", "subject_key", "content_text", "source_type", "source_summary", "source_ref"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "agent_spawn_subagent".to_owned(),
        description: "Spawn a bounded sub-agent with a tightly scoped task slice. Use only when a separate focused workstream materially helps complete the parent task.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "objective": { "type": "string" },
                "task_slice": { "type": ["string", "null"] }
            },
            "required": ["objective", "task_slice"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_script_list".to_owned(),
        description: "List Lua automation scripts owned by the current user, including status and safety metadata.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_script_get".to_owned(),
        description: "Fetch a Lua automation script's full source text and metadata by script ID."
            .to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "script_id": { "type": "string" }
            },
            "required": ["script_id"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_script_write".to_owned(),
        description: "Create or update a Lua automation script using BeaverKI's current host API, then run blocking safety review on it. Prefer `return function(ctx) ... end` as the canonical shape, though a top-level chunk that returns directly is also valid. Use `ctx.log_info`, `ctx.notify_user`, `ctx.task_defer`, `ctx.memory_read`, `ctx.memory_write`, and `ctx.tool_call` instead of legacy globals like `run()`, `log()`, or `notify()`. New scripts remain draft until explicitly activated. Rewriting an already active script keeps it active if the new version passes safety review.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "script_id": { "type": ["string", "null"] },
                "source_text": { "type": "string" },
                "capability_profile": {
                    "type": ["object", "null"],
                    "properties": {
                        "allowed_tools": {
                            "type": ["array", "null"],
                            "items": { "type": "string" }
                        },
                        "allowed_roots": {
                            "type": ["array", "null"],
                            "items": { "type": "string" }
                        }
                    },
                    "required": ["allowed_tools", "allowed_roots"],
                    "additionalProperties": false
                },
                "intended_behavior_summary": { "type": "string" }
            },
            "required": ["script_id", "source_text", "capability_profile", "intended_behavior_summary"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_script_activate".to_owned(),
        description: "Activate a reviewed Lua automation script after user approval.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "script_id": { "type": "string" }
            },
            "required": ["script_id"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_script_run".to_owned(),
        description:
            "Run an active safety-approved Lua automation script immediately and return its result."
                .to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "script_id": { "type": "string" }
            },
            "required": ["script_id"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_script_schedule".to_owned(),
        description: "Create or update a recurring schedule for an active Lua automation script after user approval.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "script_id": { "type": "string" },
                "schedule_id": { "type": "string" },
                "cron_expr": { "type": "string" },
                "enabled": { "type": ["boolean", "null"] }
            },
            "required": ["script_id", "schedule_id", "cron_expr", "enabled"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_tool_list".to_owned(),
        description: "List database-backed Lua-defined tools owned by the current user, including status and safety metadata.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_tool_get".to_owned(),
        description: "Fetch a database-backed Lua-defined tool's full source text, schemas, and metadata by tool ID.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "tool_id": { "type": "string" }
            },
            "required": ["tool_id"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_tool_write".to_owned(),
        description: "Create or update a database-backed Lua-defined tool, then run blocking safety review on it. Provide `input_schema` and `output_schema` as JSON-encoded schema objects. Lua-defined tools execute through `return function(ctx) ... end` and read structured arguments from `ctx.input`. They may compose approved built-in tools via `ctx.tool_call` and use the reviewed Lua host helpers, but they must return JSON that matches the declared output schema. Rewriting an already active tool keeps it active if the new version passes safety review.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "tool_id": { "type": "string" },
                "description": { "type": "string" },
                "source_text": { "type": "string" },
                "input_schema": { "type": "string" },
                "output_schema": { "type": "string" },
                "capability_profile": {
                    "type": ["object", "null"],
                    "properties": {
                        "allowed_tools": {
                            "type": ["array", "null"],
                            "items": { "type": "string" }
                        },
                        "allowed_roots": {
                            "type": ["array", "null"],
                            "items": { "type": "string" }
                        }
                    },
                    "required": ["allowed_tools", "allowed_roots"],
                    "additionalProperties": false
                },
                "intended_behavior_summary": { "type": "string" }
            },
            "required": ["tool_id", "description", "source_text", "input_schema", "output_schema", "capability_profile", "intended_behavior_summary"],
            "additionalProperties": false
        }),
    });
    definitions.push(ToolDefinition {
        name: "lua_tool_activate".to_owned(),
        description: "Activate a reviewed database-backed Lua-defined tool after user approval so it appears in the normal tool list for future agent turns.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "tool_id": { "type": "string" }
            },
            "required": ["tool_id"],
            "additionalProperties": false
        }),
    });
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    definitions
}

fn build_user_prompt(
    objective: &str,
    task_context: &Option<String>,
    memories: &[MemoryRow],
    approved_shell_commands: &[String],
    approved_automation_actions: &[ApprovedAutomationAction],
) -> String {
    let mut prompt = String::new();
    prompt.push_str("Objective:\n");
    prompt.push_str(objective);

    if let Some(task_context) = task_context {
        prompt.push_str("\n\nContext:\n");
        prompt.push_str(task_context);
    }

    if !memories.is_empty() {
        prompt.push_str("\n\nRelevant memory:\n");
        for (index, memory) in memories.iter().enumerate() {
            prompt.push_str(&format!("Memory {}:\n", index + 1));
            for line in format_memory_for_prompt(memory).lines() {
                prompt.push_str("  ");
                prompt.push_str(line);
                prompt.push('\n');
            }
            if !memory.content_text.ends_with('\n') {
                prompt.push('\n');
            }
        }
    }

    if !approved_shell_commands.is_empty() {
        prompt.push_str("\n\nPreviously approved shell commands for this task:\n");
        for command in approved_shell_commands {
            prompt.push_str("- ");
            prompt.push_str(command);
            prompt.push('\n');
        }
    }

    if !approved_automation_actions.is_empty() {
        prompt.push_str("\n\nPreviously approved automation actions for this task:\n");
        for action in approved_automation_actions {
            prompt.push_str("- ");
            prompt.push_str(&action.action_type);
            prompt.push_str(": ");
            prompt.push_str(&action.target_ref);
            prompt.push('\n');
        }
    }

    prompt
}

fn system_prompt(kind: &str, role_ids: &[String], allowed_roots: &[PathBuf]) -> String {
    let roots = allowed_roots
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let roles = role_ids.join(", ");

    format!(
        "You are BeaverKI M1, a local multi-user CLI-first assistant.
Current milestone: M4 semantic memory.
Current agent kind: {kind}.
Current role set: {roles}.
Use tools when needed, but keep the task focused and auditable.
Only low-risk read-only shell commands are allowed by default. Medium/high/critical shell commands require user approval.
For exploring allowed roots and locating files, prefer filesystem_list, filesystem_read_text, and filesystem_search over shell_exec.
Use Lua tools when recurring or structured automation materially helps. Writing a Lua script triggers safety review. New scripts require explicit activation later, while rewrites of already active scripts stay active if the new version passes safety review. Scheduling requires user approval. When writing Lua, prefer `return function(ctx) ... end`, use BeaverKI host APIs such as `ctx.log_info`, `ctx.notify_user`, `ctx.task_defer`, `ctx.memory_read`, `ctx.memory_write`, and `ctx.tool_call`, and avoid legacy globals like `run()`, `log()`, or `notify()`.
Use memory_read before updating existing mutable memory or when you need to confirm the current canonical value under a subject key.
Use memory_write for mutable durable scoped state such as shopping lists, inventories, checklists, and shared notes. When updating keyed state, write the full latest canonical value under the same subject_key rather than describing a partial edit in prose.
Automatic memory retrieval for the current conversation may be narrower than your role-based memory tool access. In a private DM, you may still use explicit memory tools against household scope when the user clearly asks for a household update and the current user is allowed to access household memory.
Use memory_remember only for durable semantic facts that are likely to matter in future conversations, such as names, identities, stable preferences, or long-lived household facts. Do not store transient task progress or one-off summaries with memory_remember. Use `private` scope by default and only use `household` for explicitly shared facts. Reuse the same subject_key when correcting an existing fact.
Use memory_forget when a previously stored memory row is wrong or should stop affecting future tasks. If the user says an earlier remembered fact was incorrect, forget the wrong row and then store the corrected fact if appropriate.
Never claim memory was persisted unless a memory_write or memory_remember tool call returned success in this task.
For file writes, prefer filesystem_write_text. Never claim a denied tool succeeded.
Use agent_spawn_subagent only for a tightly bounded, materially useful child task. Any sub-agent receives only the explicit task slice you provide.
Conversation context and memory can include explicit speaker labels. Preserve who said what. Never treat assistant self-references as facts about the user.
If the context says this is a fresh conversation, answer the new turn directly while retaining stable user preferences. If it says this is a follow-up, continue only as far as the new message actually depends on the earlier exchange.
Allowed filesystem roots: {roots}.
When you are done, answer concisely with the result and any important limitations."
    )
}

fn format_memory_for_prompt(memory: &MemoryRow) -> String {
    let mut lines = vec![format!(
        "[{} {} {}]",
        memory.memory_kind, memory.subject_type, memory.scope
    )];
    lines.push(format!("Memory ID: {}", memory.memory_id));
    if let Some(subject_key) = &memory.subject_key {
        lines.push(format!("Key: {subject_key}"));
    }
    lines.push(format!("Content: {}", memory.content_text));

    if let Some(content_json) = &memory.content_json
        && let Ok(metadata) = serde_json::from_str::<Value>(content_json)
        && let Some(source_summary) = metadata.get("source_summary").and_then(Value::as_str)
    {
        lines.push(format!("Basis: {source_summary}"));
    }

    lines.push(format!("Source type: {}", memory.source_type));
    if let Some(source_ref) = &memory.source_ref {
        lines.push(format!("Source ref: {source_ref}"));
    }
    lines.join("\n")
}

fn semantic_memory_write_payload(
    write_result: &SemanticMemoryWriteResult,
    scope: MemoryScope,
    subject_type: &str,
    subject_key: &str,
    content_text: &str,
    source_type: &str,
    source_ref: Option<&str>,
) -> Value {
    let (status, memory_id, previous_memory_id) = match write_result {
        SemanticMemoryWriteResult::Created { memory_id } => ("created", memory_id.as_str(), None),
        SemanticMemoryWriteResult::Deduplicated { memory_id } => {
            ("deduplicated", memory_id.as_str(), None)
        }
        SemanticMemoryWriteResult::Corrected {
            previous_memory_id,
            memory_id,
        } => (
            "corrected",
            memory_id.as_str(),
            Some(previous_memory_id.as_str()),
        ),
    };

    json!({
        "status": status,
        "persisted": true,
        "memory_id": memory_id,
        "previous_memory_id": previous_memory_id,
        "scope": scope.as_str(),
        "subject_type": subject_type,
        "subject_key": subject_key,
        "content_text": content_text,
        "source_type": source_type,
        "source_ref": source_ref,
    })
}

fn memory_row_to_json(memory: &MemoryRow) -> Value {
    json!({
        "memory_id": memory.memory_id,
        "scope": memory.scope,
        "memory_kind": memory.memory_kind,
        "subject_type": memory.subject_type,
        "subject_key": memory.subject_key,
        "content_text": memory.content_text,
        "source_type": memory.source_type,
        "source_ref": memory.source_ref,
        "source_summary": memory_source_summary(memory),
        "created_at": memory.created_at,
        "updated_at": memory.updated_at,
    })
}

fn explicit_memory_tool_scopes(request: &AgentRequest) -> Vec<MemoryScope> {
    visible_memory_scopes(&request.role_ids)
}

fn memory_source_summary(memory: &MemoryRow) -> Option<String> {
    let content_json = memory.content_json.as_deref()?;
    let metadata = serde_json::from_str::<Value>(content_json).ok()?;
    metadata
        .get("source_summary")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn parse_shell_risk(value: Option<&str>) -> Option<ShellRisk> {
    match value? {
        "low" => Some(ShellRisk::Low),
        "medium" => Some(ShellRisk::Medium),
        "high" => Some(ShellRisk::High),
        "critical" => Some(ShellRisk::Critical),
        _ => None,
    }
}

fn required_string_arg<'a>(
    arguments: &'a Value,
    key: &str,
    tool_name: &str,
) -> std::result::Result<&'a str, ToolError> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ToolError::Failed(anyhow!("{tool_name} requires {key}")))
}

fn required_json_arg(
    arguments: &Value,
    key: &str,
    tool_name: &str,
) -> std::result::Result<Value, ToolError> {
    let value = arguments
        .get(key)
        .cloned()
        .ok_or_else(|| ToolError::Failed(anyhow!("{tool_name} requires {key}")))?;

    match value {
        Value::String(raw) => parse_json_field(&raw, key).map_err(ToolError::Failed),
        other => Ok(other),
    }
}

fn optional_string_arg<'a>(arguments: &'a Value, key: &str) -> Option<&'a str> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn roles_label(role_ids: &[String]) -> String {
    if role_ids.is_empty() {
        "none".to_owned()
    } else {
        role_ids.join(", ")
    }
}

const SHELL_REVIEW_INSTRUCTIONS: &str = r#"You are BeaverKI's safety review agent.
Review the provided shell command for necessity, reversibility, blast radius, and mismatch with the stated objective.
If the task can be completed with built-in filesystem tools such as filesystem_list, filesystem_read_text, or filesystem_search, reject shell commands that only explore directories or locate files.
Approve only if the command is a coherent way to achieve the stated task and does not introduce avoidable risk.
Return only JSON with this exact schema:
{"verdict":"approved|rejected|needs_changes","risk_level":"low|medium|high|critical","findings":["..."],"required_changes":["..."],"summary":"..."}"#;

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use beaverki_config::ProviderModels;
    use beaverki_core::{MemoryKind, TaskState};
    use beaverki_db::Database;
    use beaverki_models::ModelTurnResponse;
    use beaverki_policy::visible_memory_scopes;
    use beaverki_tools::{builtin_registry, validate_openai_tool_definitions};
    use serde_json::json;

    use super::*;

    #[derive(Clone)]
    struct FakeProvider {
        models: ProviderModels,
        responses: Arc<Mutex<VecDeque<ModelTurnResponse>>>,
    }

    impl FakeProvider {
        fn new(responses: Vec<ModelTurnResponse>) -> Self {
            Self {
                models: ProviderModels {
                    planner: "planner".to_owned(),
                    executor: "executor".to_owned(),
                    summarizer: "summarizer".to_owned(),
                    safety_review: "safety".to_owned(),
                },
                responses: Arc::new(Mutex::new(responses.into())),
            }
        }
    }

    #[async_trait]
    impl ModelProvider for FakeProvider {
        fn model_names(&self) -> &ProviderModels {
            &self.models
        }

        async fn generate_turn(
            &self,
            _model_role: &str,
            _model_name: &str,
            _instructions: &str,
            _conversation: &[ConversationItem],
            _tools: &[ToolDefinition],
        ) -> Result<ModelTurnResponse> {
            self.responses
                .lock()
                .expect("lock")
                .pop_front()
                .ok_or_else(|| anyhow!("no more fake responses"))
        }
    }

    #[derive(Clone)]
    struct RecordingProvider {
        models: ProviderModels,
        responses: Arc<Mutex<VecDeque<ModelTurnResponse>>>,
        prompts: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingProvider {
        fn new(responses: Vec<ModelTurnResponse>) -> Self {
            Self {
                models: ProviderModels {
                    planner: "planner".to_owned(),
                    executor: "executor".to_owned(),
                    summarizer: "summarizer".to_owned(),
                    safety_review: "safety".to_owned(),
                },
                responses: Arc::new(Mutex::new(responses.into())),
                prompts: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn prompts(&self) -> Arc<Mutex<Vec<String>>> {
            self.prompts.clone()
        }
    }

    #[async_trait]
    impl ModelProvider for RecordingProvider {
        fn model_names(&self) -> &ProviderModels {
            &self.models
        }

        async fn generate_turn(
            &self,
            _model_role: &str,
            _model_name: &str,
            _instructions: &str,
            conversation: &[ConversationItem],
            _tools: &[ToolDefinition],
        ) -> Result<ModelTurnResponse> {
            if let Some(ConversationItem::UserText(prompt)) = conversation.first() {
                self.prompts
                    .lock()
                    .expect("prompt lock")
                    .push(prompt.clone());
            }
            self.responses
                .lock()
                .expect("lock")
                .pop_front()
                .ok_or_else(|| anyhow!("no more fake responses"))
        }
    }

    async fn test_runner_with_provider(
        provider: Arc<dyn ModelProvider>,
    ) -> (Database, PrimaryAgentRunner) {
        let db_dir = tempfile::tempdir().expect("tempdir");
        let db_path = db_dir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            MemoryStore::new(db.clone()),
            provider,
            builtin_registry(),
            ToolContext::new(std::env::temp_dir(), vec![std::env::temp_dir()]),
            6,
        );
        std::mem::forget(db_dir);
        (db, runner)
    }

    async fn test_runner(provider: FakeProvider) -> (Database, PrimaryAgentRunner) {
        test_runner_with_provider(Arc::new(provider)).await
    }

    async fn approved_actions_for_task(
        db: &Database,
        user_id: &str,
        task_id: &str,
    ) -> Vec<ApprovedAutomationAction> {
        db.approved_approvals_for_task(task_id, user_id)
            .await
            .expect("approved approvals")
            .into_iter()
            .filter(|approval| approval.action_type != "shell_command")
            .filter_map(|approval| {
                approval
                    .target_ref
                    .map(|target_ref| ApprovedAutomationAction {
                        action_type: approval.action_type,
                        target_ref,
                    })
            })
            .collect()
    }

    async fn create_active_lua_tool(
        db: &Database,
        owner_user_id: &str,
        tool_id: &str,
        description: &str,
        source_text: &str,
        input_schema: Value,
        output_schema: Value,
        capability_profile_json: Value,
    ) {
        db.create_lua_tool(NewLuaTool {
            tool_id: Some(tool_id),
            owner_user_id,
            description,
            source_text,
            input_schema_json: input_schema,
            output_schema_json: output_schema,
            capability_profile_json,
            status: "active",
            created_from_task_id: Some("task_seed"),
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("lua tool");
    }

    #[tokio::test]
    async fn pauses_for_shell_approval_and_resumes() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir /tmp/m1_approval\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir /tmp/m1_approval" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"high\",\"findings\":[],\"required_changes\":[],\"summary\":\"The command matches the task and is acceptable with explicit approval.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"high\",\"findings\":[],\"required_changes\":[],\"summary\":\"The command matches the task and is acceptable with explicit approval.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_2",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir /tmp/m1_approval\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_2".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir /tmp/m1_approval" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "finished" }]
                })],
                tool_calls: vec![],
                output_text: "finished".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let first = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Create a directory".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("first run");
        assert_eq!(first.task.state, TaskState::WaitingApproval.as_str());

        let approval = db
            .list_approvals_for_user(&default_user.user_id, Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");
        db.resolve_approval(&approval.approval_id, "approved")
            .await
            .expect("approve");
        let resumed_task = db
            .fetch_task_for_owner(&default_user.user_id, &first.task.task_id)
            .await
            .expect("fetch task")
            .expect("task");

        let resumed = runner
            .resume_task(
                resumed_task,
                AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles.clone(),
                    objective: "Create a directory".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&roles),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: db
                        .approved_shell_commands_for_task(
                            &first.task.task_id,
                            &default_user.user_id,
                        )
                        .await
                        .expect("approved commands"),
                    approved_automation_actions: Vec::new(),
                },
            )
            .await
            .expect("resume");

        assert_eq!(resumed.task.state, TaskState::Completed.as_str());
        assert_eq!(resumed.task.result_text.as_deref(), Some("finished"));
    }

    #[tokio::test]
    async fn subagent_runs_on_task_slice_only() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "agent_spawn_subagent",
                    "arguments": "{\"objective\":\"Summarize notes\",\"task_slice\":\"Only use this snippet.\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "agent_spawn_subagent".to_owned(),
                    arguments: json!({
                        "objective": "Summarize notes",
                        "task_slice": "Only use this snippet."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "child done" }]
                })],
                tool_calls: vec![],
                output_text: "child done".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "parent done" }]
                })],
                tool_calls: vec![],
                output_text: "parent done".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Use a subagent".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let events = db
            .fetch_task_events_for_owner(&default_user.user_id, &result.task.task_id)
            .await
            .expect("events");
        assert!(
            events
                .iter()
                .any(|row| row.event_type == "subagent_spawned")
        );
    }

    #[tokio::test]
    async fn safety_review_can_block_risky_shell_command() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir /tmp/rejected_by_safety\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir /tmp/rejected_by_safety" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"rejected\",\"risk_level\":\"high\",\"findings\":[\"Creates a directory without enough justification.\"],\"required_changes\":[\"Use a read-only inspection command or provide a safer plan.\"],\"summary\":\"The shell command is riskier than necessary for the task.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"rejected\",\"risk_level\":\"high\",\"findings\":[\"Creates a directory without enough justification.\"],\"required_changes\":[\"Use a read-only inspection command or provide a safer plan.\"],\"summary\":\"The shell command is riskier than necessary for the task.\"}".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Inspect the workspace".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Failed.as_str());
        assert!(
            result
                .task
                .result_text
                .as_deref()
                .expect("result text")
                .contains("Safety review rejected")
        );
    }

    #[tokio::test]
    async fn agent_can_write_activate_and_run_lua_script_after_approval() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_write",
                    "name": "lua_script_write",
                    "arguments": "{\"script_id\":\"script_agent_test\",\"source_text\":\"return function(ctx)\\n    ctx.log_info(\\\"agent lua ran\\\")\\n    return \\\"lua ok\\\"\\nend\",\"capability_profile\":{},\"intended_behavior_summary\":\"Return a fixed success string.\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_write".to_owned(),
                    name: "lua_script_write".to_owned(),
                    arguments: json!({
                        "script_id": "script_agent_test",
                        "source_text": "return function(ctx)\n    ctx.log_info(\"agent lua ran\")\n    return \"lua ok\"\nend",
                        "capability_profile": {},
                        "intended_behavior_summary": "Return a fixed success string."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The Lua automation matches the stated intent.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The Lua automation matches the stated intent.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_activate_wait",
                    "name": "lua_script_activate",
                    "arguments": "{\"script_id\":\"script_agent_test\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_activate_wait".to_owned(),
                    name: "lua_script_activate".to_owned(),
                    arguments: json!({ "script_id": "script_agent_test" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_activate_ok",
                    "name": "lua_script_activate",
                    "arguments": "{\"script_id\":\"script_agent_test\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_activate_ok".to_owned(),
                    name: "lua_script_activate".to_owned(),
                    arguments: json!({ "script_id": "script_agent_test" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_run",
                    "name": "lua_script_run",
                    "arguments": "{\"script_id\":\"script_agent_test\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_run".to_owned(),
                    name: "lua_script_run".to_owned(),
                    arguments: json!({ "script_id": "script_agent_test" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "automation complete" }]
                })],
                tool_calls: vec![],
                output_text: "automation complete".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let first = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Create and use a Lua automation".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("first run");
        assert_eq!(first.task.state, TaskState::WaitingApproval.as_str());

        let script = db
            .fetch_script_for_owner(&default_user.user_id, "script_agent_test")
            .await
            .expect("script fetch")
            .expect("script");
        assert_eq!(script.status, "draft");
        assert_eq!(script.safety_status, "approved");

        let approval = db
            .list_approvals_for_user(&default_user.user_id, Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");
        assert_eq!(approval.action_type, "lua_script_activate");
        db.resolve_approval(&approval.approval_id, "approved")
            .await
            .expect("approve");
        let resumed_task = db
            .fetch_task_for_owner(&default_user.user_id, &first.task.task_id)
            .await
            .expect("fetch task")
            .expect("task");

        let resumed = runner
            .resume_task(
                resumed_task,
                AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles.clone(),
                    objective: "Create and use a Lua automation".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&roles),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: approved_actions_for_task(
                        &db,
                        &default_user.user_id,
                        &first.task.task_id,
                    )
                    .await,
                },
            )
            .await
            .expect("resume");

        assert_eq!(resumed.task.state, TaskState::Completed.as_str());
        assert_eq!(
            resumed.task.result_text.as_deref(),
            Some("automation complete")
        );
        let script = db
            .fetch_script_for_owner(&default_user.user_id, "script_agent_test")
            .await
            .expect("script fetch")
            .expect("script");
        assert_eq!(script.status, "active");
    }

    #[tokio::test]
    async fn rewriting_active_lua_script_keeps_it_active_after_approved_review() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_rewrite",
                    "name": "lua_script_write",
                    "arguments": "{\"script_id\":\"script_active_rewrite\",\"source_text\":\"return function(ctx)\\n    ctx.log_info(\\\"rewritten lua ran\\\")\\n    return \\\"rewritten ok\\\"\\nend\",\"capability_profile\":{},\"intended_behavior_summary\":\"Update the active script to return a new fixed success string.\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_rewrite".to_owned(),
                    name: "lua_script_write".to_owned(),
                    arguments: json!({
                        "script_id": "script_active_rewrite",
                        "source_text": "return function(ctx)\n    ctx.log_info(\"rewritten lua ran\")\n    return \"rewritten ok\"\nend",
                        "capability_profile": {},
                        "intended_behavior_summary": "Update the active script to return a new fixed success string."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The rewritten Lua automation matches the stated intent.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The rewritten Lua automation matches the stated intent.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "rewrite complete" }]
                })],
                tool_calls: vec![],
                output_text: "rewrite complete".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        db.create_script(NewScript {
            script_id: Some("script_active_rewrite"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "active",
            source_text: "return function(ctx) return \"old\" end",
            capability_profile_json: json!({}),
            created_from_task_id: None,
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("script");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Rewrite the active Lua automation".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        assert_eq!(result.task.result_text.as_deref(), Some("rewrite complete"));
        let script = db
            .fetch_script_for_owner(&default_user.user_id, "script_active_rewrite")
            .await
            .expect("script fetch")
            .expect("script");
        assert_eq!(script.status, "active");
        assert_eq!(script.safety_status, "approved");
        assert!(script.source_text.contains("rewritten ok"));
        assert!(
            db.list_approvals_for_user(&default_user.user_id, Some("pending"))
                .await
                .expect("approvals")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn lua_script_write_preserves_needs_changes_verdict() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_write",
                    "name": "lua_script_write",
                    "arguments": "{\"script_id\":\"script_needs_changes_agent\",\"source_text\":\"return function(ctx)\\n    return \\\"retry\\\"\\nend\",\"capability_profile\":{},\"intended_behavior_summary\":\"Return a retry marker.\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_write".to_owned(),
                    name: "lua_script_write".to_owned(),
                    arguments: json!({
                        "script_id": "script_needs_changes_agent",
                        "source_text": "return function(ctx)\n    return \"retry\"\nend",
                        "capability_profile": {},
                        "intended_behavior_summary": "Return a retry marker."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"needs_changes\",\"risk_level\":\"medium\",\"findings\":[\"The script behavior is still too broad.\"],\"required_changes\":[\"Constrain the script before activation.\"],\"summary\":\"The automation needs changes before it can be trusted.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"needs_changes\",\"risk_level\":\"medium\",\"findings\":[\"The script behavior is still too broad.\"],\"required_changes\":[\"Constrain the script before activation.\"],\"summary\":\"The automation needs changes before it can be trusted.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "write complete" }]
                })],
                tool_calls: vec![],
                output_text: "write complete".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Write a Lua automation that still needs changes".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let script = db
            .fetch_script_for_owner(&default_user.user_id, "script_needs_changes_agent")
            .await
            .expect("script fetch")
            .expect("script");
        assert_eq!(script.status, "blocked");
        assert_eq!(script.safety_status, "needs_changes");
    }

    #[tokio::test]
    async fn lua_script_schedule_pauses_for_approval_and_resumes() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_schedule_wait",
                    "name": "lua_script_schedule",
                    "arguments": "{\"script_id\":\"script_sched\",\"schedule_id\":\"sched_agent_test\",\"cron_expr\":\"0/5 * * * * * *\",\"enabled\":true}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_schedule_wait".to_owned(),
                    name: "lua_script_schedule".to_owned(),
                    arguments: json!({
                        "script_id": "script_sched",
                        "schedule_id": "sched_agent_test",
                        "cron_expr": "0/5 * * * * * *",
                        "enabled": true
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_schedule_ok",
                    "name": "lua_script_schedule",
                    "arguments": "{\"script_id\":\"script_sched\",\"schedule_id\":\"sched_agent_test\",\"cron_expr\":\"0/5 * * * * * *\",\"enabled\":true}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_schedule_ok".to_owned(),
                    name: "lua_script_schedule".to_owned(),
                    arguments: json!({
                        "script_id": "script_sched",
                        "schedule_id": "sched_agent_test",
                        "cron_expr": "0/5 * * * * * *",
                        "enabled": true
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "schedule complete" }]
                })],
                tool_calls: vec![],
                output_text: "schedule complete".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        db.create_script(NewScript {
            script_id: Some("script_sched"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "active",
            source_text: "return function(ctx) return \"ok\" end",
            capability_profile_json: json!({}),
            created_from_task_id: None,
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("script");

        let first = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Schedule a Lua automation".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("first run");
        assert_eq!(first.task.state, TaskState::WaitingApproval.as_str());

        let approval = db
            .list_approvals_for_user(&default_user.user_id, Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");
        assert_eq!(approval.action_type, "lua_script_schedule");
        db.resolve_approval(&approval.approval_id, "approved")
            .await
            .expect("approve");
        let resumed_task = db
            .fetch_task_for_owner(&default_user.user_id, &first.task.task_id)
            .await
            .expect("fetch task")
            .expect("task");

        let resumed = runner
            .resume_task(
                resumed_task,
                AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles.clone(),
                    objective: "Schedule a Lua automation".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&roles),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: approved_actions_for_task(
                        &db,
                        &default_user.user_id,
                        &first.task.task_id,
                    )
                    .await,
                },
            )
            .await
            .expect("resume");

        assert_eq!(resumed.task.state, TaskState::Completed.as_str());
        let schedule = db
            .fetch_schedule_for_owner(&default_user.user_id, "sched_agent_test")
            .await
            .expect("schedule fetch")
            .expect("schedule");
        assert_eq!(schedule.target_id, "script_sched");
    }

    #[tokio::test]
    async fn automation_approval_denial_fails_for_non_approver() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_write",
                    "name": "lua_script_write",
                    "arguments": "{\"script_id\":\"script_guest_test\",\"source_text\":\"return function(ctx)\\n    ctx.log_info(\\\"guest lua ran\\\")\\n    return \\\"lua ok\\\"\\nend\",\"capability_profile\":{},\"intended_behavior_summary\":\"Return a fixed success string.\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_write".to_owned(),
                    name: "lua_script_write".to_owned(),
                    arguments: json!({
                        "script_id": "script_guest_test",
                        "source_text": "return function(ctx)\n    ctx.log_info(\"guest lua ran\")\n    return \"lua ok\"\nend",
                        "capability_profile": {},
                        "intended_behavior_summary": "Return a fixed success string."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The Lua automation matches the stated intent.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The Lua automation matches the stated intent.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_activate_denied",
                    "name": "lua_script_activate",
                    "arguments": "{\"script_id\":\"script_guest_test\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_activate_denied".to_owned(),
                    name: "lua_script_activate".to_owned(),
                    arguments: json!({ "script_id": "script_guest_test" }),
                }],
                output_text: String::new(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let bootstrap = db.create_user("Guesty", &[]).await.expect("guest user");
        let user = db
            .fetch_user(&bootstrap.user_id)
            .await
            .expect("fetch user")
            .expect("user");
        let roles = db
            .list_user_roles(&user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        assert!(roles.is_empty());

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", user.user_id),
                primary_agent_id: user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: user.primary_agent_id.clone().expect("agent"),
                role_ids: roles,
                objective: "Create and activate a Lua automation".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&[]),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Failed.as_str());
        assert!(
            result
                .task
                .result_text
                .as_deref()
                .expect("result text")
                .contains("not allowed to request automation approval")
        );
        assert!(
            db.list_approvals_for_user(&user.user_id, Some("pending"))
                .await
                .expect("approvals")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn shell_approval_denial_fails_for_non_approver() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_shell",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir /tmp/non_approver_shell\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_shell".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir /tmp/non_approver_shell" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"high\",\"findings\":[],\"required_changes\":[],\"summary\":\"The command matches the task and is acceptable with explicit approval.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"high\",\"findings\":[],\"required_changes\":[],\"summary\":\"The command matches the task and is acceptable with explicit approval.\"}".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let bootstrap = db.create_user("ShellGuest", &[]).await.expect("guest user");
        let user = db
            .fetch_user(&bootstrap.user_id)
            .await
            .expect("fetch user")
            .expect("user");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", user.user_id),
                primary_agent_id: user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: user.primary_agent_id.clone().expect("agent"),
                role_ids: Vec::new(),
                objective: "Create a directory".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&[]),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Failed.as_str());
        assert!(
            result
                .task
                .result_text
                .as_deref()
                .expect("result text")
                .contains("not allowed to request approval for high-risk shell commands")
        );
        assert!(
            db.list_approvals_for_user(&user.user_id, Some("pending"))
                .await
                .expect("approvals")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn memory_remember_creates_private_semantic_memory() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_remember",
                    "name": "memory_remember",
                    "arguments": "{\"scope\":\"private\",\"subject_type\":\"identity\",\"subject_key\":\"profile.preferred_name\",\"content_text\":\"Alex\",\"source_type\":\"user_statement\",\"source_summary\":\"User said their preferred name is Alex.\",\"source_ref\":\"turn-1\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_remember".to_owned(),
                    name: "memory_remember".to_owned(),
                    arguments: json!({
                        "scope": "private",
                        "subject_type": "identity",
                        "subject_key": "profile.preferred_name",
                        "content_text": "Alex",
                        "source_type": "user_statement",
                        "source_summary": "User said their preferred name is Alex.",
                        "source_ref": "turn-1"
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "remembered" }]
                })],
                tool_calls: vec![],
                output_text: "remembered".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Remember that my preferred name is Alex.".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let memories = db
            .retrieve_memories(Some(&default_user.user_id), &[MemoryScope::Private], 10)
            .await
            .expect("retrieve memories");
        let memory = memories
            .iter()
            .find(|memory| memory.subject_key.as_deref() == Some("profile.preferred_name"))
            .expect("semantic memory");
        assert_eq!(memory.memory_kind, MemoryKind::Semantic.as_str());
        assert_eq!(memory.content_text, "Alex");
        assert_eq!(memory.source_type, "user_statement");
        let audit_events = db.list_audit_events(10).await.expect("audit events");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "semantic_memory_created")
        );
    }

    #[tokio::test]
    async fn memory_remember_corrects_existing_semantic_memory() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_correct",
                    "name": "memory_remember",
                    "arguments": "{\"scope\":\"private\",\"subject_type\":\"preference\",\"subject_key\":\"profile.favorite_drink\",\"content_text\":\"tea\",\"source_type\":\"user_statement\",\"source_summary\":\"User corrected their favorite drink to tea.\",\"source_ref\":\"turn-2\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_correct".to_owned(),
                    name: "memory_remember".to_owned(),
                    arguments: json!({
                        "scope": "private",
                        "subject_type": "preference",
                        "subject_key": "profile.favorite_drink",
                        "content_text": "tea",
                        "source_type": "user_statement",
                        "source_summary": "User corrected their favorite drink to tea.",
                        "source_ref": "turn-2"
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "updated" }]
                })],
                tool_calls: vec![],
                output_text: "updated".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let original_metadata =
            json!({ "source_summary": "User said their favorite drink is coffee." });
        let original_memory_id = db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some(&default_user.user_id),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "preference",
                subject_key: Some("profile.favorite_drink"),
                content_text: "coffee",
                content_json: Some(&original_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                task_id: Some("task_seed"),
            })
            .await
            .expect("seed memory");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Update my favorite drink to tea.".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let current = db
            .find_active_memory_by_subject(
                Some(&default_user.user_id),
                MemoryScope::Private,
                MemoryKind::Semantic,
                "preference",
                "profile.favorite_drink",
            )
            .await
            .expect("find memory")
            .expect("current memory");
        assert_eq!(current.content_text, "tea");
        let superseded = db
            .fetch_memory(&original_memory_id)
            .await
            .expect("fetch superseded")
            .expect("superseded memory");
        assert_eq!(
            superseded.superseded_by_memory_id.as_deref(),
            Some(current.memory_id.as_str())
        );
        let audit_events = db.list_audit_events(10).await.expect("audit events");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "semantic_memory_corrected")
        );
    }

    #[tokio::test]
    async fn memory_read_then_write_updates_household_shopping_list() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_read_list",
                    "name": "memory_read",
                    "arguments": "{\"scope\":\"household\",\"subject_type\":\"list\",\"subject_key\":\"household.shopping_list\",\"limit\":3}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_read_list".to_owned(),
                    name: "memory_read".to_owned(),
                    arguments: json!({
                        "scope": "household",
                        "subject_type": "list",
                        "subject_key": "household.shopping_list",
                        "limit": 3
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_write_list",
                    "name": "memory_write",
                    "arguments": "{\"scope\":\"household\",\"subject_type\":\"list\",\"subject_key\":\"household.shopping_list\",\"content_text\":\"Apfel x4\\nBananen\\nFaschiertes 400g\\nLinzerstangerl x4\\nMilch\",\"source_type\":\"user_statement\",\"source_summary\":\"User added Milch to the household shopping list.\",\"source_ref\":\"turn-2\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_write_list".to_owned(),
                    name: "memory_write".to_owned(),
                    arguments: json!({
                        "scope": "household",
                        "subject_type": "list",
                        "subject_key": "household.shopping_list",
                        "content_text": "Apfel x4\nBananen\nFaschiertes 400g\nLinzerstangerl x4\nMilch",
                        "source_type": "user_statement",
                        "source_summary": "User added Milch to the household shopping list.",
                        "source_ref": "turn-2"
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "Added Milch and persisted the updated household shopping list." }]
                })],
                tool_calls: vec![],
                output_text: "Added Milch and persisted the updated household shopping list."
                    .to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let original_metadata =
            json!({ "source_summary": "User shared the current household shopping list." });
        let original_memory_id = db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: None,
                scope: MemoryScope::Household,
                memory_kind: MemoryKind::Semantic,
                subject_type: "list",
                subject_key: Some("household.shopping_list"),
                content_text: "Apfel x4\nBananen\nFaschiertes 400g\nLinzerstangerl x4",
                content_json: Some(&original_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                task_id: Some("task_seed"),
            })
            .await
            .expect("seed shopping list");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Add Milch to the household shopping list.".to_owned(),
                scope: MemoryScope::Household,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let current = db
            .find_active_memory_by_subject(
                None,
                MemoryScope::Household,
                MemoryKind::Semantic,
                "list",
                "household.shopping_list",
            )
            .await
            .expect("find shopping list")
            .expect("current shopping list");
        assert_eq!(
            current.content_text,
            "Apfel x4\nBananen\nFaschiertes 400g\nLinzerstangerl x4\nMilch"
        );
        let superseded = db
            .fetch_memory(&original_memory_id)
            .await
            .expect("fetch old shopping list")
            .expect("superseded shopping list");
        assert_eq!(
            superseded.superseded_by_memory_id.as_deref(),
            Some(current.memory_id.as_str())
        );
        let audit_events = db.list_audit_events(20).await.expect("audit events");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "semantic_memory_read")
        );
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "semantic_memory_corrected")
        );
    }

    #[tokio::test]
    async fn memory_remember_denies_household_write_for_guest() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_household_denied",
                    "name": "memory_remember",
                    "arguments": "{\"scope\":\"household\",\"subject_type\":\"fact\",\"subject_key\":\"household.wifi_name\",\"content_text\":\"beaverki-net\",\"source_type\":\"user_statement\",\"source_summary\":\"User said the household Wi-Fi name is beaverki-net.\",\"source_ref\":\"turn-1\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_household_denied".to_owned(),
                    name: "memory_remember".to_owned(),
                    arguments: json!({
                        "scope": "household",
                        "subject_type": "fact",
                        "subject_key": "household.wifi_name",
                        "content_text": "beaverki-net",
                        "source_type": "user_statement",
                        "source_summary": "User said the household Wi-Fi name is beaverki-net.",
                        "source_ref": "turn-1"
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "could not store household memory" }]
                })],
                tool_calls: vec![],
                output_text: "could not store household memory".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let bootstrap = db.create_user("Guesty", &[]).await.expect("guest user");
        let user = db
            .fetch_user(&bootstrap.user_id)
            .await
            .expect("fetch user")
            .expect("user");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", user.user_id),
                primary_agent_id: user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: user.primary_agent_id.clone().expect("agent"),
                role_ids: Vec::new(),
                objective: "Remember the household Wi-Fi name.".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&[]),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let household_memory = db
            .find_active_memory_by_subject(
                None,
                MemoryScope::Household,
                MemoryKind::Semantic,
                "fact",
                "household.wifi_name",
            )
            .await
            .expect("lookup household memory");
        assert!(household_memory.is_none());
        let audit_events = db.list_audit_events(10).await.expect("audit events");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "semantic_memory_write_denied")
        );
    }

    #[tokio::test]
    async fn memory_write_allows_household_write_from_private_context_when_role_allows_it() {
        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_household_read",
                    "name": "memory_read",
                    "arguments": "{\"scope\":\"household\",\"subject_type\":\"list\",\"subject_key\":\"household.shopping_list\",\"limit\":3}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_household_read".to_owned(),
                    name: "memory_read".to_owned(),
                    arguments: json!({
                        "scope": "household",
                        "subject_type": "list",
                        "subject_key": "household.shopping_list",
                        "limit": 3
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_household_hidden",
                    "name": "memory_write",
                    "arguments": "{\"scope\":\"household\",\"subject_type\":\"list\",\"subject_key\":\"household.shopping_list\",\"content_text\":\"Brot\\nMilch\",\"source_type\":\"user_statement\",\"source_summary\":\"User asked to update the household shopping list.\",\"source_ref\":\"turn-1\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_household_hidden".to_owned(),
                    name: "memory_write".to_owned(),
                    arguments: json!({
                        "scope": "household",
                        "subject_type": "list",
                        "subject_key": "household.shopping_list",
                        "content_text": "Brot\nMilch",
                        "source_type": "user_statement",
                        "source_summary": "User asked to update the household shopping list.",
                        "source_ref": "turn-1"
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "stored household memory" }]
                })],
                tool_calls: vec![],
                output_text: "stored household memory".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let original_metadata =
            json!({ "source_summary": "User shared the current household shopping list." });
        db.insert_memory(beaverki_db::NewMemory {
            owner_user_id: None,
            scope: MemoryScope::Household,
            memory_kind: MemoryKind::Semantic,
            subject_type: "list",
            subject_key: Some("household.shopping_list"),
            content_text: "Brot",
            content_json: Some(&original_metadata),
            sensitivity: "normal",
            source_type: "user_statement",
            source_ref: Some("turn-0"),
            task_id: Some("task_seed"),
        })
        .await
        .expect("seed shopping list");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Update the household shopping list.".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: vec![MemoryScope::Private],
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let household_memory = db
            .find_active_memory_by_subject(
                None,
                MemoryScope::Household,
                MemoryKind::Semantic,
                "list",
                "household.shopping_list",
            )
            .await
            .expect("lookup household memory")
            .expect("household memory");
        assert_eq!(household_memory.content_text, "Brot\nMilch");
    }

    #[tokio::test]
    async fn memory_forget_hides_wrong_memory_from_future_retrieval() {
        let provider = FakeProvider::new(vec![]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let metadata = json!({ "source_summary": "User gave a wrong Wi-Fi password candidate." });
        let task = db
            .create_task_with_params(NewTask {
                owner_user_id: &default_user.user_id,
                initiating_identity_id: &format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                parent_task_id: None,
                session_id: None,
                kind: "interactive",
                objective: "Forget the wrong Wi-Fi password memory.",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");
        let memory_id = db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some(&default_user.user_id),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "fact",
                subject_key: Some("profile.wifi_password"),
                content_text: "wrong-password",
                content_json: Some(&metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                task_id: Some(&task.task_id),
            })
            .await
            .expect("insert seed memory");

        let request = AgentRequest {
            owner_user_id: default_user.user_id.clone(),
            initiating_identity_id: format!("cli:{}", default_user.user_id),
            primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
            assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
            role_ids: roles.clone(),
            objective: "Forget the wrong Wi-Fi password memory.".to_owned(),
            scope: MemoryScope::Private,
            kind: "interactive".to_owned(),
            parent_task_id: None,
            task_context: None,
            visible_scopes: visible_memory_scopes(&roles),
            memory_mode: AgentMemoryMode::ScopedRetrieval,
            approved_shell_commands: Vec::new(),
            approved_automation_actions: Vec::new(),
        };

        let output = runner
            .handle_memory_forget(
                &task,
                &request,
                json!({
                    "memory_id": memory_id.clone(),
                    "reason": "User clarified this was the Wi-Fi name, not the password."
                }),
            )
            .await
            .expect("forget memory");

        assert_eq!(output.payload["status"], json!("forgotten"));
        let memory = db
            .fetch_memory(memory_id.as_str())
            .await
            .expect("fetch memory")
            .expect("memory");
        assert!(memory.forgotten_at.is_some());
        let active = db
            .retrieve_memories(Some(&default_user.user_id), &[MemoryScope::Private], 10)
            .await
            .expect("active memories");
        assert!(!active.iter().any(|row| row.memory_id == memory_id));
        let audit_events = db.list_audit_events(10).await.expect("audit events");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "memory_forgotten")
        );
    }

    #[tokio::test]
    async fn guest_does_not_receive_household_semantic_memory_in_prompt() {
        let provider = RecordingProvider::new(vec![ModelTurnResponse {
            output_items: vec![json!({
                "type": "message",
                "content": [{ "type": "output_text", "text": "ok" }]
            })],
            tool_calls: vec![],
            output_text: "ok".to_owned(),
            usage: None,
        }]);
        let prompts = provider.prompts();
        let (db, runner) = test_runner_with_provider(Arc::new(provider)).await;
        let household_metadata =
            json!({ "source_summary": "The owner said the household alarm code is 2468." });
        db.insert_memory(beaverki_db::NewMemory {
            owner_user_id: None,
            scope: MemoryScope::Household,
            memory_kind: MemoryKind::Semantic,
            subject_type: "fact",
            subject_key: Some("household.alarm_code"),
            content_text: "Alarm code is 2468.",
            content_json: Some(&household_metadata),
            sensitivity: "normal",
            source_type: "user_statement",
            source_ref: Some("turn-household"),
            task_id: Some("task_household"),
        })
        .await
        .expect("insert household memory");

        let bootstrap = db.create_user("Guesty", &[]).await.expect("guest user");
        let user = db
            .fetch_user(&bootstrap.user_id)
            .await
            .expect("fetch user")
            .expect("user");

        let result = runner
            .run_task(AgentRequest {
                owner_user_id: user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", user.user_id),
                primary_agent_id: user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: user.primary_agent_id.clone().expect("agent"),
                role_ids: Vec::new(),
                objective: "Say hello.".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&[]),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("task");

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        let prompts = prompts.lock().expect("prompt lock");
        let prompt = prompts.first().expect("captured prompt");
        assert!(!prompt.contains("Alarm code is 2468."));
        assert!(!prompt.contains("household.alarm_code"));
    }

    #[test]
    fn lua_script_write_schema_closes_capability_profile() {
        let registry = builtin_registry();
        let schema = tool_definitions(&registry)
            .into_iter()
            .find(|definition| definition.name == "lua_script_write")
            .expect("lua_script_write definition")
            .input_schema;
        let capability_profile = &schema["properties"]["capability_profile"];

        assert_eq!(capability_profile["additionalProperties"], json!(false));
        assert_eq!(
            capability_profile["properties"]["allowed_tools"]["items"]["type"],
            json!("string")
        );
        assert_eq!(
            capability_profile["properties"]["allowed_roots"]["items"]["type"],
            json!("string")
        );
    }

    #[test]
    fn lua_tool_write_schema_uses_json_encoded_schema_fields() {
        let registry = builtin_registry();
        let schema = tool_definitions(&registry)
            .into_iter()
            .find(|definition| definition.name == "lua_tool_write")
            .expect("lua_tool_write definition")
            .input_schema;

        assert_eq!(
            schema["properties"]["input_schema"]["type"],
            json!("string")
        );
        assert_eq!(
            schema["properties"]["output_schema"]["type"],
            json!("string")
        );
    }

    #[test]
    fn lua_guidance_mentions_current_host_api() {
        let registry = builtin_registry();
        let description = tool_definitions(&registry)
            .into_iter()
            .find(|definition| definition.name == "lua_script_write")
            .expect("lua_script_write definition")
            .description;
        let prompt = system_prompt(
            "interactive",
            &["owner".to_owned()],
            &[PathBuf::from("/tmp")],
        );

        assert!(description.contains("return function(ctx)"));
        assert!(description.contains("ctx.log_info"));
        assert!(description.contains("legacy globals like `run()`, `log()`, or `notify()`"));
        assert!(description.contains("Rewriting an already active script keeps it active"));
        assert!(prompt.contains(
            "prefer filesystem_list, filesystem_read_text, and filesystem_search over shell_exec"
        ));
        assert!(prompt.contains("prefer `return function(ctx) ... end`"));
        assert!(prompt.contains("avoid legacy globals like `run()`, `log()`, or `notify()`"));
        assert!(prompt.contains("Use memory_read before updating existing mutable memory"));
        assert!(prompt.contains("Use memory_write for mutable durable scoped state"));
        assert!(prompt.contains("Automatic memory retrieval for the current conversation may be narrower than your role-based memory tool access"));
        assert!(prompt.contains("Use memory_remember only for durable semantic facts"));
    }

    #[test]
    fn exported_tool_schemas_satisfy_openai_requirements() {
        let registry = builtin_registry();
        let definitions = tool_definitions(&registry);

        validate_openai_tool_definitions(&definitions).expect("valid tool schemas");
    }

    #[tokio::test]
    async fn lua_script_get_returns_stored_source_and_metadata() {
        let (db, runner) = test_runner(FakeProvider::new(vec![])).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        db.create_script(NewScript {
            script_id: Some("script_read_test"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "draft",
            source_text: "return function(ctx) return \"ok\" end",
            capability_profile_json: json!({
                "allowed_tools": ["shell_exec"],
                "allowed_roots": ["/tmp"]
            }),
            created_from_task_id: Some("task_seed"),
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("script");

        let payload = runner
            .handle_lua_script_get(
                &AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles.clone(),
                    objective: "Inspect a Lua script".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&roles),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: Vec::new(),
                },
                json!({ "script_id": "script_read_test" }),
            )
            .await
            .expect("tool output")
            .payload;

        assert_eq!(payload["script_id"], json!("script_read_test"));
        assert_eq!(
            payload["source_text"],
            json!("return function(ctx) return \"ok\" end")
        );
        assert_eq!(payload["status"], json!("draft"));
        assert_eq!(payload["safety_status"], json!("approved"));
        assert_eq!(
            payload["capability_profile"]["allowed_tools"],
            json!(["shell_exec"])
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lua_script_run_surfaces_typed_policy_denial() {
        let (db, runner) = test_runner(FakeProvider::new(vec![])).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let task = db
            .create_task_with_params(NewTask {
                owner_user_id: &default_user.user_id,
                initiating_identity_id: &format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                parent_task_id: None,
                session_id: None,
                kind: "interactive",
                objective: "Run a Lua script with risky shell",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");
        db.create_script(NewScript {
            script_id: Some("script_risky_lua_run"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "active",
            source_text: r#"return function(ctx)
    return ctx.tool_call("shell_exec", { command = "mkdir /tmp/beaverki_agent_lua_policy_denied" })
end"#,
            capability_profile_json: json!({
                "allowed_tools": ["shell_exec"]
            }),
            created_from_task_id: Some(&task.task_id),
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("script");

        let error = runner
            .handle_lua_script_run(
                &task,
                &AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles.clone(),
                    objective: "Run a Lua script with risky shell".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&roles),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: Vec::new(),
                },
                json!({ "script_id": "script_risky_lua_run" }),
                &ToolContext::new(std::env::temp_dir(), vec![]),
            )
            .await
            .expect_err("policy denial");

        let ToolError::Failed(error) = error else {
            panic!("expected ToolError::Failed");
        };
        let policy_error = error
            .downcast_ref::<automation::LuaToolPolicyDenied>()
            .expect("typed policy error");
        assert_eq!(policy_error.tool_name, "shell_exec");
        assert_eq!(policy_error.detail["risk"], json!("high"));

        let events = db
            .fetch_task_events_for_owner(&default_user.user_id, &task.task_id)
            .await
            .expect("events");
        let denial_event = events
            .iter()
            .find(|event| event.event_type == "lua_tool_denied")
            .expect("lua_tool_denied event");
        let payload: Value = serde_json::from_str(&denial_event.payload_json).expect("payload");
        assert_eq!(payload["tool_name"], json!("shell_exec"));
        assert_eq!(payload["detail"]["risk"], json!("high"));
    }

    #[tokio::test]
    async fn lua_script_list_returns_owned_scripts() {
        let (db, runner) = test_runner(FakeProvider::new(vec![])).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        db.create_script(NewScript {
            script_id: Some("script_list_test"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "active",
            source_text: "return \"ok\"",
            capability_profile_json: json!({}),
            created_from_task_id: Some("task_seed"),
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("script");

        let payload = runner
            .handle_lua_script_list(&AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "List Lua scripts".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("tool output")
            .payload;

        let scripts = payload["scripts"].as_array().expect("scripts array");
        assert!(scripts.iter().any(|script| {
            script["script_id"] == json!("script_list_test")
                && script["status"] == json!("active")
                && script["safety_status"] == json!("approved")
        }));
    }

    #[tokio::test]
    async fn agent_can_write_activate_and_invoke_lua_defined_tool() {
        let input_schema_json = serde_json::to_string(&json!({
            "type": "object",
            "properties": { "name": { "type": "string" } },
            "required": ["name"],
            "additionalProperties": false
        }))
        .expect("input schema json");
        let output_schema_json = serde_json::to_string(&json!({
            "type": "object",
            "properties": { "greeting": { "type": "string" } },
            "required": ["greeting"],
            "additionalProperties": false
        }))
        .expect("output schema json");
        let tool_write_arguments = json!({
            "tool_id": "tool_agent_greet",
            "description": "Return a greeting object.",
            "source_text": "return function(ctx)\n    return { greeting = 'hello ' .. ctx.input.name }\nend",
            "input_schema": input_schema_json.clone(),
            "output_schema": output_schema_json.clone(),
            "capability_profile": {},
            "intended_behavior_summary": "Return a JSON object containing a greeting for the provided name."
        })
        .to_string();

        let provider = FakeProvider::new(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_tool_write",
                    "name": "lua_tool_write",
                    "arguments": tool_write_arguments.clone()
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_tool_write".to_owned(),
                    name: "lua_tool_write".to_owned(),
                    arguments: json!({
                        "tool_id": "tool_agent_greet",
                        "description": "Return a greeting object.",
                        "source_text": "return function(ctx)\n    return { greeting = 'hello ' .. ctx.input.name }\nend",
                        "input_schema": input_schema_json,
                        "output_schema": output_schema_json,
                        "capability_profile": {},
                        "intended_behavior_summary": "Return a JSON object containing a greeting for the provided name."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The Lua tool matches the stated intent.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"The Lua tool matches the stated intent.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_tool_activate_wait",
                    "name": "lua_tool_activate",
                    "arguments": "{\"tool_id\":\"tool_agent_greet\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_tool_activate_wait".to_owned(),
                    name: "lua_tool_activate".to_owned(),
                    arguments: json!({ "tool_id": "tool_agent_greet" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_tool_activate_ok",
                    "name": "lua_tool_activate",
                    "arguments": "{\"tool_id\":\"tool_agent_greet\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_tool_activate_ok".to_owned(),
                    name: "lua_tool_activate".to_owned(),
                    arguments: json!({ "tool_id": "tool_agent_greet" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_tool_run",
                    "name": "tool_agent_greet",
                    "arguments": "{\"name\":\"Alex\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_tool_run".to_owned(),
                    name: "tool_agent_greet".to_owned(),
                    arguments: json!({ "name": "Alex" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "tool complete" }]
                })],
                tool_calls: vec![],
                output_text: "tool complete".to_owned(),
                usage: None,
            },
        ]);
        let (db, runner) = test_runner(provider).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        let first = runner
            .run_task(AgentRequest {
                owner_user_id: default_user.user_id.clone(),
                initiating_identity_id: format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                role_ids: roles.clone(),
                objective: "Create and activate a Lua-defined tool".to_owned(),
                scope: MemoryScope::Private,
                kind: "interactive".to_owned(),
                parent_task_id: None,
                task_context: None,
                visible_scopes: visible_memory_scopes(&roles),
                memory_mode: AgentMemoryMode::ScopedRetrieval,
                approved_shell_commands: Vec::new(),
                approved_automation_actions: Vec::new(),
            })
            .await
            .expect("first run");
        assert_eq!(first.task.state, TaskState::WaitingApproval.as_str());

        let approval = db
            .list_approvals_for_user(&default_user.user_id, Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");
        assert_eq!(approval.action_type, "lua_tool_activate");
        db.resolve_approval(&approval.approval_id, "approved")
            .await
            .expect("approve");
        let resumed_task = db
            .fetch_task_for_owner(&default_user.user_id, &first.task.task_id)
            .await
            .expect("fetch task")
            .expect("task");

        let resumed = runner
            .resume_task(
                resumed_task,
                AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles.clone(),
                    objective: "Create and activate a Lua-defined tool".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&roles),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: approved_actions_for_task(
                        &db,
                        &default_user.user_id,
                        &first.task.task_id,
                    )
                    .await,
                },
            )
            .await
            .expect("resume");

        assert_eq!(resumed.task.state, TaskState::Completed.as_str());
        let lua_tool = db
            .fetch_lua_tool_for_owner(&default_user.user_id, "tool_agent_greet")
            .await
            .expect("tool fetch")
            .expect("tool");
        assert_eq!(lua_tool.status, "active");
        assert_eq!(lua_tool.safety_status, "approved");

        let invocations = db
            .fetch_tool_invocations_for_owner(&default_user.user_id, &first.task.task_id)
            .await
            .expect("tool invocations");
        assert!(invocations.iter().any(|invocation| {
            invocation.tool_name == "tool_agent_greet"
                && invocation.status == beaverki_core::ToolInvocationStatus::Completed.as_str()
        }));

        let audit_events = db.list_audit_events(20).await.expect("audit");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "lua_tool_written")
        );
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "lua_tool_activated")
        );
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "lua_tool_executed")
        );
    }

    #[tokio::test]
    async fn lua_defined_tool_rejects_invalid_output_schema() {
        let (db, runner) = test_runner(FakeProvider::new(vec![])).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let task = db
            .create_task_with_params(NewTask {
                owner_user_id: &default_user.user_id,
                initiating_identity_id: &format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                parent_task_id: None,
                session_id: None,
                kind: "interactive",
                objective: "Invoke a Lua-defined tool",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");
        create_active_lua_tool(
            &db,
            &default_user.user_id,
            "tool_invalid_output",
            "Return the wrong output type.",
            "return function(ctx)\n    return \"not an object\"\nend",
            json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "properties": { "greeting": { "type": "string" } },
                "required": ["greeting"],
                "additionalProperties": false
            }),
            json!({}),
        )
        .await;

        let tool_context = ToolContext::new(std::env::temp_dir(), vec![std::env::temp_dir()]);
        let lua_tool = runner
            .find_registered_lua_tool(&default_user.user_id, "tool_invalid_output", &tool_context)
            .await
            .expect("find tool")
            .expect("tool");

        let error = runner
            .handle_registered_lua_tool_call(
                &task,
                &AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles,
                    objective: "Invoke a Lua-defined tool".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&["owner".to_owned()]),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: Vec::new(),
                },
                &lua_tool,
                json!({}),
                &tool_context,
            )
            .await
            .expect_err("invalid output");

        let ToolError::Failed(error) = error else {
            panic!("expected ToolError::Failed");
        };
        assert!(error.to_string().contains("returned invalid output"));
    }

    #[tokio::test]
    async fn filesystem_packaged_lua_tool_loads_and_runs() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let skills_dir = tempdir.path().join("skills").join("demo");
        std::fs::create_dir_all(&skills_dir).expect("skills dir");
        std::fs::write(
            skills_dir.join("skill.yaml"),
            r#"tools:
  - tool_id: packaged_greet
    kind: lua
    description: Return a packaged greeting object.
    source_text: |
      return function(ctx)
          return { greeting = "hi " .. ctx.input.name }
      end
    input_schema:
      type: object
      properties:
        name:
          type: string
      required: [name]
      additionalProperties: false
    output_schema:
      type: object
      properties:
        greeting:
          type: string
      required: [greeting]
      additionalProperties: false
"#,
        )
        .expect("write manifest");

        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let provider = Arc::new(FakeProvider::new(vec![])) as Arc<dyn ModelProvider>;
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            MemoryStore::new(db.clone()),
            provider,
            builtin_registry(),
            ToolContext::new(
                tempdir.path().to_path_buf(),
                vec![tempdir.path().to_path_buf()],
            ),
            6,
        );
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let task = db
            .create_task_with_params(NewTask {
                owner_user_id: &default_user.user_id,
                initiating_identity_id: &format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                parent_task_id: None,
                session_id: None,
                kind: "interactive",
                objective: "Run packaged Lua tool",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");

        let loaded = runner
            .load_registered_lua_tools(&default_user.user_id, &runner.base_tool_context)
            .await
            .expect("load tools");
        assert!(loaded.iter().any(|tool| tool.tool_id == "packaged_greet"));
        let packaged_tool = loaded
            .into_iter()
            .find(|tool| tool.tool_id == "packaged_greet")
            .expect("packaged tool");

        let output = runner
            .handle_registered_lua_tool_call(
                &task,
                &AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles,
                    objective: "Run packaged Lua tool".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&["owner".to_owned()]),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: Vec::new(),
                },
                &packaged_tool,
                json!({ "name": "Alex" }),
                &runner.base_tool_context,
            )
            .await
            .expect("tool output");

        assert_eq!(output.payload["greeting"], json!("hi Alex"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lua_defined_tool_surfaces_typed_policy_denial() {
        let (db, runner) = test_runner(FakeProvider::new(vec![])).await;
        let default_user = db.default_user().await.expect("default").expect("user");
        let roles = db
            .list_user_roles(&default_user.user_id)
            .await
            .expect("roles")
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        let task = db
            .create_task_with_params(NewTask {
                owner_user_id: &default_user.user_id,
                initiating_identity_id: &format!("cli:{}", default_user.user_id),
                primary_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                assigned_agent_id: default_user.primary_agent_id.as_deref().expect("agent"),
                parent_task_id: None,
                session_id: None,
                kind: "interactive",
                objective: "Run a Lua-defined tool with risky shell",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");
        create_active_lua_tool(
            &db,
            &default_user.user_id,
            "tool_risky_shell",
            "Attempt a risky shell command.",
            "return function(ctx)\n    return ctx.tool_call(\"shell_exec\", { command = \"mkdir /tmp/beaverki_agent_lua_tool_policy_denied\" })\nend",
            json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
            json!({
                "allowed_tools": ["shell_exec"]
            }),
        )
        .await;

        let tool_context = ToolContext::new(std::env::temp_dir(), vec![]);
        let lua_tool = runner
            .find_registered_lua_tool(&default_user.user_id, "tool_risky_shell", &tool_context)
            .await
            .expect("find tool")
            .expect("tool");

        let error = runner
            .handle_registered_lua_tool_call(
                &task,
                &AgentRequest {
                    owner_user_id: default_user.user_id.clone(),
                    initiating_identity_id: format!("cli:{}", default_user.user_id),
                    primary_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    assigned_agent_id: default_user.primary_agent_id.clone().expect("agent"),
                    role_ids: roles,
                    objective: "Run a Lua-defined tool with risky shell".to_owned(),
                    scope: MemoryScope::Private,
                    kind: "interactive".to_owned(),
                    parent_task_id: None,
                    task_context: None,
                    visible_scopes: visible_memory_scopes(&["owner".to_owned()]),
                    memory_mode: AgentMemoryMode::ScopedRetrieval,
                    approved_shell_commands: Vec::new(),
                    approved_automation_actions: Vec::new(),
                },
                &lua_tool,
                json!({}),
                &tool_context,
            )
            .await
            .expect_err("policy denial");

        let ToolError::Failed(error) = error else {
            panic!("expected ToolError::Failed");
        };
        let policy_error = error
            .downcast_ref::<automation::LuaToolPolicyDenied>()
            .expect("typed policy error");
        assert_eq!(policy_error.tool_name, "shell_exec");
        assert_eq!(policy_error.detail["risk"], json!("high"));

        let events = db
            .fetch_task_events_for_owner(&default_user.user_id, &task.task_id)
            .await
            .expect("events");
        let denial_event = events
            .iter()
            .find(|event| event.event_type == "lua_tool_denied")
            .expect("lua_tool_denied event");
        let payload: Value = serde_json::from_str(&denial_event.payload_json).expect("payload");
        assert_eq!(payload["tool_id"], json!("tool_risky_shell"));
        assert_eq!(payload["tool_name"], json!("shell_exec"));
        assert_eq!(payload["detail"]["risk"], json!("high"));
    }
}
