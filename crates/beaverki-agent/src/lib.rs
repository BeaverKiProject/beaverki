use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use beaverki_core::{MemoryScope, ShellRisk, TaskState, ToolInvocationStatus};
use beaverki_db::{Database, NewTask, TaskRow};
use beaverki_memory::{MemoryStore, RetrievalScope};
use beaverki_models::{ConversationItem, ModelProvider};
use beaverki_policy::{can_request_shell_approval, can_spawn_subagents};
use beaverki_tools::{ToolContext, ToolDefinition, ToolError, ToolOutput, ToolRegistry};
use serde_json::{Value, json};
use tracing::info;

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
                kind: &request.kind,
                objective: &request.objective,
                context_summary: request.task_context.as_deref(),
                scope: request.scope,
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
            &memories
                .iter()
                .map(|memory| memory.content_text.clone())
                .collect::<Vec<_>>(),
            &request.approved_shell_commands,
        ))];
        let instructions = system_prompt(
            &request.kind,
            &request.role_ids,
            &self.base_tool_context.allowed_roots,
        );
        let mut tool_context = self.base_tool_context.clone();
        tool_context.approved_shell_commands = request.approved_shell_commands.clone();

        for step in 0..self.max_steps {
            let model_name = if step == 0 {
                self.provider.model_names().planner.as_str()
            } else {
                self.provider.model_names().executor.as_str()
            };
            let response = self
                .provider
                .generate_turn(
                    model_name,
                    &instructions,
                    &conversation,
                    &tool_definitions(&self.tools),
                )
                .await?;

            info!("model turn finished for task {}", task.task_id);

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
                        "tool_call_count": response.tool_calls.len(),
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
                    .tools
                    .invoke(&tool_call.name, tool_call.arguments.clone(), &tool_context)
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
            && can_request_shell_approval(&request.role_ids, risk)
        {
            let rationale = format!(
                "Task '{}' requested shell command '{}', classified as {} risk.",
                task.objective,
                command,
                risk.as_str()
            );
            let approval = self
                .db
                .create_approval(
                    &task.task_id,
                    "shell_command",
                    Some(command),
                    &request.assigned_agent_id,
                    &request.owner_user_id,
                    &rationale,
                )
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

        Ok(ToolFailureDisposition::Continue(response_json))
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
                kind: "subagent",
                objective,
                context_summary: task_slice.as_deref(),
                scope: parent_request.scope,
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

fn tool_definitions(tools: &ToolRegistry) -> Vec<ToolDefinition> {
    let mut definitions = tools.definitions();
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
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    definitions
}

fn build_user_prompt(
    objective: &str,
    task_context: &Option<String>,
    memories: &[String],
    approved_shell_commands: &[String],
) -> String {
    let mut prompt = String::new();
    prompt.push_str("Objective:\n");
    prompt.push_str(objective);

    if let Some(task_context) = task_context {
        prompt.push_str("\n\nTask slice:\n");
        prompt.push_str(task_context);
    }

    if !memories.is_empty() {
        prompt.push_str("\n\nRelevant memory:\n");
        for memory in memories {
            prompt.push_str("- ");
            prompt.push_str(memory);
            prompt.push('\n');
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
Current agent kind: {kind}.
Current role set: {roles}.
Use tools when needed, but keep the task focused and auditable.
Only low-risk read-only shell commands are allowed by default. Medium/high/critical shell commands require user approval.
For file writes, prefer filesystem_write_text. Never claim a denied tool succeeded.
Use agent_spawn_subagent only for a tightly bounded, materially useful child task. Any sub-agent receives only the explicit task slice you provide.
Allowed filesystem roots: {roots}.
When you are done, answer concisely with the result and any important limitations."
    )
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use beaverki_config::ProviderModels;
    use beaverki_core::TaskState;
    use beaverki_db::Database;
    use beaverki_models::ModelTurnResponse;
    use beaverki_policy::visible_memory_scopes;
    use beaverki_tools::builtin_registry;
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

    async fn test_runner(provider: FakeProvider) -> (Database, PrimaryAgentRunner) {
        let db_dir = tempfile::tempdir().expect("tempdir");
        let db_path = db_dir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            MemoryStore::new(db.clone()),
            Arc::new(provider),
            builtin_registry(),
            ToolContext::new(std::env::temp_dir(), vec![std::env::temp_dir()]),
            6,
        );
        std::mem::forget(db_dir);
        (db, runner)
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
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "finished" }]
                })],
                tool_calls: vec![],
                output_text: "finished".to_owned(),
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
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "child done" }]
                })],
                tool_calls: vec![],
                output_text: "child done".to_owned(),
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "parent done" }]
                })],
                tool_calls: vec![],
                output_text: "parent done".to_owned(),
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
}
