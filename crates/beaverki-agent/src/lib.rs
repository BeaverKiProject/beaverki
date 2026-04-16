use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use beaverki_core::{MemoryScope, TaskState, ToolInvocationStatus};
use beaverki_db::{Database, TaskRow};
use beaverki_memory::{MemoryStore, RetrievalScope};
use beaverki_models::{ConversationItem, ModelProvider};
use beaverki_tools::{ToolContext, ToolRegistry};
use serde_json::json;
use tracing::info;

#[derive(Debug, Clone)]
pub struct AgentRequest {
    pub owner_user_id: String,
    pub primary_agent_id: String,
    pub objective: String,
    pub scope: MemoryScope,
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
    tool_context: ToolContext,
    max_steps: usize,
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
            tool_context,
            max_steps,
        }
    }

    pub async fn run_task(&self, request: AgentRequest) -> Result<AgentResult> {
        let task = self
            .db
            .create_task(
                &request.owner_user_id,
                &request.primary_agent_id,
                &request.objective,
                request.scope,
            )
            .await?;

        self.db
            .record_audit_event(
                "agent",
                &request.primary_agent_id,
                "task_created",
                json!({
                    "task_id": task.task_id,
                    "owner_user_id": request.owner_user_id,
                }),
            )
            .await?;
        self.db
            .update_task_state(&task.task_id, TaskState::Running)
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "task_started",
                "agent",
                &request.primary_agent_id,
                json!({ "objective": request.objective }),
            )
            .await?;

        let retrieval_scope = RetrievalScope {
            owner_user_id: Some(request.owner_user_id.clone()),
            visible_scopes: vec![MemoryScope::Private, MemoryScope::Household],
            limit: 8,
        };
        let memories = self
            .memory
            .retrieve_for_agent_context(&retrieval_scope)
            .await?;
        self.db
            .record_audit_event(
                "agent",
                &request.primary_agent_id,
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

        let mut conversation = vec![ConversationItem::UserText(build_user_prompt(
            &request.objective,
            &memories
                .iter()
                .map(|memory| memory.content_text.clone())
                .collect::<Vec<_>>(),
        ))];
        let instructions = system_prompt(&self.tool_context.allowed_roots);

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
                    &self.tools.definitions(),
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
                    &request.primary_agent_id,
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
                self.memory
                    .record_summary_memory(
                        &request.owner_user_id,
                        request.scope,
                        &task.task_id,
                        &answer,
                    )
                    .await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "task_completed",
                        "agent",
                        &request.primary_agent_id,
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
                let invocation_id = self
                    .db
                    .start_tool_invocation(
                        &task.task_id,
                        &request.primary_agent_id,
                        &tool_call.name,
                        tool_call.arguments.clone(),
                    )
                    .await?;

                let invocation_result = self
                    .tools
                    .invoke(
                        &tool_call.name,
                        tool_call.arguments.clone(),
                        &self.tool_context,
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
                    Err(error) => {
                        let response_json = error.as_json();
                        let status = match error {
                            beaverki_tools::ToolError::Denied { .. } => {
                                ToolInvocationStatus::Denied
                            }
                            beaverki_tools::ToolError::Failed(_) => ToolInvocationStatus::Failed,
                        };
                        self.db
                            .finish_tool_invocation(&invocation_id, status, response_json.clone())
                            .await?;
                        self.db
                            .append_task_event(
                                &task.task_id,
                                "tool_invocation_failed",
                                "tool",
                                &tool_call.name,
                                json!({
                                    "invocation_id": invocation_id,
                                    "response": response_json,
                                }),
                            )
                            .await?;
                        conversation.push(ConversationItem::FunctionCallOutput {
                            call_id: tool_call.call_id,
                            output: response_json,
                        });
                    }
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
                &request.primary_agent_id,
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
}

fn build_user_prompt(objective: &str, memories: &[String]) -> String {
    let mut prompt = String::new();
    prompt.push_str("Objective:\n");
    prompt.push_str(objective);

    if !memories.is_empty() {
        prompt.push_str("\n\nRelevant memory:\n");
        for memory in memories {
            prompt.push_str("- ");
            prompt.push_str(memory);
            prompt.push('\n');
        }
    }

    prompt
}

fn system_prompt(allowed_roots: &[PathBuf]) -> String {
    let roots = allowed_roots
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "You are BeaverKI M0, a local CLI-first assistant.
Use tools when needed, but keep the task focused and auditable.
Only low-risk read-only shell commands are allowed in this milestone.
For file writes, prefer filesystem_write_text. Never claim a denied tool succeeded.
Allowed filesystem roots: {roots}.
When you are done, answer concisely with the result and any important limitations."
    )
}
