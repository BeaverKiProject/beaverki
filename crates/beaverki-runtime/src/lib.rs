use anyhow::{Context, Result, anyhow, bail};
use beaverki_agent::{AgentMemoryMode, AgentRequest, AgentResult, PrimaryAgentRunner};
use beaverki_automation as automation;
use beaverki_config::{
    LoadedConfig, SessionLifecycleAction, SessionLifecyclePolicy, SessionPolicyMatchInput,
    select_session_lifecycle_policy,
};
use beaverki_core::{MemoryScope, TaskState, now_rfc3339};
use beaverki_db::{
    ApprovalActionSet, ApprovalRow, BootstrapState, ConnectorIdentityRow, ConversationSessionRow,
    Database, IssueApprovalActions, MemoryRow, NewConversationSession, NewHouseholdDelivery,
    NewSchedule, NewScript, NewScriptReview, NewTask, NewWorkflowDefinition, NewWorkflowReview,
    NewWorkflowRun, NewWorkflowStage, RoleRow, RuntimeHeartbeatRow, RuntimeSessionRow, ScheduleRow,
    ScriptReviewRow, ScriptRow, TaskRow, UpdateWorkflowDefinition, UserRoleRow, UserRow,
    WorkflowDefinitionRow, WorkflowReviewRow, WorkflowRunRow, WorkflowStageRow,
};
use beaverki_memory::MemoryStore;
use beaverki_models::ModelProvider;
use beaverki_policy::{
    can_grant_approvals, can_send_household_direct_delivery, can_write_household_memory,
    visible_memory_scopes,
};
use beaverki_tools::{ToolContext, builtin_registry, validate_json_value_against_schema};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

mod connector_support;
mod daemon;
mod delivery;
mod discord;
mod secrets;
mod session;
mod types;
mod workflow;

pub use self::daemon::{
    ConnectorMessageReply, ConnectorMessageRequest, DaemonClient, DaemonRequest, DaemonResponse,
    DaemonStatus, RuntimeDaemon, latest_daemon_status, load_daemon_client,
};
pub use self::types::{
    MemoryInspection, RemoteApprovalActionOutcome, ScriptInspection, SessionLifecycleExecution,
    TaskInspection, WorkflowDefinitionInput, WorkflowInspection, WorkflowStageInput,
};

use self::connector_support::assistant_reply_for_context;
use self::delivery::{RuntimeHouseholdDeliveryDelegate, tool_error_to_anyhow};
use self::secrets::{load_discord_bot_token, load_provider};
use self::session::{
    CLI_CONVERSATION_HISTORY_LIMIT, CliConversationExchange, CliConversationStatus,
    SESSION_AUDIENCE_DIRECT_USER, SESSION_AUDIENCE_SCHEDULED_RUN, SESSION_KIND_CLI,
    SESSION_KIND_CRON_RUN, SESSION_KIND_DIRECT_MESSAGE, SESSION_KIND_GROUP_ROOM,
    SESSION_LIFECYCLE_REASON_MANUAL_RESET, build_cli_task_context, cap_scopes_to_session,
    cap_task_scope_to_session, cli_conversation_status, connector_group_room_policy,
    ensure_memory_visible_to_user, is_session_reset_command, parse_memory_kind_filter,
    parse_session_max_scope, resolve_visible_memory_scopes, session_lifecycle_is_due,
};
use self::workflow::{
    apply_stage_result, parse_json_field, validate_workflow_definition_input,
    workflow_definition_json, workflow_notify_message, workflow_notify_recipient_exact_match,
    workflow_notify_recipient_fuzzy_match, workflow_run_result_text,
};

const WORKFLOW_RUN_TASK_KIND: &str = "workflow_run";
const WORKFLOW_AGENT_STAGE_TASK_KIND: &str = "workflow_agent_stage";

pub struct Runtime {
    config: LoadedConfig,
    db: Database,
    default_user: UserRow,
    provider: Arc<dyn ModelProvider>,
    runner: PrimaryAgentRunner,
    discord_bot_token: Option<String>,
}

impl Runtime {
    async fn resolve_workflow_notify_recipient(
        &self,
        owner_user_id: &str,
        recipient_query: Option<&str>,
    ) -> Result<UserRow> {
        let Some(recipient_query) = recipient_query
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return self
                .db
                .fetch_user(owner_user_id)
                .await?
                .ok_or_else(|| anyhow!("workflow owner '{}' not found", owner_user_id));
        };

        let normalized = recipient_query.to_ascii_lowercase();
        let users = self
            .db
            .list_users()
            .await?
            .into_iter()
            .filter(|user| matches!(user.status.as_str(), "enabled" | "active"))
            .collect::<Vec<_>>();

        let exact_matches = users
            .iter()
            .filter(|user| workflow_notify_recipient_exact_match(user, &normalized))
            .cloned()
            .collect::<Vec<_>>();
        if exact_matches.len() == 1 {
            return Ok(exact_matches[0].clone());
        }
        if exact_matches.len() > 1 {
            bail!(
                "workflow recipient '{}' is ambiguous; matches {}",
                recipient_query,
                exact_matches
                    .iter()
                    .map(|user| user.display_name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let fuzzy_matches = users
            .iter()
            .filter(|user| workflow_notify_recipient_fuzzy_match(user, &normalized))
            .cloned()
            .collect::<Vec<_>>();
        if fuzzy_matches.len() == 1 {
            return Ok(fuzzy_matches[0].clone());
        }
        if fuzzy_matches.len() > 1 {
            bail!(
                "workflow recipient '{}' is ambiguous; matches {}",
                recipient_query,
                fuzzy_matches
                    .iter()
                    .map(|user| user.display_name.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        bail!(
            "workflow recipient '{}' does not resolve to an enabled user",
            recipient_query
        );
    }

    pub async fn load(config_dir: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let config = LoadedConfig::load_from_dir(config_dir)?;
        let db = Database::connect(&config.runtime.database_path).await?;
        let default_user = db
            .default_user()
            .await?
            .ok_or_else(|| anyhow!("runtime database has no bootstrap user; run setup first"))?;
        let provider = Arc::new(load_provider(&config, passphrase)?) as Arc<dyn ModelProvider>;
        let discord_bot_token = load_discord_bot_token(&config, passphrase)?;
        Self::from_parts_with_secrets(config, db, default_user, provider, discord_bot_token)
    }

    pub fn from_parts(
        config: LoadedConfig,
        db: Database,
        default_user: UserRow,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Self> {
        Self::from_parts_with_secrets(config, db, default_user, provider, None)
    }

    fn from_parts_with_secrets(
        config: LoadedConfig,
        db: Database,
        default_user: UserRow,
        provider: Arc<dyn ModelProvider>,
        discord_bot_token: Option<String>,
    ) -> Result<Self> {
        let memory = MemoryStore::new(db.clone());
        let mut allowed_roots = vec![config.runtime.workspace_root.clone()];
        if !allowed_roots.contains(&config.runtime.data_dir) {
            allowed_roots.push(config.runtime.data_dir.clone());
        }
        let mut tool_context =
            ToolContext::new(config.runtime.workspace_root.clone(), allowed_roots);
        tool_context.default_timezone = Some(config.runtime.default_timezone.clone());
        tool_context.browser_interactive_launcher =
            config.integrations.browser.interactive_launcher.clone();
        tool_context.browser_headless_program =
            config.integrations.browser.headless_browser.clone();
        tool_context.browser_headless_args = config.integrations.browser.headless_args.clone();
        let household_delivery_delegate = Arc::new(RuntimeHouseholdDeliveryDelegate {
            db: db.clone(),
            runtime_actor_id: config.runtime.instance_id.clone(),
            discord_bot_token: discord_bot_token.clone(),
        });
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            memory,
            provider.clone(),
            builtin_registry(),
            tool_context,
            Some(household_delivery_delegate),
            usize::from(config.runtime.defaults.max_agent_steps),
        );

        Ok(Self {
            config,
            db,
            default_user,
            provider,
            runner,
            discord_bot_token,
        })
    }

    pub fn config(&self) -> &LoadedConfig {
        &self.config
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn discord_bot_token(&self) -> Option<&str> {
        self.discord_bot_token.as_deref()
    }

    pub fn daemon_socket_path(&self) -> PathBuf {
        self.config.runtime.state_dir.join("daemon.sock")
    }

    pub fn daemon_log_path(&self) -> PathBuf {
        self.config.runtime.log_dir.join("daemon.log")
    }

    pub async fn run_objective(
        &self,
        user_id: Option<&str>,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<AgentResult> {
        let user = self.resolve_user(user_id).await?;
        let initiating_identity_id = format!("cli:{}", user.user_id);
        let (request, _) = self
            .prepare_cli_task_request(&user, &initiating_identity_id, objective, scope)
            .await?;
        self.runner.run_task(request).await
    }

    pub async fn enqueue_objective(
        &self,
        user_id: Option<&str>,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<TaskRow> {
        let user = self.resolve_user(user_id).await?;
        let initiating_identity_id = format!("cli:{}", user.user_id);
        let (request, session) = self
            .prepare_cli_task_request(&user, &initiating_identity_id, objective, scope)
            .await?;
        self.create_task_from_request(&request, &session, None)
            .await
    }

    pub async fn enqueue_objective_from_connector(
        &self,
        user: &UserRow,
        initiating_identity_id: &str,
        objective: &str,
        session: &ConversationSessionRow,
        task_context: Option<&str>,
        scope: MemoryScope,
    ) -> Result<TaskRow> {
        let request = self
            .build_primary_request_for_session(
                user,
                initiating_identity_id,
                objective,
                scope,
                session,
                None,
                task_context.map(ToOwned::to_owned),
            )
            .await?;
        self.create_task_from_request(&request, session, task_context)
            .await
    }

    async fn create_task_from_request(
        &self,
        request: &AgentRequest,
        session: &ConversationSessionRow,
        task_context_override: Option<&str>,
    ) -> Result<TaskRow> {
        self.db
            .create_task_with_params(NewTask {
                owner_user_id: &request.owner_user_id,
                initiating_identity_id: &request.initiating_identity_id,
                primary_agent_id: &request.primary_agent_id,
                assigned_agent_id: &request.assigned_agent_id,
                parent_task_id: request.parent_task_id.as_deref(),
                session_id: Some(&session.session_id),
                kind: &request.kind,
                objective: &request.objective,
                context_summary: task_context_override.or(request.task_context.as_deref()),
                scope: request.scope,
                wake_at: None,
            })
            .await
    }

    async fn create_reset_task(
        &self,
        user: &UserRow,
        initiating_identity_id: &str,
        objective: &str,
        session: &ConversationSessionRow,
        scope: MemoryScope,
    ) -> Result<TaskRow> {
        let primary_agent_id = user
            .primary_agent_id
            .clone()
            .ok_or_else(|| anyhow!("user '{}' has no primary agent", user.user_id))?;
        let task = self
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &user.user_id,
                initiating_identity_id,
                primary_agent_id: &primary_agent_id,
                assigned_agent_id: &primary_agent_id,
                parent_task_id: None,
                session_id: Some(&session.session_id),
                kind: "interactive",
                objective,
                context_summary: Some("Conversation session reset handled by runtime."),
                scope,
                wake_at: None,
            })
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "conversation_session_reset",
                "runtime",
                &self.config.runtime.instance_id,
                json!({
                    "session_id": session.session_id,
                    "session_kind": session.session_kind,
                    "session_key": session.session_key,
                }),
            )
            .await?;
        self.db
            .complete_task(
                &task.task_id,
                "Started a new conversation. Durable memory and audit history were left intact.",
            )
            .await?;
        self.db
            .reset_conversation_session(&session.session_id, SESSION_LIFECYCLE_REASON_MANUAL_RESET)
            .await?;
        self.db
            .record_audit_event(
                "runtime",
                &self.config.runtime.instance_id,
                "conversation_session_reset",
                json!({
                    "session_id": session.session_id,
                    "session_kind": session.session_kind,
                    "session_key": session.session_key,
                    "task_id": task.task_id,
                }),
            )
            .await?;
        self.db
            .fetch_task_for_owner(&user.user_id, &task.task_id)
            .await?
            .ok_or_else(|| anyhow!("reset task '{}' disappeared after completion", task.task_id))
    }

    pub async fn execute_task(&self, task: TaskRow) -> Result<AgentResult> {
        if task.kind == "scheduled_household_delivery" {
            return self.execute_scheduled_household_delivery_task(task).await;
        }
        if task.kind == WORKFLOW_RUN_TASK_KIND {
            return self.execute_workflow_run_task(task).await;
        }
        if task.kind == "scheduled_lua" || task.kind == "lua_script" {
            return self.execute_lua_task(task).await;
        }
        let approved_shell_commands = self
            .db
            .approved_shell_commands_for_task(&task.task_id, &task.owner_user_id)
            .await?;
        let request = self
            .build_request_from_task(&task.owner_user_id, &task, approved_shell_commands)
            .await?;
        self.runner.resume_task(task, request).await
    }

    pub async fn execute_next_runnable_task(&self) -> Result<Option<AgentResult>> {
        let _ = self.materialize_due_schedules().await?;
        let Some(task) = self.db.fetch_next_runnable_task().await? else {
            return Ok(None);
        };
        self.execute_task(task).await.map(Some)
    }

    pub async fn pending_task_count(&self) -> Result<i64> {
        self.db.pending_task_count().await
    }

    pub async fn run_session_lifecycle_cleanup(&self) -> Result<Vec<SessionLifecycleExecution>> {
        let now = Utc::now();
        let sessions = self.db.list_conversation_sessions(None, false, 512).await?;
        let mut actions = Vec::new();

        for session in sessions {
            let Some(policy) = self.match_session_lifecycle_policy(&session) else {
                continue;
            };
            if !session_lifecycle_is_due(&session, policy, now)? {
                continue;
            }

            let reason = format!("policy:{}:{}", policy.policy_id, policy.action.as_str());
            match policy.action {
                SessionLifecycleAction::Reset => {
                    self.db
                        .reset_conversation_session(&session.session_id, &reason)
                        .await?;
                }
                SessionLifecycleAction::Archive => {
                    self.db
                        .archive_conversation_session(&session.session_id, &reason)
                        .await?;
                }
            }
            self.db
                .record_audit_event(
                    "runtime",
                    &self.config.runtime.instance_id,
                    "conversation_session_lifecycle_applied",
                    json!({
                        "session_id": session.session_id,
                        "session_kind": session.session_kind,
                        "policy_id": policy.policy_id,
                        "action": policy.action.as_str(),
                        "reason": reason,
                        "last_activity_at": session.last_activity_at,
                    }),
                )
                .await?;
            actions.push(SessionLifecycleExecution {
                session_id: session.session_id,
                policy_id: policy.policy_id.clone(),
                action: policy.action.as_str().to_owned(),
                reason,
            });
        }

        Ok(actions)
    }

    pub async fn latest_runtime_session(&self) -> Result<Option<RuntimeSessionRow>> {
        self.db
            .latest_runtime_session(&self.config.runtime.instance_id)
            .await
    }

    pub async fn list_runtime_heartbeats(
        &self,
        session_id: &str,
        limit: i64,
    ) -> Result<Vec<RuntimeHeartbeatRow>> {
        self.db.list_runtime_heartbeats(session_id, limit).await
    }

    pub async fn inspect_task(
        &self,
        user_id: Option<&str>,
        task_id: &str,
    ) -> Result<TaskInspection> {
        let user = self.resolve_user(user_id).await?;
        let task = self
            .db
            .fetch_task_for_owner(&user.user_id, task_id)
            .await?
            .ok_or_else(|| anyhow!("task '{task_id}' not found"))?;
        let events = self
            .db
            .fetch_task_events_for_owner(&user.user_id, task_id)
            .await?;
        let tool_invocations = self
            .db
            .fetch_tool_invocations_for_owner(&user.user_id, task_id)
            .await?;

        Ok(TaskInspection {
            task,
            events,
            tool_invocations,
        })
    }

    pub async fn list_memories(
        &self,
        user_id: Option<&str>,
        scope: Option<&str>,
        kind: Option<&str>,
        include_superseded: bool,
        include_forgotten: bool,
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        let user = self.resolve_user(user_id).await?;
        let role_ids = self.user_role_ids(&user.user_id).await?;
        let scopes = resolve_visible_memory_scopes(&role_ids, scope)?;
        let memory_kind = parse_memory_kind_filter(kind)?;
        self.db
            .query_memories(
                Some(&user.user_id),
                &scopes,
                memory_kind,
                None,
                None,
                include_superseded,
                include_forgotten,
                limit,
            )
            .await
    }

    pub async fn inspect_memory(
        &self,
        user_id: Option<&str>,
        memory_id: &str,
    ) -> Result<MemoryInspection> {
        let user = self.resolve_user(user_id).await?;
        let role_ids = self.user_role_ids(&user.user_id).await?;
        let visible_scopes = visible_memory_scopes(&role_ids);
        let memory = self
            .db
            .fetch_memory(memory_id)
            .await?
            .ok_or_else(|| anyhow!("memory '{memory_id}' not found"))?;
        ensure_memory_visible_to_user(&memory, &user.user_id, &visible_scopes)?;
        Ok(MemoryInspection { memory })
    }

    pub async fn memory_history(
        &self,
        user_id: Option<&str>,
        subject_key: &str,
        scope: Option<&str>,
        subject_type: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        let user = self.resolve_user(user_id).await?;
        let role_ids = self.user_role_ids(&user.user_id).await?;
        let scopes = resolve_visible_memory_scopes(&role_ids, scope)?;
        self.db
            .query_memories(
                Some(&user.user_id),
                &scopes,
                None,
                subject_type,
                Some(subject_key),
                true,
                true,
                limit,
            )
            .await
    }

    pub async fn forget_memory(
        &self,
        user_id: Option<&str>,
        memory_id: &str,
        reason: &str,
    ) -> Result<MemoryInspection> {
        let user = self.resolve_user(user_id).await?;
        let role_ids = self.user_role_ids(&user.user_id).await?;
        let visible_scopes = visible_memory_scopes(&role_ids);
        let memory = self
            .db
            .fetch_memory(memory_id)
            .await?
            .ok_or_else(|| anyhow!("memory '{memory_id}' not found"))?;
        ensure_memory_visible_to_user(&memory, &user.user_id, &visible_scopes)?;
        let memory_scope = memory.scope.parse::<MemoryScope>().map_err(|_| {
            anyhow!(
                "memory '{memory_id}' has unsupported scope '{}'",
                memory.scope
            )
        })?;
        if matches!(memory_scope, MemoryScope::Household) && !can_write_household_memory(&role_ids)
        {
            bail!(
                "user '{}' is not allowed to forget household memory",
                user.user_id
            );
        }
        if memory.forgotten_at.is_some() {
            bail!("memory '{memory_id}' is already forgotten");
        }

        self.db.forget_memory(memory_id, reason).await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "memory_forgotten",
                json!({
                    "memory_id": memory_id,
                    "scope": memory.scope,
                    "memory_kind": memory.memory_kind,
                    "subject_type": memory.subject_type,
                    "subject_key": memory.subject_key,
                    "reason": reason,
                }),
            )
            .await?;
        let updated = self
            .db
            .fetch_memory(memory_id)
            .await?
            .ok_or_else(|| anyhow!("memory '{memory_id}' disappeared after forget"))?;
        Ok(MemoryInspection { memory: updated })
    }

    pub async fn create_lua_script(
        &self,
        user_id: Option<&str>,
        script_id: Option<&str>,
        source_text: &str,
        capability_profile: Value,
        created_from_task_id: Option<&str>,
        intended_behavior_summary: &str,
    ) -> Result<ScriptInspection> {
        let user = self.resolve_user(user_id).await?;
        let script = self
            .db
            .create_script(NewScript {
                script_id,
                owner_user_id: &user.user_id,
                kind: "lua",
                status: "draft",
                source_text,
                capability_profile_json: capability_profile.clone(),
                created_from_task_id,
                safety_status: "pending",
                safety_summary: Some("Awaiting safety review."),
            })
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "script_created",
                json!({
                    "script_id": script.script_id,
                    "created_from_task_id": created_from_task_id,
                    "kind": "lua",
                }),
            )
            .await?;
        let _ = self
            .review_lua_script(
                Some(&user.user_id),
                &script.script_id,
                intended_behavior_summary,
            )
            .await?;
        self.inspect_script(Some(&user.user_id), &script.script_id)
            .await
    }

    pub async fn review_lua_script(
        &self,
        user_id: Option<&str>,
        script_id: &str,
        intended_behavior_summary: &str,
    ) -> Result<ScriptReviewRow> {
        let user = self.resolve_user(user_id).await?;
        let script = self
            .db
            .fetch_script_for_owner(&user.user_id, script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{script_id}' not found"))?;
        let capability_profile: Value = serde_json::from_str(&script.capability_profile_json)
            .context("failed to parse capability profile")?;
        let review = automation::review_lua_script(
            &self.provider,
            &script.script_id,
            script.created_from_task_id.as_deref(),
            &user.user_id,
            &script.source_text,
            &capability_profile,
            intended_behavior_summary,
        )
        .await?;
        let application =
            automation::apply_script_review(&review, &script.status, Some(&script.status));
        self.db
            .update_script_safety(
                &script.script_id,
                &application.safety_status,
                &review.summary,
                Some(&application.resulting_status),
            )
            .await?;
        let review_row = self
            .db
            .create_script_review(NewScriptReview {
                script_id: &script.script_id,
                reviewer_agent_id: automation::SAFETY_AGENT_ID,
                review_type: "lua_script",
                verdict: &review.verdict,
                risk_level: &review.risk_level,
                findings_json: review.as_findings_json(),
                summary_text: &review.summary,
                reviewed_artifact_text: &script.source_text,
            })
            .await?;
        self.db
            .record_audit_event(
                "safety_agent",
                automation::SAFETY_AGENT_ID,
                "script_reviewed",
                json!({
                    "script_id": script.script_id,
                    "owner_user_id": user.user_id,
                    "verdict": review.verdict,
                    "risk_level": review.risk_level,
                    "summary": review.summary,
                }),
            )
            .await?;
        Ok(review_row)
    }

    pub async fn list_scripts(&self, user_id: Option<&str>) -> Result<Vec<ScriptRow>> {
        let user = self.resolve_user(user_id).await?;
        self.db.list_scripts_for_owner(&user.user_id).await
    }

    pub async fn inspect_script(
        &self,
        user_id: Option<&str>,
        script_id: &str,
    ) -> Result<ScriptInspection> {
        let user = self.resolve_user(user_id).await?;
        let script = self
            .db
            .fetch_script_for_owner(&user.user_id, script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{script_id}' not found"))?;
        let reviews = self.db.list_script_reviews(&script.script_id).await?;
        let schedules = self
            .db
            .list_schedules_for_owner(&user.user_id)
            .await?
            .into_iter()
            .filter(|row| row.target_type == "lua_script" && row.target_id == script.script_id)
            .collect();
        Ok(ScriptInspection {
            script,
            reviews,
            schedules,
        })
    }

    pub async fn activate_script(
        &self,
        user_id: Option<&str>,
        script_id: &str,
    ) -> Result<ScriptRow> {
        let user = self.resolve_user(user_id).await?;
        let script = self
            .db
            .fetch_script_for_owner(&user.user_id, script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{script_id}' not found"))?;
        if script.safety_status != "approved" {
            bail!(
                "script '{}' cannot be activated until safety review is approved",
                script.script_id
            );
        }
        self.db
            .update_script_status(&script.script_id, "active")
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "script_activated",
                json!({ "script_id": script.script_id }),
            )
            .await?;
        self.db
            .fetch_script_for_owner(&user.user_id, &script.script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{}' disappeared after activation", script.script_id))
    }

    pub async fn disable_script(
        &self,
        user_id: Option<&str>,
        script_id: &str,
    ) -> Result<ScriptRow> {
        let user = self.resolve_user(user_id).await?;
        let script = self
            .db
            .fetch_script_for_owner(&user.user_id, script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{script_id}' not found"))?;
        self.db
            .update_script_status(&script.script_id, "disabled")
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "script_disabled",
                json!({ "script_id": script.script_id }),
            )
            .await?;
        self.db
            .fetch_script_for_owner(&user.user_id, &script.script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{}' disappeared after disable", script.script_id))
    }

    pub async fn create_workflow_definition(
        &self,
        user_id: Option<&str>,
        workflow_id: Option<&str>,
        definition: WorkflowDefinitionInput,
        created_from_task_id: Option<&str>,
        intended_behavior_summary: &str,
    ) -> Result<WorkflowInspection> {
        let user = self.resolve_user(user_id).await?;
        validate_workflow_definition_input(&definition)?;
        let stages = definition
            .stages
            .iter()
            .map(|stage| NewWorkflowStage {
                stage_id: None,
                stage_kind: stage.kind.as_str(),
                stage_label: stage.label.as_deref(),
                artifact_ref: stage.artifact_ref.as_deref(),
                stage_config_json: stage.config.clone(),
            })
            .collect::<Vec<_>>();
        let workflow = if let Some(workflow_id) = workflow_id
            && self
                .db
                .fetch_workflow_definition_for_owner(&user.user_id, workflow_id)
                .await?
                .is_some()
        {
            self.db
                .update_workflow_definition_contents(
                    UpdateWorkflowDefinition {
                        workflow_id,
                        owner_user_id: &user.user_id,
                        name: &definition.name,
                        description: definition.description.as_deref(),
                        created_from_task_id,
                        status: "draft",
                        safety_status: "pending",
                        safety_summary: Some("Awaiting workflow safety review."),
                    },
                    &stages,
                )
                .await?
        } else {
            self.db
                .create_workflow_definition(
                    NewWorkflowDefinition {
                        workflow_id,
                        owner_user_id: &user.user_id,
                        name: &definition.name,
                        description: definition.description.as_deref(),
                        status: "draft",
                        created_from_task_id,
                        safety_status: "pending",
                        safety_summary: Some("Awaiting workflow safety review."),
                    },
                    &stages,
                )
                .await?
        };
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "workflow_written",
                json!({
                    "workflow_id": workflow.workflow_id,
                    "created_from_task_id": created_from_task_id,
                    "stage_count": definition.stages.len(),
                    "current_version_number": workflow.current_version_number,
                }),
            )
            .await?;
        let _ = self
            .review_workflow_definition(
                Some(&user.user_id),
                &workflow.workflow_id,
                intended_behavior_summary,
            )
            .await?;
        self.inspect_workflow_definition(Some(&user.user_id), &workflow.workflow_id)
            .await
    }

    pub async fn review_workflow_definition(
        &self,
        user_id: Option<&str>,
        workflow_id: &str,
        intended_behavior_summary: &str,
    ) -> Result<WorkflowReviewRow> {
        let user = self.resolve_user(user_id).await?;
        let workflow = self
            .db
            .fetch_workflow_definition_for_owner(&user.user_id, workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{workflow_id}' not found"))?;
        let stages = self.db.list_workflow_stages(&workflow.workflow_id).await?;
        let definition_json = workflow_definition_json(&workflow, &stages)?;
        let review = automation::review_workflow_definition(
            &self.provider,
            &workflow.workflow_id,
            workflow.created_from_task_id.as_deref(),
            &user.user_id,
            &definition_json,
            intended_behavior_summary,
        )
        .await?;
        let application =
            automation::apply_script_review(&review, &workflow.status, Some(&workflow.status));
        self.db
            .update_workflow_definition_safety(
                &workflow.workflow_id,
                &application.safety_status,
                &review.summary,
                Some(&application.resulting_status),
            )
            .await?;
        let review_row = self
            .db
            .create_workflow_review(NewWorkflowReview {
                workflow_id: &workflow.workflow_id,
                version_number: workflow.current_version_number,
                reviewer_agent_id: automation::SAFETY_AGENT_ID,
                review_type: "workflow_definition",
                verdict: &review.verdict,
                risk_level: &review.risk_level,
                findings_json: review.as_findings_json(),
                summary_text: &review.summary,
                reviewed_artifact_text: &definition_json.to_string(),
            })
            .await?;
        self.db
            .record_audit_event(
                "safety_agent",
                automation::SAFETY_AGENT_ID,
                "workflow_reviewed",
                json!({
                    "workflow_id": workflow.workflow_id,
                    "owner_user_id": user.user_id,
                    "verdict": review.verdict,
                    "risk_level": review.risk_level,
                    "summary": review.summary,
                }),
            )
            .await?;
        Ok(review_row)
    }

    pub async fn list_workflow_definitions(
        &self,
        user_id: Option<&str>,
    ) -> Result<Vec<WorkflowDefinitionRow>> {
        let user = self.resolve_user(user_id).await?;
        self.db
            .list_workflow_definitions_for_owner(&user.user_id)
            .await
    }

    pub async fn inspect_workflow_definition(
        &self,
        user_id: Option<&str>,
        workflow_id: &str,
    ) -> Result<WorkflowInspection> {
        let user = self.resolve_user(user_id).await?;
        let workflow = self
            .db
            .fetch_workflow_definition_for_owner(&user.user_id, workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{workflow_id}' not found"))?;
        let versions = self
            .db
            .list_workflow_versions(&workflow.workflow_id)
            .await?;
        let stages = self.db.list_workflow_stages(&workflow.workflow_id).await?;
        let reviews = self.db.list_workflow_reviews(&workflow.workflow_id).await?;
        let schedules = self
            .db
            .list_schedules_for_owner(&user.user_id)
            .await?
            .into_iter()
            .filter(|row| row.target_type == "workflow" && row.target_id == workflow.workflow_id)
            .collect();
        let runs = self
            .db
            .list_workflow_runs_for_workflow(&workflow.workflow_id)
            .await?;
        Ok(WorkflowInspection {
            workflow,
            versions,
            stages,
            reviews,
            schedules,
            runs,
        })
    }

    pub async fn activate_workflow_definition(
        &self,
        user_id: Option<&str>,
        workflow_id: &str,
    ) -> Result<WorkflowDefinitionRow> {
        let user = self.resolve_user(user_id).await?;
        let workflow = self
            .db
            .fetch_workflow_definition_for_owner(&user.user_id, workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{workflow_id}' not found"))?;
        if workflow.safety_status != "approved" {
            bail!(
                "workflow '{}' cannot be activated until workflow safety review is approved",
                workflow.workflow_id
            );
        }
        let stages = self.db.list_workflow_stages(&workflow.workflow_id).await?;
        self.validate_workflow_stage_references(&workflow, &stages)
            .await?;
        self.db
            .update_workflow_definition_status(&workflow.workflow_id, "active")
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "workflow_activated",
                json!({ "workflow_id": workflow.workflow_id }),
            )
            .await?;
        self.db
            .fetch_workflow_definition_for_owner(&user.user_id, &workflow.workflow_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "workflow '{}' disappeared after activation",
                    workflow.workflow_id
                )
            })
    }

    pub async fn disable_workflow_definition(
        &self,
        user_id: Option<&str>,
        workflow_id: &str,
    ) -> Result<WorkflowDefinitionRow> {
        let user = self.resolve_user(user_id).await?;
        let workflow = self
            .db
            .fetch_workflow_definition_for_owner(&user.user_id, workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{workflow_id}' not found"))?;
        self.db
            .update_workflow_definition_status(&workflow.workflow_id, "disabled")
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "workflow_disabled",
                json!({ "workflow_id": workflow.workflow_id }),
            )
            .await?;
        self.db
            .fetch_workflow_definition_for_owner(&user.user_id, &workflow.workflow_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "workflow '{}' disappeared after disable",
                    workflow.workflow_id
                )
            })
    }

    pub async fn replay_workflow_definition(
        &self,
        user_id: Option<&str>,
        workflow_id: &str,
    ) -> Result<TaskRow> {
        let user = self.resolve_user(user_id).await?;
        let workflow = self
            .db
            .fetch_workflow_definition_for_owner(&user.user_id, workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{workflow_id}' not found"))?;
        let primary_agent_id = self.primary_agent_id_for_owner(&user.user_id).await?;
        let session = self.create_cron_run_session(&workflow.workflow_id).await?;
        self.start_workflow_run(
            &workflow,
            format!("workflow:{}", workflow.workflow_id),
            None,
            &primary_agent_id,
            Some(&session.session_id),
            None,
        )
        .await
    }

    pub async fn create_schedule(
        &self,
        user_id: Option<&str>,
        schedule_id: Option<&str>,
        script_id: &str,
        cron_expr: &str,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        self.create_schedule_for_target(
            user_id,
            schedule_id,
            "lua_script",
            script_id,
            cron_expr,
            enabled,
        )
        .await
    }

    pub async fn create_workflow_schedule(
        &self,
        user_id: Option<&str>,
        schedule_id: Option<&str>,
        workflow_id: &str,
        cron_expr: &str,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        self.create_schedule_for_target(
            user_id,
            schedule_id,
            "workflow",
            workflow_id,
            cron_expr,
            enabled,
        )
        .await
    }

    async fn create_schedule_for_target(
        &self,
        user_id: Option<&str>,
        schedule_id: Option<&str>,
        target_type: &str,
        target_id: &str,
        cron_expr: &str,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        let user = self.resolve_user(user_id).await?;
        match target_type {
            "lua_script" => {
                let script = self
                    .db
                    .fetch_script_for_owner(&user.user_id, target_id)
                    .await?
                    .ok_or_else(|| anyhow!("script '{target_id}' not found"))?;
                if script.status != "active" {
                    bail!(
                        "script '{}' must be active before scheduling",
                        script.script_id
                    );
                }
            }
            "workflow" => {
                let workflow = self
                    .db
                    .fetch_workflow_definition_for_owner(&user.user_id, target_id)
                    .await?
                    .ok_or_else(|| anyhow!("workflow '{target_id}' not found"))?;
                if workflow.status != "active" || workflow.safety_status != "approved" {
                    bail!(
                        "workflow '{}' must be active and approved before scheduling",
                        workflow.workflow_id
                    );
                }
            }
            other => bail!("unsupported schedule target type '{other}'"),
        }
        let next_run_at = automation::next_run_after(
            cron_expr,
            &now_rfc3339(),
            Some(self.config.runtime.default_timezone.as_str()),
        )?;
        let schedule = self
            .db
            .create_schedule(NewSchedule {
                schedule_id,
                owner_user_id: &user.user_id,
                target_type,
                target_id,
                cron_expr,
                enabled,
                next_run_at: &next_run_at,
            })
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                "schedule_created",
                json!({
                    "schedule_id": schedule.schedule_id,
                    "target_type": target_type,
                    "target_id": target_id,
                    "cron_expr": cron_expr,
                    "enabled": enabled,
                }),
            )
            .await?;
        Ok(schedule)
    }

    pub async fn list_schedules(&self, user_id: Option<&str>) -> Result<Vec<ScheduleRow>> {
        let user = self.resolve_user(user_id).await?;
        self.db.list_schedules_for_owner(&user.user_id).await
    }

    pub async fn set_schedule_enabled(
        &self,
        user_id: Option<&str>,
        schedule_id: &str,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        let user = self.resolve_user(user_id).await?;
        let schedule = self
            .db
            .fetch_schedule_for_owner(&user.user_id, schedule_id)
            .await?
            .ok_or_else(|| anyhow!("schedule '{schedule_id}' not found"))?;
        let next_run_at = if enabled {
            automation::next_run_after(
                &schedule.cron_expr,
                &now_rfc3339(),
                Some(self.config.runtime.default_timezone.as_str()),
            )?
        } else {
            schedule.next_run_at.clone()
        };
        self.db
            .update_schedule_state(
                &schedule.schedule_id,
                enabled,
                &next_run_at,
                schedule.last_run_at.as_deref(),
            )
            .await?;
        self.db
            .record_audit_event(
                "user",
                &user.user_id,
                if enabled {
                    "schedule_enabled"
                } else {
                    "schedule_disabled"
                },
                json!({
                    "schedule_id": schedule.schedule_id,
                    "next_run_at": next_run_at,
                }),
            )
            .await?;
        self.db
            .fetch_schedule_for_owner(&user.user_id, &schedule.schedule_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "schedule '{}' disappeared after update",
                    schedule.schedule_id
                )
            })
    }

    pub async fn materialize_due_schedules(&self) -> Result<Vec<TaskRow>> {
        let now = now_rfc3339();
        let due = self.db.list_due_schedules(&now).await?;
        let mut tasks = Vec::new();
        for schedule in due {
            let next_run_at = automation::next_run_after(
                &schedule.cron_expr,
                &now,
                Some(self.config.runtime.default_timezone.as_str()),
            )?;
            self.db
                .update_schedule_state(
                    &schedule.schedule_id,
                    automation::schedule_enabled(&schedule),
                    &next_run_at,
                    Some(&now),
                )
                .await?;
            match schedule.target_type.as_str() {
                "lua_script" => {
                    let Some(script) = self.db.fetch_script(&schedule.target_id).await? else {
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "target_id": schedule.target_id,
                                    "reason": "script_missing",
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    };
                    if script.status != "active" || script.safety_status != "approved" {
                        let reason = if script.status != "active" {
                            "script_not_active"
                        } else {
                            "script_not_approved"
                        };
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "script_id": script.script_id,
                                    "reason": reason,
                                    "script_status": script.status,
                                    "safety_status": script.safety_status,
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    }
                    let primary_agent_id = self
                        .primary_agent_id_for_owner(&schedule.owner_user_id)
                        .await?;
                    let initiating_identity_id = format!("schedule:{}", schedule.schedule_id);
                    let objective = format!("Run Lua automation {}", script.script_id);
                    let task_context = json!({
                        "script_id": script.script_id,
                        "schedule_id": schedule.schedule_id,
                    })
                    .to_string();
                    let session = self.create_cron_run_session(&schedule.schedule_id).await?;
                    let task = self
                        .db
                        .create_task_with_params(NewTask {
                            owner_user_id: &schedule.owner_user_id,
                            initiating_identity_id: &initiating_identity_id,
                            primary_agent_id: &primary_agent_id,
                            assigned_agent_id: &primary_agent_id,
                            parent_task_id: None,
                            session_id: Some(&session.session_id),
                            kind: "scheduled_lua",
                            objective: &objective,
                            context_summary: Some(&task_context),
                            scope: MemoryScope::Private,
                            wake_at: None,
                        })
                        .await?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.config.runtime.instance_id,
                            "schedule_triggered",
                            json!({
                                "schedule_id": schedule.schedule_id,
                                "task_id": task.task_id,
                                "script_id": script.script_id,
                                "next_run_at": next_run_at,
                            }),
                        )
                        .await?;
                    tasks.push(task);
                }
                "household_delivery" => {
                    let Some(template) = self
                        .db
                        .fetch_household_delivery(&schedule.target_id)
                        .await?
                    else {
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "target_id": schedule.target_id,
                                    "reason": "delivery_template_missing",
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    };
                    if template.status == "canceled" {
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "delivery_id": template.delivery_id,
                                    "reason": "delivery_canceled",
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    }
                    let occurrence_dedupe_key =
                        format!("schedule:{}:{}", schedule.schedule_id, schedule.next_run_at);
                    let occurrence = self
                        .db
                        .create_or_reuse_household_delivery(NewHouseholdDelivery {
                            task_id: &template.task_id,
                            requester_user_id: &template.requester_user_id,
                            requester_identity_id: &template.requester_identity_id,
                            recipient_user_id: &template.recipient_user_id,
                            message_text: &template.message_text,
                            delivery_mode: "scheduled_occurrence",
                            connector_type: &template.connector_type,
                            connector_identity_id: &template.connector_identity_id,
                            target_ref: &template.target_ref,
                            fallback_target_ref: template.fallback_target_ref.as_deref(),
                            dedupe_key: &occurrence_dedupe_key,
                            initial_status: "scheduled",
                            parent_delivery_id: Some(&template.delivery_id),
                            schedule_id: Some(&schedule.schedule_id),
                            scheduled_for_at: Some(&schedule.next_run_at),
                            window_starts_at: template.window_starts_at.as_deref(),
                            window_ends_at: template.window_ends_at.as_deref(),
                            recurrence_rule: Some(&schedule.cron_expr),
                            scheduled_job_state: Some("scheduled"),
                            materialized_task_id: None,
                        })
                        .await?;
                    let objective = format!(
                        "Deliver scheduled household message to {}",
                        template.recipient_user_id
                    );
                    let task_context = json!({
                        "delivery_id": occurrence.delivery_id,
                        "schedule_id": schedule.schedule_id,
                    })
                    .to_string();
                    if let Some(existing_task_id) = occurrence.materialized_task_id.as_deref()
                        && let Some(existing_task) = self.db.fetch_task(existing_task_id).await? {
                            tasks.push(existing_task);
                            continue;
                        }
                    let primary_agent_id = self
                        .primary_agent_id_for_owner(&schedule.owner_user_id)
                        .await?;
                    let task = self
                        .db
                        .create_task_with_params(NewTask {
                            owner_user_id: &schedule.owner_user_id,
                            initiating_identity_id: &template.requester_identity_id,
                            primary_agent_id: &primary_agent_id,
                            assigned_agent_id: &primary_agent_id,
                            parent_task_id: None,
                            session_id: None,
                            kind: "scheduled_household_delivery",
                            objective: &objective,
                            context_summary: Some(&task_context),
                            scope: MemoryScope::Private,
                            wake_at: None,
                        })
                        .await?;
                    self.db
                        .mark_household_delivery_materialized(
                            &occurrence.delivery_id,
                            &task.task_id,
                        )
                        .await?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.config.runtime.instance_id,
                            "schedule_triggered",
                            json!({
                                "schedule_id": schedule.schedule_id,
                                "task_id": task.task_id,
                                "delivery_id": occurrence.delivery_id,
                                "template_delivery_id": template.delivery_id,
                                "next_run_at": next_run_at,
                            }),
                        )
                        .await?;
                    tasks.push(task);
                }
                "workflow" => {
                    let Some(workflow) = self
                        .db
                        .fetch_workflow_definition(&schedule.target_id)
                        .await?
                    else {
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "target_id": schedule.target_id,
                                    "reason": "workflow_missing",
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    };
                    if workflow.status != "active" || workflow.safety_status != "approved" {
                        let reason = if workflow.status != "active" {
                            "workflow_not_active"
                        } else {
                            "workflow_not_approved"
                        };
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "workflow_id": workflow.workflow_id,
                                    "reason": reason,
                                    "workflow_status": workflow.status,
                                    "safety_status": workflow.safety_status,
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    }
                    let stages = self.db.list_workflow_stages(&workflow.workflow_id).await?;
                    if let Err(error) = self
                        .validate_workflow_stage_references(&workflow, &stages)
                        .await
                    {
                        self.db
                            .record_audit_event(
                                "runtime",
                                &self.config.runtime.instance_id,
                                "schedule_skipped",
                                json!({
                                    "schedule_id": schedule.schedule_id,
                                    "workflow_id": workflow.workflow_id,
                                    "reason": "workflow_reference_invalid",
                                    "error": error.to_string(),
                                    "next_run_at": next_run_at,
                                }),
                            )
                            .await?;
                        continue;
                    }
                    let primary_agent_id = self
                        .primary_agent_id_for_owner(&schedule.owner_user_id)
                        .await?;
                    let session = self.create_cron_run_session(&schedule.schedule_id).await?;
                    let task = self
                        .start_workflow_run(
                            &workflow,
                            format!("schedule:{}", schedule.schedule_id),
                            Some(&schedule.schedule_id),
                            &primary_agent_id,
                            Some(&session.session_id),
                            None,
                        )
                        .await?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.config.runtime.instance_id,
                            "schedule_triggered",
                            json!({
                                "schedule_id": schedule.schedule_id,
                                "task_id": task.task_id,
                                "workflow_id": workflow.workflow_id,
                                "next_run_at": next_run_at,
                            }),
                        )
                        .await?;
                    tasks.push(task);
                }
                other => {
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.config.runtime.instance_id,
                            "schedule_skipped",
                            json!({
                                "schedule_id": schedule.schedule_id,
                                "target_type": other,
                                "reason": "unsupported_target_type",
                                "next_run_at": next_run_at,
                            }),
                        )
                        .await?;
                }
            }
        }
        Ok(tasks)
    }

    async fn execute_scheduled_household_delivery_task(
        &self,
        task: TaskRow,
    ) -> Result<AgentResult> {
        self.db.clear_task_result(&task.task_id).await?;
        self.db
            .update_task_state(&task.task_id, TaskState::Running)
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "household_delivery_task_started",
                "runtime",
                &self.config.runtime.instance_id,
                json!({ "objective": task.objective }),
            )
            .await?;

        let context: Value = task
            .context_summary
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("failed to parse scheduled household delivery task context")?
            .unwrap_or_else(|| json!({}));
        let delivery_id = context
            .get("delivery_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!(
                    "scheduled household delivery task '{}' is missing delivery_id context",
                    task.task_id
                )
            })?;
        let Some(delivery) = self.db.fetch_household_delivery(delivery_id).await? else {
            self.db
                .fail_task(&task.task_id, "scheduled household delivery is missing")
                .await?;
            let failed = self
                .db
                .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                .await?
                .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
            return Ok(AgentResult { task: failed });
        };

        if delivery.status == "sent" {
            self.db
                .complete_task(
                    &task.task_id,
                    "scheduled household delivery was already sent",
                )
                .await?;
            let completed = self
                .db
                .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                .await?
                .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
            return Ok(AgentResult { task: completed });
        }
        if delivery.status == "canceled" {
            self.db
                .complete_task(
                    &task.task_id,
                    "scheduled household delivery was canceled before execution",
                )
                .await?;
            let completed = self
                .db
                .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                .await?
                .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
            return Ok(AgentResult { task: completed });
        }
        if let Some(window_ends_at) = delivery.window_ends_at.as_deref() {
            let now = DateTime::parse_from_rfc3339(&now_rfc3339())?.with_timezone(&Utc);
            let window_end = DateTime::parse_from_rfc3339(window_ends_at)?.with_timezone(&Utc);
            if now > window_end {
                let reason = format!(
                    "scheduled household delivery window expired at {}",
                    window_ends_at
                );
                self.db
                    .mark_household_delivery_failed(&delivery.delivery_id, &reason)
                    .await?;
                self.db.fail_task(&task.task_id, &reason).await?;
                let failed = self
                    .db
                    .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
                return Ok(AgentResult { task: failed });
            }
        }

        let requester = match self.db.fetch_user(&delivery.requester_user_id).await? {
            Some(user) => user,
            None => {
                let reason = format!(
                    "requester '{}' no longer exists for scheduled household delivery",
                    delivery.requester_user_id
                );
                self.db
                    .mark_household_delivery_failed(&delivery.delivery_id, &reason)
                    .await?;
                self.db.fail_task(&task.task_id, &reason).await?;
                let failed = self
                    .db
                    .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
                return Ok(AgentResult { task: failed });
            }
        };
        let requester_roles = self
            .db
            .list_user_roles(&requester.user_id)
            .await?
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        if !can_send_household_direct_delivery(&requester_roles) {
            let reason = format!(
                "requester '{}' is no longer allowed to send household delivery",
                requester.user_id
            );
            self.db
                .mark_household_delivery_failed(&delivery.delivery_id, &reason)
                .await?;
            self.db.fail_task(&task.task_id, &reason).await?;
            let failed = self
                .db
                .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                .await?
                .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
            return Ok(AgentResult { task: failed });
        }

        let recipient = match self.db.fetch_user(&delivery.recipient_user_id).await? {
            Some(user) => user,
            None => {
                let reason = format!(
                    "recipient '{}' no longer exists for household delivery",
                    delivery.recipient_user_id
                );
                self.db
                    .mark_household_delivery_failed(&delivery.delivery_id, &reason)
                    .await?;
                self.db.fail_task(&task.task_id, &reason).await?;
                let failed = self
                    .db
                    .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
                return Ok(AgentResult { task: failed });
            }
        };
        let delegate = RuntimeHouseholdDeliveryDelegate {
            db: self.db.clone(),
            runtime_actor_id: self.config.runtime.instance_id.clone(),
            discord_bot_token: self.discord_bot_token.clone(),
        };
        let identity = match delegate
            .select_household_delivery_identity(&recipient.user_id)
            .await
        {
            Ok(identity) => identity,
            Err(error) => {
                let reason = match error {
                    beaverki_tools::ToolError::Denied { message, .. } => message,
                    beaverki_tools::ToolError::Failed(error) => error.to_string(),
                };
                self.db
                    .mark_household_delivery_failed(&delivery.delivery_id, &reason)
                    .await?;
                self.db.fail_task(&task.task_id, &reason).await?;
                let failed = self
                    .db
                    .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| anyhow!("scheduled task '{}' disappeared", task.task_id))?;
                return Ok(AgentResult { task: failed });
            }
        };

        match delegate
            .send_household_delivery(&delivery, &identity, &requester)
            .await
        {
            Ok(sent) => {
                self.db
                    .mark_household_delivery_sent(
                        &delivery.delivery_id,
                        &identity.connector_type,
                        &identity.identity_id,
                        &sent.target_ref,
                        Some(&sent.message_id),
                    )
                    .await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "household_delivery_sent",
                        "runtime",
                        &self.config.runtime.instance_id,
                        json!({
                            "delivery_id": delivery.delivery_id,
                            "requester_user_id": requester.user_id,
                            "requester_identity_id": delivery.requester_identity_id,
                            "recipient_user_id": recipient.user_id,
                            "recipient_display_name": recipient.display_name,
                            "connector_type": identity.connector_type,
                            "connector_identity_id": identity.identity_id,
                            "target_kind": sent.target_kind,
                            "target_ref": sent.target_ref,
                            "schedule_id": delivery.schedule_id,
                        }),
                    )
                    .await?;
                self.db
                    .record_audit_event(
                        "runtime",
                        &self.config.runtime.instance_id,
                        "household_delivery_sent",
                        json!({
                            "delivery_id": delivery.delivery_id,
                            "task_id": task.task_id,
                            "requester_user_id": requester.user_id,
                            "requester_identity_id": delivery.requester_identity_id,
                            "requester_display_name": requester.display_name,
                            "recipient_user_id": recipient.user_id,
                            "recipient_display_name": recipient.display_name,
                            "connector_type": identity.connector_type,
                            "connector_identity_id": identity.identity_id,
                            "target_kind": sent.target_kind,
                            "target_ref": sent.target_ref,
                            "schedule_id": delivery.schedule_id,
                        }),
                    )
                    .await?;
                self.db
                    .complete_task(
                        &task.task_id,
                        &format!(
                            "Delivered scheduled household message to {}.",
                            recipient.display_name
                        ),
                    )
                    .await?;
            }
            Err(error) => {
                let reason = error.to_string();
                self.db
                    .mark_household_delivery_failed(&delivery.delivery_id, &reason)
                    .await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "household_delivery_failed",
                        "runtime",
                        &self.config.runtime.instance_id,
                        json!({
                            "delivery_id": delivery.delivery_id,
                            "requester_user_id": requester.user_id,
                            "recipient_user_id": recipient.user_id,
                            "connector_type": identity.connector_type,
                            "connector_identity_id": identity.identity_id,
                            "schedule_id": delivery.schedule_id,
                            "error": reason,
                        }),
                    )
                    .await?;
                self.db
                    .record_audit_event(
                        "runtime",
                        &self.config.runtime.instance_id,
                        "household_delivery_failed",
                        json!({
                            "delivery_id": delivery.delivery_id,
                            "task_id": task.task_id,
                            "requester_user_id": requester.user_id,
                            "recipient_user_id": recipient.user_id,
                            "connector_type": identity.connector_type,
                            "connector_identity_id": identity.identity_id,
                            "schedule_id": delivery.schedule_id,
                            "error": reason,
                        }),
                    )
                    .await?;
                self.db.fail_task(&task.task_id, &reason).await?;
            }
        }

        let task = self
            .db
            .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "scheduled task '{}' disappeared after execution",
                    task.task_id
                )
            })?;
        Ok(AgentResult { task })
    }

    async fn execute_workflow_run_task(&self, task: TaskRow) -> Result<AgentResult> {
        self.db.clear_task_result(&task.task_id).await?;
        self.db
            .update_task_state(&task.task_id, TaskState::Running)
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "workflow_run_started",
                "runtime",
                &self.config.runtime.instance_id,
                json!({ "objective": task.objective }),
            )
            .await?;

        let context: Value = task
            .context_summary
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("failed to parse workflow task context")?
            .unwrap_or_else(|| json!({}));
        let workflow_run_id = context
            .get("workflow_run_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!(
                    "workflow task '{}' is missing workflow_run_id context",
                    task.task_id
                )
            })?;
        let mut run = self
            .db
            .fetch_workflow_run(workflow_run_id)
            .await?
            .ok_or_else(|| anyhow!("workflow run '{}' not found", workflow_run_id))?;
        let workflow = self
            .db
            .fetch_workflow_definition(&run.workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{}' not found", run.workflow_id))?;
        let stages = self.db.list_workflow_stages(&workflow.workflow_id).await?;
        let mut artifacts = parse_json_field(&run.artifacts_json, "workflow run artifacts")?;

        loop {
            let stage_index = run.current_stage_index as usize;
            if stage_index >= stages.len() {
                let result_text = workflow_run_result_text(&artifacts);
                self.db
                    .update_workflow_run(
                        &run.workflow_run_id,
                        "completed",
                        run.current_stage_index,
                        &artifacts,
                        None,
                        None,
                        None,
                        run.retry_count,
                        true,
                    )
                    .await?;
                self.db.complete_task(&task.task_id, &result_text).await?;
                self.db
                    .append_task_event(
                        &task.task_id,
                        "workflow_run_completed",
                        "runtime",
                        &self.config.runtime.instance_id,
                        json!({
                            "workflow_run_id": run.workflow_run_id,
                            "workflow_id": workflow.workflow_id,
                            "stage_count": stages.len(),
                        }),
                    )
                    .await?;
                self.db
                    .record_audit_event(
                        "runtime",
                        &self.config.runtime.instance_id,
                        "workflow_run_completed",
                        json!({
                            "workflow_run_id": run.workflow_run_id,
                            "workflow_id": workflow.workflow_id,
                            "task_id": task.task_id,
                        }),
                    )
                    .await?;
                let task = self
                    .db
                    .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "workflow task '{}' disappeared after completion",
                            task.task_id
                        )
                    })?;
                return Ok(AgentResult { task });
            }

            let stage = &stages[stage_index];
            let stage_config = parse_json_field(&stage.stage_config_json, "workflow stage config")?;
            self.db
                .append_task_event(
                    &task.task_id,
                    "workflow_stage_started",
                    "runtime",
                    &self.config.runtime.instance_id,
                    json!({
                        "workflow_run_id": run.workflow_run_id,
                        "stage_id": stage.stage_id,
                        "stage_index": stage.stage_index,
                        "stage_kind": stage.stage_kind,
                    }),
                )
                .await?;

            match stage.stage_kind.as_str() {
                "lua_script" => {
                    let Some(script_id) = stage.artifact_ref.as_deref() else {
                        return self
                            .fail_workflow_run(
                                &task,
                                &run,
                                &artifacts,
                                "workflow stage is missing a script reference",
                            )
                            .await;
                    };
                    let Some(script) = self.db.fetch_script(script_id).await? else {
                        return self
                            .fail_workflow_run(
                                &task,
                                &run,
                                &artifacts,
                                &format!("workflow script '{}' not found", script_id),
                            )
                            .await;
                    };
                    let capability_profile = parse_json_field(
                        &script.capability_profile_json,
                        "script capability profile",
                    )?;
                    let input_json = json!({
                        "workflow_run_id": run.workflow_run_id,
                        "workflow_id": workflow.workflow_id,
                        "stage_id": stage.stage_id,
                        "stage_index": stage.stage_index,
                        "last_result": artifacts.get("last_result").cloned().unwrap_or(Value::Null),
                        "artifacts": artifacts,
                        "config": stage_config,
                    });
                    match automation::execute_lua_script(automation::LuaExecutionInput {
                        db: self.db.clone(),
                        owner_user_id: task.owner_user_id.clone(),
                        task_id: task.task_id.clone(),
                        script_id: script.script_id.clone(),
                        source_text: script.source_text.clone(),
                        input_json: Some(input_json),
                        capability_profile,
                        working_dir: self.config.runtime.workspace_root.clone(),
                        allowed_roots: self.default_allowed_roots(),
                        browser_interactive_launcher: self
                            .config
                            .integrations
                            .browser
                            .interactive_launcher
                            .clone(),
                        browser_headless_program: self
                            .config
                            .integrations
                            .browser
                            .headless_browser
                            .clone(),
                        browser_headless_args: self
                            .config
                            .integrations
                            .browser
                            .headless_args
                            .clone(),
                    })
                    .await
                    {
                        Ok(execution) => {
                            self.record_workflow_lua_side_effects(
                                &task,
                                &execution.logs,
                                &execution.notifications,
                            )
                            .await?;
                            if let Some(wake_at) = execution.deferred_until.as_deref() {
                                return self
                                    .defer_workflow_run(
                                        &task,
                                        &run,
                                        &artifacts,
                                        wake_at,
                                        &format!(
                                            "Workflow deferred by Lua stage {} until {}",
                                            stage.stage_index, wake_at
                                        ),
                                    )
                                    .await;
                            }
                            apply_stage_result(
                                &mut artifacts,
                                stage,
                                execution.result_json,
                                json!({ "result_text": execution.result_text }),
                            );
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            return self
                                .fail_workflow_run(&task, &run, &artifacts, &reason)
                                .await;
                        }
                    }
                }
                "lua_tool" => {
                    let Some(tool_id) = stage.artifact_ref.as_deref() else {
                        return self
                            .fail_workflow_run(
                                &task,
                                &run,
                                &artifacts,
                                "workflow stage is missing a Lua tool reference",
                            )
                            .await;
                    };
                    let Some(tool) = self
                        .db
                        .fetch_lua_tool_for_owner(&task.owner_user_id, tool_id)
                        .await?
                    else {
                        return self
                            .fail_workflow_run(
                                &task,
                                &run,
                                &artifacts,
                                &format!("workflow Lua tool '{}' not found", tool_id),
                            )
                            .await;
                    };
                    let input_schema =
                        parse_json_field(&tool.input_schema_json, "Lua tool input schema")?;
                    let output_schema =
                        parse_json_field(&tool.output_schema_json, "Lua tool output schema")?;
                    let capability_profile = parse_json_field(
                        &tool.capability_profile_json,
                        "Lua tool capability profile",
                    )?;
                    let input_json = json!({
                        "workflow_run_id": run.workflow_run_id,
                        "workflow_id": workflow.workflow_id,
                        "stage_id": stage.stage_id,
                        "stage_index": stage.stage_index,
                        "last_result": artifacts.get("last_result").cloned().unwrap_or(Value::Null),
                        "artifacts": artifacts,
                        "config": stage_config,
                    });
                    if let Err(error) =
                        validate_json_value_against_schema(&input_json, &input_schema)
                    {
                        return self
                            .fail_workflow_run(
                                &task,
                                &run,
                                &artifacts,
                                &format!(
                                    "workflow Lua tool '{}' rejected input: {}",
                                    tool_id, error
                                ),
                            )
                            .await;
                    }
                    match automation::execute_lua_script(automation::LuaExecutionInput {
                        db: self.db.clone(),
                        owner_user_id: task.owner_user_id.clone(),
                        task_id: task.task_id.clone(),
                        script_id: tool.tool_id.clone(),
                        source_text: tool.source_text.clone(),
                        input_json: Some(input_json),
                        capability_profile,
                        working_dir: self.config.runtime.workspace_root.clone(),
                        allowed_roots: self.default_allowed_roots(),
                        browser_interactive_launcher: self
                            .config
                            .integrations
                            .browser
                            .interactive_launcher
                            .clone(),
                        browser_headless_program: self
                            .config
                            .integrations
                            .browser
                            .headless_browser
                            .clone(),
                        browser_headless_args: self
                            .config
                            .integrations
                            .browser
                            .headless_args
                            .clone(),
                    })
                    .await
                    {
                        Ok(execution) => {
                            if execution.deferred_until.is_some() {
                                return self
                                    .fail_workflow_run(
                                        &task,
                                        &run,
                                        &artifacts,
                                        &format!(
                                            "workflow Lua tool '{}' attempted to defer execution",
                                            tool_id
                                        ),
                                    )
                                    .await;
                            }
                            if let Err(error) = validate_json_value_against_schema(
                                &execution.result_json,
                                &output_schema,
                            ) {
                                return self
                                    .fail_workflow_run(
                                        &task,
                                        &run,
                                        &artifacts,
                                        &format!(
                                            "workflow Lua tool '{}' returned invalid output: {}",
                                            tool_id, error
                                        ),
                                    )
                                    .await;
                            }
                            self.record_workflow_lua_side_effects(
                                &task,
                                &execution.logs,
                                &execution.notifications,
                            )
                            .await?;
                            apply_stage_result(
                                &mut artifacts,
                                stage,
                                execution.result_json,
                                json!({ "result_text": execution.result_text }),
                            );
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            return self
                                .fail_workflow_run(&task, &run, &artifacts, &reason)
                                .await;
                        }
                    }
                }
                "agent_task" => {
                    let objective = stage_config
                        .get("prompt")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| anyhow!("agent_task stage requires config.prompt"))?;
                    let child_context = json!({
                        "workflow_run_id": run.workflow_run_id,
                        "workflow_id": workflow.workflow_id,
                        "stage_id": stage.stage_id,
                        "stage_index": stage.stage_index,
                        "last_result": artifacts.get("last_result").cloned().unwrap_or(Value::Null),
                        "artifacts": artifacts,
                        "contract": stage_config,
                    })
                    .to_string();
                    let child_task = self
                        .db
                        .create_task_with_params(NewTask {
                            owner_user_id: &task.owner_user_id,
                            initiating_identity_id: &run.initiating_identity_id,
                            primary_agent_id: &task.primary_agent_id,
                            assigned_agent_id: &task.assigned_agent_id,
                            parent_task_id: Some(&task.task_id),
                            session_id: task.session_id.as_deref(),
                            kind: WORKFLOW_AGENT_STAGE_TASK_KIND,
                            objective,
                            context_summary: Some(&child_context),
                            scope: MemoryScope::Private,
                            wake_at: None,
                        })
                        .await?;
                    let request = AgentRequest {
                        owner_user_id: task.owner_user_id.clone(),
                        initiating_identity_id: run.initiating_identity_id.clone(),
                        primary_agent_id: task.primary_agent_id.clone(),
                        assigned_agent_id: task.assigned_agent_id.clone(),
                        role_ids: self.user_role_ids(&task.owner_user_id).await?,
                        objective: objective.to_owned(),
                        scope: MemoryScope::Private,
                        kind: WORKFLOW_AGENT_STAGE_TASK_KIND.to_owned(),
                        parent_task_id: Some(task.task_id.clone()),
                        task_context: Some(child_context),
                        visible_scopes: Vec::new(),
                        memory_mode: AgentMemoryMode::TaskSliceOnly,
                        approved_shell_commands: Vec::new(),
                        approved_automation_actions: Vec::new(),
                    };
                    let child_result = self.runner.resume_task(child_task.clone(), request).await?;
                    if child_result.task.state != TaskState::Completed.as_str() {
                        return self
                            .fail_workflow_run(
                                &task,
                                &run,
                                &artifacts,
                                &format!(
                                    "workflow agent stage {} left the approved envelope with task state {}",
                                    stage.stage_index, child_result.task.state
                                ),
                            )
                            .await;
                    }
                    apply_stage_result(
                        &mut artifacts,
                        stage,
                        json!({
                            "child_task_id": child_result.task.task_id,
                            "result_text": child_result.task.result_text,
                        }),
                        json!({ "child_task_id": child_result.task.task_id }),
                    );
                }
                "user_notify" => {
                    let message = workflow_notify_message(&stage_config, &artifacts);
                    let recipient = self
                        .resolve_workflow_notify_recipient(
                            &task.owner_user_id,
                            stage_config.get("recipient").and_then(Value::as_str),
                        )
                        .await?;
                    let delegate = RuntimeHouseholdDeliveryDelegate {
                        db: self.db.clone(),
                        runtime_actor_id: self.config.runtime.instance_id.clone(),
                        discord_bot_token: self.discord_bot_token.clone(),
                    };
                    let requester =
                        self.db
                            .fetch_user(&task.owner_user_id)
                            .await?
                            .ok_or_else(|| {
                                anyhow!(
                                    "workflow owner '{}' no longer exists for notification",
                                    task.owner_user_id
                                )
                            })?;
                    let identity = delegate
                        .select_household_delivery_identity(&recipient.user_id)
                        .await
                        .map_err(tool_error_to_anyhow)?;
                    let dedupe_key = format!(
                        "workflow_notify:{}:{}:{}",
                        run.workflow_run_id, stage.stage_index, task.task_id
                    );
                    let delivery = self
                        .db
                        .create_or_reuse_household_delivery(NewHouseholdDelivery {
                            task_id: &task.task_id,
                            requester_user_id: &task.owner_user_id,
                            requester_identity_id: &run.initiating_identity_id,
                            recipient_user_id: &recipient.user_id,
                            message_text: &message,
                            delivery_mode: "workflow_notify",
                            connector_type: &identity.connector_type,
                            connector_identity_id: &identity.identity_id,
                            target_ref: &identity.external_user_id,
                            fallback_target_ref: identity.external_channel_id.as_deref(),
                            dedupe_key: &dedupe_key,
                            initial_status: "dispatching",
                            parent_delivery_id: None,
                            schedule_id: run.schedule_id.as_deref(),
                            scheduled_for_at: None,
                            window_starts_at: None,
                            window_ends_at: None,
                            recurrence_rule: None,
                            scheduled_job_state: Some("workflow_notify"),
                            materialized_task_id: None,
                        })
                        .await?;
                    info!(
                        workflow_id = %workflow.workflow_id,
                        workflow_run_id = %run.workflow_run_id,
                        task_id = %task.task_id,
                        stage_index = stage.stage_index,
                        recipient_user_id = %recipient.user_id,
                        connector_type = %identity.connector_type,
                        "dispatching workflow notification",
                    );
                    let sent = delegate
                        .send_household_delivery(&delivery, &identity, &requester)
                        .await?;
                    self.db
                        .mark_household_delivery_sent(
                            &delivery.delivery_id,
                            &identity.connector_type,
                            &identity.identity_id,
                            &sent.target_ref,
                            Some(&sent.message_id),
                        )
                        .await?;
                    self.db
                        .append_task_event(
                            &task.task_id,
                            "household_delivery_sent",
                            "runtime",
                            &self.config.runtime.instance_id,
                            json!({
                                "delivery_id": delivery.delivery_id,
                                "requester_user_id": requester.user_id,
                                "requester_identity_id": run.initiating_identity_id,
                                "recipient_user_id": recipient.user_id,
                                "recipient_display_name": recipient.display_name,
                                "connector_type": identity.connector_type,
                                "connector_identity_id": identity.identity_id,
                                "target_kind": sent.target_kind,
                                "target_ref": sent.target_ref,
                                "workflow_run_id": run.workflow_run_id,
                            }),
                        )
                        .await?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.config.runtime.instance_id,
                            "household_delivery_sent",
                            json!({
                                "delivery_id": delivery.delivery_id,
                                "task_id": task.task_id,
                                "workflow_id": workflow.workflow_id,
                                "workflow_run_id": run.workflow_run_id,
                                "requester_user_id": requester.user_id,
                                "requester_identity_id": run.initiating_identity_id,
                                "requester_display_name": requester.display_name,
                                "recipient_user_id": recipient.user_id,
                                "recipient_display_name": recipient.display_name,
                                "connector_type": identity.connector_type,
                                "connector_identity_id": identity.identity_id,
                                "target_kind": sent.target_kind,
                                "target_ref": sent.target_ref,
                            }),
                        )
                        .await?;
                    apply_stage_result(
                        &mut artifacts,
                        stage,
                        json!(message),
                        json!({ "delivery_id": delivery.delivery_id }),
                    );
                }
                other => {
                    return self
                        .fail_workflow_run(
                            &task,
                            &run,
                            &artifacts,
                            &format!("unsupported workflow stage kind '{other}'"),
                        )
                        .await;
                }
            }

            run.current_stage_index += 1;
            self.db
                .update_workflow_run(
                    &run.workflow_run_id,
                    "running",
                    run.current_stage_index,
                    &artifacts,
                    None,
                    None,
                    None,
                    run.retry_count,
                    false,
                )
                .await?;
            self.db
                .append_task_event(
                    &task.task_id,
                    "workflow_stage_completed",
                    "runtime",
                    &self.config.runtime.instance_id,
                    json!({
                        "workflow_run_id": run.workflow_run_id,
                        "stage_id": stage.stage_id,
                        "stage_index": stage.stage_index,
                        "stage_kind": stage.stage_kind,
                    }),
                )
                .await?;
        }
    }

    async fn execute_lua_task(&self, task: TaskRow) -> Result<AgentResult> {
        self.db.clear_task_result(&task.task_id).await?;
        self.db
            .update_task_state(&task.task_id, TaskState::Running)
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "lua_task_started",
                "runtime",
                &self.config.runtime.instance_id,
                json!({ "objective": task.objective }),
            )
            .await?;

        let context: Value = task
            .context_summary
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .context("failed to parse Lua task context")?
            .unwrap_or_else(|| json!({}));
        let script_id = context
            .get("script_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Lua task '{}' is missing script_id context", task.task_id))?;
        let script = self
            .db
            .fetch_script(script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{}' not found", script_id))?;
        let capability_profile: Value = serde_json::from_str(&script.capability_profile_json)
            .context("failed to parse script capability profile")?;
        let execution = automation::execute_lua_script(automation::LuaExecutionInput {
            db: self.db.clone(),
            owner_user_id: task.owner_user_id.clone(),
            task_id: task.task_id.clone(),
            script_id: script.script_id.clone(),
            source_text: script.source_text.clone(),
            input_json: None,
            capability_profile,
            working_dir: self.config.runtime.workspace_root.clone(),
            allowed_roots: self.default_allowed_roots(),
            browser_interactive_launcher: self
                .config
                .integrations
                .browser
                .interactive_launcher
                .clone(),
            browser_headless_program: self.config.integrations.browser.headless_browser.clone(),
            browser_headless_args: self.config.integrations.browser.headless_args.clone(),
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
                        .await?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.config.runtime.instance_id,
                            "lua_script_tool_denied",
                            json!({
                                "task_id": task.task_id,
                                "script_id": script.script_id,
                                "tool_name": policy_error.tool_name,
                                "detail": policy_error.detail,
                                "message": policy_error.message,
                            }),
                        )
                        .await?;
                }
                return Err(error);
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
                .await?;
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
                .await?;
        }
        if let Some(wake_at) = &execution.deferred_until {
            self.db
                .create_task_with_params(NewTask {
                    owner_user_id: &task.owner_user_id,
                    initiating_identity_id: &task.initiating_identity_id,
                    primary_agent_id: &task.primary_agent_id,
                    assigned_agent_id: &task.assigned_agent_id,
                    parent_task_id: Some(&task.task_id),
                    session_id: task.session_id.as_deref(),
                    kind: "lua_script",
                    objective: &task.objective,
                    context_summary: task.context_summary.as_deref(),
                    scope: task.scope.parse::<MemoryScope>()?,
                    wake_at: Some(wake_at),
                })
                .await?;
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_task_deferred",
                    "tool",
                    "lua",
                    json!({ "wake_at": wake_at }),
                )
                .await?;
        }

        self.db
            .complete_task(&task.task_id, &execution.result_text)
            .await?;
        self.db
            .append_task_event(
                &task.task_id,
                "lua_task_completed",
                "runtime",
                &self.config.runtime.instance_id,
                json!({
                    "script_id": script.script_id,
                    "schedule_id": context.get("schedule_id").and_then(Value::as_str),
                }),
            )
            .await?;
        self.db
            .record_audit_event(
                "runtime",
                &self.config.runtime.instance_id,
                "lua_script_executed",
                json!({
                    "task_id": task.task_id,
                    "script_id": script.script_id,
                    "schedule_id": context.get("schedule_id").and_then(Value::as_str),
                }),
            )
            .await?;
        let completed = self
            .db
            .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
            .await?
            .ok_or_else(|| anyhow!("Lua task '{}' disappeared after completion", task.task_id))?;
        Ok(AgentResult { task: completed })
    }

    pub async fn create_user(
        &self,
        display_name: &str,
        roles: &[String],
    ) -> Result<BootstrapState> {
        self.db.create_user(display_name, roles).await
    }

    pub async fn list_users(&self) -> Result<Vec<(UserRow, Vec<UserRoleRow>)>> {
        let users = self.db.list_users().await?;
        let mut result = Vec::with_capacity(users.len());
        for user in users {
            let roles = self.db.list_user_roles(&user.user_id).await?;
            result.push((user, roles));
        }
        Ok(result)
    }

    pub async fn list_roles(&self) -> Result<Vec<RoleRow>> {
        self.db.list_roles().await
    }

    pub async fn list_approvals(
        &self,
        user_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<ApprovalRow>> {
        let user = self.resolve_user(user_id).await?;
        self.db.list_approvals_for_user(&user.user_id, status).await
    }

    pub async fn resolve_approval(
        &self,
        approver_user_id: Option<&str>,
        approval_id: &str,
        approve: bool,
    ) -> Result<TaskRow> {
        let approver = self.resolve_user(approver_user_id).await?;
        let approver_roles = self.user_role_ids(&approver.user_id).await?;
        if !can_grant_approvals(&approver_roles) {
            bail!(
                "user '{}' is not allowed to grant approvals",
                approver.user_id
            );
        }

        let approval = self
            .db
            .fetch_approval_for_user(&approver.user_id, approval_id)
            .await?
            .ok_or_else(|| anyhow!("approval '{approval_id}' not found"))?;
        if approval.status != "pending" {
            bail!(
                "approval '{}' is already {}",
                approval.approval_id,
                approval.status
            );
        }

        self.db
            .resolve_approval(
                &approval.approval_id,
                if approve { "approved" } else { "denied" },
            )
            .await?;

        if !approve {
            let task = self
                .db
                .fetch_task_for_owner(&approval.requested_from_user_id, &approval.task_id)
                .await?
                .ok_or_else(|| {
                    anyhow!("task '{}' not found for denied approval", approval.task_id)
                })?;
            self.db
                .fail_task(&task.task_id, "Task denied by approval decision.")
                .await?;
            return self
                .db
                .fetch_task_for_owner(&approval.requested_from_user_id, &approval.task_id)
                .await?
                .ok_or_else(|| anyhow!("task disappeared after denial"));
        }

        let task = self
            .db
            .fetch_task_for_owner(&approval.requested_from_user_id, &approval.task_id)
            .await?
            .ok_or_else(|| anyhow!("task '{}' not found for approved task", approval.task_id))?;
        let approved_shell_commands = self
            .db
            .approved_shell_commands_for_task(&task.task_id, &approval.requested_from_user_id)
            .await?;
        let request = self
            .build_request_from_task(
                &approval.requested_from_user_id,
                &task,
                approved_shell_commands,
            )
            .await?;
        let result = self.runner.resume_task(task, request).await?;
        Ok(result.task)
    }

    pub async fn issue_approval_actions(
        &self,
        user_id: Option<&str>,
        approval_id: &str,
        connector_type: Option<&str>,
        connector_identity_id: Option<&str>,
        channel: Option<&str>,
        ttl_secs: i64,
        include_confirm: bool,
    ) -> Result<ApprovalActionSet> {
        let user = self.resolve_user(user_id).await?;
        let approval = self
            .db
            .fetch_approval_for_user(&user.user_id, approval_id)
            .await?
            .ok_or_else(|| anyhow!("approval '{approval_id}' not found"))?;
        if approval.status != "pending" {
            bail!(
                "approval '{}' is already {}",
                approval.approval_id,
                approval.status
            );
        }

        self.db
            .issue_approval_action_set(
                &approval.approval_id,
                IssueApprovalActions {
                    connector_type,
                    connector_identity_id,
                    channel,
                    ttl_secs,
                    include_confirm,
                },
            )
            .await
    }

    pub async fn resolve_approval_action(
        &self,
        approver_user_id: Option<&str>,
        action_token: &str,
        requested_action_kind: &str,
        connector_type: Option<&str>,
        connector_identity_id: Option<&str>,
        channel_id: Option<&str>,
        confirmation_ttl_secs: i64,
    ) -> Result<RemoteApprovalActionOutcome> {
        let approver = self.resolve_user(approver_user_id).await?;
        let approver_roles = self.user_role_ids(&approver.user_id).await?;
        if !can_grant_approvals(&approver_roles) {
            bail!(
                "user '{}' is not allowed to grant approvals",
                approver.user_id
            );
        }

        let action = self
            .db
            .fetch_approval_action_by_token(action_token)
            .await?
            .ok_or_else(|| anyhow!("approval action token not found"))?;
        if action.status != "pending" {
            bail!(
                "approval action '{}' is already {}",
                action.action_id,
                action.status
            );
        }
        if action.action_kind != requested_action_kind {
            bail!(
                "approval action token is for '{}' not '{}'",
                action.action_kind,
                requested_action_kind
            );
        }
        if let Some(expected_connector_type) = action.issued_to_connector_type.as_deref()
            && Some(expected_connector_type) != connector_type
        {
            bail!("approval action token is not valid for this connector");
        }
        if let Some(expected_identity_id) = action.issued_to_connector_identity_id.as_deref()
            && Some(expected_identity_id) != connector_identity_id
        {
            bail!("approval action token is not valid for this mapped identity");
        }
        if let Some(expected_channel_id) = action.issued_to_channel.as_deref()
            && Some(expected_channel_id) != channel_id
        {
            bail!("approval action token is not valid in this channel");
        }
        if approval_action_is_expired(&action) {
            self.db.expire_approval_action(&action.action_id).await?;
            bail!("approval action token has expired");
        }

        let approval = self
            .db
            .fetch_approval_for_user(&approver.user_id, &action.approval_id)
            .await?
            .ok_or_else(|| anyhow!("approval '{}' not found", action.approval_id))?;
        if approval.status != "pending" {
            bail!(
                "approval '{}' is already {}",
                approval.approval_id,
                approval.status
            );
        }

        let consumed = self
            .db
            .consume_approval_action(action_token)
            .await?
            .ok_or_else(|| anyhow!("approval action token is expired or already used"))?;

        match consumed.action_kind.as_str() {
            "inspect" => Ok(RemoteApprovalActionOutcome::Inspection {
                action: consumed,
                approval,
            }),
            "deny" => {
                let task = self
                    .resolve_approval(Some(&approver.user_id), &approval.approval_id, false)
                    .await?;
                Ok(RemoteApprovalActionOutcome::Resolved {
                    action: consumed,
                    task,
                })
            }
            "approve" => {
                if approval.risk_level.as_deref() == Some("critical") {
                    self.db
                        .supersede_pending_approval_actions(&approval.approval_id)
                        .await?;
                    let confirm_action = self
                        .db
                        .issue_approval_action(
                            &approval.approval_id,
                            "confirm",
                            connector_type,
                            connector_identity_id,
                            channel_id,
                            confirmation_ttl_secs,
                        )
                        .await?;
                    return Ok(RemoteApprovalActionOutcome::StepUpRequired {
                        action: consumed,
                        approval,
                        confirm_action,
                    });
                }
                let task = self
                    .resolve_approval(Some(&approver.user_id), &approval.approval_id, true)
                    .await?;
                Ok(RemoteApprovalActionOutcome::Resolved {
                    action: consumed,
                    task,
                })
            }
            "confirm" => {
                let task = self
                    .resolve_approval(Some(&approver.user_id), &approval.approval_id, true)
                    .await?;
                Ok(RemoteApprovalActionOutcome::Resolved {
                    action: consumed,
                    task,
                })
            }
            other => bail!("unsupported approval action kind '{other}'"),
        }
    }

    pub async fn upsert_connector_identity(
        &self,
        connector_type: &str,
        external_user_id: &str,
        external_channel_id: Option<&str>,
        mapped_user_id: &str,
        trust_level: &str,
    ) -> Result<ConnectorIdentityRow> {
        self.db
            .upsert_connector_identity(
                connector_type,
                external_user_id,
                external_channel_id,
                mapped_user_id,
                trust_level,
            )
            .await
    }

    pub async fn fetch_connector_identity(
        &self,
        connector_type: &str,
        external_user_id: &str,
    ) -> Result<Option<ConnectorIdentityRow>> {
        self.db
            .fetch_connector_identity(connector_type, external_user_id)
            .await
    }

    pub async fn list_connector_identities(
        &self,
        connector_type: Option<&str>,
    ) -> Result<Vec<ConnectorIdentityRow>> {
        self.db.list_connector_identities(connector_type).await
    }

    pub fn default_user(&self) -> &UserRow {
        &self.default_user
    }

    async fn resolve_user(&self, user_id: Option<&str>) -> Result<UserRow> {
        match user_id {
            Some(user_id) => self
                .db
                .fetch_user(user_id)
                .await?
                .ok_or_else(|| anyhow!("user '{user_id}' not found")),
            None => Ok(self.default_user.clone()),
        }
    }

    fn match_session_lifecycle_policy<'a>(
        &'a self,
        session: &ConversationSessionRow,
    ) -> Option<&'a SessionLifecyclePolicy> {
        select_session_lifecycle_policy(
            &self.config.runtime.session_management.policies,
            &SessionPolicyMatchInput {
                session_kind: &session.session_kind,
                connector_type: session.originating_connector_type.as_deref(),
                connector_target: session.originating_connector_target.as_deref(),
                audience_policy: &session.audience_policy,
                max_memory_scope: &session.max_memory_scope,
            },
        )
    }

    async fn resolve_cli_conversation_session(
        &self,
        user: &UserRow,
    ) -> Result<ConversationSessionRow> {
        let session_key = format!("{SESSION_KIND_CLI}:{}", user.user_id);
        self.db
            .ensure_conversation_session(NewConversationSession {
                session_kind: SESSION_KIND_CLI,
                session_key: &session_key,
                audience_policy: SESSION_AUDIENCE_DIRECT_USER,
                max_memory_scope: MemoryScope::Household,
                originating_connector_type: None,
                originating_connector_target: None,
            })
            .await
    }

    pub(crate) async fn resolve_connector_conversation_session(
        &self,
        mapped_user_id: &str,
        connector_type: &str,
        channel_id: &str,
        is_direct_message: bool,
    ) -> Result<ConversationSessionRow> {
        let (session_kind, session_key, audience_policy, max_memory_scope) = if is_direct_message {
            (
                SESSION_KIND_DIRECT_MESSAGE,
                format!("{SESSION_KIND_DIRECT_MESSAGE}:{connector_type}:{mapped_user_id}"),
                SESSION_AUDIENCE_DIRECT_USER,
                MemoryScope::Private,
            )
        } else {
            let (audience_policy, max_memory_scope) =
                connector_group_room_policy(&self.config, connector_type, channel_id);
            (
                SESSION_KIND_GROUP_ROOM,
                format!("{SESSION_KIND_GROUP_ROOM}:{connector_type}:{channel_id}"),
                audience_policy,
                max_memory_scope,
            )
        };
        let session = self
            .db
            .ensure_conversation_session(NewConversationSession {
                session_kind,
                session_key: &session_key,
                audience_policy,
                max_memory_scope,
                originating_connector_type: Some(connector_type),
                originating_connector_target: Some(channel_id),
            })
            .await?;

        if session.audience_policy != audience_policy
            || session.max_memory_scope != max_memory_scope.as_str()
        {
            self.db
                .update_conversation_session_policy(
                    &session.session_id,
                    audience_policy,
                    max_memory_scope,
                    Some("connector_channel_policy_sync"),
                )
                .await?;
            return self
                .db
                .fetch_conversation_session(&session.session_id)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "conversation session '{}' disappeared after policy sync",
                        session.session_id
                    )
                });
        }

        Ok(session)
    }

    async fn create_cron_run_session(&self, schedule_id: &str) -> Result<ConversationSessionRow> {
        let now = now_rfc3339();
        let session_key = format!("{SESSION_KIND_CRON_RUN}:{schedule_id}:{now}");
        self.db
            .ensure_conversation_session(NewConversationSession {
                session_kind: SESSION_KIND_CRON_RUN,
                session_key: &session_key,
                audience_policy: SESSION_AUDIENCE_SCHEDULED_RUN,
                max_memory_scope: MemoryScope::Private,
                originating_connector_type: None,
                originating_connector_target: Some(schedule_id),
            })
            .await
    }

    async fn reset_cli_conversation_session(
        &self,
        user: &UserRow,
        objective: &str,
        requested_scope: MemoryScope,
    ) -> Result<TaskRow> {
        let session = self.resolve_cli_conversation_session(user).await?;
        let effective_scope =
            cap_task_scope_to_session(requested_scope, parse_session_max_scope(&session)?);
        self.create_reset_task(
            user,
            &format!("cli:{}", user.user_id),
            objective,
            &session,
            effective_scope,
        )
        .await
    }

    pub(crate) async fn reset_connector_conversation_session(
        &self,
        user: &UserRow,
        initiating_identity_id: &str,
        objective: &str,
        session: &ConversationSessionRow,
        requested_scope: MemoryScope,
    ) -> Result<TaskRow> {
        let effective_scope =
            cap_task_scope_to_session(requested_scope, parse_session_max_scope(session)?);
        self.create_reset_task(
            user,
            initiating_identity_id,
            objective,
            session,
            effective_scope,
        )
        .await
    }

    async fn prepare_cli_task_request(
        &self,
        user: &UserRow,
        initiating_identity_id: &str,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<(AgentRequest, ConversationSessionRow)> {
        let session = self.resolve_cli_conversation_session(user).await?;
        let recent_cli_exchanges = self
            .db
            .list_recent_interactive_tasks_for_session(
                &session.session_id,
                session.last_reset_at.as_deref(),
                CLI_CONVERSATION_HISTORY_LIMIT,
            )
            .await?
            .into_iter()
            .map(|task| CliConversationExchange {
                created_at: task.created_at.clone(),
                state: task.state.clone(),
                user_text: task.objective.clone(),
                assistant_text: assistant_reply_for_context(&task),
                task_id: task.task_id.clone(),
            })
            .collect::<Vec<_>>();
        let conversation_status = recent_cli_exchanges
            .first()
            .map(|exchange| cli_conversation_status(&exchange.state, &exchange.created_at));
        let parent_task_id = if matches!(conversation_status, Some(CliConversationStatus::FollowUp))
        {
            recent_cli_exchanges
                .first()
                .map(|exchange| exchange.task_id.clone())
        } else {
            None
        };
        let task_context = Some(build_cli_task_context(&recent_cli_exchanges));
        let request = self
            .build_primary_request_for_session(
                user,
                initiating_identity_id,
                objective,
                scope,
                &session,
                parent_task_id,
                task_context,
            )
            .await?;
        Ok((request, session))
    }

    async fn build_primary_request_for_session(
        &self,
        user: &UserRow,
        initiating_identity_id: &str,
        objective: &str,
        requested_scope: MemoryScope,
        session: &ConversationSessionRow,
        parent_task_id: Option<String>,
        task_context: Option<String>,
    ) -> Result<AgentRequest> {
        let primary_agent_id = user
            .primary_agent_id
            .clone()
            .ok_or_else(|| anyhow!("user '{}' has no primary agent", user.user_id))?;
        let role_ids = self.user_role_ids(&user.user_id).await?;
        let session_max_scope = parse_session_max_scope(session)?;
        let visible_scopes =
            cap_scopes_to_session(visible_memory_scopes(&role_ids), session_max_scope);
        let scope = cap_task_scope_to_session(requested_scope, session_max_scope);
        if matches!(scope, MemoryScope::Household)
            && !visible_scopes.contains(&MemoryScope::Household)
        {
            bail!(
                "user '{}' is not allowed to create household-scoped tasks",
                user.user_id
            );
        }

        Ok(AgentRequest {
            owner_user_id: user.user_id.clone(),
            initiating_identity_id: initiating_identity_id.to_owned(),
            primary_agent_id: primary_agent_id.clone(),
            assigned_agent_id: primary_agent_id,
            role_ids: role_ids.clone(),
            objective: objective.to_owned(),
            scope,
            kind: "interactive".to_owned(),
            parent_task_id,
            task_context,
            visible_scopes,
            memory_mode: AgentMemoryMode::ScopedRetrieval,
            approved_shell_commands: Vec::new(),
            approved_automation_actions: Vec::new(),
        })
    }

    async fn build_request_from_task(
        &self,
        owner_user_id: &str,
        task: &TaskRow,
        approved_shell_commands: Vec<String>,
    ) -> Result<AgentRequest> {
        let role_ids = self.user_role_ids(owner_user_id).await?;
        let memory_mode = if task.kind == "subagent" || task.kind == WORKFLOW_AGENT_STAGE_TASK_KIND
        {
            AgentMemoryMode::TaskSliceOnly
        } else {
            AgentMemoryMode::ScopedRetrieval
        };
        let scope = task
            .scope
            .parse::<MemoryScope>()
            .map_err(|_| anyhow!("unsupported task scope '{}'", task.scope))?;
        let visible_scopes = if let Some(session_id) = task.session_id.as_deref() {
            let session = self
                .db
                .fetch_conversation_session(session_id)
                .await?
                .ok_or_else(|| anyhow!("conversation session '{}' not found", session_id))?;
            let session_max_scope = parse_session_max_scope(&session)?;
            cap_scopes_to_session(visible_memory_scopes(&role_ids), session_max_scope)
        } else {
            visible_memory_scopes(&role_ids)
        };

        let approved_automation_actions = self
            .db
            .approved_approvals_for_task(&task.task_id, owner_user_id)
            .await?
            .into_iter()
            .filter(|approval| approval.action_type != "shell_command")
            .filter_map(|approval| {
                approval
                    .target_ref
                    .map(|target_ref| beaverki_agent::ApprovedAutomationAction {
                        action_type: approval.action_type,
                        target_ref,
                    })
            })
            .collect::<Vec<_>>();

        Ok(AgentRequest {
            owner_user_id: owner_user_id.to_owned(),
            initiating_identity_id: task.initiating_identity_id.clone(),
            primary_agent_id: task.primary_agent_id.clone(),
            assigned_agent_id: task.assigned_agent_id.clone(),
            role_ids: role_ids.clone(),
            objective: task.objective.clone(),
            scope,
            kind: task.kind.clone(),
            parent_task_id: task.parent_task_id.clone(),
            task_context: task.context_summary.clone(),
            visible_scopes,
            memory_mode,
            approved_shell_commands,
            approved_automation_actions,
        })
    }

    async fn validate_workflow_stage_references(
        &self,
        workflow: &WorkflowDefinitionRow,
        stages: &[WorkflowStageRow],
    ) -> Result<()> {
        if stages.is_empty() {
            bail!(
                "workflow '{}' must contain at least one stage",
                workflow.workflow_id
            );
        }
        for stage in stages {
            match stage.stage_kind.as_str() {
                "lua_script" => {
                    let script_id = stage.artifact_ref.as_deref().ok_or_else(|| {
                        anyhow!("workflow stage '{}' is missing script_id", stage.stage_id)
                    })?;
                    let script = self
                        .db
                        .fetch_script_for_owner(&workflow.owner_user_id, script_id)
                        .await?
                        .ok_or_else(|| {
                            anyhow!("workflow stage references missing script '{}'", script_id)
                        })?;
                    if script.status != "active" || script.safety_status != "approved" {
                        bail!(
                            "workflow script '{}' must be active and approved",
                            script.script_id
                        );
                    }
                }
                "lua_tool" => {
                    let tool_id = stage.artifact_ref.as_deref().ok_or_else(|| {
                        anyhow!("workflow stage '{}' is missing tool_id", stage.stage_id)
                    })?;
                    let tool = self
                        .db
                        .fetch_lua_tool_for_owner(&workflow.owner_user_id, tool_id)
                        .await?
                        .ok_or_else(|| {
                            anyhow!("workflow stage references missing Lua tool '{}'", tool_id)
                        })?;
                    if tool.status != "active" || tool.safety_status != "approved" {
                        bail!(
                            "workflow Lua tool '{}' must be active and approved",
                            tool.tool_id
                        );
                    }
                }
                "agent_task" => {
                    let config =
                        parse_json_field(&stage.stage_config_json, "workflow stage config")?;
                    if config
                        .get("prompt")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none()
                    {
                        bail!(
                            "workflow agent stage '{}' requires config.prompt",
                            stage.stage_id
                        );
                    }
                }
                "user_notify" => {}
                other => bail!("unsupported workflow stage kind '{other}'"),
            }
        }
        Ok(())
    }

    async fn start_workflow_run(
        &self,
        workflow: &WorkflowDefinitionRow,
        initiating_identity_id: String,
        schedule_id: Option<&str>,
        primary_agent_id: &str,
        session_id: Option<&str>,
        wake_at: Option<&str>,
    ) -> Result<TaskRow> {
        let started_at = now_rfc3339();
        let run = self
            .db
            .create_workflow_run(NewWorkflowRun {
                workflow_run_id: None,
                workflow_id: &workflow.workflow_id,
                owner_user_id: &workflow.owner_user_id,
                initiating_identity_id: &initiating_identity_id,
                schedule_id,
                source_task_id: None,
                state: if wake_at.is_some() {
                    "waiting"
                } else {
                    "running"
                },
                current_stage_index: 0,
                artifacts_json: json!({
                    "workflow_id": workflow.workflow_id,
                    "workflow_name": workflow.name,
                    "stages": {},
                    "last_result": Value::Null,
                }),
                wake_at,
                block_reason: None,
                last_error: None,
                retry_count: 0,
                started_at: &started_at,
            })
            .await?;
        let task_context = json!({
            "workflow_run_id": run.workflow_run_id,
            "workflow_id": workflow.workflow_id,
            "schedule_id": schedule_id,
        })
        .to_string();
        let task = self
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &workflow.owner_user_id,
                initiating_identity_id: &initiating_identity_id,
                primary_agent_id,
                assigned_agent_id: primary_agent_id,
                parent_task_id: None,
                session_id,
                kind: WORKFLOW_RUN_TASK_KIND,
                objective: &format!("Run workflow {}", workflow.name),
                context_summary: Some(&task_context),
                scope: MemoryScope::Private,
                wake_at,
            })
            .await?;
        self.db
            .record_audit_event(
                "runtime",
                &self.config.runtime.instance_id,
                "workflow_run_created",
                json!({
                    "workflow_run_id": run.workflow_run_id,
                    "workflow_id": workflow.workflow_id,
                    "task_id": task.task_id,
                    "schedule_id": schedule_id,
                    "wake_at": wake_at,
                }),
            )
            .await?;
        Ok(task)
    }

    async fn record_workflow_lua_side_effects(
        &self,
        task: &TaskRow,
        logs: &[String],
        notifications: &[String],
    ) -> Result<()> {
        for log_line in logs {
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_log",
                    "tool",
                    "lua",
                    json!({ "message": log_line }),
                )
                .await?;
        }
        for notification in notifications {
            self.db
                .append_task_event(
                    &task.task_id,
                    "lua_notify_user",
                    "tool",
                    "lua",
                    json!({ "message": notification }),
                )
                .await?;
        }
        Ok(())
    }

    async fn defer_workflow_run(
        &self,
        task: &TaskRow,
        run: &WorkflowRunRow,
        artifacts: &Value,
        wake_at: &str,
        result_text: &str,
    ) -> Result<AgentResult> {
        self.db
            .update_workflow_run(
                &run.workflow_run_id,
                "waiting",
                run.current_stage_index,
                artifacts,
                Some(wake_at),
                None,
                None,
                run.retry_count,
                false,
            )
            .await?;
        self.db
            .create_task_with_params(NewTask {
                owner_user_id: &task.owner_user_id,
                initiating_identity_id: &task.initiating_identity_id,
                primary_agent_id: &task.primary_agent_id,
                assigned_agent_id: &task.assigned_agent_id,
                parent_task_id: Some(&task.task_id),
                session_id: task.session_id.as_deref(),
                kind: WORKFLOW_RUN_TASK_KIND,
                objective: &task.objective,
                context_summary: task.context_summary.as_deref(),
                scope: MemoryScope::Private,
                wake_at: Some(wake_at),
            })
            .await?;
        self.db.complete_task(&task.task_id, result_text).await?;
        self.db
            .append_task_event(
                &task.task_id,
                "workflow_run_deferred",
                "runtime",
                &self.config.runtime.instance_id,
                json!({
                    "workflow_run_id": run.workflow_run_id,
                    "wake_at": wake_at,
                }),
            )
            .await?;
        let task = self
            .db
            .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
            .await?
            .ok_or_else(|| anyhow!("workflow task '{}' disappeared after defer", task.task_id))?;
        Ok(AgentResult { task })
    }

    async fn fail_workflow_run(
        &self,
        task: &TaskRow,
        run: &WorkflowRunRow,
        artifacts: &Value,
        reason: &str,
    ) -> Result<AgentResult> {
        self.db
            .update_workflow_run(
                &run.workflow_run_id,
                "blocked",
                run.current_stage_index,
                artifacts,
                None,
                Some(reason),
                Some(reason),
                run.retry_count + 1,
                false,
            )
            .await?;
        self.db.fail_task(&task.task_id, reason).await?;
        self.db
            .append_task_event(
                &task.task_id,
                "workflow_run_blocked",
                "runtime",
                &self.config.runtime.instance_id,
                json!({
                    "workflow_run_id": run.workflow_run_id,
                    "reason": reason,
                }),
            )
            .await?;
        self.db
            .record_audit_event(
                "runtime",
                &self.config.runtime.instance_id,
                "workflow_run_blocked",
                json!({
                    "workflow_run_id": run.workflow_run_id,
                    "workflow_id": run.workflow_id,
                    "task_id": task.task_id,
                    "reason": reason,
                }),
            )
            .await?;
        let task = self
            .db
            .fetch_task_for_owner(&task.owner_user_id, &task.task_id)
            .await?
            .ok_or_else(|| anyhow!("workflow task '{}' disappeared after failure", task.task_id))?;
        Ok(AgentResult { task })
    }

    async fn user_role_ids(&self, user_id: &str) -> Result<Vec<String>> {
        Ok(self
            .db
            .list_user_roles(user_id)
            .await?
            .into_iter()
            .map(|row| row.role_id)
            .collect())
    }

    async fn primary_agent_id_for_owner(&self, owner_user_id: &str) -> Result<String> {
        let user = self
            .db
            .fetch_user(owner_user_id)
            .await?
            .ok_or_else(|| anyhow!("user '{owner_user_id}' not found"))?;
        user.primary_agent_id
            .ok_or_else(|| anyhow!("user '{owner_user_id}' has no primary agent"))
    }

    fn default_allowed_roots(&self) -> Vec<PathBuf> {
        let mut allowed_roots = vec![self.config.runtime.workspace_root.clone()];
        if !allowed_roots.contains(&self.config.runtime.data_dir) {
            allowed_roots.push(self.config.runtime.data_dir.clone());
        }
        allowed_roots
    }
}

fn approval_action_is_expired(action: &beaverki_db::ApprovalActionRow) -> bool {
    DateTime::parse_from_rfc3339(&action.expires_at)
        .map(|expires_at| expires_at.with_timezone(&Utc) <= Utc::now())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use anyhow::Result;
    use async_trait::async_trait;
    use beaverki_config::{
        DiscordAllowedChannel, DiscordChannelMode, ProviderModels, RuntimeConfig, RuntimeDefaults,
        RuntimeFeatures, SessionManagementConfig,
    };
    use beaverki_models::{ConversationItem, ModelTurnResponse};
    use tempfile::TempDir;
    use tokio::time;

    use super::*;
    use crate::session::SESSION_RESET_COMMAND;

    static DIRECT_SEND_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn direct_send_test_guard() -> std::sync::MutexGuard<'static, ()> {
        DIRECT_SEND_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("direct send test lock")
    }

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
            _tools: &[beaverki_tools::ToolDefinition],
        ) -> Result<ModelTurnResponse> {
            self.responses
                .lock()
                .expect("lock")
                .pop_front()
                .ok_or_else(|| anyhow!("no more fake responses"))
        }
    }

    async fn test_runtime(responses: Vec<ModelTurnResponse>) -> (TempDir, Runtime) {
        test_runtime_with_discord_token(responses, None).await
    }

    async fn test_runtime_with_discord_token(
        responses: Vec<ModelTurnResponse>,
        discord_bot_token: Option<String>,
    ) -> (TempDir, Runtime) {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config_dir = tempdir.path().join("config");
        let state_dir = tempdir.path().join("state");
        let data_dir = tempdir.path().join("data");
        let log_dir = state_dir.join("logs");
        let secret_dir = state_dir.join("secrets");
        std::fs::create_dir_all(&config_dir).expect("config dir");
        std::fs::create_dir_all(&log_dir).expect("log dir");
        std::fs::create_dir_all(&secret_dir).expect("secret dir");

        let config = LoadedConfig {
            config_dir: config_dir.clone(),
            base_dir: tempdir.path().to_path_buf(),
            runtime: RuntimeConfig {
                version: 1,
                instance_id: "test-instance".to_owned(),
                mode: "daemon".to_owned(),
                data_dir,
                state_dir: state_dir.clone(),
                log_dir,
                secret_dir,
                database_path: state_dir.join("runtime.db"),
                workspace_root: tempdir.path().to_path_buf(),
                default_timezone: "UTC".to_owned(),
                features: RuntimeFeatures {
                    markdown_exports: true,
                },
                defaults: RuntimeDefaults { max_agent_steps: 4 },
                session_management: SessionManagementConfig::default(),
            },
            providers: beaverki_config::ProvidersConfig {
                version: 1,
                active: "fake".to_owned(),
                entries: vec![],
            },
            integrations: beaverki_config::IntegrationsConfig::default(),
        };
        let db = Database::connect(&config.runtime.database_path)
            .await
            .expect("db connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let default_user = db.default_user().await.expect("default").expect("user");
        let runtime = Runtime::from_parts_with_secrets(
            config,
            db,
            default_user,
            Arc::new(FakeProvider::new(responses)),
            discord_bot_token,
        )
        .expect("runtime");
        (tempdir, runtime)
    }

    async fn set_session_timestamps(
        runtime: &Runtime,
        session_id: &str,
        last_activity_at: &str,
        last_reset_at: Option<&str>,
        archived_at: Option<&str>,
    ) {
        runtime
            .db
            .overwrite_conversation_session_timestamps(
                session_id,
                last_activity_at,
                last_reset_at,
                archived_at,
            )
            .await
            .expect("update session timestamps");
    }

    async fn wait_for_ready_or_report(
        socket_path: PathBuf,
        handle: &mut tokio::task::JoinHandle<Result<()>>,
    ) -> DaemonClient {
        match DaemonClient::wait_until_ready(socket_path.clone(), Duration::from_secs(5)).await {
            Ok(client) => client,
            Err(error) => {
                let daemon_result = time::timeout(Duration::from_millis(250), &mut *handle).await;
                panic!("daemon ready: {error}; daemon task result: {daemon_result:?}");
            }
        }
    }

    fn extract_token_for_command(reply: &str, command: &str) -> String {
        reply
            .lines()
            .find_map(|line| {
                if line
                    .to_ascii_lowercase()
                    .starts_with(&format!("{command}:"))
                {
                    line.split_whitespace()
                        .find(|word| word.starts_with("approval_token_"))
                        .map(ToOwned::to_owned)
                } else {
                    None
                }
            })
            .or_else(|| {
                reply
                    .split_whitespace()
                    .find(|word| word.starts_with("approval_token_"))
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| panic!("missing token for command '{command}' in reply: {reply}"))
    }

    #[tokio::test]
    async fn build_primary_request_adds_cli_follow_up_context() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let user = runtime.default_user().clone();
        let initiating_identity_id = format!("cli:{}", user.user_id);
        let primary_agent_id = user.primary_agent_id.clone().expect("primary agent id");
        let session = runtime
            .resolve_cli_conversation_session(&user)
            .await
            .expect("cli session");
        let prior_task = runtime
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &user.user_id,
                initiating_identity_id: &initiating_identity_id,
                primary_agent_id: &primary_agent_id,
                assigned_agent_id: &primary_agent_id,
                parent_task_id: None,
                session_id: Some(&session.session_id),
                kind: "interactive",
                objective: "How late is it?",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("prior task");
        runtime
            .db
            .complete_task(&prior_task.task_id, "It is 08:39 CEST on 2026-04-18.")
            .await
            .expect("complete prior task");

        let (request, _) = runtime
            .prepare_cli_task_request(
                &user,
                &initiating_identity_id,
                "That is fine, but how do you know that?",
                MemoryScope::Private,
            )
            .await
            .expect("build request");

        assert_eq!(
            request.parent_task_id.as_deref(),
            Some(prior_task.task_id.as_str())
        );
        let context = request.task_context.as_deref().expect("task context");
        assert!(context.contains("Conversation status: active follow-up"));
        assert!(context.contains("[CLI] User: How late is it?"));
        assert!(context.contains("[CLI] Assistant: It is 08:39 CEST on 2026-04-18."));
    }

    #[tokio::test]
    async fn cli_session_reset_clears_prior_transcript_window() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let user = runtime.default_user().clone();
        let initiating_identity_id = format!("cli:{}", user.user_id);

        runtime
            .enqueue_objective(
                Some(&user.user_id),
                "Remember this topic",
                MemoryScope::Private,
            )
            .await
            .expect("enqueue");
        runtime
            .reset_cli_conversation_session(&user, SESSION_RESET_COMMAND, MemoryScope::Private)
            .await
            .expect("reset");

        let (request, session) = runtime
            .prepare_cli_task_request(
                &user,
                &initiating_identity_id,
                "Fresh topic after reset",
                MemoryScope::Private,
            )
            .await
            .expect("build request");

        assert_eq!(request.parent_task_id, None);
        let context = request.task_context.as_deref().expect("task context");
        assert!(context.contains("new conversation"));
        assert!(!context.contains("Remember this topic"));
        assert!(session.last_reset_at.is_some());
    }

    #[tokio::test]
    async fn session_cleanup_resets_inactive_cli_sessions_without_forgetting_memory() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let user = runtime.default_user().clone();
        let initiating_identity_id = format!("cli:{}", user.user_id);
        let primary_agent_id = user.primary_agent_id.clone().expect("primary agent id");
        let session = runtime
            .resolve_cli_conversation_session(&user)
            .await
            .expect("cli session");
        let task = runtime
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &user.user_id,
                initiating_identity_id: &initiating_identity_id,
                primary_agent_id: &primary_agent_id,
                assigned_agent_id: &primary_agent_id,
                parent_task_id: None,
                session_id: Some(&session.session_id),
                kind: "interactive",
                objective: "Keep this session alive for now",
                context_summary: None,
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");
        runtime
            .db
            .complete_task(&task.task_id, "done")
            .await
            .expect("complete task");
        let memory_id = runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some(&user.user_id),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "user_profile",
                subject_key: Some("preferred_name"),
                content_text: "Alex",
                content_json: None,
                sensitivity: "normal",
                source_type: "test",
                source_ref: Some("cleanup"),
                task_id: None,
            })
            .await
            .expect("memory");
        let stale_activity_at = (Utc::now() - chrono::Duration::hours(13)).to_rfc3339();
        set_session_timestamps(
            &runtime,
            &session.session_id,
            &stale_activity_at,
            None,
            None,
        )
        .await;

        let actions = runtime
            .run_session_lifecycle_cleanup()
            .await
            .expect("cleanup");

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].policy_id, "cli_inactive_reset");
        assert_eq!(actions[0].action, "reset");
        let updated = runtime
            .db
            .fetch_conversation_session(&session.session_id)
            .await
            .expect("fetch session")
            .expect("session");
        assert!(updated.last_reset_at.is_some());
        assert!(updated.archived_at.is_none());
        assert_eq!(
            updated.lifecycle_reason.as_deref(),
            Some("policy:cli_inactive_reset:reset")
        );
        let recent_tasks = runtime
            .db
            .list_recent_interactive_tasks_for_session(
                &session.session_id,
                updated.last_reset_at.as_deref(),
                8,
            )
            .await
            .expect("recent tasks after cleanup");
        assert!(recent_tasks.is_empty());
        let memory = runtime
            .db
            .fetch_memory(&memory_id)
            .await
            .expect("fetch memory")
            .expect("memory");
        assert!(memory.forgotten_at.is_none());
    }

    #[tokio::test]
    async fn session_cleanup_honors_disabled_policies_and_archive_is_idempotent() {
        let (_tempdir, mut runtime) = test_runtime(vec![]).await;
        let policy = runtime
            .config
            .runtime
            .session_management
            .policies
            .iter_mut()
            .find(|policy| policy.policy_id == "group_room_inactive_archive")
            .expect("group room policy");
        policy.enabled = false;
        let session = runtime
            .resolve_connector_conversation_session("user_alex", "discord", "room-archive", false)
            .await
            .expect("room session");
        let stale_activity_at = (Utc::now() - chrono::Duration::days(8)).to_rfc3339();
        set_session_timestamps(
            &runtime,
            &session.session_id,
            &stale_activity_at,
            None,
            None,
        )
        .await;

        let disabled_actions = runtime
            .run_session_lifecycle_cleanup()
            .await
            .expect("cleanup disabled");
        assert!(disabled_actions.is_empty());

        let policy = runtime
            .config
            .runtime
            .session_management
            .policies
            .iter_mut()
            .find(|policy| policy.policy_id == "group_room_inactive_archive")
            .expect("group room policy");
        policy.enabled = true;

        let first_actions = runtime
            .run_session_lifecycle_cleanup()
            .await
            .expect("cleanup enabled");
        assert_eq!(first_actions.len(), 1);
        assert_eq!(first_actions[0].action, "archive");
        let archived = runtime
            .db
            .fetch_conversation_session(&session.session_id)
            .await
            .expect("fetch archived session")
            .expect("session");
        assert!(archived.last_reset_at.is_some());
        assert!(archived.archived_at.is_some());
        assert_eq!(
            archived.lifecycle_reason.as_deref(),
            Some("policy:group_room_inactive_archive:archive")
        );

        let second_actions = runtime
            .run_session_lifecycle_cleanup()
            .await
            .expect("cleanup idempotent");
        assert!(second_actions.is_empty());
    }

    #[tokio::test]
    async fn discord_dm_session_persists_across_dm_channel_ids_without_socket() {
        let (_tempdir, mut runtime) = test_runtime(vec![]).await;
        runtime.config.integrations.discord.task_wait_timeout_secs = 0;
        let identity = runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-dm",
                None,
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");
        let db = runtime.db.clone();
        let daemon = RuntimeDaemon::new(runtime);

        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: identity.external_user_id.clone(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-a".to_owned(),
                message_id: "dm-msg-1".to_owned(),
                content: "First DM message".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("first dm");
        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: identity.external_user_id.clone(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-b".to_owned(),
                message_id: "dm-msg-2".to_owned(),
                content: "Second DM message".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("second dm");

        let recent_tasks = db
            .list_recent_interactive_tasks_for_owner("user_alex", 2)
            .await
            .expect("recent tasks");
        assert_eq!(recent_tasks.len(), 2);
        assert_eq!(recent_tasks[0].session_id, recent_tasks[1].session_id);
        let context = recent_tasks[0]
            .context_summary
            .as_deref()
            .expect("dm context");
        assert!(context.contains("direct-message entrypoints"));
        assert!(context.contains("First DM message"));
    }

    #[tokio::test]
    async fn discord_channel_and_dm_sessions_are_isolated_without_socket() {
        let (_tempdir, mut runtime) = test_runtime(vec![]).await;
        runtime.config.integrations.discord.allowed_channels = vec![DiscordAllowedChannel {
            channel_id: "channel-1".to_owned(),
            mode: DiscordChannelMode::Household,
        }];
        runtime.config.integrations.discord.task_wait_timeout_secs = 0;
        let identity = runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-isolated",
                Some("channel-1"),
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");
        let db = runtime.db.clone();
        let daemon = RuntimeDaemon::new(runtime);

        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: identity.external_user_id.clone(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "channel-1".to_owned(),
                message_id: "channel-msg-1".to_owned(),
                content: "!bk First channel message".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("channel message");
        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: identity.external_user_id.clone(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-1".to_owned(),
                message_id: "dm-msg-1".to_owned(),
                content: "Fresh DM message".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("dm message");

        let recent_tasks = db
            .list_recent_interactive_tasks_for_owner("user_alex", 2)
            .await
            .expect("recent tasks");
        assert_ne!(recent_tasks[0].session_id, recent_tasks[1].session_id);
        let context = recent_tasks[0]
            .context_summary
            .as_deref()
            .expect("dm context");
        assert!(!context.contains("First channel message"));
    }

    #[tokio::test]
    async fn shared_room_session_is_shared_across_mapped_users() {
        let (_tempdir, mut runtime) = test_runtime(vec![]).await;
        runtime.config.integrations.discord.allowed_channels = vec![DiscordAllowedChannel {
            channel_id: "room-1".to_owned(),
            mode: DiscordChannelMode::Household,
        }];
        runtime.config.integrations.discord.task_wait_timeout_secs = 0;
        runtime
            .create_user("Casey", &[String::from("adult")])
            .await
            .expect("create user");
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-room-a",
                Some("room-1"),
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping alex");
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-room-b",
                Some("room-1"),
                "user_casey",
                "authenticated_message",
            )
            .await
            .expect("mapping casey");
        let db = runtime.db.clone();
        let daemon = RuntimeDaemon::new(runtime);

        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-room-a".to_owned(),
                external_display_name: Some("Alex".to_owned()),
                channel_id: "room-1".to_owned(),
                message_id: "room-msg-1".to_owned(),
                content: "!bk First room message".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("first room message");
        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-room-b".to_owned(),
                external_display_name: Some("Casey".to_owned()),
                channel_id: "room-1".to_owned(),
                message_id: "room-msg-2".to_owned(),
                content: "!bk Follow-up from Casey".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("second room message");

        let recent_tasks = db
            .list_recent_interactive_tasks_for_owner("user_casey", 1)
            .await
            .expect("casey tasks");
        let task = recent_tasks.first().expect("casey task");
        let context = task.context_summary.as_deref().expect("room context");
        assert!(context.contains("shared connector room"));
        assert!(context.contains("First room message"));
    }

    #[tokio::test]
    async fn group_room_private_cap_downgrades_household_scope() {
        let (_tempdir, mut runtime) = test_runtime(vec![]).await;
        runtime.config.integrations.discord.allowed_channels = vec![DiscordAllowedChannel {
            channel_id: "room-2".to_owned(),
            mode: DiscordChannelMode::Guest,
        }];
        runtime.config.integrations.discord.task_wait_timeout_secs = 0;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-room-cap",
                Some("room-2"),
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");
        let db = runtime.db.clone();
        let daemon = RuntimeDaemon::new(runtime);

        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-room-cap".to_owned(),
                external_display_name: Some("Alex".to_owned()),
                channel_id: "room-2".to_owned(),
                message_id: "room-cap-msg-1".to_owned(),
                content: "!bk Message in capped room".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("room message");

        let task = db
            .list_recent_interactive_tasks_for_owner("user_alex", 1)
            .await
            .expect("recent task")
            .into_iter()
            .next()
            .expect("task");
        assert_eq!(task.scope, "private");
        let request = daemon
            .runtime
            .build_request_from_task("user_alex", &task, Vec::new())
            .await
            .expect("request");
        assert_eq!(request.visible_scopes, vec![MemoryScope::Private]);
    }

    #[tokio::test]
    async fn discord_new_resets_session_without_agent_roundtrip() {
        let (_tempdir, mut runtime) = test_runtime(vec![]).await;
        runtime.config.integrations.discord.task_wait_timeout_secs = 0;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-reset",
                None,
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");
        let db = runtime.db.clone();
        let daemon = RuntimeDaemon::new(runtime);

        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-reset".to_owned(),
                external_display_name: Some("Alex".to_owned()),
                channel_id: "dm-reset-1".to_owned(),
                message_id: "dm-reset-msg-1".to_owned(),
                content: "Old DM message".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("first dm");
        let reset_reply = daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-reset".to_owned(),
                external_display_name: Some("Alex".to_owned()),
                channel_id: "dm-reset-2".to_owned(),
                message_id: "dm-reset-msg-2".to_owned(),
                content: SESSION_RESET_COMMAND.to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("reset");
        assert_eq!(
            reset_reply.reply.as_deref(),
            Some("Started a new conversation. Durable memory and audit history were left intact.")
        );
        daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-reset".to_owned(),
                external_display_name: Some("Alex".to_owned()),
                channel_id: "dm-reset-3".to_owned(),
                message_id: "dm-reset-msg-3".to_owned(),
                content: "New DM message".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("new dm");

        let latest_task = db
            .list_recent_interactive_tasks_for_owner("user_alex", 1)
            .await
            .expect("recent task")
            .into_iter()
            .next()
            .expect("task");
        let context = latest_task.context_summary.as_deref().expect("context");
        assert!(!context.contains("Old DM message"));
    }

    #[test]
    fn cli_task_context_marks_stale_exchange_as_fresh_conversation() {
        let stale_exchange = CliConversationExchange {
            created_at: "2024-01-01T00:00:00Z".to_owned(),
            state: TaskState::Completed.as_str().to_owned(),
            user_text: "Old topic".to_owned(),
            assistant_text: "Old answer".to_owned(),
            task_id: "task_old".to_owned(),
        };

        let context = build_cli_task_context(&[stale_exchange]);

        assert!(context.contains("Conversation status: fresh conversation"));
        assert!(!context.contains("Recent attributed CLI exchanges"));
    }

    #[tokio::test]
    async fn daemon_records_session_start_and_stop() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let db = runtime.db.clone();
        let instance_id = runtime.config.runtime.instance_id.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path.clone(), &mut handle).await;

        let status = client.status().await.expect("status");
        assert_eq!(status.state, "running");
        assert!(!status.session_id.is_empty());

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");

        let persisted = db
            .latest_runtime_session(&instance_id)
            .await
            .expect("latest status")
            .expect("status row");
        assert_eq!(persisted.state, "stopped");
        assert!(persisted.stopped_at.is_some());
        let heartbeats = db
            .list_runtime_heartbeats(&persisted.session_id, 8)
            .await
            .expect("heartbeats");
        assert!(!heartbeats.is_empty());
    }

    #[tokio::test]
    async fn daemon_executes_task_submitted_over_ipc() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
            output_items: vec![json!({
                "type": "message",
                "content": [{ "type": "output_text", "text": "daemon done" }]
            })],
            tool_calls: vec![],
            output_text: "daemon done".to_owned(),
            usage: None,
        }])
        .await;
        let db = runtime.db.clone();
        let instance_id = runtime.config.runtime.instance_id.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let task = client
            .run_task(
                None,
                "Summarize the workspace".to_owned(),
                "private".to_owned(),
                true,
            )
            .await
            .expect("run task");
        assert_eq!(task.state, TaskState::Completed.as_str());
        assert_eq!(task.result_text.as_deref(), Some("daemon done"));

        let inspection = client
            .show_task(None, task.task_id.clone())
            .await
            .expect("inspection");
        assert_eq!(inspection.task.task_id, task.task_id);
        assert!(!inspection.events.is_empty());

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");

        let persisted = db
            .latest_runtime_session(&instance_id)
            .await
            .expect("latest status")
            .expect("status row");
        assert_eq!(persisted.state, "stopped");
    }

    #[tokio::test]
    async fn daemon_runs_persisted_pending_task_after_restart() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
            output_items: vec![json!({
                "type": "message",
                "content": [{ "type": "output_text", "text": "recovered" }]
            })],
            tool_calls: vec![],
            output_text: "recovered".to_owned(),
            usage: None,
        }])
        .await;
        let queued = runtime
            .enqueue_objective(None, "Recover queued task", MemoryScope::Private)
            .await
            .expect("enqueue");
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let recovered = loop {
            let inspection = client
                .show_task(None, queued.task_id.clone())
                .await
                .expect("inspection");
            if inspection.task.state == TaskState::Completed.as_str() {
                break inspection.task;
            }
            time::sleep(Duration::from_millis(150)).await;
        };

        assert_eq!(recovered.result_text.as_deref(), Some("recovered"));

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn scheduled_lua_script_is_materialized_and_executed() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
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
        }])
        .await;
        let db = runtime.db.clone();
        let script = runtime
            .create_lua_script(
                None,
                Some("script_schedule_test"),
                r#"return function(ctx)
    ctx.log_info("scheduled lua ran")
    return "scheduled ok"
end"#,
                json!({}),
                None,
                "Return a confirmation string for the schedule test.",
            )
            .await
            .expect("create script")
            .script;
        runtime
            .activate_script(None, &script.script_id)
            .await
            .expect("activate script");
        db.create_schedule(NewSchedule {
            schedule_id: Some("sched_schedule_test"),
            owner_user_id: &script.owner_user_id,
            target_type: "lua_script",
            target_id: &script.script_id,
            cron_expr: "0/1 * * * * * *",
            enabled: true,
            next_run_at: &now_rfc3339(),
        })
        .await
        .expect("create schedule");

        let result = runtime
            .execute_next_runnable_task()
            .await
            .expect("execute next")
            .expect("task");
        assert_eq!(result.task.kind, "scheduled_lua");
        if result.task.state != TaskState::Completed.as_str() {
            panic!(
                "workflow task state={} result={:?}",
                result.task.state, result.task.result_text
            );
        }
        assert_eq!(result.task.result_text.as_deref(), Some("scheduled ok"));

        let inspection = runtime
            .inspect_task(None, &result.task.task_id)
            .await
            .expect("inspection");
        assert!(
            inspection
                .events
                .iter()
                .any(|event| event.event_type == "lua_log")
        );
    }

    #[tokio::test]
    async fn scheduled_lua_script_can_run_top_level_chunk() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
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
        }])
        .await;
        let db = runtime.db.clone();
        let script = runtime
            .create_lua_script(
                None,
                Some("script_schedule_top_level"),
                r#"ctx.log_info("top level lua ran")
return "top level ok""#,
                json!({}),
                None,
                "Return a confirmation string for the schedule test.",
            )
            .await
            .expect("create script")
            .script;
        runtime
            .activate_script(None, &script.script_id)
            .await
            .expect("activate script");
        db.create_schedule(NewSchedule {
            schedule_id: Some("sched_schedule_top_level"),
            owner_user_id: &script.owner_user_id,
            target_type: "lua_script",
            target_id: &script.script_id,
            cron_expr: "0/1 * * * * * *",
            enabled: true,
            next_run_at: &now_rfc3339(),
        })
        .await
        .expect("create schedule");

        let result = runtime
            .execute_next_runnable_task()
            .await
            .expect("execute next")
            .expect("task");
        assert_eq!(result.task.kind, "scheduled_lua");
        assert_eq!(result.task.state, TaskState::Completed.as_str());
        assert_eq!(result.task.result_text.as_deref(), Some("top level ok"));

        let inspection = runtime
            .inspect_task(None, &result.task.task_id)
            .await
            .expect("inspection");
        assert!(
            inspection
                .events
                .iter()
                .any(|event| event.event_type == "lua_log")
        );
    }

    #[tokio::test]
    async fn create_lua_script_preserves_needs_changes_verdict() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
            output_items: vec![json!({
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "{\"verdict\":\"needs_changes\",\"risk_level\":\"medium\",\"findings\":[\"Writes files more broadly than needed.\"],\"required_changes\":[\"Limit writes to the intended target.\"],\"summary\":\"The script needs a narrower scope before it can be trusted.\"}"
                }]
            })],
            tool_calls: vec![],
            output_text: "{\"verdict\":\"needs_changes\",\"risk_level\":\"medium\",\"findings\":[\"Writes files more broadly than needed.\"],\"required_changes\":[\"Limit writes to the intended target.\"],\"summary\":\"The script needs a narrower scope before it can be trusted.\"}".to_owned(),
            usage: None,
        }])
        .await;

        let inspection = runtime
            .create_lua_script(
                None,
                Some("script_needs_changes_runtime"),
                r#"return function(ctx)
    return "not yet"
end"#,
                json!({}),
                None,
                "Return a placeholder value.",
            )
            .await
            .expect("create script");

        assert_eq!(inspection.script.status, "blocked");
        assert_eq!(inspection.script.safety_status, "needs_changes");
        assert_eq!(inspection.reviews[0].verdict, "needs_changes");
    }

    #[tokio::test]
    async fn blocked_script_schedule_is_skipped_and_advanced() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let db = runtime.db.clone();
        let default_user = runtime.default_user.clone();
        db.create_script(NewScript {
            script_id: Some("script_blocked_sched"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "blocked",
            source_text: "return function(ctx) return \"blocked\" end",
            capability_profile_json: json!({}),
            created_from_task_id: None,
            safety_status: "needs_changes",
            safety_summary: Some("needs changes"),
        })
        .await
        .expect("script");
        let original_next_run_at = now_rfc3339();
        db.create_schedule(NewSchedule {
            schedule_id: Some("sched_blocked_script"),
            owner_user_id: &default_user.user_id,
            target_type: "lua_script",
            target_id: "script_blocked_sched",
            cron_expr: "0/5 * * * * * *",
            enabled: true,
            next_run_at: &original_next_run_at,
        })
        .await
        .expect("schedule");

        let tasks = runtime
            .materialize_due_schedules()
            .await
            .expect("materialize schedules");
        assert!(tasks.is_empty());
        assert_eq!(db.pending_task_count().await.expect("pending tasks"), 0);

        let updated = db
            .fetch_schedule_for_owner(&default_user.user_id, "sched_blocked_script")
            .await
            .expect("fetch schedule")
            .expect("schedule");
        assert_ne!(updated.next_run_at, original_next_run_at);
        assert!(updated.last_run_at.is_some());

        let audit = db
            .list_audit_events(8)
            .await
            .expect("audit events")
            .into_iter()
            .find(|event| event.event_type == "schedule_skipped")
            .expect("schedule_skipped audit");
        let payload: Value = serde_json::from_str(&audit.payload_json).expect("payload");
        assert_eq!(payload["schedule_id"], json!("sched_blocked_script"));
        assert_eq!(payload["reason"], json!("script_not_active"));
        assert_eq!(payload["safety_status"], json!("needs_changes"));
    }

    #[tokio::test]
    async fn scheduled_workflow_pipeline_executes_all_stages_and_notifies_user() {
        let _direct_send_guard = direct_send_test_guard();
        let (_tempdir, runtime) = test_runtime_with_discord_token(
            vec![ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "Morning digest ready: ORF headline summary." }]
                })],
                tool_calls: vec![],
                output_text: "Morning digest ready: ORF headline summary.".to_owned(),
                usage: None,
            }],
            Some("discord-token".to_owned()),
        )
        .await;
        let db = runtime.db.clone();
        let default_user = runtime.default_user.clone();
        db.upsert_connector_identity(
            "discord",
            "discord-alex",
            None,
            &default_user.user_id,
            "authenticated_message",
        )
        .await
        .expect("map Alex");
        let direct_send_calls = Arc::new(Mutex::new(Vec::new()));
        discord::set_test_direct_send_capture(Some(Arc::clone(&direct_send_calls)));
        db.create_script(NewScript {
            script_id: Some("script_workflow_fetch"),
            owner_user_id: &default_user.user_id,
            kind: "lua",
            status: "active",
            source_text: r#"return function(ctx)
    if ctx.input.last_result == nil then
        return "ORF headline"
    end
    return ctx.input.last_result
end"#,
            capability_profile_json: json!({}),
            created_from_task_id: None,
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("script");
        db.create_lua_tool(beaverki_db::NewLuaTool {
            tool_id: Some("lua_tool_digest_summary"),
            owner_user_id: &default_user.user_id,
            description: "Digest summarizer",
            source_text: r#"return function(ctx)
    local headline = tostring(ctx.input.last_result)
    return { digest = headline .. " summary" }
end"#,
            input_schema_json: json!({
                "type": "object",
                "required": ["workflow_run_id", "workflow_id", "stage_id", "stage_index", "last_result", "artifacts", "config"],
                "properties": {
                    "workflow_run_id": { "type": "string" },
                    "workflow_id": { "type": "string" },
                    "stage_id": { "type": "string" },
                    "stage_index": { "type": "integer" },
                    "last_result": {},
                    "artifacts": { "type": "object" },
                    "config": { "type": "object" }
                }
            }),
            output_schema_json: json!({
                "type": "object",
                "required": ["digest"],
                "properties": {
                    "digest": { "type": "string" }
                }
            }),
            capability_profile_json: json!({}),
            status: "active",
            created_from_task_id: None,
            safety_status: "approved",
            safety_summary: Some("approved"),
        })
        .await
        .expect("tool");
        db.create_workflow_definition(
            NewWorkflowDefinition {
                workflow_id: Some("workflow_news_digest"),
                owner_user_id: &default_user.user_id,
                name: "Morning digest",
                description: Some("Fetch, summarize, and notify."),
                status: "active",
                created_from_task_id: None,
                safety_status: "approved",
                safety_summary: Some("approved"),
            },
            &[
                NewWorkflowStage {
                    stage_id: Some("workflow_stage_fetch"),
                    stage_kind: "lua_script",
                    stage_label: Some("Fetch headlines"),
                    artifact_ref: Some("script_workflow_fetch"),
                    stage_config_json: json!({}),
                },
                NewWorkflowStage {
                    stage_id: Some("workflow_stage_digest"),
                    stage_kind: "lua_tool",
                    stage_label: Some("Summarize"),
                    artifact_ref: Some("lua_tool_digest_summary"),
                    stage_config_json: json!({}),
                },
                NewWorkflowStage {
                    stage_id: Some("workflow_stage_agent"),
                    stage_kind: "agent_task",
                    stage_label: Some("Write digest"),
                    artifact_ref: None,
                    stage_config_json: json!({
                        "prompt": "Write a one-line digest using the workflow artifacts."
                    }),
                },
                NewWorkflowStage {
                    stage_id: Some("workflow_stage_notify"),
                    stage_kind: "user_notify",
                    stage_label: Some("Notify"),
                    artifact_ref: None,
                    stage_config_json: json!({
                        "recipient": default_user.display_name,
                        "message_template": "Daily ORF digest:\n\n{{stages[2].result}}",
                    }),
                },
            ],
        )
        .await
        .expect("workflow");
        db.create_schedule(NewSchedule {
            schedule_id: Some("sched_workflow_digest"),
            owner_user_id: &default_user.user_id,
            target_type: "workflow",
            target_id: "workflow_news_digest",
            cron_expr: "0/1 * * * * * *",
            enabled: true,
            next_run_at: &now_rfc3339(),
        })
        .await
        .expect("schedule");

        let result = runtime
            .execute_next_runnable_task()
            .await
            .expect("execute next")
            .expect("task");
        discord::set_test_direct_send_capture(None);
        assert_eq!(result.task.kind, WORKFLOW_RUN_TASK_KIND);
        if result.task.state != TaskState::Completed.as_str() {
            panic!(
                "workflow task state={} result={:?}",
                result.task.state, result.task.result_text
            );
        }
        assert_eq!(
            result.task.result_text.as_deref(),
            Some("Daily ORF digest:\n\nMorning digest ready: ORF headline summary.")
        );

        let inspection = runtime
            .inspect_task(None, &result.task.task_id)
            .await
            .expect("inspection");
        assert!(
            inspection
                .events
                .iter()
                .any(|event| event.event_type == "workflow_stage_completed")
        );

        let workflow_runs = db
            .list_workflow_runs_for_workflow("workflow_news_digest")
            .await
            .expect("workflow runs");
        assert_eq!(workflow_runs.len(), 1);
        assert_eq!(workflow_runs[0].state, "completed");

        let deliveries = db
            .list_household_deliveries_for_task(&result.task.task_id)
            .await
            .expect("deliveries");
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].delivery_mode, "workflow_notify");
        assert_eq!(deliveries[0].status, "sent");
        assert_eq!(deliveries[0].connector_type, "discord");
        assert_eq!(deliveries[0].target_ref, "dm-casey");
        assert_eq!(
            deliveries[0].external_message_id.as_deref(),
            Some("msg-casey-1")
        );
        assert!(
            deliveries[0]
                .message_text
                .starts_with("Daily ORF digest:\n\nMorning digest ready: ORF headline summary.")
        );
        assert!(
            inspection
                .events
                .iter()
                .any(|event| event.event_type == "household_delivery_sent")
        );
        let calls = direct_send_calls.lock().expect("captured sends");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains("recipient=discord-alex"));
        assert!(calls[0].contains("Household message from Alex"));
        assert!(calls[0].contains("Daily ORF digest:"));
    }

    #[tokio::test]
    async fn workflow_rewrite_creates_new_version_and_keeps_history() {
        let (_tempdir, runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"approved\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"approved\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"approved\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"low\",\"findings\":[],\"required_changes\":[],\"summary\":\"approved\"}".to_owned(),
                usage: None,
            },
        ])
        .await;

        let first = runtime
            .create_workflow_definition(
                None,
                Some("workflow_versioned"),
                WorkflowDefinitionInput {
                    name: "Versioned workflow".to_owned(),
                    description: Some("v1".to_owned()),
                    stages: vec![WorkflowStageInput {
                        kind: "user_notify".to_owned(),
                        label: Some("Notify".to_owned()),
                        artifact_ref: None,
                        config: json!({ "message": "v1" }),
                    }],
                },
                None,
                "Create the first version.",
            )
            .await
            .expect("create v1");
        assert_eq!(first.workflow.current_version_number, 1);

        let second = runtime
            .create_workflow_definition(
                None,
                Some("workflow_versioned"),
                WorkflowDefinitionInput {
                    name: "Versioned workflow".to_owned(),
                    description: Some("v2".to_owned()),
                    stages: vec![
                        WorkflowStageInput {
                            kind: "user_notify".to_owned(),
                            label: Some("Notify".to_owned()),
                            artifact_ref: None,
                            config: json!({ "message": "v2" }),
                        },
                        WorkflowStageInput {
                            kind: "agent_task".to_owned(),
                            label: Some("Agent".to_owned()),
                            artifact_ref: None,
                            config: json!({ "prompt": "Inspect the workflow output." }),
                        },
                    ],
                },
                None,
                "Create the second version.",
            )
            .await
            .expect("create v2");
        assert_eq!(second.workflow.current_version_number, 2);
        assert_eq!(second.versions.len(), 2);
        assert_eq!(second.stages.len(), 2);

        let v1_stages = runtime
            .db
            .list_workflow_stages_for_version("workflow_versioned", 1)
            .await
            .expect("v1 stages");
        assert_eq!(v1_stages.len(), 1);
        let v2_stages = runtime
            .db
            .list_workflow_stages_for_version("workflow_versioned", 2)
            .await
            .expect("v2 stages");
        assert_eq!(v2_stages.len(), 2);
    }

    #[tokio::test]
    async fn automation_lifecycle_emits_auditable_event_chain() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
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
        }])
        .await;
        let db = runtime.db.clone();

        let inspection = runtime
            .create_lua_script(
                None,
                Some("script_audit_chain"),
                r#"return function(ctx)
    ctx.log_info("audit chain lua ran")
    return "audit chain ok"
end"#,
                json!({}),
                None,
                "Return a confirmation string for audit testing.",
            )
            .await
            .expect("create script");
        let script = inspection.script;

        runtime
            .activate_script(None, &script.script_id)
            .await
            .expect("activate script");
        runtime
            .create_schedule(
                None,
                Some("sched_audit_chain"),
                &script.script_id,
                "0/5 * * * * * *",
                true,
            )
            .await
            .expect("create schedule");

        db.update_schedule_state("sched_audit_chain", true, &now_rfc3339(), None)
            .await
            .expect("force due schedule");

        let result = runtime
            .execute_next_runnable_task()
            .await
            .expect("execute next")
            .expect("task");
        assert_eq!(result.task.state, TaskState::Completed.as_str());

        let inspection = runtime
            .inspect_task(None, &result.task.task_id)
            .await
            .expect("inspect task");
        let task_event_types = inspection
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(task_event_types.contains("lua_task_started"));
        assert!(task_event_types.contains("lua_log"));
        assert!(task_event_types.contains("lua_task_completed"));

        let audit_events = db.list_audit_events(16).await.expect("audit events");
        let audit_event_types = audit_events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(audit_event_types.contains("script_created"));
        assert!(audit_event_types.contains("script_reviewed"));
        assert!(audit_event_types.contains("script_activated"));
        assert!(audit_event_types.contains("schedule_created"));
        assert!(audit_event_types.contains("schedule_triggered"));
        assert!(audit_event_types.contains("lua_script_executed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lua_runtime_records_policy_denial_for_risky_shell_tool_call() {
        let (_tempdir, runtime) = test_runtime(vec![ModelTurnResponse {
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
        }])
        .await;
        let script = runtime
            .create_lua_script(
                None,
                Some("script_policy_denied_runtime"),
                r#"return function(ctx)
    return ctx.tool_call("shell_exec", { command = "mkdir /tmp/beaverki_lua_policy_denied" })
end"#,
                json!({
                    "allowed_tools": ["shell_exec"]
                }),
                None,
                "Attempt to create a directory from a Lua script.",
            )
            .await
            .expect("create script")
            .script;
        runtime
            .activate_script(None, &script.script_id)
            .await
            .expect("activate script");

        let primary_agent_id = runtime
            .default_user
            .primary_agent_id
            .clone()
            .expect("primary agent");
        let task_context = json!({ "script_id": script.script_id }).to_string();
        let task = runtime
            .db
            .create_task_with_params(NewTask {
                owner_user_id: &script.owner_user_id,
                initiating_identity_id: "test:lua-runtime",
                primary_agent_id: &primary_agent_id,
                assigned_agent_id: &primary_agent_id,
                parent_task_id: None,
                session_id: None,
                kind: "lua_script",
                objective: "Run a denied Lua script",
                context_summary: Some(&task_context),
                scope: MemoryScope::Private,
                wake_at: None,
            })
            .await
            .expect("task");

        let error = runtime
            .execute_task(task.clone())
            .await
            .expect_err("policy denial");
        let policy_error = error
            .downcast_ref::<automation::LuaToolPolicyDenied>()
            .expect("typed policy error");
        assert_eq!(policy_error.tool_name, "shell_exec");
        assert_eq!(policy_error.detail["risk"], json!("high"));

        let inspection = runtime
            .inspect_task(None, &task.task_id)
            .await
            .expect("inspection");
        let denial_event = inspection
            .events
            .iter()
            .find(|event| event.event_type == "lua_tool_denied")
            .expect("lua_tool_denied event");
        let payload: Value = serde_json::from_str(&denial_event.payload_json).expect("payload");
        assert_eq!(payload["tool_name"], json!("shell_exec"));
        assert_eq!(payload["detail"]["risk"], json!("high"));
    }

    #[tokio::test]
    async fn household_direct_delivery_sends_once_and_persists_audit_state() {
        let _direct_send_guard = direct_send_test_guard();
        let (_tempdir, runtime) = test_runtime_with_discord_token(
            vec![
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "household_send_message",
                        "arguments": "{\"recipient\":\"Casey\",\"message\":\"Dinner is ready.\"}"
                    })],
                    tool_calls: vec![beaverki_models::ModelToolCall {
                        call_id: "call_1".to_owned(),
                        name: "household_send_message".to_owned(),
                        arguments: json!({
                            "recipient": "Casey",
                            "message": "Dinner is ready."
                        }),
                    }],
                    output_text: String::new(),
                    usage: None,
                },
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "function_call",
                        "call_id": "call_2",
                        "name": "household_send_message",
                        "arguments": "{\"recipient\":\"Casey\",\"message\":\"Dinner is ready.\"}"
                    })],
                    tool_calls: vec![beaverki_models::ModelToolCall {
                        call_id: "call_2".to_owned(),
                        name: "household_send_message".to_owned(),
                        arguments: json!({
                            "recipient": "Casey",
                            "message": "Dinner is ready."
                        }),
                    }],
                    output_text: String::new(),
                    usage: None,
                },
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "message",
                        "content": [{ "type": "output_text", "text": "Casey has been notified." }]
                    })],
                    tool_calls: vec![],
                    output_text: "Casey has been notified.".to_owned(),
                    usage: None,
                },
            ],
            Some("discord-token".to_owned()),
        )
        .await;
        let recipient = runtime
            .create_user("Casey", &[String::from("adult")])
            .await
            .expect("create Casey");
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-casey",
                None,
                &recipient.user_id,
                "authenticated_message",
            )
            .await
            .expect("map Casey");
        let direct_send_calls = Arc::new(Mutex::new(Vec::new()));
        discord::set_test_direct_send_capture(Some(Arc::clone(&direct_send_calls)));

        let result = runtime
            .run_objective(None, "Tell Casey dinner is ready.", MemoryScope::Private)
            .await
            .expect("run objective");

        discord::set_test_direct_send_capture(None);

        assert_eq!(result.task.state, TaskState::Completed.as_str());
        assert_eq!(
            result.task.result_text.as_deref(),
            Some("Casey has been notified.")
        );

        let deliveries = runtime
            .db
            .list_household_deliveries_for_task(&result.task.task_id)
            .await
            .expect("deliveries");
        let inspection = runtime
            .inspect_task(None, &result.task.task_id)
            .await
            .expect("inspect task");
        let tool_statuses = inspection
            .tool_invocations
            .iter()
            .map(|invocation| format!("{}:{}", invocation.tool_name, invocation.status))
            .collect::<Vec<_>>();
        assert_eq!(
            deliveries.len(),
            1,
            "unexpected tool statuses: {}",
            tool_statuses.join(", ")
        );
        assert_eq!(deliveries[0].recipient_user_id, recipient.user_id);
        assert_eq!(deliveries[0].status, "sent");
        assert_eq!(deliveries[0].target_ref, "dm-casey");
        assert_eq!(
            deliveries[0].external_message_id.as_deref(),
            Some("msg-casey-1")
        );
        assert!(
            inspection
                .tool_invocations
                .iter()
                .all(|invocation| invocation.status == "completed"),
            "unexpected tool statuses: {}",
            tool_statuses.join(", ")
        );
        let sent_event = inspection
            .events
            .iter()
            .find(|event| event.event_type == "household_delivery_sent")
            .expect("household delivery sent event");
        let sent_payload: Value =
            serde_json::from_str(&sent_event.payload_json).expect("sent payload");
        assert_eq!(sent_payload["recipient_display_name"], json!("Casey"));
        assert_eq!(sent_payload["target_kind"], json!("discord_dm"));

        let calls = direct_send_calls.lock().expect("captured sends");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains("recipient=discord-casey"));
        assert!(calls[0].contains("Household message from Alex"));
        assert!(calls[0].contains("Dinner is ready."));
    }

    #[tokio::test]
    async fn scheduled_household_delivery_pauses_for_approval_and_executes_later() {
        let _direct_send_guard = direct_send_test_guard();
        let (_tempdir, runtime) = test_runtime_with_discord_token(
            vec![
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "household_schedule_message",
                        "arguments": "{\"delivery_id\":null,\"recipient\":\"Casey\",\"message\":\"Buy kiwis.\",\"deliver_at\":\"2099-01-01T09:00:00Z\",\"cron_expr\":null,\"window_start_at\":null,\"window_end_at\":null,\"enabled\":true}"
                    })],
                    tool_calls: vec![beaverki_models::ModelToolCall {
                        call_id: "call_1".to_owned(),
                        name: "household_schedule_message".to_owned(),
                        arguments: json!({
                            "delivery_id": Value::Null,
                            "recipient": "Casey",
                            "message": "Buy kiwis.",
                            "deliver_at": "2099-01-01T09:00:00Z",
                            "cron_expr": Value::Null,
                            "window_start_at": Value::Null,
                            "window_end_at": Value::Null,
                            "enabled": true
                        }),
                    }],
                    output_text: String::new(),
                    usage: None,
                },
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "function_call",
                        "call_id": "call_2",
                        "name": "household_schedule_message",
                        "arguments": "{\"delivery_id\":null,\"recipient\":\"Casey\",\"message\":\"Buy kiwis.\",\"deliver_at\":\"2099-01-01T09:00:00Z\",\"cron_expr\":null,\"window_start_at\":null,\"window_end_at\":null,\"enabled\":true}"
                    })],
                    tool_calls: vec![beaverki_models::ModelToolCall {
                        call_id: "call_2".to_owned(),
                        name: "household_schedule_message".to_owned(),
                        arguments: json!({
                            "delivery_id": Value::Null,
                            "recipient": "Casey",
                            "message": "Buy kiwis.",
                            "deliver_at": "2099-01-01T09:00:00Z",
                            "cron_expr": Value::Null,
                            "window_start_at": Value::Null,
                            "window_end_at": Value::Null,
                            "enabled": true
                        }),
                    }],
                    output_text: String::new(),
                    usage: None,
                },
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "message",
                        "content": [{ "type": "output_text", "text": "I scheduled that reminder for Casey." }]
                    })],
                    tool_calls: vec![],
                    output_text: "I scheduled that reminder for Casey.".to_owned(),
                    usage: None,
                },
            ],
            Some("discord-token".to_owned()),
        )
        .await;
        let recipient = runtime
            .create_user("Casey", &[String::from("adult")])
            .await
            .expect("create Casey");
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-casey",
                None,
                &recipient.user_id,
                "authenticated_message",
            )
            .await
            .expect("map Casey");

        let first = runtime
            .run_objective(
                None,
                "Remind Casey to buy kiwis tomorrow morning.",
                MemoryScope::Private,
            )
            .await
            .expect("first run");
        assert_eq!(first.task.state, TaskState::WaitingApproval.as_str());

        let approval = runtime
            .list_approvals(None, Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");
        let resumed = runtime
            .resolve_approval(None, &approval.approval_id, true)
            .await
            .expect("approve");
        assert_eq!(resumed.state, TaskState::Completed.as_str());

        let deliveries = runtime
            .db
            .list_scheduled_household_deliveries_for_requester(&runtime.default_user.user_id)
            .await
            .expect("scheduled deliveries");
        assert_eq!(deliveries.len(), 1);
        let delivery = &deliveries[0];
        assert_eq!(delivery.delivery_mode, "scheduled_once");
        let scheduled_task_id = delivery
            .materialized_task_id
            .as_deref()
            .expect("scheduled task id");

        runtime
            .db
            .update_task_wake_at(scheduled_task_id, Some(&now_rfc3339()))
            .await
            .expect("wake task");
        let direct_send_calls = Arc::new(Mutex::new(Vec::new()));
        discord::set_test_direct_send_capture(Some(Arc::clone(&direct_send_calls)));

        let executed = runtime
            .execute_next_runnable_task()
            .await
            .expect("execute next")
            .expect("scheduled task");

        discord::set_test_direct_send_capture(None);

        assert_eq!(executed.task.state, TaskState::Completed.as_str());
        let sent = runtime
            .db
            .fetch_household_delivery(&delivery.delivery_id)
            .await
            .expect("fetch delivery")
            .expect("delivery");
        assert_eq!(sent.status, "sent");
        assert_eq!(sent.recipient_user_id, recipient.user_id);

        let calls = direct_send_calls.lock().expect("captured sends");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains("Buy kiwis."));
    }

    #[tokio::test]
    async fn scheduled_household_delivery_can_send_to_requester_when_route_exists() {
        let _direct_send_guard = direct_send_test_guard();
        let (_tempdir, runtime) = test_runtime_with_discord_token(
            vec![
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "household_schedule_message",
                        "arguments": "{\"delivery_id\":null,\"recipient\":\"Alex\",\"message\":\"Take vitamins.\",\"deliver_at\":\"2099-01-01T09:00:00Z\",\"cron_expr\":null,\"window_start_at\":null,\"window_end_at\":null,\"enabled\":true}"
                    })],
                    tool_calls: vec![beaverki_models::ModelToolCall {
                        call_id: "call_1".to_owned(),
                        name: "household_schedule_message".to_owned(),
                        arguments: json!({
                            "delivery_id": Value::Null,
                            "recipient": "Alex",
                            "message": "Take vitamins.",
                            "deliver_at": "2099-01-01T09:00:00Z",
                            "cron_expr": Value::Null,
                            "window_start_at": Value::Null,
                            "window_end_at": Value::Null,
                            "enabled": true
                        }),
                    }],
                    output_text: String::new(),
                    usage: None,
                },
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "function_call",
                        "call_id": "call_2",
                        "name": "household_schedule_message",
                        "arguments": "{\"delivery_id\":null,\"recipient\":\"Alex\",\"message\":\"Take vitamins.\",\"deliver_at\":\"2099-01-01T09:00:00Z\",\"cron_expr\":null,\"window_start_at\":null,\"window_end_at\":null,\"enabled\":true}"
                    })],
                    tool_calls: vec![beaverki_models::ModelToolCall {
                        call_id: "call_2".to_owned(),
                        name: "household_schedule_message".to_owned(),
                        arguments: json!({
                            "delivery_id": Value::Null,
                            "recipient": "Alex",
                            "message": "Take vitamins.",
                            "deliver_at": "2099-01-01T09:00:00Z",
                            "cron_expr": Value::Null,
                            "window_start_at": Value::Null,
                            "window_end_at": Value::Null,
                            "enabled": true
                        }),
                    }],
                    output_text: String::new(),
                    usage: None,
                },
                ModelTurnResponse {
                    output_items: vec![json!({
                        "type": "message",
                        "content": [{ "type": "output_text", "text": "I scheduled that reminder for you." }]
                    })],
                    tool_calls: vec![],
                    output_text: "I scheduled that reminder for you.".to_owned(),
                    usage: None,
                },
            ],
            Some("discord-token".to_owned()),
        )
        .await;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-alex",
                None,
                &runtime.default_user.user_id,
                "authenticated_message",
            )
            .await
            .expect("map Alex");

        let first = runtime
            .run_objective(
                None,
                "Remind me to take vitamins tomorrow morning.",
                MemoryScope::Private,
            )
            .await
            .expect("first run");
        assert_eq!(first.task.state, TaskState::WaitingApproval.as_str());

        let approval = runtime
            .list_approvals(None, Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");
        let resumed = runtime
            .resolve_approval(None, &approval.approval_id, true)
            .await
            .expect("approve");
        assert_eq!(resumed.state, TaskState::Completed.as_str());

        let deliveries = runtime
            .db
            .list_scheduled_household_deliveries_for_requester(&runtime.default_user.user_id)
            .await
            .expect("scheduled deliveries");
        assert_eq!(deliveries.len(), 1);
        let delivery = &deliveries[0];
        let scheduled_task_id = delivery
            .materialized_task_id
            .as_deref()
            .expect("scheduled task id");

        runtime
            .db
            .update_task_wake_at(scheduled_task_id, Some(&now_rfc3339()))
            .await
            .expect("wake task");
        let direct_send_calls = Arc::new(Mutex::new(Vec::new()));
        discord::set_test_direct_send_capture(Some(Arc::clone(&direct_send_calls)));

        let executed = runtime
            .execute_next_runnable_task()
            .await
            .expect("execute next")
            .expect("scheduled task");

        discord::set_test_direct_send_capture(None);

        assert_eq!(executed.task.state, TaskState::Completed.as_str());
        let sent = runtime
            .db
            .fetch_household_delivery(&delivery.delivery_id)
            .await
            .expect("fetch delivery")
            .expect("delivery");
        assert_eq!(sent.status, "sent");
        assert_eq!(sent.recipient_user_id, runtime.default_user.user_id);

        let calls = direct_send_calls.lock().expect("captured sends");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains("recipient=discord-alex"));
        assert!(calls[0].contains("Take vitamins."));
    }

    #[tokio::test]
    async fn recurring_household_delivery_revalidates_requester_roles_at_send_time() {
        let _direct_send_guard = direct_send_test_guard();
        let (_tempdir, runtime) =
            test_runtime_with_discord_token(vec![], Some("discord-token".to_owned())).await;
        let requester = runtime
            .create_user("Sam", &[String::from("child")])
            .await
            .expect("create requester");
        let recipient = runtime
            .create_user("Casey", &[String::from("adult")])
            .await
            .expect("create recipient");
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-casey",
                None,
                &recipient.user_id,
                "authenticated_message",
            )
            .await
            .expect("map Casey");
        let template = runtime
            .db
            .create_or_reuse_household_delivery(NewHouseholdDelivery {
                task_id: "task_seed",
                requester_user_id: &requester.user_id,
                requester_identity_id: "cli:sam",
                recipient_user_id: &recipient.user_id,
                message_text: "This should be blocked later.",
                delivery_mode: "scheduled_recurring",
                connector_type: "discord",
                connector_identity_id: "discord-casey",
                target_ref: "discord-casey",
                fallback_target_ref: None,
                dedupe_key: "schedule-template",
                initial_status: "scheduled",
                parent_delivery_id: None,
                schedule_id: None,
                scheduled_for_at: None,
                window_starts_at: None,
                window_ends_at: None,
                recurrence_rule: Some("0/5 * * * * * *"),
                scheduled_job_state: Some("scheduled"),
                materialized_task_id: None,
            })
            .await
            .expect("template");
        let scheduled_for = now_rfc3339();
        runtime
            .db
            .create_schedule(NewSchedule {
                schedule_id: Some("sched_household_blocked"),
                owner_user_id: &requester.user_id,
                target_type: "household_delivery",
                target_id: &template.delivery_id,
                cron_expr: "0/5 * * * * * *",
                enabled: true,
                next_run_at: &scheduled_for,
            })
            .await
            .expect("schedule");
        let direct_send_calls = Arc::new(Mutex::new(Vec::new()));
        discord::set_test_direct_send_capture(Some(Arc::clone(&direct_send_calls)));

        let tasks = runtime
            .materialize_due_schedules()
            .await
            .expect("materialize");
        assert_eq!(tasks.len(), 1);
        let result = runtime
            .execute_task(tasks[0].clone())
            .await
            .expect("execute scheduled task");

        discord::set_test_direct_send_capture(None);

        assert_eq!(result.task.state, TaskState::Failed.as_str());
        let occurrence = runtime
            .db
            .fetch_household_delivery_by_dedupe_key(&format!(
                "schedule:{}:{}",
                "sched_household_blocked", scheduled_for,
            ))
            .await
            .expect("occurrence")
            .expect("occurrence row");
        assert_eq!(occurrence.status, "failed");
        assert!(
            occurrence
                .failure_reason
                .as_deref()
                .expect("failure reason")
                .contains("no longer allowed")
        );
        let calls = direct_send_calls.lock().expect("captured sends");
        assert!(calls.is_empty());
    }

    #[tokio::test]
    async fn household_direct_delivery_denies_child_sender() {
        let (_tempdir, runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "household_send_message",
                    "arguments": "{\"recipient\":\"Alex\",\"message\":\"Dinner is ready.\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "household_send_message".to_owned(),
                    arguments: json!({
                        "recipient": "Alex",
                        "message": "Dinner is ready."
                    }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "I cannot send that message." }]
                })],
                tool_calls: vec![],
                output_text: "I cannot send that message.".to_owned(),
                usage: None,
            },
        ])
        .await;
        let child = runtime
            .create_user("Casey", &[String::from("child")])
            .await
            .expect("create child");

        let result = runtime
            .run_objective(
                Some(&child.user_id),
                "Tell Alex dinner is ready.",
                MemoryScope::Private,
            )
            .await
            .expect("run objective as child");

        let deliveries = runtime
            .db
            .list_household_deliveries_for_task(&result.task.task_id)
            .await
            .expect("deliveries");
        assert!(deliveries.is_empty());

        let inspection = runtime
            .inspect_task(Some(&child.user_id), &result.task.task_id)
            .await
            .expect("inspect task");
        assert!(
            inspection
                .tool_invocations
                .iter()
                .any(
                    |invocation| invocation.tool_name == "household_send_message"
                        && invocation.status == "denied"
                )
        );

        let audit = runtime.db.list_audit_events(8).await.expect("audit events");
        let denial = audit
            .into_iter()
            .find(|event| event.event_type == "household_delivery_denied")
            .expect("denial audit event");
        let payload: Value = serde_json::from_str(&denial.payload_json).expect("payload");
        assert_eq!(payload["reason"], json!("sender_not_allowed"));
        assert_eq!(payload["owner_user_id"], json!(child.user_id));
    }

    #[tokio::test]
    async fn discord_message_from_allowlisted_channel_enqueues_task() {
        let (_tempdir, mut runtime) = test_runtime(vec![ModelTurnResponse {
            output_items: vec![json!({
                "type": "message",
                "content": [{ "type": "output_text", "text": "discord done" }]
            })],
            tool_calls: vec![],
            output_text: "discord done".to_owned(),
            usage: None,
        }])
        .await;
        runtime.config.integrations.discord.allowed_channels = vec![DiscordAllowedChannel {
            channel_id: "channel-1".to_owned(),
            mode: DiscordChannelMode::Household,
        }];
        let mapping = runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-1",
                Some("channel-1"),
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");

        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-1".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "channel-1".to_owned(),
                message_id: "msg-1".to_owned(),
                content: "!bk Summarize the allowlisted channel request".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("connector message");

        assert!(reply.accepted);
        let reply_text = reply.reply.as_deref().expect("reply");
        assert!(reply_text.contains("discord done"));
        assert!(!reply_text.contains(&mapping.identity_id));
        assert!(!reply_text.starts_with("Task "));

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn discord_command_in_non_allowlisted_channel_is_audited_as_trigger_attempt() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let db = runtime.db.clone();
        let daemon = RuntimeDaemon::new(runtime);

        let reply = daemon
            .handle_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-unlisted".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "channel-unlisted".to_owned(),
                message_id: "msg-unlisted-1".to_owned(),
                content: "!bk Summarize the latest task activity".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("connector message");

        assert!(!reply.accepted);
        assert_eq!(reply.reply, None);

        let ignored_event = db
            .list_audit_events(8)
            .await
            .expect("audit events")
            .into_iter()
            .find(|event| event.event_type == "connector_message_ignored")
            .expect("ignored audit event");
        let payload: Value = serde_json::from_str(&ignored_event.payload_json).expect("payload");
        assert_eq!(payload["reason"], json!("channel_not_allowlisted"));
        assert_eq!(payload["channel_id"], json!("channel-unlisted"));
        assert_eq!(payload["attempted_trigger"], json!(true));
    }

    #[tokio::test]
    async fn discord_context_carries_recent_history_across_channels_for_same_user() {
        let (_tempdir, mut runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "channel reply" }]
                })],
                tool_calls: vec![],
                output_text: "channel reply".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "dm reply" }]
                })],
                tool_calls: vec![],
                output_text: "dm reply".to_owned(),
                usage: None,
            },
        ])
        .await;
        runtime.config.integrations.discord.allowed_channels = vec![DiscordAllowedChannel {
            channel_id: "channel-1".to_owned(),
            mode: DiscordChannelMode::Household,
        }];
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-3",
                Some("channel-1"),
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");

        let db = runtime.db.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let channel_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-3".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "channel-1".to_owned(),
                message_id: "msg-cross-channel-1".to_owned(),
                content: "!bk First channel message".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("channel message");
        assert_eq!(channel_reply.reply.as_deref(), Some("channel reply"));

        let dm_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-3".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-2".to_owned(),
                message_id: "msg-cross-channel-2".to_owned(),
                content: "Follow up in DM".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("dm message");
        assert_eq!(dm_reply.reply.as_deref(), Some("dm reply"));

        let recent_tasks = db
            .list_recent_interactive_tasks_for_owner("user_alex", 2)
            .await
            .expect("recent tasks");
        let latest_task = recent_tasks
            .iter()
            .find(|task| task.objective == "Follow up in DM")
            .expect("latest dm task");
        let context = latest_task
            .context_summary
            .as_deref()
            .expect("context summary");
        assert!(context.contains("direct-message entrypoints"));
        assert!(!context.contains(
            "[Discord channel | channel channel-1] User (Torlenor): First channel message"
        ));

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn discord_tokenized_approval_message_resolves_pending_task() {
        let (_tempdir, runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
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
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_2".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "approved through discord" }]
                })],
                tool_calls: vec![],
                output_text: "approved through discord".to_owned(),
                usage: None,
            },
        ])
        .await;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-2",
                None,
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");

        let db = runtime.db.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let pending_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-2".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-1".to_owned(),
                message_id: "msg-approval-1".to_owned(),
                content: "Create a directory that needs approval".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("connector message");

        assert!(pending_reply.accepted);
        let pending_reply_text = pending_reply.reply.as_deref().expect("reply");
        assert!(pending_reply_text.contains("Approval required"));
        let approve_token = extract_token_for_command(pending_reply_text, "Approve");

        let approval = db
            .list_approvals_for_user("user_alex", Some("pending"))
            .await
            .expect("approvals")
            .into_iter()
            .next()
            .expect("approval");

        let approval_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-2".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-1".to_owned(),
                message_id: "msg-approval-2".to_owned(),
                content: format!("approve {approve_token}"),
                is_direct_message: true,
            })
            .await
            .expect("approval connector message");

        assert!(approval_reply.accepted);
        assert!(
            approval_reply
                .reply
                .as_deref()
                .expect("approval reply")
                .contains("approved through discord")
        );

        let completed_task = db
            .fetch_task(&approval.task_id)
            .await
            .expect("task fetch")
            .expect("task");
        assert_eq!(completed_task.state, TaskState::Completed.as_str());

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn discord_approval_from_channel_redirects_to_dm_and_dm_inbox_resolves() {
        let (_tempdir, mut runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
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
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_2".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "approved from dm inbox" }]
                })],
                tool_calls: vec![],
                output_text: "approved from dm inbox".to_owned(),
                usage: None,
            },
        ])
        .await;
        runtime.config.integrations.discord.allowed_channels = vec![DiscordAllowedChannel {
            channel_id: "channel-1".to_owned(),
            mode: DiscordChannelMode::Household,
        }];
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-4",
                Some("channel-1"),
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");

        let db = runtime.db.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let channel_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-4".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "channel-1".to_owned(),
                message_id: "msg-approval-channel-1".to_owned(),
                content: "!bk Create a directory that needs approval".to_owned(),
                is_direct_message: false,
            })
            .await
            .expect("channel approval message");
        assert!(channel_reply.accepted);
        assert!(
            channel_reply
                .reply
                .as_deref()
                .expect("channel reply")
                .contains("open a Discord DM")
        );

        let inbox_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-4".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-4".to_owned(),
                message_id: "msg-approval-dm-1".to_owned(),
                content: "approvals".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("dm inbox");
        let inbox_text = inbox_reply.reply.as_deref().expect("inbox reply");
        assert!(inbox_text.contains("Pending approvals"));
        let approve_token = extract_token_for_command(inbox_text, "Approve");

        let approval_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-4".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-4".to_owned(),
                message_id: "msg-approval-dm-2".to_owned(),
                content: format!("approve {approve_token}"),
                is_direct_message: true,
            })
            .await
            .expect("approval from inbox");
        assert_eq!(
            approval_reply.reply.as_deref(),
            Some("approved from dm inbox")
        );

        let approval = db
            .list_approvals_for_user("user_alex", Some("approved"))
            .await
            .expect("approved approvals")
            .into_iter()
            .next()
            .expect("approved approval");
        let completed_task = db
            .fetch_task(&approval.task_id)
            .await
            .expect("task fetch")
            .expect("task");
        assert_eq!(completed_task.state, TaskState::Completed.as_str());

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn discord_approval_token_cannot_be_replayed() {
        let (_tempdir, runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
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
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_2".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "approved through discord" }]
                })],
                tool_calls: vec![],
                output_text: "approved through discord".to_owned(),
                usage: None,
            },
        ])
        .await;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-5",
                None,
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");

        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let pending_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-5".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-5".to_owned(),
                message_id: "msg-replay-1".to_owned(),
                content: "Create a directory that needs approval".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("connector message");
        let approve_token =
            extract_token_for_command(pending_reply.reply.as_deref().expect("reply"), "Approve");

        let approval_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-5".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-5".to_owned(),
                message_id: "msg-replay-2".to_owned(),
                content: format!("approve {approve_token}"),
                is_direct_message: true,
            })
            .await
            .expect("approve once");
        assert_eq!(
            approval_reply.reply.as_deref(),
            Some("approved through discord")
        );

        let replay_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-5".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-5".to_owned(),
                message_id: "msg-replay-3".to_owned(),
                content: format!("approve {approve_token}"),
                is_direct_message: true,
            })
            .await
            .expect("replay");
        assert!(
            replay_reply
                .reply
                .as_deref()
                .expect("replay reply")
                .contains("Could not approve that approval token")
        );

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn discord_approval_token_rejects_mismatched_identity() {
        let (_tempdir, runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"mkdir approved-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "mkdir approved-dir" }),
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
        ])
        .await;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-6",
                None,
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping one");
        let second_user = runtime
            .db
            .create_user("Casey", &["adult".to_owned()])
            .await
            .expect("second user");
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-7",
                None,
                &second_user.user_id,
                "authenticated_message",
            )
            .await
            .expect("mapping two");

        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let pending_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-6".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-6".to_owned(),
                message_id: "msg-mismatch-1".to_owned(),
                content: "Create a directory that needs approval".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("first request");
        let approve_token =
            extract_token_for_command(pending_reply.reply.as_deref().expect("reply"), "Approve");

        let mismatch_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-7".to_owned(),
                external_display_name: Some("Casey".to_owned()),
                channel_id: "dm-7".to_owned(),
                message_id: "msg-mismatch-2".to_owned(),
                content: format!("approve {approve_token}"),
                is_direct_message: true,
            })
            .await
            .expect("mismatch reply");
        assert!(
            mismatch_reply
                .reply
                .as_deref()
                .expect("reply")
                .contains("mapped identity")
        );

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn discord_critical_approval_requires_confirm_token() {
        let (_tempdir, runtime) = test_runtime(vec![
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"rm critical-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_1".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "rm critical-dir" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"verdict\":\"approved\",\"risk_level\":\"critical\",\"findings\":[],\"required_changes\":[],\"summary\":\"The command is acceptable only with explicit confirmation.\"}"
                    }]
                })],
                tool_calls: vec![],
                output_text: "{\"verdict\":\"approved\",\"risk_level\":\"critical\",\"findings\":[],\"required_changes\":[],\"summary\":\"The command is acceptable only with explicit confirmation.\"}".to_owned(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "function_call",
                    "call_id": "call_2",
                    "name": "shell_exec",
                    "arguments": "{\"command\":\"rm critical-dir\"}"
                })],
                tool_calls: vec![beaverki_models::ModelToolCall {
                    call_id: "call_2".to_owned(),
                    name: "shell_exec".to_owned(),
                    arguments: json!({ "command": "rm critical-dir" }),
                }],
                output_text: String::new(),
                usage: None,
            },
            ModelTurnResponse {
                output_items: vec![json!({
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "critical approval completed" }]
                })],
                tool_calls: vec![],
                output_text: "critical approval completed".to_owned(),
                usage: None,
            },
        ])
        .await;
        runtime
            .db
            .upsert_connector_identity(
                "discord",
                "discord-user-8",
                None,
                "user_alex",
                "authenticated_message",
            )
            .await
            .expect("mapping");

        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let mut handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = wait_for_ready_or_report(socket_path, &mut handle).await;

        let pending_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-8".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-8".to_owned(),
                message_id: "msg-critical-1".to_owned(),
                content: "Remove a critical directory".to_owned(),
                is_direct_message: true,
            })
            .await
            .expect("critical request");
        let approve_token =
            extract_token_for_command(pending_reply.reply.as_deref().expect("reply"), "Approve");

        let step_up_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-8".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-8".to_owned(),
                message_id: "msg-critical-2".to_owned(),
                content: format!("approve {approve_token}"),
                is_direct_message: true,
            })
            .await
            .expect("step up");
        let step_up_text = step_up_reply.reply.as_deref().expect("step up reply");
        assert!(step_up_text.contains("second confirmation"));
        let confirm_token = extract_token_for_command(step_up_text, "Confirm");

        let confirm_reply = client
            .submit_connector_message(ConnectorMessageRequest {
                connector_type: "discord".to_owned(),
                external_user_id: "discord-user-8".to_owned(),
                external_display_name: Some("Torlenor".to_owned()),
                channel_id: "dm-8".to_owned(),
                message_id: "msg-critical-3".to_owned(),
                content: format!("confirm {confirm_token}"),
                is_direct_message: true,
            })
            .await
            .expect("confirm critical");
        assert_eq!(
            confirm_reply.reply.as_deref(),
            Some("critical approval completed")
        );

        client.shutdown().await.expect("shutdown request");
        handle.await.expect("join").expect("daemon result");
    }

    #[tokio::test]
    async fn memory_list_respects_visibility_filters() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let private_metadata = json!({ "source_summary": "Alex likes tea." });
        let household_metadata = json!({ "source_summary": "Household Wi-Fi is beaverki-net." });
        runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some("user_alex"),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "preference",
                subject_key: Some("profile.favorite_drink"),
                content_text: "Tea",
                content_json: Some(&private_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-private"),
                task_id: Some("task_private"),
            })
            .await
            .expect("insert private");
        runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: None,
                scope: MemoryScope::Household,
                memory_kind: MemoryKind::Semantic,
                subject_type: "fact",
                subject_key: Some("household.wifi_name"),
                content_text: "beaverki-net",
                content_json: Some(&household_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-household"),
                task_id: Some("task_household"),
            })
            .await
            .expect("insert household");

        let guest = runtime
            .db
            .create_user("Guesty", &[])
            .await
            .expect("guest user");
        let guest_memories = runtime
            .list_memories(Some(&guest.user_id), None, None, false, false, 10)
            .await
            .expect("guest memories");
        assert_eq!(guest_memories.len(), 0);

        let owner_memories = runtime
            .list_memories(Some("user_alex"), None, None, false, false, 10)
            .await
            .expect("owner memories");
        assert_eq!(owner_memories.len(), 2);
    }

    #[tokio::test]
    async fn memory_history_includes_superseded_versions() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let old_metadata = json!({ "source_summary": "User first said coffee." });
        let new_metadata = json!({ "source_summary": "User corrected to tea." });
        let old_id = runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some("user_alex"),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "preference",
                subject_key: Some("profile.favorite_drink"),
                content_text: "Coffee",
                content_json: Some(&old_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                task_id: Some("task_1"),
            })
            .await
            .expect("insert old memory");
        let new_id = runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some("user_alex"),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "preference",
                subject_key: Some("profile.favorite_drink"),
                content_text: "Tea",
                content_json: Some(&new_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-2"),
                task_id: Some("task_2"),
            })
            .await
            .expect("insert new memory");
        runtime
            .db
            .mark_memory_superseded(&old_id, &new_id)
            .await
            .expect("mark superseded");

        let history = runtime
            .memory_history(
                Some("user_alex"),
                "profile.favorite_drink",
                Some("private"),
                Some("preference"),
                10,
            )
            .await
            .expect("history");
        assert_eq!(history.len(), 2);
        assert!(history.iter().any(|memory| memory.memory_id == old_id));
        assert!(history.iter().any(|memory| memory.memory_id == new_id));
    }

    #[tokio::test]
    async fn inspect_memory_rejects_household_memory_for_guest() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let household_metadata = json!({ "source_summary": "Owner shared the alarm code." });
        let household_memory_id = runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: None,
                scope: MemoryScope::Household,
                memory_kind: MemoryKind::Semantic,
                subject_type: "fact",
                subject_key: Some("household.alarm_code"),
                content_text: "2468",
                content_json: Some(&household_metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                task_id: Some("task_household"),
            })
            .await
            .expect("insert household memory");
        let guest = runtime
            .db
            .create_user("Guesty", &[])
            .await
            .expect("guest user");

        let error = runtime
            .inspect_memory(Some(&guest.user_id), &household_memory_id)
            .await
            .expect_err("guest should not inspect household memory");
        assert!(error.to_string().contains("not visible"));
    }

    #[tokio::test]
    async fn forget_memory_hides_entry_from_active_retrieval() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let metadata = json!({ "source_summary": "User first gave the wrong Wi-Fi password." });
        let memory_id = runtime
            .db
            .insert_memory(beaverki_db::NewMemory {
                owner_user_id: Some("user_alex"),
                scope: MemoryScope::Private,
                memory_kind: MemoryKind::Semantic,
                subject_type: "fact",
                subject_key: Some("profile.wifi_password"),
                content_text: "wrong-password",
                content_json: Some(&metadata),
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                task_id: Some("task_wrong"),
            })
            .await
            .expect("insert memory");

        let forgotten = runtime
            .forget_memory(
                Some("user_alex"),
                &memory_id,
                "User clarified that this was not the password.",
            )
            .await
            .expect("forget memory");
        assert!(forgotten.memory.forgotten_at.is_some());

        let active = runtime
            .list_memories(Some("user_alex"), None, None, false, false, 10)
            .await
            .expect("list active memories");
        assert!(!active.iter().any(|memory| memory.memory_id == memory_id));

        let history = runtime
            .memory_history(
                Some("user_alex"),
                "profile.wifi_password",
                Some("private"),
                Some("fact"),
                10,
            )
            .await
            .expect("memory history");
        assert!(history.iter().any(|memory| memory.memory_id == memory_id));
        let audit_events = runtime.db.list_audit_events(8).await.expect("audit events");
        assert!(
            audit_events
                .iter()
                .any(|event| event.event_type == "memory_forgotten")
        );
    }
}
