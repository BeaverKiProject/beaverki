use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use beaverki_agent::{AgentMemoryMode, AgentRequest, AgentResult, PrimaryAgentRunner};
use beaverki_automation as automation;
use beaverki_config::{
    LoadedConfig, SecretStore, SessionLifecycleAction, SessionLifecyclePolicy,
    SessionPolicyMatchInput, select_session_lifecycle_policy,
};
use beaverki_core::{MemoryKind, MemoryScope, TaskState, now_rfc3339};
use beaverki_db::{
    ApprovalActionRow, ApprovalActionSet, ApprovalRow, BootstrapState, ConnectorIdentityRow,
    ConversationSessionRow, Database, IssueApprovalActions, MemoryRow, NewConversationSession,
    NewSchedule, NewScript, NewScriptReview, NewTask, RoleRow, RuntimeHeartbeatRow,
    RuntimeSessionRow, ScheduleRow, ScriptReviewRow, ScriptRow, TaskEventRow, TaskRow,
    ToolInvocationRow, UserRoleRow, UserRow,
};
use beaverki_memory::MemoryStore;
use beaverki_models::{ModelProvider, OpenAiProvider};
use beaverki_policy::{can_grant_approvals, can_write_household_memory, visible_memory_scopes};
use beaverki_tools::{ToolContext, builtin_registry};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, RwLock};
use tokio::time::{self, Instant};
use tracing::warn;

mod discord;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const QUEUE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const TASK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(150);
const CONNECTOR_MESSAGE_CONTEXT_EVENT: &str = "connector_message_context";
const CLI_CONVERSATION_HISTORY_LIMIT: i64 = 4;
const CLI_ACTIVE_CONVERSATION_WINDOW_SECS: i64 = 45 * 60;
const SESSION_RESET_COMMAND: &str = "/new";
const SESSION_KIND_CLI: &str = "cli";
const SESSION_KIND_DIRECT_MESSAGE: &str = "direct_message";
const SESSION_KIND_GROUP_ROOM: &str = "group_room";
const SESSION_KIND_CRON_RUN: &str = "cron_run";
const SESSION_AUDIENCE_DIRECT_USER: &str = "direct_user";
const SESSION_AUDIENCE_SHARED_ROOM: &str = "shared_room";
const SESSION_AUDIENCE_SCHEDULED_RUN: &str = "scheduled_run";
const SESSION_LIFECYCLE_REASON_MANUAL_RESET: &str = "manual_reset";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInspection {
    pub task: TaskRow,
    pub events: Vec<TaskEventRow>,
    pub tool_invocations: Vec<ToolInvocationRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptInspection {
    pub script: ScriptRow,
    pub reviews: Vec<ScriptReviewRow>,
    pub schedules: Vec<ScheduleRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInspection {
    pub memory: MemoryRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLifecycleExecution {
    pub session_id: String,
    pub policy_id: String,
    pub action: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteApprovalActionOutcome {
    Inspection {
        action: ApprovalActionRow,
        approval: ApprovalRow,
    },
    StepUpRequired {
        action: ApprovalActionRow,
        approval: ApprovalRow,
        confirm_action: ApprovalActionRow,
    },
    Resolved {
        action: ApprovalActionRow,
        task: TaskRow,
    },
}

pub struct Runtime {
    config: LoadedConfig,
    db: Database,
    default_user: UserRow,
    provider: Arc<dyn ModelProvider>,
    runner: PrimaryAgentRunner,
    discord_bot_token: Option<String>,
}

impl Runtime {
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
        tool_context.browser_interactive_launcher =
            config.integrations.browser.interactive_launcher.clone();
        tool_context.browser_headless_program =
            config.integrations.browser.headless_browser.clone();
        tool_context.browser_headless_args = config.integrations.browser.headless_args.clone();
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            memory,
            provider.clone(),
            builtin_registry(),
            tool_context,
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

    pub async fn create_schedule(
        &self,
        user_id: Option<&str>,
        schedule_id: Option<&str>,
        script_id: &str,
        cron_expr: &str,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        let user = self.resolve_user(user_id).await?;
        let script = self
            .db
            .fetch_script_for_owner(&user.user_id, script_id)
            .await?
            .ok_or_else(|| anyhow!("script '{script_id}' not found"))?;
        if script.status != "active" {
            bail!(
                "script '{}' must be active before scheduling",
                script.script_id
            );
        }
        let next_run_at = automation::next_run_after(cron_expr, &now_rfc3339())?;
        let schedule = self
            .db
            .create_schedule(NewSchedule {
                schedule_id,
                owner_user_id: &user.user_id,
                target_type: "lua_script",
                target_id: &script.script_id,
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
                    "script_id": script.script_id,
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
            automation::next_run_after(&schedule.cron_expr, &now_rfc3339())?
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
            let next_run_at = automation::next_run_after(&schedule.cron_expr, &now)?;
            let Some(script) = self.db.fetch_script(&schedule.target_id).await? else {
                self.db
                    .update_schedule_state(
                        &schedule.schedule_id,
                        automation::schedule_enabled(&schedule),
                        &next_run_at,
                        Some(&now),
                    )
                    .await?;
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
                    .update_schedule_state(
                        &schedule.schedule_id,
                        automation::schedule_enabled(&schedule),
                        &next_run_at,
                        Some(&now),
                    )
                    .await?;
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
            self.db
                .update_schedule_state(
                    &schedule.schedule_id,
                    automation::schedule_enabled(&schedule),
                    &next_run_at,
                    Some(&now),
                )
                .await?;
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
        Ok(tasks)
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
            (
                SESSION_KIND_GROUP_ROOM,
                format!("{SESSION_KIND_GROUP_ROOM}:{connector_type}:{channel_id}"),
                SESSION_AUDIENCE_SHARED_ROOM,
                MemoryScope::Household,
            )
        };
        self.db
            .ensure_conversation_session(NewConversationSession {
                session_kind,
                session_key: &session_key,
                audience_policy,
                max_memory_scope,
                originating_connector_type: Some(connector_type),
                originating_connector_target: Some(channel_id),
            })
            .await
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
        let memory_mode = if task.kind == "subagent" {
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

fn approval_action_is_expired(action: &ApprovalActionRow) -> bool {
    DateTime::parse_from_rfc3339(&action.expires_at)
        .map(|expires_at| expires_at.with_timezone(&Utc) <= Utc::now())
        .unwrap_or(false)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub session_id: String,
    pub instance_id: String,
    pub state: String,
    pub pid: u32,
    pub socket_path: String,
    pub queue_depth: i64,
    pub active_task_id: Option<String>,
    pub automation_planning_enabled: bool,
    pub started_at: String,
    pub last_heartbeat_at: Option<String>,
    pub stopped_at: Option<String>,
    pub last_error: Option<String>,
}

impl DaemonStatus {
    fn initial(instance_id: &str, socket_path: &Path) -> Self {
        Self {
            session_id: String::new(),
            instance_id: instance_id.to_owned(),
            state: "starting".to_owned(),
            pid: std::process::id(),
            socket_path: socket_path.display().to_string(),
            queue_depth: 0,
            active_task_id: None,
            automation_planning_enabled: false,
            started_at: String::new(),
            last_heartbeat_at: None,
            stopped_at: None,
            last_error: None,
        }
    }

    fn from_row(row: RuntimeSessionRow) -> Self {
        Self {
            session_id: row.session_id,
            instance_id: row.instance_id,
            state: row.state,
            pid: row.pid as u32,
            socket_path: row.socket_path,
            queue_depth: row.queue_depth,
            active_task_id: row.active_task_id,
            automation_planning_enabled: row.automation_planning_enabled != 0,
            started_at: row.started_at,
            last_heartbeat_at: row.last_heartbeat_at,
            stopped_at: row.stopped_at,
            last_error: row.last_error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonRequest {
    Ping,
    Status,
    RunTask {
        user_id: Option<String>,
        objective: String,
        scope: String,
        wait: bool,
    },
    ShowTask {
        user_id: Option<String>,
        task_id: String,
    },
    ListMemories {
        user_id: Option<String>,
        scope: Option<String>,
        kind: Option<String>,
        include_superseded: bool,
        include_forgotten: bool,
        limit: i64,
    },
    ShowMemory {
        user_id: Option<String>,
        memory_id: String,
    },
    MemoryHistory {
        user_id: Option<String>,
        subject_key: String,
        scope: Option<String>,
        subject_type: Option<String>,
        limit: i64,
    },
    ForgetMemory {
        user_id: Option<String>,
        memory_id: String,
        reason: String,
    },
    ListApprovals {
        user_id: Option<String>,
        status: Option<String>,
    },
    ResolveApproval {
        user_id: Option<String>,
        approval_id: String,
        approve: bool,
    },
    SubmitConnectorMessage {
        message: ConnectorMessageRequest,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonResponse {
    Pong { status: DaemonStatus },
    Status { status: DaemonStatus },
    Task { task: TaskRow },
    Inspection { inspection: TaskInspection },
    Memory { memory: MemoryInspection },
    Memories { memories: Vec<MemoryRow> },
    Approvals { approvals: Vec<ApprovalRow> },
    ConnectorReply { reply: ConnectorMessageReply },
    ShutdownAck { status: DaemonStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMessageRequest {
    pub connector_type: String,
    pub external_user_id: String,
    pub external_display_name: Option<String>,
    pub channel_id: String,
    pub message_id: String,
    pub content: String,
    pub is_direct_message: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMessageReply {
    pub accepted: bool,
    pub reply: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum DaemonEnvelope {
    Ok { response: DaemonResponse },
    Error { message: String },
}

#[derive(Clone)]
pub struct DaemonClient {
    socket_path: PathBuf,
}

impl DaemonClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn ping(&self) -> Result<DaemonStatus> {
        match self.request(DaemonRequest::Ping).await? {
            DaemonResponse::Pong { status } => Ok(status),
            other => Err(anyhow!("unexpected daemon ping response: {other:?}")),
        }
    }

    pub async fn status(&self) -> Result<DaemonStatus> {
        match self.request(DaemonRequest::Status).await? {
            DaemonResponse::Status { status } => Ok(status),
            other => Err(anyhow!("unexpected daemon status response: {other:?}")),
        }
    }

    pub async fn run_task(
        &self,
        user_id: Option<String>,
        objective: String,
        scope: String,
        wait: bool,
    ) -> Result<TaskRow> {
        match self
            .request(DaemonRequest::RunTask {
                user_id,
                objective,
                scope,
                wait,
            })
            .await?
        {
            DaemonResponse::Task { task } => Ok(task),
            other => Err(anyhow!("unexpected daemon task response: {other:?}")),
        }
    }

    pub async fn show_task(
        &self,
        user_id: Option<String>,
        task_id: String,
    ) -> Result<TaskInspection> {
        match self
            .request(DaemonRequest::ShowTask { user_id, task_id })
            .await?
        {
            DaemonResponse::Inspection { inspection } => Ok(inspection),
            other => Err(anyhow!("unexpected daemon inspection response: {other:?}")),
        }
    }

    pub async fn list_memories(
        &self,
        user_id: Option<String>,
        scope: Option<String>,
        kind: Option<String>,
        include_superseded: bool,
        include_forgotten: bool,
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        match self
            .request(DaemonRequest::ListMemories {
                user_id,
                scope,
                kind,
                include_superseded,
                include_forgotten,
                limit,
            })
            .await?
        {
            DaemonResponse::Memories { memories } => Ok(memories),
            other => Err(anyhow!("unexpected daemon memories response: {other:?}")),
        }
    }

    pub async fn show_memory(
        &self,
        user_id: Option<String>,
        memory_id: String,
    ) -> Result<MemoryInspection> {
        match self
            .request(DaemonRequest::ShowMemory { user_id, memory_id })
            .await?
        {
            DaemonResponse::Memory { memory } => Ok(memory),
            other => Err(anyhow!("unexpected daemon memory response: {other:?}")),
        }
    }

    pub async fn memory_history(
        &self,
        user_id: Option<String>,
        subject_key: String,
        scope: Option<String>,
        subject_type: Option<String>,
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        match self
            .request(DaemonRequest::MemoryHistory {
                user_id,
                subject_key,
                scope,
                subject_type,
                limit,
            })
            .await?
        {
            DaemonResponse::Memories { memories } => Ok(memories),
            other => Err(anyhow!(
                "unexpected daemon memory history response: {other:?}"
            )),
        }
    }

    pub async fn forget_memory(
        &self,
        user_id: Option<String>,
        memory_id: String,
        reason: String,
    ) -> Result<MemoryInspection> {
        match self
            .request(DaemonRequest::ForgetMemory {
                user_id,
                memory_id,
                reason,
            })
            .await?
        {
            DaemonResponse::Memory { memory } => Ok(memory),
            other => Err(anyhow!(
                "unexpected daemon forget memory response: {other:?}"
            )),
        }
    }

    pub async fn list_approvals(
        &self,
        user_id: Option<String>,
        status: Option<String>,
    ) -> Result<Vec<ApprovalRow>> {
        match self
            .request(DaemonRequest::ListApprovals { user_id, status })
            .await?
        {
            DaemonResponse::Approvals { approvals } => Ok(approvals),
            other => Err(anyhow!("unexpected daemon approvals response: {other:?}")),
        }
    }

    pub async fn resolve_approval(
        &self,
        user_id: Option<String>,
        approval_id: String,
        approve: bool,
    ) -> Result<TaskRow> {
        match self
            .request(DaemonRequest::ResolveApproval {
                user_id,
                approval_id,
                approve,
            })
            .await?
        {
            DaemonResponse::Task { task } => Ok(task),
            other => Err(anyhow!("unexpected daemon approval response: {other:?}")),
        }
    }

    pub async fn shutdown(&self) -> Result<DaemonStatus> {
        match self.request(DaemonRequest::Shutdown).await? {
            DaemonResponse::ShutdownAck { status } => Ok(status),
            other => Err(anyhow!("unexpected daemon shutdown response: {other:?}")),
        }
    }

    pub async fn submit_connector_message(
        &self,
        message: ConnectorMessageRequest,
    ) -> Result<ConnectorMessageReply> {
        match self
            .request(DaemonRequest::SubmitConnectorMessage { message })
            .await?
        {
            DaemonResponse::ConnectorReply { reply } => Ok(reply),
            other => Err(anyhow!(
                "unexpected daemon connector reply response: {other:?}"
            )),
        }
    }

    pub async fn request(&self, request: DaemonRequest) -> Result<DaemonResponse> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| format!("failed to connect to {}", self.socket_path.display()))?;
        let request_line =
            serde_json::to_string(&request).context("failed to encode daemon request")?;
        stream
            .write_all(request_line.as_bytes())
            .await
            .context("failed to write daemon request")?;
        stream
            .write_all(b"\n")
            .await
            .context("failed to finalize daemon request")?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .context("failed to read daemon response")?;
        if line.trim().is_empty() {
            bail!("daemon closed the connection without a response");
        }

        match serde_json::from_str::<DaemonEnvelope>(&line)
            .context("failed to decode daemon response")?
        {
            DaemonEnvelope::Ok { response } => Ok(response),
            DaemonEnvelope::Error { message } => Err(anyhow!(message)),
        }
    }

    pub async fn is_reachable(socket_path: impl Into<PathBuf>) -> bool {
        let client = Self::new(socket_path);
        client.ping().await.is_ok()
    }

    pub async fn wait_until_ready(
        socket_path: impl Into<PathBuf>,
        timeout: Duration,
    ) -> Result<Self> {
        let client = Self::new(socket_path);
        let deadline = Instant::now() + timeout;
        let last_error = loop {
            match client.ping().await {
                Ok(_) => return Ok(client),
                Err(error) if Instant::now() >= deadline => break error.to_string(),
                Err(_) => {}
            }
            time::sleep(Duration::from_millis(150)).await;
        };
        bail!(
            "timed out waiting for daemon at {} (last ping error: {})",
            client.socket_path.display(),
            last_error
        );
    }
}

pub struct RuntimeDaemon {
    runtime: Arc<Runtime>,
    socket_path: PathBuf,
    shutdown: Arc<Notify>,
    wake_worker: Arc<Notify>,
    status: Arc<RwLock<DaemonStatus>>,
}

impl RuntimeDaemon {
    pub fn new(runtime: Runtime) -> Self {
        let socket_path = runtime.daemon_socket_path();
        let status = DaemonStatus::initial(&runtime.config.runtime.instance_id, &socket_path);
        Self {
            runtime: Arc::new(runtime),
            socket_path,
            shutdown: Arc::new(Notify::new()),
            wake_worker: Arc::new(Notify::new()),
            status: Arc::new(RwLock::new(status)),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub async fn run_until<F>(self, shutdown_signal: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        self.prepare_socket().await?;

        let listener = UnixListener::bind(&self.socket_path)
            .with_context(|| format!("failed to bind {}", self.socket_path.display()))?;

        let session_row = self
            .runtime
            .db
            .begin_runtime_session(
                &self.runtime.config.runtime.instance_id,
                i64::from(std::process::id()),
                &self.socket_path.display().to_string(),
                false,
            )
            .await?;
        let session_id = session_row.session_id.clone();
        {
            let mut status = self.status.write().await;
            *status = DaemonStatus::from_row(session_row);
        }
        self.runtime
            .db
            .record_audit_event(
                "runtime",
                &session_id,
                "daemon_started",
                json!({
                    "instance_id": self.runtime.config.runtime.instance_id,
                    "socket_path": self.socket_path.display().to_string(),
                    "pid": std::process::id(),
                }),
            )
            .await?;
        self.refresh_heartbeat(None).await?;

        let daemon = Arc::new(self);
        let worker = tokio::spawn(Self::worker_loop(Arc::clone(&daemon)));
        let heartbeat = tokio::spawn(Self::heartbeat_loop(Arc::clone(&daemon)));
        let cleanup = daemon.start_session_cleanup_loop();
        let connector = daemon.start_connector_loop();
        let accept = tokio::spawn(Self::accept_loop(
            Arc::clone(&daemon),
            listener,
            shutdown_signal,
        ));

        let accept_result = accept.await.context("daemon accept loop join failure")?;
        daemon.shutdown.notify_waiters();
        let worker_result = worker.await.context("daemon worker loop join failure")?;
        let heartbeat_result = heartbeat
            .await
            .context("daemon heartbeat loop join failure")?;
        let cleanup_result = if let Some(cleanup) = cleanup {
            Some(
                cleanup
                    .await
                    .context("daemon session cleanup loop join failure")?,
            )
        } else {
            None
        };
        let connector_result = if let Some(connector) = connector {
            Some(
                connector
                    .await
                    .context("daemon connector loop join failure")?,
            )
        } else {
            None
        };

        let final_error = accept_result
            .err()
            .or(worker_result.err())
            .or(heartbeat_result.err())
            .or_else(|| cleanup_result.and_then(Result::err))
            .or_else(|| connector_result.and_then(Result::err));
        daemon
            .finish(final_error.as_ref().map(ToString::to_string))
            .await?;

        if let Some(error) = final_error {
            return Err(error);
        }
        Ok(())
    }

    async fn accept_loop<F>(
        daemon: Arc<Self>,
        listener: UnixListener,
        shutdown_signal: F,
    ) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::pin!(shutdown_signal);
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return Ok(()),
                _ = &mut shutdown_signal => {
                    daemon.shutdown.notify_waiters();
                    return Ok(());
                }
                accept_result = listener.accept() => {
                    let (stream, _) = accept_result.context("failed to accept daemon client")?;
                    let daemon = Arc::clone(&daemon);
                    tokio::spawn(async move {
                        if let Err(error) = daemon.handle_connection(stream).await {
                            warn!("daemon connection failed: {error:#}");
                        }
                    });
                }
            }
        }
    }

    async fn worker_loop(daemon: Arc<Self>) -> Result<()> {
        let mut interval = time::interval(QUEUE_POLL_INTERVAL);
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return Ok(()),
                _ = daemon.wake_worker.notified() => {},
                _ = interval.tick() => {},
            }

            loop {
                if daemon.shutdown_notified().await {
                    return Ok(());
                }

                let _ = daemon.runtime.materialize_due_schedules().await?;
                let next_task = daemon.runtime.db.fetch_next_runnable_task().await?;
                let Some(task) = next_task else {
                    break;
                };
                daemon.set_active_task(Some(task.task_id.clone())).await?;
                let task_id = task.task_id.clone();
                let owner_user_id = task.owner_user_id.clone();
                let run_result = daemon.runtime.execute_task(task).await;
                daemon.set_active_task(None).await?;

                if let Ok(result) = &run_result {
                    daemon.dispatch_connector_follow_up(&result.task).await?;
                }

                if let Err(error) = run_result {
                    let error_text = format!("{error:#}");
                    daemon.runtime.db.fail_task(&task_id, &error_text).await?;
                    daemon
                        .runtime
                        .db
                        .append_task_event(
                            &task_id,
                            "task_failed",
                            "runtime",
                            &daemon.current_session_id().await,
                            json!({ "reason": error_text }),
                        )
                        .await?;
                    daemon
                        .runtime
                        .db
                        .record_audit_event(
                            "runtime",
                            &daemon.current_session_id().await,
                            "task_execution_failed",
                            json!({
                                "task_id": task_id,
                                "owner_user_id": owner_user_id,
                                "error": error.to_string(),
                            }),
                        )
                        .await?;
                    if let Some(task) = daemon.runtime.db.fetch_task(&task_id).await? {
                        daemon.dispatch_connector_follow_up(&task).await?;
                    }
                }

                daemon.refresh_heartbeat(None).await?;
            }
        }
    }

    async fn heartbeat_loop(daemon: Arc<Self>) -> Result<()> {
        let mut interval = time::interval(HEARTBEAT_INTERVAL);
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return Ok(()),
                _ = interval.tick() => daemon.refresh_heartbeat(None).await?,
            }
        }
    }

    async fn session_cleanup_loop(daemon: Arc<Self>, interval_secs: u64) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(interval_secs));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return Ok(()),
                _ = interval.tick() => {
                    let actions = daemon.runtime.run_session_lifecycle_cleanup().await?;
                    if !actions.is_empty() {
                        daemon.refresh_heartbeat(None).await?;
                    }
                }
            }
        }
    }

    async fn handle_connection(&self, stream: UnixStream) -> Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .context("failed to read daemon request")?;
        if line.trim().is_empty() {
            bail!("received empty daemon request");
        }

        let request = serde_json::from_str::<DaemonRequest>(&line)
            .context("failed to decode daemon request")?;
        let (envelope, should_shutdown) = match self.handle_request(request).await {
            Ok((response, should_shutdown)) => (DaemonEnvelope::Ok { response }, should_shutdown),
            Err(error) => (
                DaemonEnvelope::Error {
                    message: error.to_string(),
                },
                false,
            ),
        };

        let mut stream = reader.into_inner();
        let response_line =
            serde_json::to_string(&envelope).context("failed to encode daemon response")?;
        stream
            .write_all(response_line.as_bytes())
            .await
            .context("failed to write daemon response")?;
        stream
            .write_all(b"\n")
            .await
            .context("failed to finalize daemon response")?;

        if should_shutdown {
            self.shutdown.notify_waiters();
        }
        Ok(())
    }

    async fn handle_request(&self, request: DaemonRequest) -> Result<(DaemonResponse, bool)> {
        match request {
            DaemonRequest::Ping => Ok((
                DaemonResponse::Pong {
                    status: self.status_snapshot().await,
                },
                false,
            )),
            DaemonRequest::Status => Ok((
                DaemonResponse::Status {
                    status: self.status_snapshot().await,
                },
                false,
            )),
            DaemonRequest::RunTask {
                user_id,
                objective,
                scope,
                wait,
            } => {
                let scope = parse_scope(&scope)?;
                let user = self.runtime.resolve_user(user_id.as_deref()).await?;
                let is_reset = is_session_reset_command(&objective);
                let task = if is_reset {
                    self.runtime
                        .reset_cli_conversation_session(&user, &objective, scope)
                        .await?
                } else {
                    self.runtime
                        .enqueue_objective(Some(&user.user_id), &objective, scope)
                        .await?
                };
                if is_reset {
                    self.runtime
                        .db
                        .record_audit_event(
                            "runtime",
                            &self.current_session_id().await,
                            "conversation_session_reset_requested",
                            json!({
                                "task_id": task.task_id,
                                "owner_user_id": task.owner_user_id,
                                "scope": task.scope,
                            }),
                        )
                        .await?;
                } else {
                    self.runtime
                        .db
                        .record_audit_event(
                            "runtime",
                            &self.current_session_id().await,
                            "task_enqueued",
                            json!({
                                "task_id": task.task_id,
                                "owner_user_id": task.owner_user_id,
                                "scope": task.scope,
                            }),
                        )
                        .await?;
                    self.wake_worker.notify_one();
                }

                let task = if wait {
                    self.wait_for_task_state(&task.owner_user_id, &task.task_id)
                        .await?
                } else {
                    task
                };

                Ok((DaemonResponse::Task { task }, false))
            }
            DaemonRequest::ShowTask { user_id, task_id } => Ok((
                DaemonResponse::Inspection {
                    inspection: self
                        .runtime
                        .inspect_task(user_id.as_deref(), &task_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::ListMemories {
                user_id,
                scope,
                kind,
                include_superseded,
                include_forgotten,
                limit,
            } => Ok((
                DaemonResponse::Memories {
                    memories: self
                        .runtime
                        .list_memories(
                            user_id.as_deref(),
                            scope.as_deref(),
                            kind.as_deref(),
                            include_superseded,
                            include_forgotten,
                            limit,
                        )
                        .await?,
                },
                false,
            )),
            DaemonRequest::ShowMemory { user_id, memory_id } => Ok((
                DaemonResponse::Memory {
                    memory: self
                        .runtime
                        .inspect_memory(user_id.as_deref(), &memory_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::MemoryHistory {
                user_id,
                subject_key,
                scope,
                subject_type,
                limit,
            } => Ok((
                DaemonResponse::Memories {
                    memories: self
                        .runtime
                        .memory_history(
                            user_id.as_deref(),
                            &subject_key,
                            scope.as_deref(),
                            subject_type.as_deref(),
                            limit,
                        )
                        .await?,
                },
                false,
            )),
            DaemonRequest::ForgetMemory {
                user_id,
                memory_id,
                reason,
            } => Ok((
                DaemonResponse::Memory {
                    memory: self
                        .runtime
                        .forget_memory(user_id.as_deref(), &memory_id, &reason)
                        .await?,
                },
                false,
            )),
            DaemonRequest::ListApprovals { user_id, status } => Ok((
                DaemonResponse::Approvals {
                    approvals: self
                        .runtime
                        .list_approvals(user_id.as_deref(), status.as_deref())
                        .await?,
                },
                false,
            )),
            DaemonRequest::ResolveApproval {
                user_id,
                approval_id,
                approve,
            } => {
                let task = self
                    .runtime
                    .resolve_approval(user_id.as_deref(), &approval_id, approve)
                    .await?;
                self.dispatch_connector_follow_up(&task).await?;
                self.refresh_heartbeat(None).await?;
                Ok((DaemonResponse::Task { task }, false))
            }
            DaemonRequest::SubmitConnectorMessage { message } => Ok((
                DaemonResponse::ConnectorReply {
                    reply: self.handle_connector_message(message).await?,
                },
                false,
            )),
            DaemonRequest::Shutdown => {
                self.runtime
                    .db
                    .record_audit_event(
                        "runtime",
                        &self.current_session_id().await,
                        "daemon_shutdown_requested",
                        json!({ "pid": std::process::id() }),
                    )
                    .await?;
                self.refresh_heartbeat(Some("stopping")).await?;
                Ok((
                    DaemonResponse::ShutdownAck {
                        status: self.status_snapshot().await,
                    },
                    true,
                ))
            }
        }
    }

    async fn wait_for_task_state(&self, owner_user_id: &str, task_id: &str) -> Result<TaskRow> {
        loop {
            let task = self
                .runtime
                .db
                .fetch_task_for_owner(owner_user_id, task_id)
                .await?
                .ok_or_else(|| anyhow!("task '{task_id}' disappeared while waiting"))?;
            match task.state.parse::<TaskState>()? {
                TaskState::Pending | TaskState::Running => {
                    if self.shutdown_notified().await {
                        bail!("daemon shutdown interrupted task wait for '{task_id}'");
                    }
                    time::sleep(TASK_WAIT_POLL_INTERVAL).await;
                }
                TaskState::WaitingApproval
                | TaskState::Blocked
                | TaskState::Completed
                | TaskState::Failed => return Ok(task),
            }
        }
    }

    async fn dispatch_connector_follow_up(&self, task: &TaskRow) -> Result<()> {
        let events = self
            .runtime
            .db
            .fetch_task_events_for_owner(&task.owner_user_id, &task.task_id)
            .await?;
        let Some(connector_type) = connector_type_from_events(&events) else {
            return Ok(());
        };

        match connector_type.as_str() {
            "discord" => discord::maybe_send_task_follow_up(self, task, &events).await,
            _ => Ok(()),
        }
    }

    async fn prepare_socket(&self) -> Result<()> {
        if tokio::fs::try_exists(&self.socket_path)
            .await
            .with_context(|| format!("failed to inspect {}", self.socket_path.display()))?
        {
            if DaemonClient::is_reachable(self.socket_path.clone()).await {
                bail!(
                    "daemon is already running at {}",
                    self.socket_path.display()
                );
            }
            tokio::fs::remove_file(&self.socket_path)
                .await
                .with_context(|| {
                    format!("failed to remove stale {}", self.socket_path.display())
                })?;
        }
        Ok(())
    }

    async fn set_active_task(&self, task_id: Option<String>) -> Result<()> {
        {
            let mut status = self.status.write().await;
            status.active_task_id = task_id;
        }
        self.refresh_heartbeat(None).await
    }

    async fn refresh_heartbeat(&self, state_override: Option<&str>) -> Result<()> {
        let queue_depth = self.runtime.pending_task_count().await?;
        let mut status = self.status.write().await;
        if let Some(state) = state_override {
            status.state = state.to_owned();
        } else if status.state != "stopping" && status.state != "stopped" {
            status.state = "running".to_owned();
        }
        let heartbeat_at = now_rfc3339();
        status.queue_depth = queue_depth;
        status.last_heartbeat_at = Some(heartbeat_at.clone());

        self.runtime
            .db
            .heartbeat_runtime_session(
                &status.session_id,
                &status.state,
                status.queue_depth,
                status.active_task_id.as_deref(),
                status.automation_planning_enabled,
            )
            .await?;
        Ok(())
    }

    async fn finish(&self, last_error: Option<String>) -> Result<()> {
        let queue_depth = self.runtime.pending_task_count().await.unwrap_or_default();
        {
            let mut status = self.status.write().await;
            status.state = if last_error.is_some() {
                "failed".to_owned()
            } else {
                "stopped".to_owned()
            };
            status.queue_depth = queue_depth;
            status.active_task_id = None;
            status.stopped_at = Some(now_rfc3339());
            status.last_error = last_error.clone();
        }

        let status = self.status_snapshot().await;
        self.runtime
            .db
            .finish_runtime_session(
                &status.session_id,
                &status.state,
                status.queue_depth,
                status.active_task_id.as_deref(),
                status.last_error.as_deref(),
            )
            .await?;
        self.runtime
            .db
            .record_audit_event(
                "runtime",
                &status.session_id,
                if status.state == "failed" {
                    "daemon_stopped_with_error"
                } else {
                    "daemon_stopped"
                },
                json!({
                    "state": status.state,
                    "queue_depth": status.queue_depth,
                    "last_error": status.last_error,
                }),
            )
            .await?;

        if tokio::fs::try_exists(&self.socket_path)
            .await
            .unwrap_or(false)
        {
            tokio::fs::remove_file(&self.socket_path)
                .await
                .with_context(|| format!("failed to remove {}", self.socket_path.display()))?;
        }
        Ok(())
    }

    async fn status_snapshot(&self) -> DaemonStatus {
        self.status.read().await.clone()
    }

    async fn current_session_id(&self) -> String {
        self.status.read().await.session_id.clone()
    }

    async fn shutdown_notified(&self) -> bool {
        self.status.read().await.state == "stopping"
    }

    fn start_connector_loop(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<Result<()>>> {
        if self.runtime.config.integrations.discord.enabled {
            let daemon = Arc::clone(self);
            Some(tokio::spawn(async move {
                discord::run_discord_loop(daemon).await
            }))
        } else {
            None
        }
    }

    fn start_session_cleanup_loop(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<Result<()>>> {
        let interval_secs = self
            .runtime
            .config
            .runtime
            .session_management
            .cleanup_interval_secs;
        if interval_secs == 0 {
            return None;
        }

        let daemon = Arc::clone(self);
        Some(tokio::spawn(async move {
            Self::session_cleanup_loop(daemon, interval_secs).await
        }))
    }
}

pub async fn load_daemon_client(config_dir: impl AsRef<Path>) -> Result<DaemonClient> {
    let config = LoadedConfig::load_from_dir(config_dir)?;
    Ok(DaemonClient::new(
        config.runtime.state_dir.join("daemon.sock"),
    ))
}

pub async fn latest_daemon_status(config_dir: impl AsRef<Path>) -> Result<Option<DaemonStatus>> {
    let config = LoadedConfig::load_from_dir(config_dir)?;
    let db = Database::connect(&config.runtime.database_path).await?;
    Ok(db
        .latest_runtime_session(&config.runtime.instance_id)
        .await?
        .map(DaemonStatus::from_row))
}

fn load_provider(config: &LoadedConfig, passphrase: &str) -> Result<OpenAiProvider> {
    let provider_entry = config.providers.active_provider()?;
    if provider_entry.auth.mode != "api_token" {
        bail!(
            "provider auth mode '{}' is not implemented in M0/M1",
            provider_entry.auth.mode
        );
    }

    let secret_store = SecretStore::new(&config.runtime.secret_dir);
    let api_token = secret_store.read_secret(&provider_entry.auth.secret_ref, passphrase)?;
    OpenAiProvider::from_entry(provider_entry, api_token)
}

fn load_discord_bot_token(config: &LoadedConfig, passphrase: &str) -> Result<Option<String>> {
    if !config.integrations.discord.enabled {
        return Ok(None);
    }

    let Some(secret_ref) = config.integrations.discord.bot_token_secret_ref.as_deref() else {
        bail!("Discord integration is enabled but bot_token_secret_ref is not configured");
    };

    let secret_store = SecretStore::new(&config.runtime.secret_dir);
    Ok(Some(secret_store.read_secret(secret_ref, passphrase)?))
}

fn session_lifecycle_is_due(
    session: &ConversationSessionRow,
    policy: &SessionLifecyclePolicy,
    now: DateTime<Utc>,
) -> Result<bool> {
    let last_activity_at = DateTime::parse_from_rfc3339(&session.last_activity_at)
        .with_context(|| {
            format!(
                "conversation session '{}' has invalid last_activity_at '{}'",
                session.session_id, session.last_activity_at
            )
        })?
        .with_timezone(&Utc);
    let inactive_for = now.signed_duration_since(last_activity_at);
    let inactivity_threshold = chrono::Duration::seconds(
        i64::try_from(policy.inactivity_after_secs)
            .context("policy inactivity TTL is too large")?,
    );
    if inactive_for < inactivity_threshold {
        return Ok(false);
    }

    let already_applied_at = match policy.action {
        SessionLifecycleAction::Reset => session.last_reset_at.as_deref(),
        SessionLifecycleAction::Archive => session.archived_at.as_deref(),
    };
    let Some(already_applied_at) = already_applied_at else {
        return Ok(true);
    };
    let already_applied_at = DateTime::parse_from_rfc3339(already_applied_at)
        .with_context(|| {
            format!(
                "conversation session '{}' has invalid lifecycle timestamp '{}'",
                session.session_id, already_applied_at
            )
        })?
        .with_timezone(&Utc);
    Ok(already_applied_at < last_activity_at)
}

fn parse_scope(value: &str) -> Result<MemoryScope> {
    value
        .parse::<MemoryScope>()
        .map_err(|_| anyhow!("unsupported scope '{value}', expected private or household"))
}

fn parse_session_max_scope(session: &ConversationSessionRow) -> Result<MemoryScope> {
    parse_scope(&session.max_memory_scope).with_context(|| {
        format!(
            "conversation session '{}' has unsupported max memory scope '{}'",
            session.session_id, session.max_memory_scope
        )
    })
}

fn cap_task_scope_to_session(scope: MemoryScope, session_max_scope: MemoryScope) -> MemoryScope {
    match (scope, session_max_scope) {
        (_, MemoryScope::Private) => MemoryScope::Private,
        _ => scope,
    }
}

fn cap_scopes_to_session(
    scopes: Vec<MemoryScope>,
    session_max_scope: MemoryScope,
) -> Vec<MemoryScope> {
    scopes
        .into_iter()
        .filter(|scope| {
            matches!(session_max_scope, MemoryScope::Household)
                || matches!(scope, MemoryScope::Private)
        })
        .collect()
}

fn is_session_reset_command(objective: &str) -> bool {
    objective.trim() == SESSION_RESET_COMMAND
}

fn parse_memory_kind_filter(value: Option<&str>) -> Result<Option<MemoryKind>> {
    match value {
        None => Ok(None),
        Some("all") => Ok(None),
        Some(value) => value.parse::<MemoryKind>().map(Some).map_err(|_| {
            anyhow!("unsupported memory kind '{value}', expected semantic or episodic")
        }),
    }
}

fn resolve_visible_memory_scopes(
    role_ids: &[String],
    requested_scope: Option<&str>,
) -> Result<Vec<MemoryScope>> {
    let visible_scopes = visible_memory_scopes(role_ids);
    match requested_scope {
        None | Some("all") => Ok(visible_scopes),
        Some(scope_value) => {
            let scope = parse_scope(scope_value)?;
            if !visible_scopes.contains(&scope) {
                bail!("scope '{scope}' is not visible to the selected user");
            }
            Ok(vec![scope])
        }
    }
}

fn ensure_memory_visible_to_user(
    memory: &MemoryRow,
    user_id: &str,
    visible_scopes: &[MemoryScope],
) -> Result<()> {
    let memory_scope = memory.scope.parse::<MemoryScope>().map_err(|_| {
        anyhow!(
            "memory '{}' has unsupported scope '{}'",
            memory.memory_id,
            memory.scope
        )
    })?;
    if !visible_scopes.contains(&memory_scope) {
        bail!(
            "memory '{}' is not visible to the selected user",
            memory.memory_id
        );
    }

    match memory.owner_user_id.as_deref() {
        Some(owner_user_id) if owner_user_id == user_id => Ok(()),
        None => Ok(()),
        _ => bail!(
            "memory '{}' is not visible to the selected user",
            memory.memory_id
        ),
    }
}

#[derive(Debug, Clone)]
struct CliConversationExchange {
    created_at: String,
    state: String,
    user_text: String,
    assistant_text: String,
    task_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliConversationStatus {
    FollowUp,
    FreshStart,
}

fn build_cli_task_context(recent_exchanges: &[CliConversationExchange]) -> String {
    let mut context = String::new();
    context.push_str("CLI context:\n");
    context.push_str("- Source: CLI.\n");
    context.push_str(
        "- Conversation history for CLI requests is scoped to this BeaverKI user and entrypoint.\n",
    );

    let Some(latest_exchange) = recent_exchanges.first() else {
        context.push_str(
            "- Conversation status: new conversation. No recent prior CLI exchange is available.\n",
        );
        context.push_str(
            "- Treat the current command as a new conversation unless the user explicitly refers to older context.\n",
        );
        return context;
    };

    let age_text = latest_exchange_age_text(&latest_exchange.created_at)
        .unwrap_or_else(|| "at an unknown time".to_owned());
    match cli_conversation_status(&latest_exchange.state, &latest_exchange.created_at) {
        CliConversationStatus::FollowUp => {
            context.push_str(&format!(
                "- Conversation status: active follow-up. The latest CLI exchange with this user was {}.\n",
                age_text
            ));
            context.push_str(
                "- Continue the prior topic only if the new command depends on it. If the user starts a new topic, answer the new topic directly.\n",
            );
            context.push_str("Recent attributed CLI exchanges:\n");
            for exchange in recent_exchanges.iter().rev() {
                context.push_str(&format!(
                    "- [CLI] User: {}\n",
                    compact_message_text(&exchange.user_text)
                ));
                context.push_str(&format!(
                    "- [CLI] Assistant: {}\n",
                    compact_message_text(&exchange.assistant_text)
                ));
            }
        }
        CliConversationStatus::FreshStart => {
            context.push_str(&format!(
                "- Conversation status: fresh conversation. The latest CLI exchange with this user was {}.\n",
                age_text
            ));
            context.push_str(
                "- Treat the current command as a new conversation unless the user explicitly refers to the earlier topic. Keep stable facts and preferences if they are reliable.\n",
            );
        }
    }

    context
}

fn cli_conversation_status(state: &str, created_at: &str) -> CliConversationStatus {
    if matches!(
        state.parse::<TaskState>(),
        Ok(TaskState::Pending | TaskState::Running | TaskState::WaitingApproval)
    ) {
        return CliConversationStatus::FollowUp;
    }

    let Some(created_at) = parse_timestamp(created_at) else {
        return CliConversationStatus::FreshStart;
    };
    let age = Utc::now().signed_duration_since(created_at).num_seconds();
    if age <= CLI_ACTIVE_CONVERSATION_WINDOW_SECS {
        CliConversationStatus::FollowUp
    } else {
        CliConversationStatus::FreshStart
    }
}

fn latest_exchange_age_text(created_at: &str) -> Option<String> {
    let created_at = parse_timestamp(created_at)?;
    let age = Utc::now().signed_duration_since(created_at);
    if age.num_minutes() < 1 {
        Some("less than a minute ago".to_owned())
    } else if age.num_hours() < 1 {
        Some(format!("{} minutes ago", age.num_minutes()))
    } else if age.num_days() < 1 {
        Some(format!("{} hours ago", age.num_hours()))
    } else {
        Some(format!("{} days ago", age.num_days()))
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn assistant_reply_for_context(task: &TaskRow) -> String {
    match task.state.parse::<TaskState>() {
        Ok(TaskState::Completed) => task
            .result_text
            .clone()
            .unwrap_or_else(|| "Task completed without a recorded reply.".to_owned()),
        Ok(TaskState::WaitingApproval) => task.result_text.clone().unwrap_or_else(|| {
            "The assistant is waiting for approval before it can continue.".to_owned()
        }),
        Ok(TaskState::Pending | TaskState::Running) => {
            "The assistant is still working on that request.".to_owned()
        }
        Ok(TaskState::Failed) => task
            .result_text
            .clone()
            .unwrap_or_else(|| "The assistant failed without a recorded explanation.".to_owned()),
        Ok(TaskState::Blocked) | Err(_) => task
            .result_text
            .clone()
            .unwrap_or_else(|| format!("Task state: {}.", task.state)),
    }
}

fn compact_message_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn connector_type_from_events(events: &[TaskEventRow]) -> Option<String> {
    let event = events
        .iter()
        .find(|event| event.event_type == CONNECTOR_MESSAGE_CONTEXT_EVENT)?;
    let payload: Value = serde_json::from_str(&event.payload_json).ok()?;
    payload
        .get("connector_type")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use beaverki_config::{
        ProviderModels, RuntimeConfig, RuntimeDefaults, RuntimeFeatures, SessionManagementConfig,
    };
    use beaverki_models::{ConversationItem, ModelTurnResponse};
    use tempfile::TempDir;

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
        let runtime = Runtime::from_parts(
            config,
            db,
            default_user,
            Arc::new(FakeProvider::new(responses)),
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
        runtime.config.integrations.discord.allowed_channel_ids = vec!["channel-1".to_owned()];
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
        runtime.config.integrations.discord.allowed_channel_ids = vec!["room-1".to_owned()];
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
        runtime.config.integrations.discord.allowed_channel_ids = vec!["room-2".to_owned()];
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
        let session = runtime
            .resolve_connector_conversation_session("user_alex", "discord", "room-2", false)
            .await
            .expect("session");
        runtime
            .db
            .update_conversation_session_policy(
                &session.session_id,
                "guest_room",
                MemoryScope::Private,
                Some("test_cap"),
            )
            .await
            .expect("cap session");
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
        assert_eq!(result.task.state, TaskState::Completed.as_str());
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
        runtime.config.integrations.discord.allowed_channel_ids = vec!["channel-1".to_owned()];
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
        runtime.config.integrations.discord.allowed_channel_ids = vec!["channel-1".to_owned()];
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
        runtime.config.integrations.discord.allowed_channel_ids = vec!["channel-1".to_owned()];
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
