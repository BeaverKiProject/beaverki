use std::path::Path;

use anyhow::{Context, Result, anyhow};
use beaverki_core::{
    MemoryKind, MemoryScope, TaskState, ToolInvocationStatus, new_prefixed_id, now_rfc3339,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{FromRow, QueryBuilder, Sqlite, SqlitePool};

#[derive(Debug, Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    pub async fn connect(database_path: &Path) -> Result<Self> {
        if let Some(parent) = database_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .context("failed to connect sqlite database")?;

        let db = Self { pool };
        db.migrate().await?;
        db.reconcile_single_user_owner_role().await?;
        Ok(db)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .context("failed to run sqlite migrations")
    }

    async fn reconcile_single_user_owner_role(&self) -> Result<()> {
        let user_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await
            .context("failed to count users during owner-role reconciliation")?;
        let user_role_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_roles")
            .fetch_one(&self.pool)
            .await
            .context("failed to count user roles during owner-role reconciliation")?;

        if user_count == 1
            && user_role_count == 0
            && let Some(user) = self.default_user().await?
        {
            self.assign_role(&user.user_id, "owner").await?;
        }

        Ok(())
    }

    pub async fn bootstrap_single_user(&self, display_name: &str) -> Result<BootstrapState> {
        if let Some(existing) = self.default_user().await? {
            self.assign_role(&existing.user_id, "owner").await?;
            return Ok(BootstrapState {
                user_id: existing.user_id,
                primary_agent_id: existing
                    .primary_agent_id
                    .unwrap_or_else(|| "agent_owner".to_owned()),
            });
        }

        let slug = slugify(display_name);
        let user_id = format!("user_{slug}");
        let agent_id = format!("agent_{slug}");
        let timestamp = now_rfc3339();

        self.insert_user_row(&user_id, display_name, &agent_id, &timestamp)
            .await
            .context("failed to insert default user")?;
        self.insert_primary_agent_row(&agent_id, &user_id, display_name, &timestamp)
            .await
            .context("failed to insert primary agent")?;
        self.assign_role(&user_id, "owner").await?;

        Ok(BootstrapState {
            user_id,
            primary_agent_id: agent_id,
        })
    }

    pub async fn create_user(
        &self,
        display_name: &str,
        roles: &[String],
    ) -> Result<BootstrapState> {
        let slug = unique_user_slug(display_name, &self.pool).await?;
        let user_id = format!("user_{slug}");
        let agent_id = format!("agent_{slug}");
        let timestamp = now_rfc3339();

        self.insert_user_row(&user_id, display_name, &agent_id, &timestamp)
            .await
            .with_context(|| format!("failed to insert user '{display_name}'"))?;
        self.insert_primary_agent_row(&agent_id, &user_id, display_name, &timestamp)
            .await
            .context("failed to insert primary agent")?;
        for role_id in roles {
            self.assign_role(&user_id, role_id).await?;
        }

        Ok(BootstrapState {
            user_id,
            primary_agent_id: agent_id,
        })
    }

    pub async fn default_user(&self) -> Result<Option<UserRow>> {
        let user = sqlx::query_as::<_, UserRow>(
            "SELECT user_id, display_name, status, primary_agent_id, created_at, updated_at
             FROM users
             ORDER BY created_at
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch default user")?;

        Ok(user)
    }

    pub async fn fetch_user(&self, user_id: &str) -> Result<Option<UserRow>> {
        let user = sqlx::query_as::<_, UserRow>(
            "SELECT user_id, display_name, status, primary_agent_id, created_at, updated_at
             FROM users
             WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch user")?;
        Ok(user)
    }

    pub async fn list_users(&self) -> Result<Vec<UserRow>> {
        let users = sqlx::query_as::<_, UserRow>(
            "SELECT user_id, display_name, status, primary_agent_id, created_at, updated_at
             FROM users
             ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list users")?;
        Ok(users)
    }

    pub async fn list_roles(&self) -> Result<Vec<RoleRow>> {
        let roles = sqlx::query_as::<_, RoleRow>(
            "SELECT role_id, description FROM roles ORDER BY role_id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list roles")?;
        Ok(roles)
    }

    pub async fn list_user_roles(&self, user_id: &str) -> Result<Vec<UserRoleRow>> {
        let roles = sqlx::query_as::<_, UserRoleRow>(
            "SELECT user_id, role_id, created_at
             FROM user_roles
             WHERE user_id = ?
             ORDER BY role_id ASC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list user roles")?;
        Ok(roles)
    }

    pub async fn ensure_conversation_session(
        &self,
        input: NewConversationSession<'_>,
    ) -> Result<ConversationSessionRow> {
        let timestamp = now_rfc3339();
        let session_id = new_prefixed_id("conversation_session");
        sqlx::query(
            "INSERT OR IGNORE INTO conversation_sessions
             (session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)",
        )
        .bind(&session_id)
        .bind(input.session_kind)
        .bind(input.session_key)
        .bind(input.audience_policy)
        .bind(input.max_memory_scope.as_str())
        .bind(input.originating_connector_type)
        .bind(input.originating_connector_target)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to ensure conversation session")?;

        self.fetch_conversation_session_by_key(input.session_key)
            .await?
            .ok_or_else(|| anyhow!("conversation session missing after insert"))
    }

    pub async fn fetch_conversation_session(
        &self,
        session_id: &str,
    ) -> Result<Option<ConversationSessionRow>> {
        let row = sqlx::query_as::<_, ConversationSessionRow>(
            "SELECT session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at
             FROM conversation_sessions
             WHERE session_id = ?",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch conversation session")?;
        Ok(row)
    }

    pub async fn fetch_conversation_session_by_key(
        &self,
        session_key: &str,
    ) -> Result<Option<ConversationSessionRow>> {
        let row = sqlx::query_as::<_, ConversationSessionRow>(
            "SELECT session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at
             FROM conversation_sessions
             WHERE session_key = ?",
        )
        .bind(session_key)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch conversation session by key")?;
        Ok(row)
    }

    pub async fn touch_conversation_session(
        &self,
        session_id: &str,
        lifecycle_reason: Option<&str>,
    ) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE conversation_sessions
             SET last_activity_at = ?, archived_at = NULL, lifecycle_reason = COALESCE(?, lifecycle_reason), updated_at = ?
             WHERE session_id = ?",
        )
        .bind(&timestamp)
        .bind(lifecycle_reason)
        .bind(&timestamp)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to touch conversation session")?;
        Ok(())
    }

    pub async fn reset_conversation_session(
        &self,
        session_id: &str,
        lifecycle_reason: &str,
    ) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE conversation_sessions
             SET last_reset_at = ?, lifecycle_reason = ?, updated_at = ?
             WHERE session_id = ?",
        )
        .bind(&timestamp)
        .bind(lifecycle_reason)
        .bind(&timestamp)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to reset conversation session")?;
        Ok(())
    }

    pub async fn archive_conversation_session(
        &self,
        session_id: &str,
        lifecycle_reason: &str,
    ) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE conversation_sessions
             SET last_reset_at = ?, archived_at = ?, lifecycle_reason = ?, updated_at = ?
             WHERE session_id = ?",
        )
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(lifecycle_reason)
        .bind(&timestamp)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to archive conversation session")?;
        Ok(())
    }

    pub async fn update_conversation_session_policy(
        &self,
        session_id: &str,
        audience_policy: &str,
        max_memory_scope: MemoryScope,
        lifecycle_reason: Option<&str>,
    ) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE conversation_sessions
             SET audience_policy = ?, max_memory_scope = ?, lifecycle_reason = COALESCE(?, lifecycle_reason), updated_at = ?
             WHERE session_id = ?",
        )
        .bind(audience_policy)
        .bind(max_memory_scope.as_str())
        .bind(lifecycle_reason)
        .bind(&timestamp)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to update conversation session policy")?;
        Ok(())
    }

    pub async fn list_conversation_sessions(
        &self,
        owner_user_id: Option<&str>,
        include_archived: bool,
        limit: i64,
    ) -> Result<Vec<ConversationSessionRow>> {
        let rows = match (owner_user_id, include_archived) {
            (Some(owner_user_id), true) => {
                sqlx::query_as::<_, ConversationSessionRow>(
                    "SELECT session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at
                     FROM conversation_sessions
                     WHERE EXISTS (
                       SELECT 1
                       FROM tasks
                       WHERE tasks.session_id = conversation_sessions.session_id
                         AND tasks.owner_user_id = ?
                     )
                     ORDER BY last_activity_at DESC
                     LIMIT ?",
                )
                .bind(owner_user_id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (Some(owner_user_id), false) => {
                sqlx::query_as::<_, ConversationSessionRow>(
                    "SELECT session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at
                     FROM conversation_sessions
                     WHERE archived_at IS NULL
                       AND EXISTS (
                         SELECT 1
                         FROM tasks
                         WHERE tasks.session_id = conversation_sessions.session_id
                           AND tasks.owner_user_id = ?
                       )
                     ORDER BY last_activity_at DESC
                     LIMIT ?",
                )
                .bind(owner_user_id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (None, true) => {
                sqlx::query_as::<_, ConversationSessionRow>(
                    "SELECT session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at
                     FROM conversation_sessions
                     ORDER BY last_activity_at DESC
                     LIMIT ?",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (None, false) => {
                sqlx::query_as::<_, ConversationSessionRow>(
                    "SELECT session_id, session_kind, session_key, audience_policy, max_memory_scope, originating_connector_type, originating_connector_target, last_activity_at, last_reset_at, archived_at, lifecycle_reason, created_at, updated_at
                     FROM conversation_sessions
                     WHERE archived_at IS NULL
                     ORDER BY last_activity_at DESC
                     LIMIT ?",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
        }
        .context("failed to list conversation sessions")?;
        Ok(rows)
    }

    pub async fn count_tasks_for_session(&self, session_id: &str) -> Result<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM tasks
             WHERE session_id = ?",
        )
        .bind(session_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count tasks for conversation session")?;
        Ok(count)
    }

    pub async fn list_session_owner_user_ids(&self, session_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT owner_user_id
             FROM tasks
             WHERE session_id = ?
             ORDER BY owner_user_id ASC",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list conversation session owners")?;
        Ok(rows)
    }

    pub async fn overwrite_conversation_session_timestamps(
        &self,
        session_id: &str,
        last_activity_at: &str,
        last_reset_at: Option<&str>,
        archived_at: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE conversation_sessions
             SET last_activity_at = ?, last_reset_at = ?, archived_at = ?, updated_at = ?
             WHERE session_id = ?",
        )
        .bind(last_activity_at)
        .bind(last_reset_at)
        .bind(archived_at)
        .bind(last_activity_at)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to overwrite conversation session timestamps")?;
        Ok(())
    }

    pub async fn assign_role(&self, user_id: &str, role_id: &str) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO user_roles (user_id, role_id, created_at)
             VALUES (?, ?, ?)",
        )
        .bind(user_id)
        .bind(role_id)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await
        .context("failed to assign role")?;
        Ok(())
    }

    async fn insert_user_row(
        &self,
        user_id: &str,
        display_name: &str,
        primary_agent_id: &str,
        timestamp: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (user_id, display_name, status, primary_agent_id, created_at, updated_at)
             VALUES (?, ?, 'enabled', ?, ?, ?)",
        )
        .bind(user_id)
        .bind(display_name)
        .bind(primary_agent_id)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert user")?;
        Ok(())
    }

    async fn insert_primary_agent_row(
        &self,
        agent_id: &str,
        user_id: &str,
        display_name: &str,
        timestamp: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO agents
             (agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at)
             VALUES (?, 'primary', ?, NULL, 'active', ?, 'private', 'builtin_primary', ?, ?)",
        )
        .bind(agent_id)
        .bind(user_id)
        .bind(format!("Primary agent for {display_name}"))
        .bind(timestamp)
        .bind(timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert primary agent")?;
        Ok(())
    }

    pub async fn create_task(
        &self,
        owner_user_id: &str,
        primary_agent_id: &str,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<TaskRow> {
        self.create_task_with_params(NewTask {
            owner_user_id,
            initiating_identity_id: &format!("cli:{owner_user_id}"),
            primary_agent_id,
            assigned_agent_id: primary_agent_id,
            parent_task_id: None,
            session_id: None,
            kind: "interactive",
            objective,
            context_summary: None,
            scope,
            wake_at: None,
        })
        .await
    }

    pub async fn create_task_with_params(&self, input: NewTask<'_>) -> Result<TaskRow> {
        let task_id = new_prefixed_id("task");
        let timestamp = now_rfc3339();

        sqlx::query(
            "INSERT INTO tasks
             (task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL)",
        )
        .bind(&task_id)
        .bind(input.owner_user_id)
        .bind(input.initiating_identity_id)
        .bind(input.primary_agent_id)
        .bind(input.assigned_agent_id)
        .bind(input.parent_task_id)
        .bind(input.session_id)
        .bind(input.kind)
        .bind(TaskState::Pending.as_str())
        .bind(input.objective)
        .bind(input.context_summary)
        .bind(input.scope.as_str())
        .bind(input.wake_at)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert task")?;

        if let Some(session_id) = input.session_id {
            self.touch_conversation_session(session_id, Some("task_created"))
                .await?;
        }

        self.fetch_task_for_owner(input.owner_user_id, &task_id)
            .await?
            .context("task missing after insert")
    }

    pub async fn update_task_state(&self, task_id: &str, state: TaskState) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query("UPDATE tasks SET state = ?, updated_at = ? WHERE task_id = ?")
            .bind(state.as_str())
            .bind(&timestamp)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .context("failed to update task state")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(())
    }

    pub async fn update_task_wake_at(&self, task_id: &str, wake_at: Option<&str>) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query("UPDATE tasks SET wake_at = ?, updated_at = ? WHERE task_id = ?")
            .bind(wake_at)
            .bind(&timestamp)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .context("failed to update task wake_at")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(())
    }

    pub async fn set_task_waiting_approval(&self, task_id: &str, message: &str) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE tasks
             SET state = ?, result_text = ?, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(TaskState::WaitingApproval.as_str())
        .bind(message)
        .bind(&timestamp)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .context("failed to mark task waiting approval")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(())
    }

    pub async fn clear_task_result(&self, task_id: &str) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query("UPDATE tasks SET result_text = NULL, completed_at = NULL, updated_at = ? WHERE task_id = ?")
            .bind(&timestamp)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .context("failed to clear task result")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(())
    }

    pub async fn complete_task(&self, task_id: &str, result_text: &str) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE tasks
             SET state = ?, result_text = ?, completed_at = ?, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(TaskState::Completed.as_str())
        .bind(result_text)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .context("failed to complete task")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(())
    }

    pub async fn fail_task(&self, task_id: &str, error_text: &str) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE tasks
             SET state = ?, result_text = ?, completed_at = ?, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(TaskState::Failed.as_str())
        .bind(error_text)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .context("failed to fail task")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(())
    }

    pub async fn append_task_event(
        &self,
        task_id: &str,
        event_type: &str,
        actor_type: &str,
        actor_id: &str,
        payload: Value,
    ) -> Result<String> {
        let event_id = new_prefixed_id("evt");
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO task_events
             (event_id, task_id, event_type, actor_type, actor_id, payload_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&event_id)
        .bind(task_id)
        .bind(event_type)
        .bind(actor_type)
        .bind(actor_id)
        .bind(payload.to_string())
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to append task event")?;
        self.touch_session_for_task(task_id, &timestamp).await?;
        Ok(event_id)
    }

    async fn touch_session_for_task(&self, task_id: &str, timestamp: &str) -> Result<()> {
        sqlx::query(
            "UPDATE conversation_sessions
             SET last_activity_at = ?, updated_at = ?
             WHERE session_id = (
               SELECT session_id
               FROM tasks
               WHERE task_id = ?
                 AND session_id IS NOT NULL
             )",
        )
        .bind(timestamp)
        .bind(timestamp)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .context("failed to touch conversation session for task")?;
        Ok(())
    }

    pub async fn start_tool_invocation(
        &self,
        task_id: &str,
        agent_id: &str,
        tool_name: &str,
        request: Value,
    ) -> Result<String> {
        let invocation_id = new_prefixed_id("tool");
        sqlx::query(
            "INSERT INTO tool_invocations
             (invocation_id, task_id, agent_id, tool_name, request_json, response_json, status, started_at, finished_at)
             VALUES (?, ?, ?, ?, ?, NULL, ?, ?, NULL)",
        )
        .bind(&invocation_id)
        .bind(task_id)
        .bind(agent_id)
        .bind(tool_name)
        .bind(request.to_string())
        .bind(ToolInvocationStatus::Running.as_str())
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await
        .context("failed to start tool invocation")?;
        Ok(invocation_id)
    }

    pub async fn finish_tool_invocation(
        &self,
        invocation_id: &str,
        status: ToolInvocationStatus,
        response: Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE tool_invocations
             SET status = ?, response_json = ?, finished_at = ?
             WHERE invocation_id = ?",
        )
        .bind(status.as_str())
        .bind(response.to_string())
        .bind(now_rfc3339())
        .bind(invocation_id)
        .execute(&self.pool)
        .await
        .context("failed to finish tool invocation")?;
        Ok(())
    }

    pub async fn create_subagent(
        &self,
        owner_user_id: &str,
        parent_agent_id: &str,
        persona: &str,
        permission_profile: &str,
    ) -> Result<AgentRow> {
        let agent_id = new_prefixed_id("agent");
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO agents
             (agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at)
             VALUES (?, 'subagent', ?, ?, 'active', ?, 'task', ?, ?, ?)",
        )
        .bind(&agent_id)
        .bind(owner_user_id)
        .bind(parent_agent_id)
        .bind(persona)
        .bind(permission_profile)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create subagent")?;

        self.fetch_agent(&agent_id)
            .await?
            .context("subagent missing after insert")
    }

    pub async fn fetch_agent(&self, agent_id: &str) -> Result<Option<AgentRow>> {
        let row = sqlx::query_as::<_, AgentRow>(
            "SELECT agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at
             FROM agents
             WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch agent")?;
        Ok(row)
    }

    pub async fn insert_memory(&self, input: NewMemory<'_>) -> Result<String> {
        let memory_id = new_prefixed_id("mem");
        let timestamp = now_rfc3339();
        let content_json = input.content_json.map(Value::to_string);
        sqlx::query(
            "INSERT INTO memories
             (memory_id, owner_user_id, scope, memory_kind, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&memory_id)
        .bind(input.owner_user_id)
        .bind(input.scope.as_str())
        .bind(input.memory_kind.as_str())
        .bind(input.subject_type)
        .bind(input.subject_key)
        .bind(input.content_text)
        .bind(content_json)
        .bind(input.sensitivity)
        .bind(input.source_type)
        .bind(input.source_ref)
        .bind(input.task_id)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert memory")?;
        Ok(memory_id)
    }

    pub async fn retrieve_memories(
        &self,
        owner_user_id: Option<&str>,
        visible_scopes: &[MemoryScope],
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT memory_id, owner_user_id, scope, memory_kind, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at, superseded_by_memory_id
             , forgotten_at, forgotten_reason
             FROM memories WHERE superseded_by_memory_id IS NULL AND forgotten_at IS NULL AND ",
        );

        builder.push("scope IN (");
        {
            let mut separated = builder.separated(", ");
            for scope in visible_scopes {
                separated.push_bind(scope.as_str());
            }
        }
        builder.push(")");

        match owner_user_id {
            Some(user_id) => {
                builder.push(" AND (owner_user_id = ");
                builder.push_bind(user_id);
                builder.push(" OR owner_user_id IS NULL)");
            }
            None => {
                builder.push(" AND owner_user_id IS NULL");
            }
        }

        builder.push(
            " ORDER BY CASE memory_kind WHEN 'semantic' THEN 0 ELSE 1 END, updated_at DESC LIMIT ",
        );
        builder.push_bind(limit);

        let query = builder.build_query_as::<MemoryRow>();
        let memories = query
            .fetch_all(&self.pool)
            .await
            .context("failed to retrieve memories")?;

        Ok(memories)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query_memories(
        &self,
        owner_user_id: Option<&str>,
        visible_scopes: &[MemoryScope],
        memory_kind: Option<MemoryKind>,
        subject_type: Option<&str>,
        subject_key: Option<&str>,
        include_superseded: bool,
        include_forgotten: bool,
        limit: i64,
    ) -> Result<Vec<MemoryRow>> {
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT memory_id, owner_user_id, scope, memory_kind, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at, superseded_by_memory_id, forgotten_at, forgotten_reason
             FROM memories WHERE ",
        );

        if !include_superseded {
            builder.push("superseded_by_memory_id IS NULL AND ");
        }
        if !include_forgotten {
            builder.push("forgotten_at IS NULL AND ");
        }

        builder.push("scope IN (");
        {
            let mut separated = builder.separated(", ");
            for scope in visible_scopes {
                separated.push_bind(scope.as_str());
            }
        }
        builder.push(")");

        match owner_user_id {
            Some(user_id) => {
                builder.push(" AND (owner_user_id = ");
                builder.push_bind(user_id);
                builder.push(" OR owner_user_id IS NULL)");
            }
            None => {
                builder.push(" AND owner_user_id IS NULL");
            }
        }

        if let Some(memory_kind) = memory_kind {
            builder.push(" AND memory_kind = ");
            builder.push_bind(memory_kind.as_str());
        }

        if let Some(subject_type) = subject_type {
            builder.push(" AND subject_type = ");
            builder.push_bind(subject_type);
        }

        if let Some(subject_key) = subject_key {
            builder.push(" AND subject_key = ");
            builder.push_bind(subject_key);
        }

        builder.push(
            " ORDER BY CASE memory_kind WHEN 'semantic' THEN 0 ELSE 1 END, updated_at DESC LIMIT ",
        );
        builder.push_bind(limit);

        let query = builder.build_query_as::<MemoryRow>();
        let memories = query
            .fetch_all(&self.pool)
            .await
            .context("failed to query memories")?;
        Ok(memories)
    }

    pub async fn find_active_memory_by_subject(
        &self,
        owner_user_id: Option<&str>,
        scope: MemoryScope,
        memory_kind: MemoryKind,
        subject_type: &str,
        subject_key: &str,
    ) -> Result<Option<MemoryRow>> {
        let row = match owner_user_id {
            Some(owner_user_id) => {
                sqlx::query_as::<_, MemoryRow>(
                    "SELECT memory_id, owner_user_id, scope, memory_kind, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at, superseded_by_memory_id, forgotten_at, forgotten_reason
                     FROM memories
                     WHERE owner_user_id = ?
                       AND scope = ?
                       AND memory_kind = ?
                       AND subject_type = ?
                       AND subject_key = ?
                       AND superseded_by_memory_id IS NULL
                       AND forgotten_at IS NULL
                     ORDER BY updated_at DESC
                     LIMIT 1",
                )
                .bind(owner_user_id)
                .bind(scope.as_str())
                .bind(memory_kind.as_str())
                .bind(subject_type)
                .bind(subject_key)
                .fetch_optional(&self.pool)
                .await
            }
            None => {
                sqlx::query_as::<_, MemoryRow>(
                    "SELECT memory_id, owner_user_id, scope, memory_kind, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at, superseded_by_memory_id, forgotten_at, forgotten_reason
                     FROM memories
                     WHERE owner_user_id IS NULL
                       AND scope = ?
                       AND memory_kind = ?
                       AND subject_type = ?
                       AND subject_key = ?
                       AND superseded_by_memory_id IS NULL
                       AND forgotten_at IS NULL
                     ORDER BY updated_at DESC
                     LIMIT 1",
                )
                .bind(scope.as_str())
                .bind(memory_kind.as_str())
                .bind(subject_type)
                .bind(subject_key)
                .fetch_optional(&self.pool)
                .await
            }
        }
        .context("failed to fetch active memory by subject")?;

        Ok(row)
    }

    pub async fn fetch_memory(&self, memory_id: &str) -> Result<Option<MemoryRow>> {
        let row = sqlx::query_as::<_, MemoryRow>(
            "SELECT memory_id, owner_user_id, scope, memory_kind, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at, superseded_by_memory_id, forgotten_at, forgotten_reason
             FROM memories
             WHERE memory_id = ?",
        )
        .bind(memory_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch memory")?;
        Ok(row)
    }

    pub async fn mark_memory_superseded(
        &self,
        memory_id: &str,
        replacement_memory_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE memories
             SET superseded_by_memory_id = ?, updated_at = ?
             WHERE memory_id = ?",
        )
        .bind(replacement_memory_id)
        .bind(now_rfc3339())
        .bind(memory_id)
        .execute(&self.pool)
        .await
        .context("failed to mark memory superseded")?;
        Ok(())
    }

    pub async fn forget_memory(&self, memory_id: &str, reason: &str) -> Result<()> {
        sqlx::query(
            "UPDATE memories
             SET forgotten_at = ?, forgotten_reason = ?, updated_at = ?
             WHERE memory_id = ?",
        )
        .bind(now_rfc3339())
        .bind(reason)
        .bind(now_rfc3339())
        .bind(memory_id)
        .execute(&self.pool)
        .await
        .context("failed to mark memory forgotten")?;
        Ok(())
    }

    pub async fn record_audit_event(
        &self,
        actor_type: &str,
        actor_id: &str,
        event_type: &str,
        payload: Value,
    ) -> Result<String> {
        let audit_id = new_prefixed_id("audit");
        sqlx::query(
            "INSERT INTO audit_events
             (audit_id, actor_type, actor_id, event_type, payload_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&audit_id)
        .bind(actor_type)
        .bind(actor_id)
        .bind(event_type)
        .bind(payload.to_string())
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await
        .context("failed to record audit event")?;
        Ok(audit_id)
    }

    pub async fn list_audit_events(&self, limit: i64) -> Result<Vec<AuditEventRow>> {
        let rows = sqlx::query_as::<_, AuditEventRow>(
            "SELECT audit_id, actor_type, actor_id, event_type, payload_json, created_at
             FROM audit_events
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list audit events")?;
        Ok(rows)
    }

    pub async fn begin_runtime_session(
        &self,
        instance_id: &str,
        pid: i64,
        socket_path: &str,
        automation_planning_enabled: bool,
    ) -> Result<RuntimeSessionRow> {
        let session_id = new_prefixed_id("runtime");
        let started_at = now_rfc3339();
        sqlx::query(
            "INSERT INTO runtime_sessions
             (session_id, instance_id, state, pid, socket_path, queue_depth, active_task_id, automation_planning_enabled, started_at, last_heartbeat_at, stopped_at, last_error)
             VALUES (?, ?, 'starting', ?, ?, 0, NULL, ?, ?, NULL, NULL, NULL)",
        )
        .bind(&session_id)
        .bind(instance_id)
        .bind(pid)
        .bind(socket_path)
        .bind(if automation_planning_enabled { 1_i64 } else { 0_i64 })
        .bind(&started_at)
        .execute(&self.pool)
        .await
        .context("failed to create runtime session")?;

        self.fetch_runtime_session(&session_id)
            .await?
            .context("runtime session missing after insert")
    }

    pub async fn heartbeat_runtime_session(
        &self,
        session_id: &str,
        state: &str,
        queue_depth: i64,
        active_task_id: Option<&str>,
        automation_planning_enabled: bool,
    ) -> Result<()> {
        let observed_at = now_rfc3339();
        sqlx::query(
            "UPDATE runtime_sessions
             SET state = ?, queue_depth = ?, active_task_id = ?, automation_planning_enabled = ?, last_heartbeat_at = ?
             WHERE session_id = ?",
        )
        .bind(state)
        .bind(queue_depth)
        .bind(active_task_id)
        .bind(if automation_planning_enabled { 1_i64 } else { 0_i64 })
        .bind(&observed_at)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to update runtime session heartbeat")?;

        let heartbeat_id = new_prefixed_id("heartbeat");
        sqlx::query(
            "INSERT INTO runtime_heartbeats
             (heartbeat_id, session_id, state, queue_depth, active_task_id, automation_planning_enabled, observed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&heartbeat_id)
        .bind(session_id)
        .bind(state)
        .bind(queue_depth)
        .bind(active_task_id)
        .bind(if automation_planning_enabled { 1_i64 } else { 0_i64 })
        .bind(&observed_at)
        .execute(&self.pool)
        .await
        .context("failed to record runtime heartbeat")?;

        Ok(())
    }

    pub async fn finish_runtime_session(
        &self,
        session_id: &str,
        state: &str,
        queue_depth: i64,
        active_task_id: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<()> {
        let stopped_at = now_rfc3339();
        sqlx::query(
            "UPDATE runtime_sessions
             SET state = ?, queue_depth = ?, active_task_id = ?, last_error = ?, stopped_at = ?, last_heartbeat_at = ?
             WHERE session_id = ?",
        )
        .bind(state)
        .bind(queue_depth)
        .bind(active_task_id)
        .bind(last_error)
        .bind(&stopped_at)
        .bind(&stopped_at)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("failed to finish runtime session")?;
        Ok(())
    }

    pub async fn fetch_runtime_session(
        &self,
        session_id: &str,
    ) -> Result<Option<RuntimeSessionRow>> {
        let row = sqlx::query_as::<_, RuntimeSessionRow>(
            "SELECT session_id, instance_id, state, pid, socket_path, queue_depth, active_task_id, automation_planning_enabled, started_at, last_heartbeat_at, stopped_at, last_error
             FROM runtime_sessions
             WHERE session_id = ?",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch runtime session")?;
        Ok(row)
    }

    pub async fn latest_runtime_session(
        &self,
        instance_id: &str,
    ) -> Result<Option<RuntimeSessionRow>> {
        let row = sqlx::query_as::<_, RuntimeSessionRow>(
            "SELECT session_id, instance_id, state, pid, socket_path, queue_depth, active_task_id, automation_planning_enabled, started_at, last_heartbeat_at, stopped_at, last_error
             FROM runtime_sessions
             WHERE instance_id = ?
             ORDER BY started_at DESC
             LIMIT 1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch latest runtime session")?;
        Ok(row)
    }

    pub async fn list_runtime_heartbeats(
        &self,
        session_id: &str,
        limit: i64,
    ) -> Result<Vec<RuntimeHeartbeatRow>> {
        let rows = sqlx::query_as::<_, RuntimeHeartbeatRow>(
            "SELECT heartbeat_id, session_id, state, queue_depth, active_task_id, automation_planning_enabled, observed_at
             FROM runtime_heartbeats
             WHERE session_id = ?
             ORDER BY observed_at DESC
             LIMIT ?",
        )
        .bind(session_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list runtime heartbeats")?;
        Ok(rows)
    }

    pub async fn create_approval(&self, input: NewApproval<'_>) -> Result<ApprovalRow> {
        let approval_id = new_prefixed_id("approval");
        let created_at = now_rfc3339();
        sqlx::query(
            "INSERT INTO approvals
             (approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, risk_level, action_summary, requester_display_name, target_details, decided_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, NULL, ?)",
        )
        .bind(&approval_id)
        .bind(input.task_id)
        .bind(input.action_type)
        .bind(input.target_ref)
        .bind(input.requested_by_agent_id)
        .bind(input.requested_from_user_id)
        .bind(input.rationale_text)
        .bind(input.risk_level)
        .bind(input.action_summary)
        .bind(input.requester_display_name)
        .bind(input.target_details)
        .bind(&created_at)
        .execute(&self.pool)
        .await
        .context("failed to create approval")?;
        self.fetch_approval_for_user(input.requested_from_user_id, &approval_id)
            .await?
            .context("approval missing after insert")
    }

    pub async fn issue_approval_action_set(
        &self,
        approval_id: &str,
        input: IssueApprovalActions<'_>,
    ) -> Result<ApprovalActionSet> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to start approval action transaction")?;
        sqlx::query(
            "UPDATE approval_actions
             SET status = 'superseded'
             WHERE approval_id = ? AND status = 'pending'",
        )
        .bind(approval_id)
        .execute(&mut *transaction)
        .await
        .context("failed to supersede prior approval actions")?;
        let approve = insert_approval_action(
            &mut transaction,
            approval_id,
            "approve",
            input.connector_type,
            input.connector_identity_id,
            input.channel,
            input.ttl_secs,
        )
        .await?;
        let deny = insert_approval_action(
            &mut transaction,
            approval_id,
            "deny",
            input.connector_type,
            input.connector_identity_id,
            input.channel,
            input.ttl_secs,
        )
        .await?;
        let inspect = insert_approval_action(
            &mut transaction,
            approval_id,
            "inspect",
            input.connector_type,
            input.connector_identity_id,
            input.channel,
            input.ttl_secs,
        )
        .await?;
        let confirm = if input.include_confirm {
            Some(
                insert_approval_action(
                    &mut transaction,
                    approval_id,
                    "confirm",
                    input.connector_type,
                    input.connector_identity_id,
                    input.channel,
                    input.ttl_secs,
                )
                .await?,
            )
        } else {
            None
        };
        transaction
            .commit()
            .await
            .context("failed to commit approval action transaction")?;
        Ok(ApprovalActionSet {
            approve,
            deny,
            inspect,
            confirm,
        })
    }

    pub async fn issue_approval_action(
        &self,
        approval_id: &str,
        action_kind: &str,
        connector_type: Option<&str>,
        connector_identity_id: Option<&str>,
        channel: Option<&str>,
        ttl_secs: i64,
    ) -> Result<ApprovalActionRow> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to start single approval action transaction")?;
        let action = insert_approval_action(
            &mut transaction,
            approval_id,
            action_kind,
            connector_type,
            connector_identity_id,
            channel,
            ttl_secs,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit single approval action transaction")?;
        Ok(action)
    }

    pub async fn fetch_approval_action_by_token(
        &self,
        action_token: &str,
    ) -> Result<Option<ApprovalActionRow>> {
        let row = sqlx::query_as::<_, ApprovalActionRow>(
            "SELECT action_id, approval_id, action_kind, action_token, status, issued_to_connector_type, issued_to_connector_identity_id, issued_to_channel, expires_at, consumed_at, created_at
             FROM approval_actions
             WHERE action_token = ?",
        )
        .bind(action_token)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch approval action by token")?;
        Ok(row)
    }

    pub async fn list_approval_actions(&self, approval_id: &str) -> Result<Vec<ApprovalActionRow>> {
        let rows = sqlx::query_as::<_, ApprovalActionRow>(
            "SELECT action_id, approval_id, action_kind, action_token, status, issued_to_connector_type, issued_to_connector_identity_id, issued_to_channel, expires_at, consumed_at, created_at
             FROM approval_actions
             WHERE approval_id = ?
             ORDER BY created_at ASC",
        )
        .bind(approval_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list approval actions")?;
        Ok(rows)
    }

    pub async fn supersede_pending_approval_actions(&self, approval_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE approval_actions
             SET status = 'superseded'
             WHERE approval_id = ? AND status = 'pending'",
        )
        .bind(approval_id)
        .execute(&self.pool)
        .await
        .context("failed to supersede pending approval actions")?;
        Ok(())
    }

    pub async fn expire_approval_action(&self, action_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE approval_actions
             SET status = 'expired'
             WHERE action_id = ? AND status = 'pending'",
        )
        .bind(action_id)
        .execute(&self.pool)
        .await
        .context("failed to mark approval action expired")?;
        Ok(())
    }

    pub async fn pending_approval_for_task(
        &self,
        requested_from_user_id: &str,
        task_id: &str,
    ) -> Result<Option<ApprovalRow>> {
        let row = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, risk_level, action_summary, requester_display_name, target_details, decided_at, created_at
             FROM approvals
             WHERE requested_from_user_id = ?
               AND task_id = ?
               AND status = 'pending'
             ORDER BY created_at DESC
             LIMIT 1",
        )
        .bind(requested_from_user_id)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch pending approval for task")?;
        Ok(row)
    }

    pub async fn consume_approval_action(
        &self,
        action_token: &str,
    ) -> Result<Option<ApprovalActionRow>> {
        let now = now_rfc3339();
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to start approval action consume transaction")?;
        let Some(mut row) = sqlx::query_as::<_, ApprovalActionRow>(
            "SELECT action_id, approval_id, action_kind, action_token, status, issued_to_connector_type, issued_to_connector_identity_id, issued_to_channel, expires_at, consumed_at, created_at
             FROM approval_actions
             WHERE action_token = ?",
        )
        .bind(action_token)
        .fetch_optional(&mut *transaction)
        .await
        .context("failed to fetch approval action for consume")?
        else {
            transaction
                .commit()
                .await
                .context("failed to finalize empty approval action consume")?;
            return Ok(None);
        };

        if row.status != "pending" {
            transaction
                .commit()
                .await
                .context("failed to finalize already-resolved approval action consume")?;
            return Ok(None);
        }

        if row.expires_at <= now {
            sqlx::query(
                "UPDATE approval_actions
                 SET status = 'expired'
                 WHERE action_id = ? AND status = 'pending'",
            )
            .bind(&row.action_id)
            .execute(&mut *transaction)
            .await
            .context("failed to expire approval action")?;
            transaction
                .commit()
                .await
                .context("failed to finalize expired approval action consume")?;
            return Ok(None);
        }

        let result = sqlx::query(
            "UPDATE approval_actions
             SET status = 'consumed', consumed_at = ?
             WHERE action_id = ? AND status = 'pending'",
        )
        .bind(&now)
        .bind(&row.action_id)
        .execute(&mut *transaction)
        .await
        .context("failed to consume approval action")?;
        if result.rows_affected() == 0 {
            transaction
                .commit()
                .await
                .context("failed to finalize raced approval action consume")?;
            return Ok(None);
        }

        row.status = "consumed".to_owned();
        row.consumed_at = Some(now);
        transaction
            .commit()
            .await
            .context("failed to commit approval action consume")?;
        Ok(Some(row))
    }

    pub async fn fetch_approval_for_user(
        &self,
        requested_from_user_id: &str,
        approval_id: &str,
    ) -> Result<Option<ApprovalRow>> {
        let row = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, risk_level, action_summary, requester_display_name, target_details, decided_at, created_at
             FROM approvals
             WHERE requested_from_user_id = ? AND approval_id = ?",
        )
        .bind(requested_from_user_id)
        .bind(approval_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch approval")?;
        Ok(row)
    }

    pub async fn list_approvals_for_user(
        &self,
        requested_from_user_id: &str,
        status: Option<&str>,
    ) -> Result<Vec<ApprovalRow>> {
        let approvals = if let Some(status) = status {
            sqlx::query_as::<_, ApprovalRow>(
                "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, risk_level, action_summary, requester_display_name, target_details, decided_at, created_at
                 FROM approvals
                 WHERE requested_from_user_id = ? AND status = ?
                 ORDER BY created_at DESC",
            )
            .bind(requested_from_user_id)
            .bind(status)
            .fetch_all(&self.pool)
            .await
            .context("failed to list approvals")?
        } else {
            sqlx::query_as::<_, ApprovalRow>(
                "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, risk_level, action_summary, requester_display_name, target_details, decided_at, created_at
                 FROM approvals
                 WHERE requested_from_user_id = ?
                 ORDER BY created_at DESC",
            )
            .bind(requested_from_user_id)
            .fetch_all(&self.pool)
            .await
            .context("failed to list approvals")?
        };
        Ok(approvals)
    }

    pub async fn resolve_approval(&self, approval_id: &str, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE approvals
             SET status = ?, decided_at = ?
             WHERE approval_id = ?",
        )
        .bind(status)
        .bind(now_rfc3339())
        .bind(approval_id)
        .execute(&self.pool)
        .await
        .context("failed to resolve approval")?;
        Ok(())
    }

    pub async fn approved_shell_commands_for_task(
        &self,
        task_id: &str,
        requested_from_user_id: &str,
    ) -> Result<Vec<String>> {
        let approvals = sqlx::query_scalar::<_, String>(
            "SELECT target_ref
             FROM approvals
             WHERE task_id = ?
               AND requested_from_user_id = ?
               AND action_type = 'shell_command'
               AND status = 'approved'
               AND target_ref IS NOT NULL
             ORDER BY created_at ASC",
        )
        .bind(task_id)
        .bind(requested_from_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch approved shell commands")?;
        Ok(approvals)
    }

    pub async fn approved_approvals_for_task(
        &self,
        task_id: &str,
        requested_from_user_id: &str,
    ) -> Result<Vec<ApprovalRow>> {
        let approvals = sqlx::query_as::<_, ApprovalRow>(
                        "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, risk_level, action_summary, requester_display_name, target_details, decided_at, created_at
             FROM approvals
             WHERE task_id = ?
               AND requested_from_user_id = ?
               AND status = 'approved'
             ORDER BY created_at ASC",
        )
        .bind(task_id)
        .bind(requested_from_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch approved approvals")?;
        Ok(approvals)
    }

    pub async fn create_script(&self, input: NewScript<'_>) -> Result<ScriptRow> {
        let script_id = input
            .script_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| new_prefixed_id("script"));
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO scripts
             (script_id, owner_user_id, kind, status, source_text, capability_profile_json, created_from_task_id, safety_status, safety_summary, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&script_id)
        .bind(input.owner_user_id)
        .bind(input.kind)
        .bind(input.status)
        .bind(input.source_text)
        .bind(input.capability_profile_json.to_string())
        .bind(input.created_from_task_id)
        .bind(input.safety_status)
        .bind(input.safety_summary)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create script")?;

        self.fetch_script_for_owner(input.owner_user_id, &script_id)
            .await?
            .context("script missing after insert")
    }

    pub async fn fetch_script_for_owner(
        &self,
        owner_user_id: &str,
        script_id: &str,
    ) -> Result<Option<ScriptRow>> {
        let row = sqlx::query_as::<_, ScriptRow>(
            "SELECT script_id, owner_user_id, kind, status, source_text, capability_profile_json, created_from_task_id, safety_status, safety_summary, created_at, updated_at
             FROM scripts
             WHERE owner_user_id = ? AND script_id = ?",
        )
        .bind(owner_user_id)
        .bind(script_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch script")?;
        Ok(row)
    }

    pub async fn fetch_script(&self, script_id: &str) -> Result<Option<ScriptRow>> {
        let row = sqlx::query_as::<_, ScriptRow>(
            "SELECT script_id, owner_user_id, kind, status, source_text, capability_profile_json, created_from_task_id, safety_status, safety_summary, created_at, updated_at
             FROM scripts
             WHERE script_id = ?",
        )
        .bind(script_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch script")?;
        Ok(row)
    }

    pub async fn list_scripts_for_owner(&self, owner_user_id: &str) -> Result<Vec<ScriptRow>> {
        let rows = sqlx::query_as::<_, ScriptRow>(
            "SELECT script_id, owner_user_id, kind, status, source_text, capability_profile_json, created_from_task_id, safety_status, safety_summary, created_at, updated_at
             FROM scripts
             WHERE owner_user_id = ?
             ORDER BY created_at DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list scripts")?;
        Ok(rows)
    }

    pub async fn update_script_status(&self, script_id: &str, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE scripts
             SET status = ?, updated_at = ?
             WHERE script_id = ?",
        )
        .bind(status)
        .bind(now_rfc3339())
        .bind(script_id)
        .execute(&self.pool)
        .await
        .context("failed to update script status")?;
        Ok(())
    }

    pub async fn update_script_contents(&self, input: UpdateScript<'_>) -> Result<()> {
        sqlx::query(
            "UPDATE scripts
             SET source_text = ?, capability_profile_json = ?, created_from_task_id = ?, status = ?, safety_status = ?, safety_summary = ?, updated_at = ?
             WHERE script_id = ? AND owner_user_id = ?",
        )
        .bind(input.source_text)
        .bind(input.capability_profile_json.to_string())
        .bind(input.created_from_task_id)
        .bind(input.status)
        .bind(input.safety_status)
        .bind(input.safety_summary)
        .bind(now_rfc3339())
        .bind(input.script_id)
        .bind(input.owner_user_id)
        .execute(&self.pool)
        .await
        .context("failed to update script contents")?;
        Ok(())
    }

    pub async fn update_script_safety(
        &self,
        script_id: &str,
        safety_status: &str,
        safety_summary: &str,
        status: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE scripts
             SET safety_status = ?, safety_summary = ?, status = COALESCE(?, status), updated_at = ?
             WHERE script_id = ?",
        )
        .bind(safety_status)
        .bind(safety_summary)
        .bind(status)
        .bind(now_rfc3339())
        .bind(script_id)
        .execute(&self.pool)
        .await
        .context("failed to update script safety")?;
        Ok(())
    }

    pub async fn create_script_review(
        &self,
        input: NewScriptReview<'_>,
    ) -> Result<ScriptReviewRow> {
        let review_id = new_prefixed_id("review");
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO script_reviews
             (review_id, script_id, reviewer_agent_id, review_type, verdict, risk_level, findings_json, summary_text, reviewed_artifact_text, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&review_id)
        .bind(input.script_id)
        .bind(input.reviewer_agent_id)
        .bind(input.review_type)
        .bind(input.verdict)
        .bind(input.risk_level)
        .bind(input.findings_json.to_string())
        .bind(input.summary_text)
        .bind(input.reviewed_artifact_text)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create script review")?;

        self.list_script_reviews(input.script_id)
            .await?
            .into_iter()
            .find(|row| row.review_id == review_id)
            .ok_or_else(|| anyhow!("script review missing after insert"))
    }

    pub async fn list_script_reviews(&self, script_id: &str) -> Result<Vec<ScriptReviewRow>> {
        let rows = sqlx::query_as::<_, ScriptReviewRow>(
            "SELECT review_id, script_id, reviewer_agent_id, review_type, verdict, risk_level, findings_json, summary_text, reviewed_artifact_text, created_at
             FROM script_reviews
             WHERE script_id = ?
             ORDER BY created_at DESC",
        )
        .bind(script_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list script reviews")?;
        Ok(rows)
    }

    pub async fn create_lua_tool(&self, input: NewLuaTool<'_>) -> Result<LuaToolRow> {
        let tool_id = input
            .tool_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| new_prefixed_id("lua_tool"));
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO lua_tools
             (tool_id, owner_user_id, description, source_text, input_schema_json, output_schema_json, capability_profile_json, status, created_from_task_id, safety_status, safety_summary, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&tool_id)
        .bind(input.owner_user_id)
        .bind(input.description)
        .bind(input.source_text)
        .bind(input.input_schema_json.to_string())
        .bind(input.output_schema_json.to_string())
        .bind(input.capability_profile_json.to_string())
        .bind(input.status)
        .bind(input.created_from_task_id)
        .bind(input.safety_status)
        .bind(input.safety_summary)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create lua tool")?;

        self.fetch_lua_tool_for_owner(input.owner_user_id, &tool_id)
            .await?
            .context("lua tool missing after insert")
    }

    pub async fn fetch_lua_tool_for_owner(
        &self,
        owner_user_id: &str,
        tool_id: &str,
    ) -> Result<Option<LuaToolRow>> {
        let row = sqlx::query_as::<_, LuaToolRow>(
            "SELECT tool_id, owner_user_id, description, source_text, input_schema_json, output_schema_json, capability_profile_json, status, created_from_task_id, safety_status, safety_summary, created_at, updated_at
             FROM lua_tools
             WHERE owner_user_id = ? AND tool_id = ?",
        )
        .bind(owner_user_id)
        .bind(tool_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch lua tool")?;
        Ok(row)
    }

    pub async fn list_lua_tools_for_owner(&self, owner_user_id: &str) -> Result<Vec<LuaToolRow>> {
        let rows = sqlx::query_as::<_, LuaToolRow>(
            "SELECT tool_id, owner_user_id, description, source_text, input_schema_json, output_schema_json, capability_profile_json, status, created_from_task_id, safety_status, safety_summary, created_at, updated_at
             FROM lua_tools
             WHERE owner_user_id = ?
             ORDER BY created_at DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list lua tools")?;
        Ok(rows)
    }

    pub async fn list_active_lua_tools_for_owner(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<LuaToolRow>> {
        let rows = sqlx::query_as::<_, LuaToolRow>(
            "SELECT tool_id, owner_user_id, description, source_text, input_schema_json, output_schema_json, capability_profile_json, status, created_from_task_id, safety_status, safety_summary, created_at, updated_at
             FROM lua_tools
             WHERE owner_user_id = ?
               AND status = 'active'
               AND safety_status = 'approved'
             ORDER BY created_at DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list active lua tools")?;
        Ok(rows)
    }

    pub async fn update_lua_tool_contents(&self, input: UpdateLuaTool<'_>) -> Result<()> {
        sqlx::query(
            "UPDATE lua_tools
             SET description = ?, source_text = ?, input_schema_json = ?, output_schema_json = ?, capability_profile_json = ?, created_from_task_id = ?, status = ?, safety_status = ?, safety_summary = ?, updated_at = ?
             WHERE tool_id = ? AND owner_user_id = ?",
        )
        .bind(input.description)
        .bind(input.source_text)
        .bind(input.input_schema_json.to_string())
        .bind(input.output_schema_json.to_string())
        .bind(input.capability_profile_json.to_string())
        .bind(input.created_from_task_id)
        .bind(input.status)
        .bind(input.safety_status)
        .bind(input.safety_summary)
        .bind(now_rfc3339())
        .bind(input.tool_id)
        .bind(input.owner_user_id)
        .execute(&self.pool)
        .await
        .context("failed to update lua tool contents")?;
        Ok(())
    }

    pub async fn update_lua_tool_status(&self, tool_id: &str, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE lua_tools
             SET status = ?, updated_at = ?
             WHERE tool_id = ?",
        )
        .bind(status)
        .bind(now_rfc3339())
        .bind(tool_id)
        .execute(&self.pool)
        .await
        .context("failed to update lua tool status")?;
        Ok(())
    }

    pub async fn update_lua_tool_safety(
        &self,
        tool_id: &str,
        safety_status: &str,
        safety_summary: &str,
        status: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE lua_tools
             SET safety_status = ?, safety_summary = ?, status = COALESCE(?, status), updated_at = ?
             WHERE tool_id = ?",
        )
        .bind(safety_status)
        .bind(safety_summary)
        .bind(status)
        .bind(now_rfc3339())
        .bind(tool_id)
        .execute(&self.pool)
        .await
        .context("failed to update lua tool safety")?;
        Ok(())
    }

    pub async fn create_lua_tool_review(
        &self,
        input: NewLuaToolReview<'_>,
    ) -> Result<LuaToolReviewRow> {
        let review_id = new_prefixed_id("review");
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO lua_tool_reviews
             (review_id, tool_id, reviewer_agent_id, review_type, verdict, risk_level, findings_json, summary_text, reviewed_artifact_text, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&review_id)
        .bind(input.tool_id)
        .bind(input.reviewer_agent_id)
        .bind(input.review_type)
        .bind(input.verdict)
        .bind(input.risk_level)
        .bind(input.findings_json.to_string())
        .bind(input.summary_text)
        .bind(input.reviewed_artifact_text)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create lua tool review")?;

        self.list_lua_tool_reviews(input.tool_id)
            .await?
            .into_iter()
            .find(|row| row.review_id == review_id)
            .ok_or_else(|| anyhow!("lua tool review missing after insert"))
    }

    pub async fn list_lua_tool_reviews(&self, tool_id: &str) -> Result<Vec<LuaToolReviewRow>> {
        let rows = sqlx::query_as::<_, LuaToolReviewRow>(
            "SELECT review_id, tool_id, reviewer_agent_id, review_type, verdict, risk_level, findings_json, summary_text, reviewed_artifact_text, created_at
             FROM lua_tool_reviews
             WHERE tool_id = ?
             ORDER BY created_at DESC",
        )
        .bind(tool_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list lua tool reviews")?;
        Ok(rows)
    }

    pub async fn create_workflow_definition(
        &self,
        input: NewWorkflowDefinition<'_>,
        stages: &[NewWorkflowStage<'_>],
    ) -> Result<WorkflowDefinitionRow> {
        let workflow_id = input
            .workflow_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| new_prefixed_id("workflow"));
        let timestamp = now_rfc3339();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start workflow definition transaction")?;

        sqlx::query(
            "INSERT INTO workflow_definitions
             (workflow_id, owner_user_id, name, description, status, created_from_task_id, safety_status, safety_summary, current_version_number, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&workflow_id)
        .bind(input.owner_user_id)
        .bind(input.name)
        .bind(input.description)
        .bind(input.status)
        .bind(input.created_from_task_id)
        .bind(input.safety_status)
        .bind(input.safety_summary)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&mut *tx)
        .await
        .context("failed to create workflow definition")?;

        let version_id = new_prefixed_id("workflow_version");
        sqlx::query(
            "INSERT INTO workflow_versions
             (version_id, workflow_id, version_number, name, description, created_from_task_id, created_at)
             VALUES (?, ?, 1, ?, ?, ?, ?)",
        )
        .bind(&version_id)
        .bind(&workflow_id)
        .bind(input.name)
        .bind(input.description)
        .bind(input.created_from_task_id)
        .bind(&timestamp)
        .execute(&mut *tx)
        .await
        .context("failed to create workflow version")?;

        for (index, stage) in stages.iter().enumerate() {
            let stage_id = stage
                .stage_id
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| new_prefixed_id("workflow_stage"));
            sqlx::query(
                "INSERT INTO workflow_stages
                 (stage_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at, updated_at)
                 VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&stage_id)
            .bind(&workflow_id)
            .bind(index as i64)
            .bind(stage.stage_kind)
            .bind(stage.stage_label)
            .bind(stage.artifact_ref)
            .bind(stage.stage_config_json.to_string())
            .bind(&timestamp)
            .bind(&timestamp)
            .execute(&mut *tx)
            .await
            .context("failed to create workflow stage")?;
            let stage_version_id = new_prefixed_id("workflow_stage_version");
            sqlx::query(
                "INSERT INTO workflow_stage_versions
                 (stage_version_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at)
                 VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&stage_version_id)
            .bind(&workflow_id)
            .bind(index as i64)
            .bind(stage.stage_kind)
            .bind(stage.stage_label)
            .bind(stage.artifact_ref)
            .bind(stage.stage_config_json.to_string())
            .bind(&timestamp)
            .execute(&mut *tx)
            .await
            .context("failed to create workflow stage version")?;
        }

        tx.commit()
            .await
            .context("failed to commit workflow definition transaction")?;

        self.fetch_workflow_definition_for_owner(input.owner_user_id, &workflow_id)
            .await?
            .context("workflow definition missing after insert")
    }

    pub async fn update_workflow_definition_contents(
        &self,
        input: UpdateWorkflowDefinition<'_>,
        stages: &[NewWorkflowStage<'_>],
    ) -> Result<WorkflowDefinitionRow> {
        let existing = self
            .fetch_workflow_definition_for_owner(input.owner_user_id, input.workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{}' not found", input.workflow_id))?;
        let next_version_number = existing.current_version_number + 1;
        let timestamp = now_rfc3339();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start workflow update transaction")?;

        sqlx::query(
            "UPDATE workflow_definitions
             SET name = ?, description = ?, created_from_task_id = ?, status = ?, safety_status = ?, safety_summary = ?, current_version_number = ?, updated_at = ?
             WHERE workflow_id = ? AND owner_user_id = ?",
        )
        .bind(input.name)
        .bind(input.description)
        .bind(input.created_from_task_id)
        .bind(input.status)
        .bind(input.safety_status)
        .bind(input.safety_summary)
        .bind(next_version_number)
        .bind(&timestamp)
        .bind(input.workflow_id)
        .bind(input.owner_user_id)
        .execute(&mut *tx)
        .await
        .context("failed to update workflow definition")?;

        let version_id = new_prefixed_id("workflow_version");
        sqlx::query(
            "INSERT INTO workflow_versions
             (version_id, workflow_id, version_number, name, description, created_from_task_id, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&version_id)
        .bind(input.workflow_id)
        .bind(next_version_number)
        .bind(input.name)
        .bind(input.description)
        .bind(input.created_from_task_id)
        .bind(&timestamp)
        .execute(&mut *tx)
        .await
        .context("failed to create workflow version")?;

        sqlx::query("DELETE FROM workflow_stages WHERE workflow_id = ?")
            .bind(input.workflow_id)
            .execute(&mut *tx)
            .await
            .context("failed to clear current workflow stages")?;

        for (index, stage) in stages.iter().enumerate() {
            let stage_id = stage
                .stage_id
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| new_prefixed_id("workflow_stage"));
            sqlx::query(
                "INSERT INTO workflow_stages
                 (stage_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&stage_id)
            .bind(input.workflow_id)
            .bind(next_version_number)
            .bind(index as i64)
            .bind(stage.stage_kind)
            .bind(stage.stage_label)
            .bind(stage.artifact_ref)
            .bind(stage.stage_config_json.to_string())
            .bind(&timestamp)
            .bind(&timestamp)
            .execute(&mut *tx)
            .await
            .context("failed to create current workflow stage")?;
            let stage_version_id = new_prefixed_id("workflow_stage_version");
            sqlx::query(
                "INSERT INTO workflow_stage_versions
                 (stage_version_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&stage_version_id)
            .bind(input.workflow_id)
            .bind(next_version_number)
            .bind(index as i64)
            .bind(stage.stage_kind)
            .bind(stage.stage_label)
            .bind(stage.artifact_ref)
            .bind(stage.stage_config_json.to_string())
            .bind(&timestamp)
            .execute(&mut *tx)
            .await
            .context("failed to create workflow stage history")?;
        }

        tx.commit()
            .await
            .context("failed to commit workflow update transaction")?;

        self.fetch_workflow_definition_for_owner(input.owner_user_id, input.workflow_id)
            .await?
            .context("workflow definition missing after update")
    }

    pub async fn fetch_workflow_definition_for_owner(
        &self,
        owner_user_id: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowDefinitionRow>> {
        let row = sqlx::query_as::<_, WorkflowDefinitionRow>(
            "SELECT workflow_id, owner_user_id, name, description, status, created_from_task_id, safety_status, safety_summary, current_version_number, created_at, updated_at
             FROM workflow_definitions
             WHERE owner_user_id = ? AND workflow_id = ?",
        )
        .bind(owner_user_id)
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch workflow definition")?;
        Ok(row)
    }

    pub async fn fetch_workflow_definition(
        &self,
        workflow_id: &str,
    ) -> Result<Option<WorkflowDefinitionRow>> {
        let row = sqlx::query_as::<_, WorkflowDefinitionRow>(
            "SELECT workflow_id, owner_user_id, name, description, status, created_from_task_id, safety_status, safety_summary, current_version_number, created_at, updated_at
             FROM workflow_definitions
             WHERE workflow_id = ?",
        )
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch workflow definition")?;
        Ok(row)
    }

    pub async fn list_workflow_definitions_for_owner(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<WorkflowDefinitionRow>> {
        let rows = sqlx::query_as::<_, WorkflowDefinitionRow>(
            "SELECT workflow_id, owner_user_id, name, description, status, created_from_task_id, safety_status, safety_summary, current_version_number, created_at, updated_at
             FROM workflow_definitions
             WHERE owner_user_id = ?
             ORDER BY created_at DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow definitions")?;
        Ok(rows)
    }

    pub async fn list_workflow_stages(&self, workflow_id: &str) -> Result<Vec<WorkflowStageRow>> {
        let rows = sqlx::query_as::<_, WorkflowStageRow>(
            "SELECT stage_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at, updated_at
             FROM workflow_stages
             WHERE workflow_id = ?
             ORDER BY stage_index ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow stages")?;
        Ok(rows)
    }

    pub async fn list_workflow_stages_for_version(
        &self,
        workflow_id: &str,
        version_number: i64,
    ) -> Result<Vec<WorkflowStageRow>> {
        let current_version_number = self
            .fetch_workflow_definition(workflow_id)
            .await?
            .ok_or_else(|| anyhow!("workflow '{workflow_id}' not found"))?
            .current_version_number;
        if version_number == current_version_number {
            return self.list_workflow_stages(workflow_id).await;
        }
        let rows = sqlx::query_as::<_, WorkflowStageRow>(
            "SELECT stage_version_id AS stage_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at, created_at AS updated_at
             FROM workflow_stage_versions
             WHERE workflow_id = ?
               AND version_number = ?
             ORDER BY stage_index ASC",
        )
        .bind(workflow_id)
        .bind(version_number)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow stages for version")?;
        Ok(rows)
    }

    pub async fn list_workflow_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowVersionRow>> {
        let rows = sqlx::query_as::<_, WorkflowVersionRow>(
            "SELECT version_id, workflow_id, version_number, name, description, created_from_task_id, created_at
             FROM workflow_versions
             WHERE workflow_id = ?
             ORDER BY version_number DESC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow versions")?;
        Ok(rows)
    }

    pub async fn update_workflow_definition_status(
        &self,
        workflow_id: &str,
        status: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE workflow_definitions
             SET status = ?, updated_at = ?
             WHERE workflow_id = ?",
        )
        .bind(status)
        .bind(now_rfc3339())
        .bind(workflow_id)
        .execute(&self.pool)
        .await
        .context("failed to update workflow definition status")?;
        Ok(())
    }

    pub async fn update_workflow_definition_safety(
        &self,
        workflow_id: &str,
        safety_status: &str,
        safety_summary: &str,
        status: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE workflow_definitions
             SET safety_status = ?, safety_summary = ?, status = COALESCE(?, status), updated_at = ?
             WHERE workflow_id = ?",
        )
        .bind(safety_status)
        .bind(safety_summary)
        .bind(status)
        .bind(now_rfc3339())
        .bind(workflow_id)
        .execute(&self.pool)
        .await
        .context("failed to update workflow definition safety")?;
        Ok(())
    }

    pub async fn delete_workflow_definition_for_owner(
        &self,
        owner_user_id: &str,
        workflow_id: &str,
    ) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to start workflow delete transaction")?;
        sqlx::query(
            "DELETE FROM schedules
             WHERE owner_user_id = ?
               AND target_type = 'workflow'
               AND target_id = ?",
        )
        .bind(owner_user_id)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await
        .context("failed to delete workflow schedules")?;
        sqlx::query("DELETE FROM workflow_stage_versions WHERE workflow_id = ?")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete workflow stage versions")?;
        sqlx::query("DELETE FROM workflow_stages WHERE workflow_id = ?")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete workflow stages")?;
        sqlx::query("DELETE FROM workflow_versions WHERE workflow_id = ?")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete workflow versions")?;
        sqlx::query("DELETE FROM workflow_reviews WHERE workflow_id = ?")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete workflow reviews")?;
        sqlx::query("DELETE FROM workflow_runs WHERE workflow_id = ?")
            .bind(workflow_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete workflow runs")?;
        sqlx::query(
            "DELETE FROM workflow_definitions
             WHERE owner_user_id = ? AND workflow_id = ?",
        )
        .bind(owner_user_id)
        .bind(workflow_id)
        .execute(&mut *tx)
        .await
        .context("failed to delete workflow definition")?;
        tx.commit()
            .await
            .context("failed to commit workflow delete transaction")?;
        Ok(())
    }

    pub async fn create_workflow_review(
        &self,
        input: NewWorkflowReview<'_>,
    ) -> Result<WorkflowReviewRow> {
        let review_id = new_prefixed_id("review");
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO workflow_reviews
             (review_id, workflow_id, version_number, reviewer_agent_id, review_type, verdict, risk_level, findings_json, summary_text, reviewed_artifact_text, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&review_id)
        .bind(input.workflow_id)
        .bind(input.version_number)
        .bind(input.reviewer_agent_id)
        .bind(input.review_type)
        .bind(input.verdict)
        .bind(input.risk_level)
        .bind(input.findings_json.to_string())
        .bind(input.summary_text)
        .bind(input.reviewed_artifact_text)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create workflow review")?;

        self.list_workflow_reviews(input.workflow_id)
            .await?
            .into_iter()
            .find(|row| row.review_id == review_id)
            .ok_or_else(|| anyhow!("workflow review missing after insert"))
    }

    pub async fn list_workflow_reviews(&self, workflow_id: &str) -> Result<Vec<WorkflowReviewRow>> {
        let rows = sqlx::query_as::<_, WorkflowReviewRow>(
            "SELECT review_id, workflow_id, version_number, reviewer_agent_id, review_type, verdict, risk_level, findings_json, summary_text, reviewed_artifact_text, created_at
             FROM workflow_reviews
             WHERE workflow_id = ?
             ORDER BY created_at DESC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow reviews")?;
        Ok(rows)
    }

    pub async fn create_workflow_run(&self, input: NewWorkflowRun<'_>) -> Result<WorkflowRunRow> {
        let workflow_run_id = input
            .workflow_run_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| new_prefixed_id("workflow_run"));
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO workflow_runs
             (workflow_run_id, workflow_id, owner_user_id, initiating_identity_id, schedule_id, source_task_id, state, current_stage_index, artifacts_json, wake_at, block_reason, last_error, retry_count, started_at, completed_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
        )
        .bind(&workflow_run_id)
        .bind(input.workflow_id)
        .bind(input.owner_user_id)
        .bind(input.initiating_identity_id)
        .bind(input.schedule_id)
        .bind(input.source_task_id)
        .bind(input.state)
        .bind(input.current_stage_index)
        .bind(input.artifacts_json.to_string())
        .bind(input.wake_at)
        .bind(input.block_reason)
        .bind(input.last_error)
        .bind(input.retry_count)
        .bind(input.started_at)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create workflow run")?;

        self.fetch_workflow_run(&workflow_run_id)
            .await?
            .ok_or_else(|| anyhow!("workflow run missing after insert"))
    }

    pub async fn fetch_workflow_run(
        &self,
        workflow_run_id: &str,
    ) -> Result<Option<WorkflowRunRow>> {
        let row = sqlx::query_as::<_, WorkflowRunRow>(
            "SELECT workflow_run_id, workflow_id, owner_user_id, initiating_identity_id, schedule_id, source_task_id, state, current_stage_index, artifacts_json, wake_at, block_reason, last_error, retry_count, started_at, completed_at, created_at, updated_at
             FROM workflow_runs
             WHERE workflow_run_id = ?",
        )
        .bind(workflow_run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch workflow run")?;
        Ok(row)
    }

    pub async fn list_workflow_runs_for_workflow(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowRunRow>> {
        let rows = sqlx::query_as::<_, WorkflowRunRow>(
            "SELECT workflow_run_id, workflow_id, owner_user_id, initiating_identity_id, schedule_id, source_task_id, state, current_stage_index, artifacts_json, wake_at, block_reason, last_error, retry_count, started_at, completed_at, created_at, updated_at
             FROM workflow_runs
             WHERE workflow_id = ?
             ORDER BY created_at DESC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow runs")?;
        Ok(rows)
    }

    pub async fn fetch_workflow_run_for_owner(
        &self,
        owner_user_id: &str,
        workflow_run_id: &str,
    ) -> Result<Option<WorkflowRunRow>> {
        let row = sqlx::query_as::<_, WorkflowRunRow>(
            "SELECT workflow_run_id, workflow_id, owner_user_id, initiating_identity_id, schedule_id, source_task_id, state, current_stage_index, artifacts_json, wake_at, block_reason, last_error, retry_count, started_at, completed_at, created_at, updated_at
             FROM workflow_runs
             WHERE owner_user_id = ?
               AND workflow_run_id = ?",
        )
        .bind(owner_user_id)
        .bind(workflow_run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch workflow run for owner")?;
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_workflow_run(
        &self,
        workflow_run_id: &str,
        state: &str,
        current_stage_index: i64,
        artifacts_json: &Value,
        wake_at: Option<&str>,
        block_reason: Option<&str>,
        last_error: Option<&str>,
        retry_count: i64,
        completed: bool,
    ) -> Result<()> {
        let completed_at = if completed { Some(now_rfc3339()) } else { None };
        sqlx::query(
            "UPDATE workflow_runs
             SET state = ?, current_stage_index = ?, artifacts_json = ?, wake_at = ?, block_reason = ?, last_error = ?, retry_count = ?, completed_at = COALESCE(?, completed_at), updated_at = ?
             WHERE workflow_run_id = ?",
        )
        .bind(state)
        .bind(current_stage_index)
        .bind(artifacts_json.to_string())
        .bind(wake_at)
        .bind(block_reason)
        .bind(last_error)
        .bind(retry_count)
        .bind(completed_at.as_deref())
        .bind(now_rfc3339())
        .bind(workflow_run_id)
        .execute(&self.pool)
        .await
        .context("failed to update workflow run")?;
        Ok(())
    }

    pub async fn create_schedule(&self, input: NewSchedule<'_>) -> Result<ScheduleRow> {
        let schedule_id = input
            .schedule_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| new_prefixed_id("sched"));
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO schedules
             (schedule_id, owner_user_id, target_type, target_id, cron_expr, enabled, next_run_at, last_run_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
        )
        .bind(&schedule_id)
        .bind(input.owner_user_id)
        .bind(input.target_type)
        .bind(input.target_id)
        .bind(input.cron_expr)
        .bind(if input.enabled { 1_i64 } else { 0_i64 })
        .bind(input.next_run_at)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create schedule")?;

        self.fetch_schedule_for_owner(input.owner_user_id, &schedule_id)
            .await?
            .context("schedule missing after insert")
    }

    pub async fn upsert_schedule(&self, input: NewSchedule<'_>) -> Result<ScheduleRow> {
        if let Some(schedule_id) = input.schedule_id
            && self
                .fetch_schedule_for_owner(input.owner_user_id, schedule_id)
                .await?
                .is_some()
        {
            sqlx::query(
                "UPDATE schedules
                 SET target_type = ?, target_id = ?, cron_expr = ?, enabled = ?, next_run_at = ?, updated_at = ?
                 WHERE owner_user_id = ? AND schedule_id = ?",
            )
            .bind(input.target_type)
            .bind(input.target_id)
            .bind(input.cron_expr)
            .bind(if input.enabled { 1_i64 } else { 0_i64 })
            .bind(input.next_run_at)
            .bind(now_rfc3339())
            .bind(input.owner_user_id)
            .bind(schedule_id)
            .execute(&self.pool)
            .await
            .context("failed to update schedule")?;

            return self
                .fetch_schedule_for_owner(input.owner_user_id, schedule_id)
                .await?
                .context("schedule missing after update");
        }

        self.create_schedule(input).await
    }

    pub async fn fetch_schedule_for_owner(
        &self,
        owner_user_id: &str,
        schedule_id: &str,
    ) -> Result<Option<ScheduleRow>> {
        let row = sqlx::query_as::<_, ScheduleRow>(
            "SELECT schedule_id, owner_user_id, target_type, target_id, cron_expr, enabled, next_run_at, last_run_at, created_at, updated_at
             FROM schedules
             WHERE owner_user_id = ? AND schedule_id = ?",
        )
        .bind(owner_user_id)
        .bind(schedule_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch schedule")?;
        Ok(row)
    }

    pub async fn fetch_schedule(&self, schedule_id: &str) -> Result<Option<ScheduleRow>> {
        let row = sqlx::query_as::<_, ScheduleRow>(
            "SELECT schedule_id, owner_user_id, target_type, target_id, cron_expr, enabled, next_run_at, last_run_at, created_at, updated_at
             FROM schedules
             WHERE schedule_id = ?",
        )
        .bind(schedule_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch schedule")?;
        Ok(row)
    }

    pub async fn list_schedules_for_owner(&self, owner_user_id: &str) -> Result<Vec<ScheduleRow>> {
        let rows = sqlx::query_as::<_, ScheduleRow>(
            "SELECT schedule_id, owner_user_id, target_type, target_id, cron_expr, enabled, next_run_at, last_run_at, created_at, updated_at
             FROM schedules
             WHERE owner_user_id = ?
             ORDER BY created_at DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list schedules")?;
        Ok(rows)
    }

    pub async fn list_due_schedules(&self, now: &str) -> Result<Vec<ScheduleRow>> {
        let rows = sqlx::query_as::<_, ScheduleRow>(
            "SELECT schedule_id, owner_user_id, target_type, target_id, cron_expr, enabled, next_run_at, last_run_at, created_at, updated_at
             FROM schedules
             WHERE enabled = 1
               AND next_run_at <= ?
             ORDER BY next_run_at ASC",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context("failed to list due schedules")?;
        Ok(rows)
    }

    pub async fn update_schedule_state(
        &self,
        schedule_id: &str,
        enabled: bool,
        next_run_at: &str,
        last_run_at: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE schedules
             SET enabled = ?, next_run_at = ?, last_run_at = ?, updated_at = ?
             WHERE schedule_id = ?",
        )
        .bind(if enabled { 1_i64 } else { 0_i64 })
        .bind(next_run_at)
        .bind(last_run_at)
        .bind(now_rfc3339())
        .bind(schedule_id)
        .execute(&self.pool)
        .await
        .context("failed to update schedule state")?;
        Ok(())
    }

    pub async fn delete_schedule_for_owner(
        &self,
        owner_user_id: &str,
        schedule_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM schedules
             WHERE owner_user_id = ? AND schedule_id = ?",
        )
        .bind(owner_user_id)
        .bind(schedule_id)
        .execute(&self.pool)
        .await
        .context("failed to delete schedule")?;
        Ok(())
    }

    pub async fn upsert_connector_identity(
        &self,
        connector_type: &str,
        external_user_id: &str,
        external_channel_id: Option<&str>,
        mapped_user_id: &str,
        trust_level: &str,
    ) -> Result<ConnectorIdentityRow> {
        let existing = sqlx::query_as::<_, ConnectorIdentityRow>(
            "SELECT identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at
             FROM connector_identities
             WHERE connector_type = ? AND external_user_id = ?",
        )
        .bind(connector_type)
        .bind(external_user_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch connector identity")?;

        let timestamp = now_rfc3339();
        if let Some(existing) = existing {
            sqlx::query(
                "UPDATE connector_identities
                 SET external_channel_id = ?, mapped_user_id = ?, trust_level = ?, updated_at = ?
                 WHERE identity_id = ?",
            )
            .bind(external_channel_id)
            .bind(mapped_user_id)
            .bind(trust_level)
            .bind(&timestamp)
            .bind(&existing.identity_id)
            .execute(&self.pool)
            .await
            .context("failed to update connector identity")?;
            return self
                .fetch_connector_identity(connector_type, external_user_id)
                .await?
                .context("connector identity missing after update");
        }

        let identity_id = new_prefixed_id("identity");
        sqlx::query(
            "INSERT INTO connector_identities
             (identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&identity_id)
        .bind(connector_type)
        .bind(external_user_id)
        .bind(external_channel_id)
        .bind(mapped_user_id)
        .bind(trust_level)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create connector identity")?;
        self.fetch_connector_identity(connector_type, external_user_id)
            .await?
            .context("connector identity missing after insert")
    }

    pub async fn fetch_connector_identity(
        &self,
        connector_type: &str,
        external_user_id: &str,
    ) -> Result<Option<ConnectorIdentityRow>> {
        let row = sqlx::query_as::<_, ConnectorIdentityRow>(
            "SELECT identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at
             FROM connector_identities
             WHERE connector_type = ? AND external_user_id = ?",
        )
        .bind(connector_type)
        .bind(external_user_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch connector identity")?;
        Ok(row)
    }

    pub async fn list_connector_identities(
        &self,
        connector_type: Option<&str>,
    ) -> Result<Vec<ConnectorIdentityRow>> {
        let rows = if let Some(connector_type) = connector_type {
            sqlx::query_as::<_, ConnectorIdentityRow>(
                "SELECT identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at
                 FROM connector_identities
                 WHERE connector_type = ?
                 ORDER BY connector_type ASC, external_user_id ASC",
            )
            .bind(connector_type)
            .fetch_all(&self.pool)
            .await
            .context("failed to list connector identities")?
        } else {
            sqlx::query_as::<_, ConnectorIdentityRow>(
                "SELECT identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at
                 FROM connector_identities
                 ORDER BY connector_type ASC, external_user_id ASC",
            )
            .fetch_all(&self.pool)
            .await
            .context("failed to list connector identities")?
        };
        Ok(rows)
    }

    pub async fn list_connector_identities_for_mapped_user(
        &self,
        mapped_user_id: &str,
    ) -> Result<Vec<ConnectorIdentityRow>> {
        let rows = sqlx::query_as::<_, ConnectorIdentityRow>(
            "SELECT identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at
             FROM connector_identities
             WHERE mapped_user_id = ?
             ORDER BY connector_type ASC, updated_at DESC, external_user_id ASC",
        )
        .bind(mapped_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list connector identities for mapped user")?;
        Ok(rows)
    }

    pub async fn fetch_connector_identity_by_id(
        &self,
        identity_id: &str,
    ) -> Result<Option<ConnectorIdentityRow>> {
        let row = sqlx::query_as::<_, ConnectorIdentityRow>(
            "SELECT identity_id, connector_type, external_user_id, external_channel_id, mapped_user_id, trust_level, created_at, updated_at
             FROM connector_identities
             WHERE identity_id = ?",
        )
        .bind(identity_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch connector identity by id")?;
        Ok(row)
    }

    pub async fn create_or_reuse_household_delivery(
        &self,
        input: NewHouseholdDelivery<'_>,
    ) -> Result<HouseholdDeliveryRow> {
        if let Some(existing) = self
            .fetch_household_delivery_by_dedupe_key(input.dedupe_key)
            .await?
        {
            return Ok(existing);
        }

        let delivery_id = new_prefixed_id("delivery");
        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO household_deliveries
             (delivery_id, task_id, requester_user_id, requester_identity_id, recipient_user_id, message_text, delivery_mode, connector_type, connector_identity_id, target_ref, fallback_target_ref, dedupe_key, status, failure_reason, external_message_id, delivered_at, parent_delivery_id, schedule_id, scheduled_for_at, window_starts_at, window_ends_at, recurrence_rule, scheduled_job_state, materialized_task_id, completed_at, canceled_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
        )
        .bind(&delivery_id)
        .bind(input.task_id)
        .bind(input.requester_user_id)
        .bind(input.requester_identity_id)
        .bind(input.recipient_user_id)
        .bind(input.message_text)
        .bind(input.delivery_mode)
        .bind(input.connector_type)
        .bind(input.connector_identity_id)
        .bind(input.target_ref)
        .bind(input.fallback_target_ref)
        .bind(input.dedupe_key)
        .bind(input.initial_status)
        .bind(input.parent_delivery_id)
        .bind(input.schedule_id)
        .bind(input.scheduled_for_at)
        .bind(input.window_starts_at)
        .bind(input.window_ends_at)
        .bind(input.recurrence_rule)
        .bind(input.scheduled_job_state)
        .bind(input.materialized_task_id)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to create household delivery")?;

        self.fetch_household_delivery(&delivery_id)
            .await?
            .ok_or_else(|| anyhow!("household delivery missing after insert"))
    }

    pub async fn fetch_household_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<HouseholdDeliveryRow>> {
        let row = sqlx::query_as::<_, HouseholdDeliveryRow>(
            "SELECT delivery_id, task_id, requester_user_id, requester_identity_id, recipient_user_id, message_text, delivery_mode, connector_type, connector_identity_id, target_ref, fallback_target_ref, dedupe_key, status, failure_reason, external_message_id, delivered_at, parent_delivery_id, schedule_id, scheduled_for_at, window_starts_at, window_ends_at, recurrence_rule, scheduled_job_state, materialized_task_id, completed_at, canceled_at, created_at, updated_at
             FROM household_deliveries
             WHERE delivery_id = ?",
        )
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch household delivery")?;
        Ok(row)
    }

    pub async fn fetch_household_delivery_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> Result<Option<HouseholdDeliveryRow>> {
        let row = sqlx::query_as::<_, HouseholdDeliveryRow>(
            "SELECT delivery_id, task_id, requester_user_id, requester_identity_id, recipient_user_id, message_text, delivery_mode, connector_type, connector_identity_id, target_ref, fallback_target_ref, dedupe_key, status, failure_reason, external_message_id, delivered_at, parent_delivery_id, schedule_id, scheduled_for_at, window_starts_at, window_ends_at, recurrence_rule, scheduled_job_state, materialized_task_id, completed_at, canceled_at, created_at, updated_at
             FROM household_deliveries
             WHERE dedupe_key = ?",
        )
        .bind(dedupe_key)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch household delivery by dedupe key")?;
        Ok(row)
    }

    pub async fn list_household_deliveries_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<HouseholdDeliveryRow>> {
        let rows = sqlx::query_as::<_, HouseholdDeliveryRow>(
            "SELECT delivery_id, task_id, requester_user_id, requester_identity_id, recipient_user_id, message_text, delivery_mode, connector_type, connector_identity_id, target_ref, fallback_target_ref, dedupe_key, status, failure_reason, external_message_id, delivered_at, parent_delivery_id, schedule_id, scheduled_for_at, window_starts_at, window_ends_at, recurrence_rule, scheduled_job_state, materialized_task_id, completed_at, canceled_at, created_at, updated_at
             FROM household_deliveries
             WHERE task_id = ?
             ORDER BY created_at ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list household deliveries for task")?;
        Ok(rows)
    }

    pub async fn fetch_scheduled_household_delivery_for_requester(
        &self,
        requester_user_id: &str,
        delivery_id: &str,
    ) -> Result<Option<HouseholdDeliveryRow>> {
        let row = sqlx::query_as::<_, HouseholdDeliveryRow>(
            "SELECT delivery_id, task_id, requester_user_id, requester_identity_id, recipient_user_id, message_text, delivery_mode, connector_type, connector_identity_id, target_ref, fallback_target_ref, dedupe_key, status, failure_reason, external_message_id, delivered_at, parent_delivery_id, schedule_id, scheduled_for_at, window_starts_at, window_ends_at, recurrence_rule, scheduled_job_state, materialized_task_id, completed_at, canceled_at, created_at, updated_at
             FROM household_deliveries
             WHERE requester_user_id = ?
               AND delivery_id = ?
               AND parent_delivery_id IS NULL
               AND delivery_mode != 'immediate'",
        )
        .bind(requester_user_id)
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch scheduled household delivery")?;
        Ok(row)
    }

    pub async fn list_scheduled_household_deliveries_for_requester(
        &self,
        requester_user_id: &str,
    ) -> Result<Vec<HouseholdDeliveryRow>> {
        let rows = sqlx::query_as::<_, HouseholdDeliveryRow>(
            "SELECT delivery_id, task_id, requester_user_id, requester_identity_id, recipient_user_id, message_text, delivery_mode, connector_type, connector_identity_id, target_ref, fallback_target_ref, dedupe_key, status, failure_reason, external_message_id, delivered_at, parent_delivery_id, schedule_id, scheduled_for_at, window_starts_at, window_ends_at, recurrence_rule, scheduled_job_state, materialized_task_id, completed_at, canceled_at, created_at, updated_at
             FROM household_deliveries
             WHERE requester_user_id = ?
               AND parent_delivery_id IS NULL
               AND delivery_mode != 'immediate'
             ORDER BY created_at DESC",
        )
        .bind(requester_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list scheduled household deliveries")?;
        Ok(rows)
    }

    pub async fn mark_household_delivery_sent(
        &self,
        delivery_id: &str,
        connector_type: &str,
        connector_identity_id: &str,
        target_ref: &str,
        external_message_id: Option<&str>,
    ) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE household_deliveries
             SET status = 'sent', connector_type = ?, connector_identity_id = ?, target_ref = ?, external_message_id = ?, delivered_at = ?, failure_reason = NULL, scheduled_job_state = 'completed', completed_at = ?, updated_at = ?
             WHERE delivery_id = ?",
        )
        .bind(connector_type)
        .bind(connector_identity_id)
        .bind(target_ref)
        .bind(external_message_id)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(delivery_id)
        .execute(&self.pool)
        .await
        .context("failed to mark household delivery sent")?;
        Ok(())
    }

    pub async fn mark_household_delivery_failed(
        &self,
        delivery_id: &str,
        reason: &str,
    ) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE household_deliveries
             SET status = 'failed', failure_reason = ?, scheduled_job_state = 'failed', completed_at = ?, updated_at = ?
             WHERE delivery_id = ?",
        )
        .bind(reason)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(delivery_id)
        .execute(&self.pool)
        .await
        .context("failed to mark household delivery failed")?;
        Ok(())
    }

    pub async fn mark_household_delivery_materialized(
        &self,
        delivery_id: &str,
        materialized_task_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE household_deliveries
             SET materialized_task_id = ?, scheduled_job_state = 'materialized', updated_at = ?
             WHERE delivery_id = ?",
        )
        .bind(materialized_task_id)
        .bind(now_rfc3339())
        .bind(delivery_id)
        .execute(&self.pool)
        .await
        .context("failed to mark household delivery materialized")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_household_delivery_schedule(
        &self,
        delivery_id: &str,
        recipient_user_id: &str,
        message_text: &str,
        connector_type: &str,
        connector_identity_id: &str,
        target_ref: &str,
        fallback_target_ref: Option<&str>,
        schedule_id: Option<&str>,
        scheduled_for_at: Option<&str>,
        window_starts_at: Option<&str>,
        window_ends_at: Option<&str>,
        recurrence_rule: Option<&str>,
        scheduled_job_state: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE household_deliveries
             SET recipient_user_id = ?, message_text = ?, connector_type = ?, connector_identity_id = ?, target_ref = ?, fallback_target_ref = ?, schedule_id = ?, scheduled_for_at = ?, window_starts_at = ?, window_ends_at = ?, recurrence_rule = ?, scheduled_job_state = ?, failure_reason = NULL, canceled_at = NULL, updated_at = ?
             WHERE delivery_id = ?",
        )
        .bind(recipient_user_id)
        .bind(message_text)
        .bind(connector_type)
        .bind(connector_identity_id)
        .bind(target_ref)
        .bind(fallback_target_ref)
        .bind(schedule_id)
        .bind(scheduled_for_at)
        .bind(window_starts_at)
        .bind(window_ends_at)
        .bind(recurrence_rule)
        .bind(scheduled_job_state)
        .bind(now_rfc3339())
        .bind(delivery_id)
        .execute(&self.pool)
        .await
        .context("failed to update scheduled household delivery")?;
        Ok(())
    }

    pub async fn cancel_household_delivery(&self, delivery_id: &str, reason: &str) -> Result<()> {
        let timestamp = now_rfc3339();
        sqlx::query(
            "UPDATE household_deliveries
             SET status = 'canceled', failure_reason = ?, scheduled_job_state = 'canceled', canceled_at = ?, completed_at = ?, updated_at = ?
             WHERE delivery_id = ?",
        )
        .bind(reason)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(delivery_id)
        .execute(&self.pool)
        .await
        .context("failed to cancel household delivery")?;
        Ok(())
    }

    pub async fn fetch_task_for_owner(
        &self,
        owner_user_id: &str,
        task_id: &str,
    ) -> Result<Option<TaskRow>> {
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
             FROM tasks
             WHERE owner_user_id = ? AND task_id = ?",
        )
        .bind(owner_user_id)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch task")?;
        Ok(row)
    }

    pub async fn list_recent_interactive_tasks_for_owner(
        &self,
        owner_user_id: &str,
        limit: i64,
    ) -> Result<Vec<TaskRow>> {
        let rows = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
             FROM tasks
             WHERE owner_user_id = ?
               AND kind = 'interactive'
             ORDER BY created_at DESC
             LIMIT ?",
        )
        .bind(owner_user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list recent interactive tasks for owner")?;
        Ok(rows)
    }

    pub async fn list_recent_interactive_tasks_for_session(
        &self,
        session_id: &str,
        since_inclusive: Option<&str>,
        limit: i64,
    ) -> Result<Vec<TaskRow>> {
        let rows = if let Some(since_inclusive) = since_inclusive {
            sqlx::query_as::<_, TaskRow>(
                "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
                 FROM tasks
                 WHERE session_id = ?
                   AND kind = 'interactive'
                   AND created_at >= ?
                 ORDER BY created_at DESC
                 LIMIT ?",
            )
            .bind(session_id)
            .bind(since_inclusive)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, TaskRow>(
                "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
                 FROM tasks
                 WHERE session_id = ?
                   AND kind = 'interactive'
                 ORDER BY created_at DESC
                 LIMIT ?",
            )
            .bind(session_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        }
        .context("failed to list recent interactive tasks for session")?;
        Ok(rows)
    }

    pub async fn fetch_task(&self, task_id: &str) -> Result<Option<TaskRow>> {
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
             FROM tasks
             WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch task")?;
        Ok(row)
    }

    pub async fn fetch_next_runnable_task(&self) -> Result<Option<TaskRow>> {
        let now = now_rfc3339();
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, session_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
             FROM tasks
             WHERE state = ?
               AND (wake_at IS NULL OR wake_at <= ?)
             ORDER BY created_at ASC
             LIMIT 1",
        )
        .bind(TaskState::Pending.as_str())
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch next runnable task")?;
        Ok(row)
    }

    pub async fn pending_task_count(&self) -> Result<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM tasks
             WHERE state = ?",
        )
        .bind(TaskState::Pending.as_str())
        .fetch_one(&self.pool)
        .await
        .context("failed to count pending tasks")?;
        Ok(count)
    }

    pub async fn fetch_task_events_for_owner(
        &self,
        owner_user_id: &str,
        task_id: &str,
    ) -> Result<Vec<TaskEventRow>> {
        let events = sqlx::query_as::<_, TaskEventRow>(
            "SELECT event_id, task_id, event_type, actor_type, actor_id, payload_json, created_at
             FROM task_events
             WHERE task_id = ? AND EXISTS (
               SELECT 1 FROM tasks
               WHERE tasks.task_id = task_events.task_id
                 AND tasks.owner_user_id = ?
             )
             ORDER BY created_at ASC",
        )
        .bind(task_id)
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch task events")?;
        Ok(events)
    }

    pub async fn fetch_task_events(&self, task_id: &str) -> Result<Vec<TaskEventRow>> {
        let events = sqlx::query_as::<_, TaskEventRow>(
            "SELECT event_id, task_id, event_type, actor_type, actor_id, payload_json, created_at
             FROM task_events
             WHERE task_id = ?
             ORDER BY created_at ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch task events")?;
        Ok(events)
    }

    pub async fn list_task_events_for_session(
        &self,
        session_id: &str,
        since_inclusive: Option<&str>,
    ) -> Result<Vec<TaskEventRow>> {
        let events = if let Some(since_inclusive) = since_inclusive {
            sqlx::query_as::<_, TaskEventRow>(
                "SELECT task_events.event_id, task_events.task_id, task_events.event_type, task_events.actor_type, task_events.actor_id, task_events.payload_json, task_events.created_at
                 FROM task_events
                 INNER JOIN tasks ON tasks.task_id = task_events.task_id
                 WHERE tasks.session_id = ?
                   AND tasks.created_at >= ?
                 ORDER BY task_events.created_at ASC",
            )
            .bind(session_id)
            .bind(since_inclusive)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, TaskEventRow>(
                "SELECT task_events.event_id, task_events.task_id, task_events.event_type, task_events.actor_type, task_events.actor_id, task_events.payload_json, task_events.created_at
                 FROM task_events
                 INNER JOIN tasks ON tasks.task_id = task_events.task_id
                 WHERE tasks.session_id = ?
                 ORDER BY task_events.created_at ASC",
            )
            .bind(session_id)
            .fetch_all(&self.pool)
            .await
        }
        .context("failed to list task events for session")?;
        Ok(events)
    }

    pub async fn fetch_tool_invocations_for_owner(
        &self,
        owner_user_id: &str,
        task_id: &str,
    ) -> Result<Vec<ToolInvocationRow>> {
        let invocations = sqlx::query_as::<_, ToolInvocationRow>(
            "SELECT invocation_id, task_id, agent_id, tool_name, request_json, response_json, status, started_at, finished_at
             FROM tool_invocations
             WHERE task_id = ? AND EXISTS (
               SELECT 1 FROM tasks
               WHERE tasks.task_id = tool_invocations.task_id
                 AND tasks.owner_user_id = ?
             )
             ORDER BY started_at ASC",
        )
        .bind(task_id)
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch tool invocations")?;
        Ok(invocations)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapState {
    pub user_id: String,
    pub primary_agent_id: String,
}

#[derive(Debug, Clone, Copy)]
pub struct NewConversationSession<'a> {
    pub session_kind: &'a str,
    pub session_key: &'a str,
    pub audience_policy: &'a str,
    pub max_memory_scope: MemoryScope,
    pub originating_connector_type: Option<&'a str>,
    pub originating_connector_target: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct NewTask<'a> {
    pub owner_user_id: &'a str,
    pub initiating_identity_id: &'a str,
    pub primary_agent_id: &'a str,
    pub assigned_agent_id: &'a str,
    pub parent_task_id: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub kind: &'a str,
    pub objective: &'a str,
    pub context_summary: Option<&'a str>,
    pub scope: MemoryScope,
    pub wake_at: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct NewMemory<'a> {
    pub owner_user_id: Option<&'a str>,
    pub scope: MemoryScope,
    pub memory_kind: MemoryKind,
    pub subject_type: &'a str,
    pub subject_key: Option<&'a str>,
    pub content_text: &'a str,
    pub content_json: Option<&'a Value>,
    pub sensitivity: &'a str,
    pub source_type: &'a str,
    pub source_ref: Option<&'a str>,
    pub task_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct NewScript<'a> {
    pub script_id: Option<&'a str>,
    pub owner_user_id: &'a str,
    pub kind: &'a str,
    pub status: &'a str,
    pub source_text: &'a str,
    pub capability_profile_json: Value,
    pub created_from_task_id: Option<&'a str>,
    pub safety_status: &'a str,
    pub safety_summary: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct NewLuaTool<'a> {
    pub tool_id: Option<&'a str>,
    pub owner_user_id: &'a str,
    pub description: &'a str,
    pub source_text: &'a str,
    pub input_schema_json: Value,
    pub output_schema_json: Value,
    pub capability_profile_json: Value,
    pub status: &'a str,
    pub created_from_task_id: Option<&'a str>,
    pub safety_status: &'a str,
    pub safety_summary: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct NewApproval<'a> {
    pub task_id: &'a str,
    pub action_type: &'a str,
    pub target_ref: Option<&'a str>,
    pub requested_by_agent_id: &'a str,
    pub requested_from_user_id: &'a str,
    pub rationale_text: &'a str,
    pub risk_level: Option<&'a str>,
    pub action_summary: Option<&'a str>,
    pub requester_display_name: Option<&'a str>,
    pub target_details: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct NewHouseholdDelivery<'a> {
    pub task_id: &'a str,
    pub requester_user_id: &'a str,
    pub requester_identity_id: &'a str,
    pub recipient_user_id: &'a str,
    pub message_text: &'a str,
    pub delivery_mode: &'a str,
    pub connector_type: &'a str,
    pub connector_identity_id: &'a str,
    pub target_ref: &'a str,
    pub fallback_target_ref: Option<&'a str>,
    pub dedupe_key: &'a str,
    pub initial_status: &'a str,
    pub parent_delivery_id: Option<&'a str>,
    pub schedule_id: Option<&'a str>,
    pub scheduled_for_at: Option<&'a str>,
    pub window_starts_at: Option<&'a str>,
    pub window_ends_at: Option<&'a str>,
    pub recurrence_rule: Option<&'a str>,
    pub scheduled_job_state: Option<&'a str>,
    pub materialized_task_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct IssueApprovalActions<'a> {
    pub connector_type: Option<&'a str>,
    pub connector_identity_id: Option<&'a str>,
    pub channel: Option<&'a str>,
    pub ttl_secs: i64,
    pub include_confirm: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateScript<'a> {
    pub script_id: &'a str,
    pub owner_user_id: &'a str,
    pub source_text: &'a str,
    pub capability_profile_json: Value,
    pub created_from_task_id: Option<&'a str>,
    pub status: &'a str,
    pub safety_status: &'a str,
    pub safety_summary: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct UpdateLuaTool<'a> {
    pub tool_id: &'a str,
    pub owner_user_id: &'a str,
    pub description: &'a str,
    pub source_text: &'a str,
    pub input_schema_json: Value,
    pub output_schema_json: Value,
    pub capability_profile_json: Value,
    pub created_from_task_id: Option<&'a str>,
    pub status: &'a str,
    pub safety_status: &'a str,
    pub safety_summary: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct NewScriptReview<'a> {
    pub script_id: &'a str,
    pub reviewer_agent_id: &'a str,
    pub review_type: &'a str,
    pub verdict: &'a str,
    pub risk_level: &'a str,
    pub findings_json: Value,
    pub summary_text: &'a str,
    pub reviewed_artifact_text: &'a str,
}

#[derive(Debug, Clone)]
pub struct NewLuaToolReview<'a> {
    pub tool_id: &'a str,
    pub reviewer_agent_id: &'a str,
    pub review_type: &'a str,
    pub verdict: &'a str,
    pub risk_level: &'a str,
    pub findings_json: Value,
    pub summary_text: &'a str,
    pub reviewed_artifact_text: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct NewSchedule<'a> {
    pub schedule_id: Option<&'a str>,
    pub owner_user_id: &'a str,
    pub target_type: &'a str,
    pub target_id: &'a str,
    pub cron_expr: &'a str,
    pub enabled: bool,
    pub next_run_at: &'a str,
}

#[derive(Debug, Clone)]
pub struct NewWorkflowDefinition<'a> {
    pub workflow_id: Option<&'a str>,
    pub owner_user_id: &'a str,
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub status: &'a str,
    pub created_from_task_id: Option<&'a str>,
    pub safety_status: &'a str,
    pub safety_summary: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct NewWorkflowStage<'a> {
    pub stage_id: Option<&'a str>,
    pub stage_kind: &'a str,
    pub stage_label: Option<&'a str>,
    pub artifact_ref: Option<&'a str>,
    pub stage_config_json: Value,
}

#[derive(Debug, Clone)]
pub struct NewWorkflowReview<'a> {
    pub workflow_id: &'a str,
    pub version_number: i64,
    pub reviewer_agent_id: &'a str,
    pub review_type: &'a str,
    pub verdict: &'a str,
    pub risk_level: &'a str,
    pub findings_json: Value,
    pub summary_text: &'a str,
    pub reviewed_artifact_text: &'a str,
}

#[derive(Debug, Clone)]
pub struct NewWorkflowRun<'a> {
    pub workflow_run_id: Option<&'a str>,
    pub workflow_id: &'a str,
    pub owner_user_id: &'a str,
    pub initiating_identity_id: &'a str,
    pub schedule_id: Option<&'a str>,
    pub source_task_id: Option<&'a str>,
    pub state: &'a str,
    pub current_stage_index: i64,
    pub artifacts_json: Value,
    pub wake_at: Option<&'a str>,
    pub block_reason: Option<&'a str>,
    pub last_error: Option<&'a str>,
    pub retry_count: i64,
    pub started_at: &'a str,
}

#[derive(Debug, Clone)]
pub struct UpdateWorkflowDefinition<'a> {
    pub workflow_id: &'a str,
    pub owner_user_id: &'a str,
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub created_from_task_id: Option<&'a str>,
    pub status: &'a str,
    pub safety_status: &'a str,
    pub safety_summary: Option<&'a str>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct UserRow {
    pub user_id: String,
    pub display_name: String,
    pub status: String,
    pub primary_agent_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RoleRow {
    pub role_id: String,
    pub description: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct UserRoleRow {
    pub user_id: String,
    pub role_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AgentRow {
    pub agent_id: String,
    pub kind: String,
    pub owner_user_id: Option<String>,
    pub parent_agent_id: Option<String>,
    pub status: String,
    pub persona: Option<String>,
    pub memory_scope: String,
    pub permission_profile: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TaskRow {
    pub task_id: String,
    pub owner_user_id: String,
    pub initiating_identity_id: String,
    pub primary_agent_id: String,
    pub assigned_agent_id: String,
    pub parent_task_id: Option<String>,
    pub session_id: Option<String>,
    pub kind: String,
    pub state: String,
    pub objective: String,
    pub context_summary: Option<String>,
    pub result_text: Option<String>,
    pub scope: String,
    pub wake_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ConversationSessionRow {
    pub session_id: String,
    pub session_kind: String,
    pub session_key: String,
    pub audience_policy: String,
    pub max_memory_scope: String,
    pub originating_connector_type: Option<String>,
    pub originating_connector_target: Option<String>,
    pub last_activity_at: String,
    pub last_reset_at: Option<String>,
    pub archived_at: Option<String>,
    pub lifecycle_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TaskEventRow {
    pub event_id: String,
    pub task_id: String,
    pub event_type: String,
    pub actor_type: String,
    pub actor_id: String,
    pub payload_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AuditEventRow {
    pub audit_id: String,
    pub actor_type: String,
    pub actor_id: String,
    pub event_type: String,
    pub payload_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct MemoryRow {
    pub memory_id: String,
    pub owner_user_id: Option<String>,
    pub scope: String,
    pub memory_kind: String,
    pub subject_type: String,
    pub subject_key: Option<String>,
    pub content_text: String,
    pub content_json: Option<String>,
    pub sensitivity: String,
    pub source_type: String,
    pub source_ref: Option<String>,
    pub task_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub last_accessed_at: Option<String>,
    pub superseded_by_memory_id: Option<String>,
    pub forgotten_at: Option<String>,
    pub forgotten_reason: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ToolInvocationRow {
    pub invocation_id: String,
    pub task_id: String,
    pub agent_id: String,
    pub tool_name: String,
    pub request_json: String,
    pub response_json: Option<String>,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ApprovalRow {
    pub approval_id: String,
    pub task_id: String,
    pub action_type: String,
    pub target_ref: Option<String>,
    pub requested_by_agent_id: String,
    pub requested_from_user_id: String,
    pub status: String,
    pub rationale_text: Option<String>,
    pub risk_level: Option<String>,
    pub action_summary: Option<String>,
    pub requester_display_name: Option<String>,
    pub target_details: Option<String>,
    pub decided_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ApprovalActionRow {
    pub action_id: String,
    pub approval_id: String,
    pub action_kind: String,
    pub action_token: String,
    pub status: String,
    pub issued_to_connector_type: Option<String>,
    pub issued_to_connector_identity_id: Option<String>,
    pub issued_to_channel: Option<String>,
    pub expires_at: String,
    pub consumed_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalActionSet {
    pub approve: ApprovalActionRow,
    pub deny: ApprovalActionRow,
    pub inspect: ApprovalActionRow,
    pub confirm: Option<ApprovalActionRow>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ScriptRow {
    pub script_id: String,
    pub owner_user_id: String,
    pub kind: String,
    pub status: String,
    pub source_text: String,
    pub capability_profile_json: String,
    pub created_from_task_id: Option<String>,
    pub safety_status: String,
    pub safety_summary: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ScriptReviewRow {
    pub review_id: String,
    pub script_id: String,
    pub reviewer_agent_id: String,
    pub review_type: String,
    pub verdict: String,
    pub risk_level: String,
    pub findings_json: String,
    pub summary_text: String,
    pub reviewed_artifact_text: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct LuaToolRow {
    pub tool_id: String,
    pub owner_user_id: String,
    pub description: String,
    pub source_text: String,
    pub input_schema_json: String,
    pub output_schema_json: String,
    pub capability_profile_json: String,
    pub status: String,
    pub created_from_task_id: Option<String>,
    pub safety_status: String,
    pub safety_summary: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct LuaToolReviewRow {
    pub review_id: String,
    pub tool_id: String,
    pub reviewer_agent_id: String,
    pub review_type: String,
    pub verdict: String,
    pub risk_level: String,
    pub findings_json: String,
    pub summary_text: String,
    pub reviewed_artifact_text: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct WorkflowDefinitionRow {
    pub workflow_id: String,
    pub owner_user_id: String,
    pub name: String,
    pub description: Option<String>,
    pub status: String,
    pub created_from_task_id: Option<String>,
    pub safety_status: String,
    pub safety_summary: Option<String>,
    pub current_version_number: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct WorkflowStageRow {
    pub stage_id: String,
    pub workflow_id: String,
    pub version_number: i64,
    pub stage_index: i64,
    pub stage_kind: String,
    pub stage_label: Option<String>,
    pub artifact_ref: Option<String>,
    pub stage_config_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct WorkflowReviewRow {
    pub review_id: String,
    pub workflow_id: String,
    pub version_number: i64,
    pub reviewer_agent_id: String,
    pub review_type: String,
    pub verdict: String,
    pub risk_level: String,
    pub findings_json: String,
    pub summary_text: String,
    pub reviewed_artifact_text: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct WorkflowVersionRow {
    pub version_id: String,
    pub workflow_id: String,
    pub version_number: i64,
    pub name: String,
    pub description: Option<String>,
    pub created_from_task_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct WorkflowRunRow {
    pub workflow_run_id: String,
    pub workflow_id: String,
    pub owner_user_id: String,
    pub initiating_identity_id: String,
    pub schedule_id: Option<String>,
    pub source_task_id: Option<String>,
    pub state: String,
    pub current_stage_index: i64,
    pub artifacts_json: String,
    pub wake_at: Option<String>,
    pub block_reason: Option<String>,
    pub last_error: Option<String>,
    pub retry_count: i64,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ScheduleRow {
    pub schedule_id: String,
    pub owner_user_id: String,
    pub target_type: String,
    pub target_id: String,
    pub cron_expr: String,
    pub enabled: i64,
    pub next_run_at: String,
    pub last_run_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ConnectorIdentityRow {
    pub identity_id: String,
    pub connector_type: String,
    pub external_user_id: String,
    pub external_channel_id: Option<String>,
    pub mapped_user_id: String,
    pub trust_level: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct HouseholdDeliveryRow {
    pub delivery_id: String,
    pub task_id: String,
    pub requester_user_id: String,
    pub requester_identity_id: String,
    pub recipient_user_id: String,
    pub message_text: String,
    pub delivery_mode: String,
    pub connector_type: String,
    pub connector_identity_id: String,
    pub target_ref: String,
    pub fallback_target_ref: Option<String>,
    pub dedupe_key: String,
    pub status: String,
    pub failure_reason: Option<String>,
    pub external_message_id: Option<String>,
    pub delivered_at: Option<String>,
    pub parent_delivery_id: Option<String>,
    pub schedule_id: Option<String>,
    pub scheduled_for_at: Option<String>,
    pub window_starts_at: Option<String>,
    pub window_ends_at: Option<String>,
    pub recurrence_rule: Option<String>,
    pub scheduled_job_state: Option<String>,
    pub materialized_task_id: Option<String>,
    pub completed_at: Option<String>,
    pub canceled_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RuntimeSessionRow {
    pub session_id: String,
    pub instance_id: String,
    pub state: String,
    pub pid: i64,
    pub socket_path: String,
    pub queue_depth: i64,
    pub active_task_id: Option<String>,
    pub automation_planning_enabled: i64,
    pub started_at: String,
    pub last_heartbeat_at: Option<String>,
    pub stopped_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RuntimeHeartbeatRow {
    pub heartbeat_id: String,
    pub session_id: String,
    pub state: String,
    pub queue_depth: i64,
    pub active_task_id: Option<String>,
    pub automation_planning_enabled: i64,
    pub observed_at: String,
}

async fn insert_approval_action(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    approval_id: &str,
    action_kind: &str,
    connector_type: Option<&str>,
    connector_identity_id: Option<&str>,
    channel: Option<&str>,
    ttl_secs: i64,
) -> Result<ApprovalActionRow> {
    let action_id = new_prefixed_id("approval_action");
    let action_token = new_prefixed_id("approval_token");
    let created_at = now_rfc3339();
    let expires_at = (Utc::now() + ChronoDuration::seconds(ttl_secs)).to_rfc3339();
    sqlx::query(
        "INSERT INTO approval_actions
         (action_id, approval_id, action_kind, action_token, status, issued_to_connector_type, issued_to_connector_identity_id, issued_to_channel, expires_at, consumed_at, created_at)
         VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, NULL, ?)",
    )
    .bind(&action_id)
    .bind(approval_id)
    .bind(action_kind)
    .bind(&action_token)
    .bind(connector_type)
    .bind(connector_identity_id)
    .bind(channel)
    .bind(&expires_at)
    .bind(&created_at)
    .execute(&mut **transaction)
    .await
    .context("failed to insert approval action")?;

    Ok(ApprovalActionRow {
        action_id,
        approval_id: approval_id.to_owned(),
        action_kind: action_kind.to_owned(),
        action_token,
        status: "pending".to_owned(),
        issued_to_connector_type: connector_type.map(ToOwned::to_owned),
        issued_to_connector_identity_id: connector_identity_id.map(ToOwned::to_owned),
        issued_to_channel: channel.map(ToOwned::to_owned),
        expires_at,
        consumed_at: None,
        created_at,
    })
}

fn slugify(input: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash {
            slug.push('_');
            previous_dash = true;
        }
    }

    slug.trim_matches('_').to_owned()
}

async fn unique_user_slug(display_name: &str, pool: &SqlitePool) -> Result<String> {
    let base = slugify(display_name);
    let base = if base.is_empty() {
        "user".to_owned()
    } else {
        base
    };

    for attempt in 0..1000 {
        let candidate = if attempt == 0 {
            base.clone()
        } else {
            format!("{base}_{attempt}")
        };
        let user_id = format!("user_{candidate}");
        let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(1) FROM users WHERE user_id = ?")
            .bind(&user_id)
            .fetch_one(pool)
            .await
            .context("failed to check for existing user slug")?;
        if exists == 0 {
            return Ok(candidate);
        }
    }

    Err(anyhow!("could not allocate a unique user slug"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_retrieval_enforces_scope_filters() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");

        db.insert_memory(NewMemory {
            owner_user_id: Some("user_alex"),
            scope: MemoryScope::Private,
            memory_kind: MemoryKind::Episodic,
            subject_type: "summary",
            subject_key: None,
            content_text: "private memory",
            content_json: None,
            sensitivity: "normal",
            source_type: "tool",
            source_ref: None,
            task_id: None,
        })
        .await
        .expect("private insert");
        db.insert_memory(NewMemory {
            owner_user_id: Some("user_alex"),
            scope: MemoryScope::Household,
            memory_kind: MemoryKind::Episodic,
            subject_type: "summary",
            subject_key: None,
            content_text: "household memory",
            content_json: None,
            sensitivity: "normal",
            source_type: "tool",
            source_ref: None,
            task_id: None,
        })
        .await
        .expect("household insert");

        let household_only = db
            .retrieve_memories(Some("user_alex"), &[MemoryScope::Household], 10)
            .await
            .expect("retrieve");

        assert_eq!(household_only.len(), 1);
        assert_eq!(household_only[0].content_text, "household memory");
    }

    #[tokio::test]
    async fn task_queries_enforce_owner_scope() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");

        let timestamp = now_rfc3339();
        sqlx::query(
            "INSERT INTO users (user_id, display_name, status, primary_agent_id, created_at, updated_at)
             VALUES (?, ?, 'enabled', ?, ?, ?)",
        )
        .bind("user_casey")
        .bind("Casey")
        .bind("agent_casey")
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(db.pool())
        .await
        .expect("insert second user");
        sqlx::query(
            "INSERT INTO agents
             (agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at)
             VALUES (?, 'primary', ?, NULL, 'active', ?, 'private', 'm0_single_user', ?, ?)",
        )
        .bind("agent_casey")
        .bind("user_casey")
        .bind("Primary agent for Casey")
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(db.pool())
        .await
        .expect("insert second agent");

        let alex_task = db
            .create_task(
                "user_alex",
                "agent_alex",
                "Summarize Alex task",
                MemoryScope::Private,
            )
            .await
            .expect("create task");
        db.append_task_event(
            &alex_task.task_id,
            "task_started",
            "agent",
            "agent_alex",
            Value::String("started".to_owned()),
        )
        .await
        .expect("append event");
        let invocation_id = db
            .start_tool_invocation(
                &alex_task.task_id,
                "agent_alex",
                "filesystem_read_text",
                Value::Object(Default::default()),
            )
            .await
            .expect("start invocation");
        db.finish_tool_invocation(
            &invocation_id,
            ToolInvocationStatus::Completed,
            Value::Object(Default::default()),
        )
        .await
        .expect("finish invocation");

        assert!(
            db.fetch_task_for_owner("user_casey", &alex_task.task_id)
                .await
                .expect("fetch task")
                .is_none()
        );
        assert!(
            db.fetch_task_events_for_owner("user_casey", &alex_task.task_id)
                .await
                .expect("fetch events")
                .is_empty()
        );
        assert!(
            db.fetch_tool_invocations_for_owner("user_casey", &alex_task.task_id)
                .await
                .expect("fetch invocations")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn connect_repairs_single_user_owner_role_when_missing() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        let timestamp = now_rfc3339();

        sqlx::query(
            "INSERT INTO users (user_id, display_name, status, primary_agent_id, created_at, updated_at)
             VALUES (?, ?, 'enabled', ?, ?, ?)",
        )
        .bind("user_alex")
        .bind("Alex")
        .bind("agent_alex")
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(db.pool())
        .await
        .expect("insert user");
        sqlx::query(
            "INSERT INTO agents
             (agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at)
             VALUES (?, 'primary', ?, NULL, 'active', ?, 'private', 'm0_single_user', ?, ?)",
        )
        .bind("agent_alex")
        .bind("user_alex")
        .bind("Primary agent for Alex")
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(db.pool())
        .await
        .expect("insert agent");
        drop(db);

        let repaired = Database::connect(&db_path).await.expect("reconnect");
        let roles = repaired.list_user_roles("user_alex").await.expect("roles");

        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].role_id, "owner");
    }

    #[tokio::test]
    async fn bootstrap_single_user_repairs_existing_default_owner_role() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        let timestamp = now_rfc3339();

        sqlx::query(
            "INSERT INTO users (user_id, display_name, status, primary_agent_id, created_at, updated_at)
             VALUES (?, ?, 'enabled', ?, ?, ?)",
        )
        .bind("user_alex")
        .bind("Alex")
        .bind("agent_alex")
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(db.pool())
        .await
        .expect("insert user");
        sqlx::query(
            "INSERT INTO agents
             (agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at)
             VALUES (?, 'primary', ?, NULL, 'active', ?, 'private', 'm0_single_user', ?, ?)",
        )
        .bind("agent_alex")
        .bind("user_alex")
        .bind("Primary agent for Alex")
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(db.pool())
        .await
        .expect("insert agent");

        let bootstrap = db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let roles = db.list_user_roles("user_alex").await.expect("roles");

        assert_eq!(bootstrap.user_id, "user_alex");
        assert_eq!(bootstrap.primary_agent_id, "agent_alex");
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].role_id, "owner");
    }

    #[tokio::test]
    async fn approval_actions_are_issued_and_consumed_once() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        let bootstrap = db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let task = db
            .create_task(
                &bootstrap.user_id,
                &bootstrap.primary_agent_id,
                "Need approval",
                MemoryScope::Private,
            )
            .await
            .expect("task");
        let approval = db
            .create_approval(NewApproval {
                task_id: &task.task_id,
                action_type: "shell_command",
                target_ref: Some("mkdir approved-dir"),
                requested_by_agent_id: &bootstrap.primary_agent_id,
                requested_from_user_id: &bootstrap.user_id,
                rationale_text: "Need permission to create a directory.",
                risk_level: Some("high"),
                action_summary: Some("Run shell command 'mkdir approved-dir'"),
                requester_display_name: Some("Primary agent for Alex"),
                target_details: Some("mkdir approved-dir"),
            })
            .await
            .expect("approval");

        let action_set = db
            .issue_approval_action_set(
                &approval.approval_id,
                IssueApprovalActions {
                    connector_type: Some("discord"),
                    connector_identity_id: Some("identity_1"),
                    channel: Some("dm-1"),
                    ttl_secs: 600,
                    include_confirm: false,
                },
            )
            .await
            .expect("issue actions");

        assert_eq!(action_set.approve.action_kind, "approve");
        assert_eq!(action_set.deny.action_kind, "deny");
        assert_eq!(action_set.inspect.action_kind, "inspect");
        assert!(action_set.confirm.is_none());

        let listed = db
            .list_approval_actions(&approval.approval_id)
            .await
            .expect("list actions");
        assert_eq!(listed.len(), 3);

        let consumed = db
            .consume_approval_action(&action_set.approve.action_token)
            .await
            .expect("consume action")
            .expect("consumed action");
        assert_eq!(consumed.status, "consumed");
        assert!(consumed.consumed_at.is_some());

        let second_consume = db
            .consume_approval_action(&action_set.approve.action_token)
            .await
            .expect("second consume");
        assert!(second_consume.is_none());
    }

    #[tokio::test]
    async fn issuing_new_approval_actions_supersedes_prior_pending_tokens() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        let bootstrap = db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let task = db
            .create_task(
                &bootstrap.user_id,
                &bootstrap.primary_agent_id,
                "Need approval",
                MemoryScope::Private,
            )
            .await
            .expect("task");
        let approval = db
            .create_approval(NewApproval {
                task_id: &task.task_id,
                action_type: "shell_command",
                target_ref: Some("mkdir approved-dir"),
                requested_by_agent_id: &bootstrap.primary_agent_id,
                requested_from_user_id: &bootstrap.user_id,
                rationale_text: "Need permission to create a directory.",
                risk_level: Some("high"),
                action_summary: Some("Run shell command 'mkdir approved-dir'"),
                requester_display_name: Some("Primary agent for Alex"),
                target_details: Some("mkdir approved-dir"),
            })
            .await
            .expect("approval");

        let first = db
            .issue_approval_action_set(
                &approval.approval_id,
                IssueApprovalActions {
                    connector_type: Some("discord"),
                    connector_identity_id: Some("identity_1"),
                    channel: Some("dm-1"),
                    ttl_secs: 600,
                    include_confirm: false,
                },
            )
            .await
            .expect("first issue");
        let second = db
            .issue_approval_action_set(
                &approval.approval_id,
                IssueApprovalActions {
                    connector_type: Some("discord"),
                    connector_identity_id: Some("identity_1"),
                    channel: Some("dm-1"),
                    ttl_secs: 600,
                    include_confirm: false,
                },
            )
            .await
            .expect("second issue");

        let first_row = db
            .fetch_approval_action_by_token(&first.approve.action_token)
            .await
            .expect("fetch first")
            .expect("first row");
        let second_row = db
            .fetch_approval_action_by_token(&second.approve.action_token)
            .await
            .expect("fetch second")
            .expect("second row");

        assert_eq!(first_row.status, "superseded");
        assert_eq!(second_row.status, "pending");
    }

    #[tokio::test]
    async fn expired_approval_actions_are_marked_unusable() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        let bootstrap = db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let task = db
            .create_task(
                &bootstrap.user_id,
                &bootstrap.primary_agent_id,
                "Need approval",
                MemoryScope::Private,
            )
            .await
            .expect("task");
        let approval = db
            .create_approval(NewApproval {
                task_id: &task.task_id,
                action_type: "shell_command",
                target_ref: Some("mkdir approved-dir"),
                requested_by_agent_id: &bootstrap.primary_agent_id,
                requested_from_user_id: &bootstrap.user_id,
                rationale_text: "Need permission to create a directory.",
                risk_level: Some("high"),
                action_summary: Some("Run shell command 'mkdir approved-dir'"),
                requester_display_name: Some("Primary agent for Alex"),
                target_details: Some("mkdir approved-dir"),
            })
            .await
            .expect("approval");

        let action_set = db
            .issue_approval_action_set(
                &approval.approval_id,
                IssueApprovalActions {
                    connector_type: Some("discord"),
                    connector_identity_id: Some("identity_1"),
                    channel: Some("dm-1"),
                    ttl_secs: -1,
                    include_confirm: false,
                },
            )
            .await
            .expect("issue actions");

        let consumed = db
            .consume_approval_action(&action_set.approve.action_token)
            .await
            .expect("consume expired action");
        assert!(consumed.is_none());

        let expired = db
            .fetch_approval_action_by_token(&action_set.approve.action_token)
            .await
            .expect("fetch action")
            .expect("action row");
        assert_eq!(expired.status, "expired");
    }
}
