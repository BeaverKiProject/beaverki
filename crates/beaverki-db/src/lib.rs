use std::path::Path;

use anyhow::{Context, Result, anyhow};
use beaverki_core::{MemoryScope, TaskState, ToolInvocationStatus, new_prefixed_id, now_rfc3339};
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
             (task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL)",
        )
        .bind(&task_id)
        .bind(input.owner_user_id)
        .bind(input.initiating_identity_id)
        .bind(input.primary_agent_id)
        .bind(input.assigned_agent_id)
        .bind(input.parent_task_id)
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

        self.fetch_task_for_owner(input.owner_user_id, &task_id)
            .await?
            .context("task missing after insert")
    }

    pub async fn update_task_state(&self, task_id: &str, state: TaskState) -> Result<()> {
        sqlx::query("UPDATE tasks SET state = ?, updated_at = ? WHERE task_id = ?")
            .bind(state.as_str())
            .bind(now_rfc3339())
            .bind(task_id)
            .execute(&self.pool)
            .await
            .context("failed to update task state")?;
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
        Ok(())
    }

    pub async fn clear_task_result(&self, task_id: &str) -> Result<()> {
        sqlx::query("UPDATE tasks SET result_text = NULL, completed_at = NULL, updated_at = ? WHERE task_id = ?")
            .bind(now_rfc3339())
            .bind(task_id)
            .execute(&self.pool)
            .await
            .context("failed to clear task result")?;
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
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await
        .context("failed to append task event")?;
        Ok(event_id)
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
        sqlx::query(
            "INSERT INTO memories
             (memory_id, owner_user_id, scope, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at)
             VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&memory_id)
        .bind(input.owner_user_id)
        .bind(input.scope.as_str())
        .bind(input.subject_type)
        .bind(input.subject_key)
        .bind(input.content_text)
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
            "SELECT memory_id, owner_user_id, scope, subject_type, subject_key, content_text, content_json, sensitivity, source_type, source_ref, task_id, created_at, updated_at, last_accessed_at
             FROM memories WHERE ",
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

        builder.push(" ORDER BY updated_at DESC LIMIT ");
        builder.push_bind(limit);

        let query = builder.build_query_as::<MemoryRow>();
        let memories = query
            .fetch_all(&self.pool)
            .await
            .context("failed to retrieve memories")?;

        Ok(memories)
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

    pub async fn fetch_task_for_owner(
        &self,
        owner_user_id: &str,
        task_id: &str,
    ) -> Result<Option<TaskRow>> {
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
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
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
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

    pub async fn fetch_task(&self, task_id: &str) -> Result<Option<TaskRow>> {
        let row = sqlx::query_as::<_, TaskRow>(
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
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
            "SELECT task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at
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
pub struct NewTask<'a> {
    pub owner_user_id: &'a str,
    pub initiating_identity_id: &'a str,
    pub primary_agent_id: &'a str,
    pub assigned_agent_id: &'a str,
    pub parent_task_id: Option<&'a str>,
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
    pub subject_type: &'a str,
    pub subject_key: Option<&'a str>,
    pub content_text: &'a str,
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
pub struct MemoryRow {
    pub memory_id: String,
    pub owner_user_id: Option<String>,
    pub scope: String,
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
            subject_type: "summary",
            subject_key: None,
            content_text: "private memory",
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
            subject_type: "summary",
            subject_key: None,
            content_text: "household memory",
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
