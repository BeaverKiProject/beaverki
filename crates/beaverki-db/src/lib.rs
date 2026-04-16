use std::path::Path;

use anyhow::{Context, Result, anyhow};
use beaverki_core::{MemoryScope, TaskState, ToolInvocationStatus, new_prefixed_id, now_rfc3339};
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

    pub async fn bootstrap_single_user(&self, display_name: &str) -> Result<BootstrapState> {
        if let Some(existing) = self.default_user().await? {
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
            primary_agent_id,
            assigned_agent_id: primary_agent_id,
            parent_task_id: None,
            kind: "interactive",
            objective,
            context_summary: None,
            scope,
        })
        .await
    }

    pub async fn create_task_with_params(&self, input: NewTask<'_>) -> Result<TaskRow> {
        let task_id = new_prefixed_id("task");
        let timestamp = now_rfc3339();

        sqlx::query(
            "INSERT INTO tasks
             (task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, NULL)",
        )
        .bind(&task_id)
        .bind(input.owner_user_id)
        .bind(format!("cli:{}", input.owner_user_id))
        .bind(input.primary_agent_id)
        .bind(input.assigned_agent_id)
        .bind(input.parent_task_id)
        .bind(input.kind)
        .bind(TaskState::Pending.as_str())
        .bind(input.objective)
        .bind(input.context_summary)
        .bind(input.scope.as_str())
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

    pub async fn create_approval(
        &self,
        task_id: &str,
        action_type: &str,
        target_ref: Option<&str>,
        requested_by_agent_id: &str,
        requested_from_user_id: &str,
        rationale_text: &str,
    ) -> Result<ApprovalRow> {
        let approval_id = new_prefixed_id("approval");
        let created_at = now_rfc3339();
        sqlx::query(
            "INSERT INTO approvals
             (approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, decided_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?)",
        )
        .bind(&approval_id)
        .bind(task_id)
        .bind(action_type)
        .bind(target_ref)
        .bind(requested_by_agent_id)
        .bind(requested_from_user_id)
        .bind(rationale_text)
        .bind(&created_at)
        .execute(&self.pool)
        .await
        .context("failed to create approval")?;
        self.fetch_approval_for_user(requested_from_user_id, &approval_id)
            .await?
            .context("approval missing after insert")
    }

    pub async fn fetch_approval_for_user(
        &self,
        requested_from_user_id: &str,
        approval_id: &str,
    ) -> Result<Option<ApprovalRow>> {
        let row = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, decided_at, created_at
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
                "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, decided_at, created_at
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
                "SELECT approval_id, task_id, action_type, target_ref, requested_by_agent_id, requested_from_user_id, status, rationale_text, decided_at, created_at
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
    pub primary_agent_id: &'a str,
    pub assigned_agent_id: &'a str,
    pub parent_task_id: Option<&'a str>,
    pub kind: &'a str,
    pub objective: &'a str,
    pub context_summary: Option<&'a str>,
    pub scope: MemoryScope,
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
    pub decided_at: Option<String>,
    pub created_at: String,
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
}
