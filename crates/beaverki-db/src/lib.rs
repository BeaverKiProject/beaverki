use std::path::Path;

use anyhow::{Context, Result};
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

        sqlx::query(
            "INSERT INTO users (user_id, display_name, status, primary_agent_id, created_at, updated_at)
             VALUES (?, ?, 'enabled', ?, ?, ?)",
        )
        .bind(&user_id)
        .bind(display_name)
        .bind(&agent_id)
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert default user")?;

        sqlx::query(
            "INSERT INTO agents
             (agent_id, kind, owner_user_id, parent_agent_id, status, persona, memory_scope, permission_profile, created_at, updated_at)
             VALUES (?, 'primary', ?, NULL, 'active', ?, 'private', 'm0_single_user', ?, ?)",
        )
        .bind(&agent_id)
        .bind(&user_id)
        .bind(format!("Primary agent for {display_name}"))
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert primary agent")?;

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

    pub async fn create_task(
        &self,
        owner_user_id: &str,
        primary_agent_id: &str,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<TaskRow> {
        let task_id = new_prefixed_id("task");
        let timestamp = now_rfc3339();

        sqlx::query(
            "INSERT INTO tasks
             (task_id, owner_user_id, initiating_identity_id, primary_agent_id, assigned_agent_id, parent_task_id, kind, state, objective, context_summary, result_text, scope, wake_at, created_at, updated_at, completed_at)
             VALUES (?, ?, ?, ?, ?, NULL, 'interactive', ?, ?, NULL, NULL, ?, NULL, ?, ?, NULL)",
        )
        .bind(&task_id)
        .bind(owner_user_id)
        .bind(format!("cli:{owner_user_id}"))
        .bind(primary_agent_id)
        .bind(primary_agent_id)
        .bind(TaskState::Pending.as_str())
        .bind(objective)
        .bind(scope.as_str())
        .bind(&timestamp)
        .bind(&timestamp)
        .execute(&self.pool)
        .await
        .context("failed to insert task")?;

        self.fetch_task(&task_id)
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

    pub async fn fetch_tool_invocations(&self, task_id: &str) -> Result<Vec<ToolInvocationRow>> {
        let invocations = sqlx::query_as::<_, ToolInvocationRow>(
            "SELECT invocation_id, task_id, agent_id, tool_name, request_json, response_json, status, started_at, finished_at
             FROM tool_invocations
             WHERE task_id = ?
             ORDER BY started_at ASC",
        )
        .bind(task_id)
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

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct UserRow {
    pub user_id: String,
    pub display_name: String,
    pub status: String,
    pub primary_agent_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize)]
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

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct TaskEventRow {
    pub event_id: String,
    pub task_id: String,
    pub event_type: String,
    pub actor_type: String,
    pub actor_id: String,
    pub payload_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize)]
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

#[derive(Debug, Clone, FromRow, Serialize)]
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
}
