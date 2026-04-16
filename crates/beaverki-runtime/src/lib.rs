use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use beaverki_agent::{AgentMemoryMode, AgentRequest, AgentResult, PrimaryAgentRunner};
use beaverki_config::{LoadedConfig, SecretStore};
use beaverki_core::{MemoryScope, TaskState, now_rfc3339};
use beaverki_db::{
    ApprovalRow, BootstrapState, Database, NewTask, RoleRow, RuntimeHeartbeatRow,
    RuntimeSessionRow, TaskEventRow, TaskRow, ToolInvocationRow, UserRoleRow, UserRow,
};
use beaverki_memory::MemoryStore;
use beaverki_models::{ModelProvider, OpenAiProvider};
use beaverki_policy::{can_grant_approvals, visible_memory_scopes};
use beaverki_tools::{ToolContext, builtin_registry};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, RwLock};
use tokio::time::{self, Instant};
use tracing::warn;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const QUEUE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const TASK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(150);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInspection {
    pub task: TaskRow,
    pub events: Vec<TaskEventRow>,
    pub tool_invocations: Vec<ToolInvocationRow>,
}

pub struct Runtime {
    config: LoadedConfig,
    db: Database,
    default_user: UserRow,
    runner: PrimaryAgentRunner,
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
        Self::from_parts(config, db, default_user, provider)
    }

    pub fn from_parts(
        config: LoadedConfig,
        db: Database,
        default_user: UserRow,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Self> {
        let memory = MemoryStore::new(db.clone());
        let mut allowed_roots = vec![config.runtime.workspace_root.clone()];
        if !allowed_roots.contains(&config.runtime.data_dir) {
            allowed_roots.push(config.runtime.data_dir.clone());
        }
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            memory,
            provider,
            builtin_registry(),
            ToolContext::new(config.runtime.workspace_root.clone(), allowed_roots),
            usize::from(config.runtime.defaults.max_agent_steps),
        );

        Ok(Self {
            config,
            db,
            default_user,
            runner,
        })
    }

    pub fn config(&self) -> &LoadedConfig {
        &self.config
    }

    pub fn db(&self) -> &Database {
        &self.db
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
        let request = self.build_primary_request(&user, objective, scope).await?;
        self.runner.run_task(request).await
    }

    pub async fn enqueue_objective(
        &self,
        user_id: Option<&str>,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<TaskRow> {
        let user = self.resolve_user(user_id).await?;
        let request = self.build_primary_request(&user, objective, scope).await?;
        self.db
            .create_task_with_params(NewTask {
                owner_user_id: &request.owner_user_id,
                primary_agent_id: &request.primary_agent_id,
                assigned_agent_id: &request.assigned_agent_id,
                parent_task_id: request.parent_task_id.as_deref(),
                kind: &request.kind,
                objective: &request.objective,
                context_summary: request.task_context.as_deref(),
                scope: request.scope,
            })
            .await
    }

    pub async fn execute_task(&self, task: TaskRow) -> Result<AgentResult> {
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
        let Some(task) = self.db.fetch_next_runnable_task().await? else {
            return Ok(None);
        };
        self.execute_task(task).await.map(Some)
    }

    pub async fn pending_task_count(&self) -> Result<i64> {
        self.db.pending_task_count().await
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

    async fn build_primary_request(
        &self,
        user: &UserRow,
        objective: &str,
        scope: MemoryScope,
    ) -> Result<AgentRequest> {
        let primary_agent_id = user
            .primary_agent_id
            .clone()
            .ok_or_else(|| anyhow!("user '{}' has no primary agent", user.user_id))?;
        let role_ids = self.user_role_ids(&user.user_id).await?;
        let visible_scopes = visible_memory_scopes(&role_ids);
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
            primary_agent_id: primary_agent_id.clone(),
            assigned_agent_id: primary_agent_id,
            role_ids: role_ids.clone(),
            objective: objective.to_owned(),
            scope,
            kind: "interactive".to_owned(),
            parent_task_id: None,
            task_context: None,
            visible_scopes,
            memory_mode: AgentMemoryMode::ScopedRetrieval,
            approved_shell_commands: Vec::new(),
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

        Ok(AgentRequest {
            owner_user_id: owner_user_id.to_owned(),
            primary_agent_id: task.primary_agent_id.clone(),
            assigned_agent_id: task.assigned_agent_id.clone(),
            role_ids: role_ids.clone(),
            objective: task.objective.clone(),
            scope,
            kind: task.kind.clone(),
            parent_task_id: task.parent_task_id.clone(),
            task_context: task.context_summary.clone(),
            visible_scopes: visible_memory_scopes(&role_ids),
            memory_mode,
            approved_shell_commands,
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
    ListApprovals {
        user_id: Option<String>,
        status: Option<String>,
    },
    ResolveApproval {
        user_id: Option<String>,
        approval_id: String,
        approve: bool,
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
    Approvals { approvals: Vec<ApprovalRow> },
    ShutdownAck { status: DaemonStatus },
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
        loop {
            if client.ping().await.is_ok() {
                return Ok(client);
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for daemon at {}",
                    client.socket_path.display()
                );
            }
            time::sleep(Duration::from_millis(150)).await;
        }
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

        let final_error = accept_result
            .err()
            .or(worker_result.err())
            .or(heartbeat_result.err());
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

                let next_task = daemon.runtime.db.fetch_next_runnable_task().await?;
                let Some(task) = next_task else {
                    break;
                };
                daemon.set_active_task(Some(task.task_id.clone())).await?;
                let task_id = task.task_id.clone();
                let owner_user_id = task.owner_user_id.clone();
                let run_result = daemon.runtime.execute_task(task).await;
                daemon.set_active_task(None).await?;

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
                let task = self
                    .runtime
                    .enqueue_objective(user_id.as_deref(), &objective, scope)
                    .await?;
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
                self.refresh_heartbeat(None).await?;
                Ok((DaemonResponse::Task { task }, false))
            }
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

fn parse_scope(value: &str) -> Result<MemoryScope> {
    value
        .parse::<MemoryScope>()
        .map_err(|_| anyhow!("unsupported scope '{value}', expected private or household"))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use beaverki_config::{ProviderModels, RuntimeConfig, RuntimeDefaults, RuntimeFeatures};
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
            },
            providers: beaverki_config::ProvidersConfig {
                active: "fake".to_owned(),
                entries: vec![],
            },
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

    #[tokio::test]
    async fn daemon_records_session_start_and_stop() {
        let (_tempdir, runtime) = test_runtime(vec![]).await;
        let db = runtime.db.clone();
        let instance_id = runtime.config.runtime.instance_id.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = DaemonClient::wait_until_ready(socket_path.clone(), Duration::from_secs(5))
            .await
            .expect("daemon ready");

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
        }])
        .await;
        let db = runtime.db.clone();
        let instance_id = runtime.config.runtime.instance_id.clone();
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = DaemonClient::wait_until_ready(socket_path, Duration::from_secs(5))
            .await
            .expect("daemon ready");

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
        }])
        .await;
        let queued = runtime
            .enqueue_objective(None, "Recover queued task", MemoryScope::Private)
            .await
            .expect("enqueue");
        let socket_path = runtime.daemon_socket_path();
        let daemon = RuntimeDaemon::new(runtime);
        let handle = tokio::spawn(async move { daemon.run_until(pending()).await });
        let client = DaemonClient::wait_until_ready(socket_path, Duration::from_secs(5))
            .await
            .expect("daemon ready");

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
}
