use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use beaverki_config::LoadedConfig;
use beaverki_core::{TaskState, now_rfc3339};
use beaverki_db::{
    ApprovalRow, Database, MemoryRow, RoleRow, RuntimeSessionRow, ScheduleRow, TaskRow,
    WorkflowDefinitionRow,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, RwLock};
use tokio::time::{self, Instant};
use tracing::{error, info, warn};

use crate::connector_support::connector_type_from_events;
use crate::discord;
use crate::session::{is_session_reset_command, is_session_status_command, parse_scope};
use crate::{
    AutomationCatalog, MemoryInspection, Runtime, SessionSummary, TaskInspection, UserSummary,
    WorkflowDefinitionInput, WorkflowInspection,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const QUEUE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const TASK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(150);

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
    ListUsers,
    ListRoles,
    CreateUser {
        display_name: String,
        roles: Vec<String>,
    },
    RunTask {
        user_id: Option<String>,
        objective: String,
        scope: String,
        wait: bool,
    },
    ListTasks {
        user_id: Option<String>,
        limit: i64,
    },
    ShowTask {
        user_id: Option<String>,
        task_id: String,
    },
    ListSessions {
        user_id: Option<String>,
        include_archived: bool,
        limit: i64,
    },
    ResetSession {
        user_id: Option<String>,
        session_id: String,
    },
    ArchiveSession {
        user_id: Option<String>,
        session_id: String,
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
    ListAutomationCatalog {
        user_id: Option<String>,
    },
    ShowWorkflow {
        user_id: Option<String>,
        workflow_id: String,
    },
    UpsertWorkflow {
        user_id: Option<String>,
        workflow_id: Option<String>,
        definition: WorkflowDefinitionInput,
        intended_behavior_summary: String,
    },
    ActivateWorkflow {
        user_id: Option<String>,
        workflow_id: String,
    },
    DisableWorkflow {
        user_id: Option<String>,
        workflow_id: String,
    },
    ReplayWorkflow {
        user_id: Option<String>,
        workflow_id: String,
    },
    DeleteWorkflow {
        user_id: Option<String>,
        workflow_id: String,
    },
    UpsertWorkflowSchedule {
        user_id: Option<String>,
        schedule_id: Option<String>,
        workflow_id: String,
        cron_expr: String,
        enabled: bool,
    },
    SetScheduleEnabled {
        user_id: Option<String>,
        schedule_id: String,
        enabled: bool,
    },
    DeleteSchedule {
        user_id: Option<String>,
        schedule_id: String,
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
    Users { users: Vec<UserSummary> },
    User { user: UserSummary },
    Roles { roles: Vec<RoleRow> },
    Task { task: TaskRow },
    Tasks { tasks: Vec<TaskRow> },
    Inspection { inspection: TaskInspection },
    Sessions { sessions: Vec<SessionSummary> },
    Session { session: SessionSummary },
    Memory { memory: MemoryInspection },
    Memories { memories: Vec<MemoryRow> },
    Approvals { approvals: Vec<ApprovalRow> },
    AutomationCatalog { catalog: AutomationCatalog },
    Workflow { workflow: WorkflowInspection },
    WorkflowDefinition { workflow: WorkflowDefinitionRow },
    Schedule { schedule: ScheduleRow },
    Ack { message: String },
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

    pub async fn list_users(&self) -> Result<Vec<UserSummary>> {
        match self.request(DaemonRequest::ListUsers).await? {
            DaemonResponse::Users { users } => Ok(users),
            other => Err(anyhow!("unexpected daemon users response: {other:?}")),
        }
    }

    pub async fn list_roles(&self) -> Result<Vec<RoleRow>> {
        match self.request(DaemonRequest::ListRoles).await? {
            DaemonResponse::Roles { roles } => Ok(roles),
            other => Err(anyhow!("unexpected daemon roles response: {other:?}")),
        }
    }

    pub async fn create_user(
        &self,
        display_name: String,
        roles: Vec<String>,
    ) -> Result<UserSummary> {
        match self
            .request(DaemonRequest::CreateUser {
                display_name,
                roles,
            })
            .await?
        {
            DaemonResponse::User { user } => Ok(user),
            other => Err(anyhow!("unexpected daemon user response: {other:?}")),
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

    pub async fn list_tasks(&self, user_id: Option<String>, limit: i64) -> Result<Vec<TaskRow>> {
        match self
            .request(DaemonRequest::ListTasks { user_id, limit })
            .await?
        {
            DaemonResponse::Tasks { tasks } => Ok(tasks),
            other => Err(anyhow!("unexpected daemon tasks response: {other:?}")),
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

    pub async fn list_sessions(
        &self,
        user_id: Option<String>,
        include_archived: bool,
        limit: i64,
    ) -> Result<Vec<SessionSummary>> {
        match self
            .request(DaemonRequest::ListSessions {
                user_id,
                include_archived,
                limit,
            })
            .await?
        {
            DaemonResponse::Sessions { sessions } => Ok(sessions),
            other => Err(anyhow!("unexpected daemon sessions response: {other:?}")),
        }
    }

    pub async fn reset_session(
        &self,
        user_id: Option<String>,
        session_id: String,
    ) -> Result<SessionSummary> {
        match self
            .request(DaemonRequest::ResetSession {
                user_id,
                session_id,
            })
            .await?
        {
            DaemonResponse::Session { session } => Ok(session),
            other => Err(anyhow!("unexpected daemon session response: {other:?}")),
        }
    }

    pub async fn archive_session(
        &self,
        user_id: Option<String>,
        session_id: String,
    ) -> Result<SessionSummary> {
        match self
            .request(DaemonRequest::ArchiveSession {
                user_id,
                session_id,
            })
            .await?
        {
            DaemonResponse::Session { session } => Ok(session),
            other => Err(anyhow!("unexpected daemon session response: {other:?}")),
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

    pub async fn automation_catalog(&self, user_id: Option<String>) -> Result<AutomationCatalog> {
        match self
            .request(DaemonRequest::ListAutomationCatalog { user_id })
            .await?
        {
            DaemonResponse::AutomationCatalog { catalog } => Ok(catalog),
            other => Err(anyhow!("unexpected automation catalog response: {other:?}")),
        }
    }

    pub async fn show_workflow(
        &self,
        user_id: Option<String>,
        workflow_id: String,
    ) -> Result<WorkflowInspection> {
        match self
            .request(DaemonRequest::ShowWorkflow {
                user_id,
                workflow_id,
            })
            .await?
        {
            DaemonResponse::Workflow { workflow } => Ok(workflow),
            other => Err(anyhow!("unexpected workflow response: {other:?}")),
        }
    }

    pub async fn upsert_workflow(
        &self,
        user_id: Option<String>,
        workflow_id: Option<String>,
        definition: WorkflowDefinitionInput,
        intended_behavior_summary: String,
    ) -> Result<WorkflowInspection> {
        match self
            .request(DaemonRequest::UpsertWorkflow {
                user_id,
                workflow_id,
                definition,
                intended_behavior_summary,
            })
            .await?
        {
            DaemonResponse::Workflow { workflow } => Ok(workflow),
            other => Err(anyhow!("unexpected workflow response: {other:?}")),
        }
    }

    pub async fn activate_workflow(
        &self,
        user_id: Option<String>,
        workflow_id: String,
    ) -> Result<WorkflowDefinitionRow> {
        match self
            .request(DaemonRequest::ActivateWorkflow {
                user_id,
                workflow_id,
            })
            .await?
        {
            DaemonResponse::WorkflowDefinition { workflow } => Ok(workflow),
            other => Err(anyhow!(
                "unexpected workflow definition response: {other:?}"
            )),
        }
    }

    pub async fn disable_workflow(
        &self,
        user_id: Option<String>,
        workflow_id: String,
    ) -> Result<WorkflowDefinitionRow> {
        match self
            .request(DaemonRequest::DisableWorkflow {
                user_id,
                workflow_id,
            })
            .await?
        {
            DaemonResponse::WorkflowDefinition { workflow } => Ok(workflow),
            other => Err(anyhow!(
                "unexpected workflow definition response: {other:?}"
            )),
        }
    }

    pub async fn replay_workflow(
        &self,
        user_id: Option<String>,
        workflow_id: String,
    ) -> Result<TaskRow> {
        match self
            .request(DaemonRequest::ReplayWorkflow {
                user_id,
                workflow_id,
            })
            .await?
        {
            DaemonResponse::Task { task } => Ok(task),
            other => Err(anyhow!("unexpected workflow replay response: {other:?}")),
        }
    }

    pub async fn delete_workflow(
        &self,
        user_id: Option<String>,
        workflow_id: String,
    ) -> Result<()> {
        match self
            .request(DaemonRequest::DeleteWorkflow {
                user_id,
                workflow_id,
            })
            .await?
        {
            DaemonResponse::Ack { .. } => Ok(()),
            other => Err(anyhow!("unexpected workflow delete response: {other:?}")),
        }
    }

    pub async fn upsert_workflow_schedule(
        &self,
        user_id: Option<String>,
        schedule_id: Option<String>,
        workflow_id: String,
        cron_expr: String,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        match self
            .request(DaemonRequest::UpsertWorkflowSchedule {
                user_id,
                schedule_id,
                workflow_id,
                cron_expr,
                enabled,
            })
            .await?
        {
            DaemonResponse::Schedule { schedule } => Ok(schedule),
            other => Err(anyhow!("unexpected schedule response: {other:?}")),
        }
    }

    pub async fn set_schedule_enabled(
        &self,
        user_id: Option<String>,
        schedule_id: String,
        enabled: bool,
    ) -> Result<ScheduleRow> {
        match self
            .request(DaemonRequest::SetScheduleEnabled {
                user_id,
                schedule_id,
                enabled,
            })
            .await?
        {
            DaemonResponse::Schedule { schedule } => Ok(schedule),
            other => Err(anyhow!("unexpected schedule response: {other:?}")),
        }
    }

    pub async fn delete_schedule(
        &self,
        user_id: Option<String>,
        schedule_id: String,
    ) -> Result<()> {
        match self
            .request(DaemonRequest::DeleteSchedule {
                user_id,
                schedule_id,
            })
            .await?
        {
            DaemonResponse::Ack { .. } => Ok(()),
            other => Err(anyhow!("unexpected schedule delete response: {other:?}")),
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
    pub(crate) runtime: Arc<Runtime>,
    socket_path: PathBuf,
    pub(crate) shutdown: Arc<Notify>,
    pub(crate) wake_worker: Arc<Notify>,
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

        let cfg = &self.runtime.config;
        let discord_channels = cfg.integrations.discord.allowed_channels.len();
        info!(
            session_id = %session_id,
            instance_id = %cfg.runtime.instance_id,
            mode = %cfg.runtime.mode,
            workspace_root = %cfg.runtime.workspace_root.display(),
            discord_enabled = cfg.integrations.discord.enabled,
            discord_channels = discord_channels,
            discord_prefix = %cfg.integrations.discord.command_prefix,
            notion_enabled = cfg.integrations.notion.enabled,
            browser_headless = cfg.integrations.browser.headless_browser.is_some(),
            browser_interactive = cfg.integrations.browser.interactive_launcher.is_some(),
            "daemon session started",
        );

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
                info!(
                    task_id = %task.task_id,
                    task_kind = %task.kind,
                    owner_user_id = %task.owner_user_id,
                    "daemon worker picked task",
                );
                daemon.set_active_task(Some(task.task_id.clone())).await?;
                let task_id = task.task_id.clone();
                let owner_user_id = task.owner_user_id.clone();
                let run_result = daemon.runtime.execute_task(task).await;
                daemon.set_active_task(None).await?;

                if let Ok(result) = &run_result {
                    info!(
                        task_id = %result.task.task_id,
                        task_kind = %result.task.kind,
                        task_state = %result.task.state,
                        "daemon worker finished task",
                    );
                    daemon.dispatch_connector_follow_up(&result.task).await?;
                }

                if let Err(error) = run_result {
                    let error_text = format!("{error:#}");
                    error!(
                        task_id = %task_id,
                        owner_user_id = %owner_user_id,
                        error = %error_text,
                        "daemon worker task execution failed",
                    );
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
        info!(request = ?request, "daemon received request");
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
            DaemonRequest::ListUsers => Ok((
                DaemonResponse::Users {
                    users: self.runtime.list_user_summaries().await?,
                },
                false,
            )),
            DaemonRequest::ListRoles => Ok((
                DaemonResponse::Roles {
                    roles: self.runtime.list_roles().await?,
                },
                false,
            )),
            DaemonRequest::CreateUser {
                display_name,
                roles,
            } => {
                let bootstrap = self.runtime.create_user(&display_name, &roles).await?;
                let user = self
                    .runtime
                    .list_user_summaries()
                    .await?
                    .into_iter()
                    .find(|summary| summary.user.user_id == bootstrap.user_id)
                    .ok_or_else(|| anyhow!("created user '{}' not found", bootstrap.user_id))?;
                Ok((DaemonResponse::User { user }, false))
            }
            DaemonRequest::RunTask {
                user_id,
                objective,
                scope,
                wait,
            } => {
                let scope = parse_scope(&scope)?;
                let user = self.runtime.resolve_user(user_id.as_deref()).await?;
                let is_reset = is_session_reset_command(&objective);
                let is_status = is_session_status_command(&objective);
                let queue_depth = if is_status {
                    self.runtime.pending_task_count().await?
                } else {
                    0
                };
                let task = if is_reset {
                    self.runtime
                        .reset_cli_conversation_session(&user, &objective, scope)
                        .await?
                } else if is_status {
                    self.runtime
                        .status_cli_conversation_session(&user, &objective, scope, queue_depth)
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
                } else if is_status {
                    self.runtime
                        .db
                        .record_audit_event(
                            "runtime",
                            &self.current_session_id().await,
                            "conversation_session_status_requested",
                            json!({
                                "task_id": task.task_id,
                                "owner_user_id": task.owner_user_id,
                                "scope": task.scope,
                                "queue_depth": queue_depth,
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
            DaemonRequest::ListTasks { user_id, limit } => Ok((
                DaemonResponse::Tasks {
                    tasks: self.runtime.list_tasks(user_id.as_deref(), limit).await?,
                },
                false,
            )),
            DaemonRequest::ShowTask { user_id, task_id } => Ok((
                DaemonResponse::Inspection {
                    inspection: self
                        .runtime
                        .inspect_task(user_id.as_deref(), &task_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::ListSessions {
                user_id,
                include_archived,
                limit,
            } => Ok((
                DaemonResponse::Sessions {
                    sessions: self
                        .runtime
                        .list_sessions(user_id.as_deref(), include_archived, limit)
                        .await?,
                },
                false,
            )),
            DaemonRequest::ResetSession {
                user_id,
                session_id,
            } => Ok((
                DaemonResponse::Session {
                    session: self
                        .runtime
                        .reset_session(user_id.as_deref(), &session_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::ArchiveSession {
                user_id,
                session_id,
            } => Ok((
                DaemonResponse::Session {
                    session: self
                        .runtime
                        .archive_session(user_id.as_deref(), &session_id)
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
            DaemonRequest::ListAutomationCatalog { user_id } => Ok((
                DaemonResponse::AutomationCatalog {
                    catalog: self
                        .runtime
                        .list_automation_catalog(user_id.as_deref())
                        .await?,
                },
                false,
            )),
            DaemonRequest::ShowWorkflow {
                user_id,
                workflow_id,
            } => Ok((
                DaemonResponse::Workflow {
                    workflow: self
                        .runtime
                        .inspect_workflow_definition(user_id.as_deref(), &workflow_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::UpsertWorkflow {
                user_id,
                workflow_id,
                definition,
                intended_behavior_summary,
            } => Ok((
                DaemonResponse::Workflow {
                    workflow: self
                        .runtime
                        .create_workflow_definition(
                            user_id.as_deref(),
                            workflow_id.as_deref(),
                            definition,
                            None,
                            &intended_behavior_summary,
                        )
                        .await?,
                },
                false,
            )),
            DaemonRequest::ActivateWorkflow {
                user_id,
                workflow_id,
            } => Ok((
                DaemonResponse::WorkflowDefinition {
                    workflow: self
                        .runtime
                        .activate_workflow_definition(user_id.as_deref(), &workflow_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::DisableWorkflow {
                user_id,
                workflow_id,
            } => Ok((
                DaemonResponse::WorkflowDefinition {
                    workflow: self
                        .runtime
                        .disable_workflow_definition(user_id.as_deref(), &workflow_id)
                        .await?,
                },
                false,
            )),
            DaemonRequest::ReplayWorkflow {
                user_id,
                workflow_id,
            } => {
                let task = self
                    .runtime
                    .replay_workflow_definition(user_id.as_deref(), &workflow_id)
                    .await?;
                self.wake_worker.notify_one();
                self.refresh_heartbeat(None).await?;
                Ok((DaemonResponse::Task { task }, false))
            }
            DaemonRequest::DeleteWorkflow {
                user_id,
                workflow_id,
            } => {
                self.runtime
                    .delete_workflow_definition(user_id.as_deref(), &workflow_id)
                    .await?;
                Ok((
                    DaemonResponse::Ack {
                        message: format!("workflow '{workflow_id}' deleted"),
                    },
                    false,
                ))
            }
            DaemonRequest::UpsertWorkflowSchedule {
                user_id,
                schedule_id,
                workflow_id,
                cron_expr,
                enabled,
            } => Ok((
                DaemonResponse::Schedule {
                    schedule: self
                        .runtime
                        .upsert_workflow_schedule(
                            user_id.as_deref(),
                            schedule_id.as_deref(),
                            &workflow_id,
                            &cron_expr,
                            enabled,
                        )
                        .await?,
                },
                false,
            )),
            DaemonRequest::SetScheduleEnabled {
                user_id,
                schedule_id,
                enabled,
            } => Ok((
                DaemonResponse::Schedule {
                    schedule: self
                        .runtime
                        .set_schedule_enabled(user_id.as_deref(), &schedule_id, enabled)
                        .await?,
                },
                false,
            )),
            DaemonRequest::DeleteSchedule {
                user_id,
                schedule_id,
            } => {
                self.runtime
                    .delete_schedule(user_id.as_deref(), &schedule_id)
                    .await?;
                Ok((
                    DaemonResponse::Ack {
                        message: format!("schedule '{schedule_id}' deleted"),
                    },
                    false,
                ))
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

    pub(crate) async fn wait_for_task_state(
        &self,
        owner_user_id: &str,
        task_id: &str,
    ) -> Result<TaskRow> {
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

    pub(crate) async fn shutdown_notified(&self) -> bool {
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
