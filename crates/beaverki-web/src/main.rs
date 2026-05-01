use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Form, Router,
    extract::{Path, Query, State},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use beaverki_config::LoadedConfig;
use beaverki_core::TaskState;
use beaverki_db::{ApprovalRow, MemoryRow, RoleRow, ScheduleRow, TaskEventRow, ToolInvocationRow};
use beaverki_runtime::{
    AutomationCatalog, DaemonClient, SessionSummary, UserSummary, WorkflowDefinitionInput,
    WorkflowInspection, WorkflowStageInput,
};
use clap::Parser;
use maud::{DOCTYPE, Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Parser)]
#[command(name = "beaverki-web")]
#[command(about = "Local web UI for BeaverKi")]
struct Args {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long, default_value = "127.0.0.1:7676")]
    listen_addr: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    daemon: DaemonClient,
    listen_addr: SocketAddr,
}

#[derive(Debug)]
struct AppError(anyhow::Error);

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let markup = page_shell(
            "BeaverKi UI Error",
            None,
            html! {},
            html! {
                section class="hero" {
                    div class="hero-copy" {
                        p class="eyebrow" { "Local web UI" }
                        h1 { "Something blocked the UI request." }
                        p class="lede" {
                            (self.0.to_string())
                        }
                        p class="hint" {
                            "The web UI only talks to the local BeaverKi daemon. Make sure the daemon is running and reachable on this machine."
                        }
                        a class="primary-link" href="/" { "Back to dashboard" }
                    }
                }
            },
        );
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, markup).into_response()
    }
}

#[derive(Debug, Default, Deserialize)]
struct DashboardQuery {
    user: Option<String>,
    include_archived: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct UserQuery {
    user: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ScheduleEditQuery {
    user: Option<String>,
    schedule_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskForm {
    user: Option<String>,
    objective: String,
    scope: String,
}

#[derive(Debug, Deserialize)]
struct ActionForm {
    user: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowForm {
    user: Option<String>,
    workflow_id: Option<String>,
    name: String,
    description: String,
    intended_behavior_summary: String,
    stage_kind: Vec<String>,
    stage_label: Vec<String>,
    stage_artifact_ref: Vec<String>,
    stage_prompt: Vec<String>,
    stage_recipient: Vec<String>,
    stage_message: Vec<String>,
    stage_message_template: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ScheduleFormInput {
    user: Option<String>,
    schedule_id: Option<String>,
    schedule_mode: String,
    schedule_time: Option<String>,
    custom_cron: Option<String>,
    enabled: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UserCreateForm {
    display_name: String,
    roles: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !args.listen_addr.ip().is_loopback() {
        bail!("the web UI must bind to a loopback address such as 127.0.0.1");
    }

    let config_dir = args.config_dir.unwrap_or_else(|| {
        beaverki_config::default_app_paths()
            .expect("default app paths")
            .config_dir
    });
    let config = LoadedConfig::load_from_dir(&config_dir)
        .with_context(|| format!("failed to load {}", config_dir.display()))?;
    let daemon = DaemonClient::new(config.runtime.state_dir.join("daemon.sock"));
    daemon.status().await.with_context(
        || "the BeaverKi daemon is not reachable; start it before launching the web UI",
    )?;

    let state = AppState {
        daemon,
        listen_addr: args.listen_addr,
    };
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/users", post(create_user))
        .route("/tasks", post(submit_task))
        .route("/tasks/{task_id}", get(task_detail))
        .route("/approvals/{approval_id}/approve", post(approve_task))
        .route("/approvals/{approval_id}/deny", post(deny_task))
        .route("/sessions/{session_id}/reset", post(reset_session))
        .route("/sessions/{session_id}/archive", post(archive_session))
        .route("/workflows/new", get(new_workflow))
        .route("/workflows/save", post(save_workflow))
        .route("/workflows/{workflow_id}", get(show_workflow))
        .route("/workflows/{workflow_id}/edit", get(edit_workflow))
        .route("/workflows/{workflow_id}/activate", post(activate_workflow))
        .route("/workflows/{workflow_id}/disable", post(disable_workflow))
        .route("/workflows/{workflow_id}/replay", post(replay_workflow))
        .route("/workflows/{workflow_id}/delete", post(delete_workflow))
        .route(
            "/workflows/{workflow_id}/schedules/save",
            post(save_workflow_schedule),
        )
        .route("/schedules/{schedule_id}/toggle", post(toggle_schedule))
        .route("/schedules/{schedule_id}/delete", post(delete_schedule))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(state.listen_addr).await?;
    println!("BeaverKi web UI listening on http://{}", state.listen_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn dashboard(
    State(state): State<AppState>,
    Query(query): Query<DashboardQuery>,
) -> Result<Markup, AppError> {
    let selected_user = normalize_user(query.user.clone());
    let include_archived = query.include_archived.unwrap_or(false);
    let users_client = state.daemon.clone();
    let tasks_client = state.daemon.clone();
    let approvals_client = state.daemon.clone();
    let sessions_client = state.daemon.clone();
    let memories_client = state.daemon.clone();
    let catalog_client = state.daemon.clone();
    let status_client = state.daemon.clone();
    let roles_client = state.daemon.clone();
    let (users, tasks, approvals, sessions, memories, catalog, status, roles) = tokio::join!(
        users_client.list_users(),
        tasks_client.list_tasks(selected_user.clone(), 10),
        approvals_client.list_approvals(selected_user.clone(), Some("pending".to_owned())),
        sessions_client.list_sessions(selected_user.clone(), include_archived, 10),
        memories_client.list_memories(selected_user.clone(), None, None, false, false, 10),
        catalog_client.automation_catalog(selected_user.clone()),
        status_client.status(),
        roles_client.list_roles(),
    );
    let users = users?;
    let tasks = tasks?;
    let approvals = approvals?;
    let sessions = sessions?;
    let memories = memories?;
    let catalog = catalog?;
    let status = status?;
    let roles = roles?;
    let active_user = selected_user
        .clone()
        .or_else(|| users.first().map(|user| user.user.user_id.clone()));
    let title = active_user
        .as_deref()
        .unwrap_or("BeaverKi dashboard")
        .to_owned();

    let topbar_user_form = html! {
        form method="get" action="/" class="topbar-userform" {
            select name="user" onchange="this.form.submit()" {
                @for user in &users {
                    option
                        value=(user.user.user_id)
                        selected[active_user.as_deref() == Some(user.user.user_id.as_str())] {
                        (user.user.display_name)
                    }
                }
            }
            label class="topbar-check" {
                input type="checkbox" name="include_archived" value="true"
                      checked[include_archived] onchange="this.form.submit()";
                span { "Archived" }
            }
        }
    };
    Ok(page_shell(
        &format!("BeaverKi Web UI | {title}"),
        active_user.as_deref(),
        topbar_user_form,
        html! {
            section class="hero" {
                div class="hero-copy" {
                    p class="eyebrow" { "Local operator console" }
                    h1 { "BeaverKi dashboard" }
                    p class="lede" {
                        "Run tasks, inspect approvals, review sessions, and manage automation from one local view."
                    }
                }
                div class="hero-stats" {
                    (stat_card("Daemon", &status.state))
                    (stat_card("Pending approvals", &approvals.len().to_string()))
                    (stat_card("Workflows", &catalog.workflows.len().to_string()))
                    (stat_card("Schedules", &catalog.schedules.len().to_string()))
                }
            }
            div class="tab-bar" role="tablist" {
                button type="button" class="tab-btn" data-tab="operations" role="tab" {
                    "Operations"
                    @if !approvals.is_empty() {
                        span class="tab-badge" { (approvals.len()) }
                    }
                }
                button type="button" class="tab-btn" data-tab="people" role="tab" { "People" }
                button type="button" class="tab-btn" data-tab="automation" role="tab" {
                    "Automation"
                    span class="tab-badge tab-badge-muted" { (catalog.workflows.len()) " wf" }
                    @if !catalog.schedules.is_empty() {
                        span class="tab-badge tab-badge-muted" { (catalog.schedules.len()) " sch" }
                    }
                }
            }
            section class="section-block tab-panel" id="operations" {
                div class="section-heading" {
                    p class="eyebrow" { "Operations" }
                    h2 { "Tasks and runtime state" }
                    p { "Day-to-day operator actions stay together here: submit work, clear approvals, inspect sessions, and verify memory." }
                }
                section class="grid two-up" {
                    div class="panel" id="tasks" {
                        div class="panel-header" {
                            h2 { "Run a task" }
                            p { "Plain language works. The daemon continues in the background." }
                        }
                        form method="post" action="/tasks" class="stacked-form" {
                            (hidden_user_input(active_user.as_deref()))
                            label {
                                span { "What should BeaverKi do?" }
                                textarea name="objective" rows="4" placeholder="Summarize today’s open work, then propose the next three actions." {}
                            }
                            label {
                                span { "Memory scope" }
                                select name="scope" {
                                    option value="private" selected { "Private" }
                                    option value="household" { "Household" }
                                }
                            }
                            button type="submit" class="primary" { "Submit task" }
                        }
                        (tasks_panel(&tasks, active_user.as_deref()))
                    }
                    div class="panel" id="approvals" {
                        div class="panel-header" {
                            h2 { "Approvals" }
                            p { "Sensitive actions pause here until an operator approves or denies them." }
                        }
                        (approvals_panel(&approvals, active_user.as_deref()))
                    }
                }
                section class="grid two-up" {
                    div class="panel" id="sessions" {
                        div class="panel-header" {
                            h2 { "Sessions" }
                            p { "Inspect conversation threads and clean them up without losing durable memory." }
                        }
                        (sessions_panel(&sessions, active_user.as_deref()))
                    }
                    div class="panel" id="memory" {
                        div class="panel-header" {
                            h2 { "Recent memory" }
                            p { "A quick read on the memory store BeaverKi is currently using." }
                        }
                        (memories_panel(&memories))
                    }
                }
            }
            section class="section-block tab-panel" id="people" {
                div class="section-heading" {
                    p class="eyebrow" { "People" }
                    h2 { "Users and roles" }
                    p { "Keep user management separate from runtime operations so account changes do not blend into daily task handling." }
                }
                section class="grid two-up" {
                    div class="panel" id="users" {
                        div class="panel-header" {
                            h2 { "Household users" }
                            p { "Who already has access to this BeaverKi instance and which roles they carry." }
                        }
                        (users_panel(&users, active_user.as_deref()))
                    }
                    div class="panel" id="user-create" {
                        div class="panel-header" {
                            h2 { "Add user" }
                            p { "Create a new household user and assign one or more built-in roles." }
                        }
                        (user_create_panel(&roles))
                    }
                }
            }
            section class="section-block tab-panel" id="automation" {
                div class="section-heading" {
                    p class="eyebrow" { "Automation" }
                    h2 { "Workflows and supporting assets" }
                    p { "Workflow management is separated from user administration and task handling so scheduled automation reads as its own system." }
                }
                section class="panel" {
                    div class="panel-header" {
                        h2 { "Automation catalog" }
                        p { "Workflows stay daemon-backed. Use the editor to add or revise them, then schedule them for repeat runs." }
                    }
                    div class="toolbar" {
                        a class="primary-link" href=(workflow_editor_link(None, active_user.as_deref())) { "Create workflow" }
                    }
                    (automation_panel(&catalog, active_user.as_deref()))
                }
            }
            script {
                (PreEscaped(r#"
(function() {
  var TABS = ['operations', 'people', 'automation'];
  var btns = document.querySelectorAll('.tab-btn');
  var panels = document.querySelectorAll('.tab-panel');
  function showTab(id) {
    if (!TABS.includes(id)) id = TABS[0];
    btns.forEach(function(b) { b.classList.toggle('active', b.dataset.tab === id); });
    panels.forEach(function(p) { p.hidden = p.id !== id; });
    history.replaceState(null, '', '#' + id);
  }
  btns.forEach(function(b) { b.addEventListener('click', function() { showTab(b.dataset.tab); }); });
  var hash = window.location.hash.replace('#', '');
  showTab(hash);
})();
"#))
            }
        },
    ))
}

async fn submit_task(
    State(state): State<AppState>,
    Form(form): Form<TaskForm>,
) -> Result<Redirect, AppError> {
    let scope = if form.scope.trim().is_empty() {
        "private".to_owned()
    } else {
        form.scope
    };
    let task = state
        .daemon
        .run_task(
            normalize_user(form.user.clone()),
            form.objective,
            scope,
            false,
        )
        .await?;
    Ok(Redirect::to(&task_link(
        &task.task_id,
        normalize_user(form.user).as_deref(),
    )))
}

async fn create_user(
    State(state): State<AppState>,
    Form(form): Form<UserCreateForm>,
) -> Result<Redirect, AppError> {
    state
        .daemon
        .create_user(
            form.display_name,
            form.roles
                .into_iter()
                .filter(|role| !role.trim().is_empty())
                .collect(),
        )
        .await?;
    Ok(Redirect::to("/#users"))
}

async fn task_detail(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(query): Query<UserQuery>,
) -> Result<Markup, AppError> {
    let selected_user = normalize_user(query.user.clone());
    let task = state
        .daemon
        .show_task(selected_user.clone(), task_id.clone())
        .await?;
    let approvals = state
        .daemon
        .list_approvals(selected_user.clone(), Some("pending".to_owned()))
        .await?
        .into_iter()
        .filter(|approval| approval.task_id == task.task.task_id)
        .collect::<Vec<_>>();
    let auto_refresh = matches!(
        TaskState::from_str(&task.task.state).unwrap_or(TaskState::Pending),
        TaskState::Pending | TaskState::Running | TaskState::WaitingApproval
    );

    Ok(page_shell(
        &format!("Task {}", task.task.task_id),
        selected_user.as_deref(),
        html! {},
        html! {
            section class="hero compact" {
                div class="hero-copy" {
                    p class="eyebrow" { "Task detail" }
                    h1 { (task.task.objective) }
                    p class="lede" {
                        "State: " (status_chip(&task.task.state))
                    }
                    p class="hint" {
                        "Task ID: " code { (task.task.task_id) }
                    }
                }
                div class="hero-actions" {
                    a class="secondary-link" href=(dashboard_link(selected_user.as_deref(), false)) { "Back to dashboard" }
                }
            }
            @if auto_refresh {
                script { "window.setTimeout(() => window.location.reload(), 3000);" }
            }
            @if !approvals.is_empty() {
                section class="panel" {
                    div class="panel-header" {
                        h2 { "Pending approval" }
                        p { "This task is waiting on an operator decision." }
                    }
                    (approvals_panel(&approvals, selected_user.as_deref()))
                }
            }
            section class="panel" {
                div class="panel-header" {
                    h2 { "Result" }
                    p { "The daemon stores the latest result text directly on the task record." }
                }
                @if let Some(result) = task.task.result_text.as_deref() {
                    pre class="code-block" { (result) }
                } @else {
                    p class="empty" { "No result text has been recorded yet." }
                }
            }
            section class="grid two-up" {
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Task events" }
                        p { "Execution milestones recorded by the runtime." }
                    }
                    (task_events_panel(&task.events))
                }
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Tool invocations" }
                        p { "Requests and responses captured for tool use." }
                    }
                    (tool_invocations_panel(&task.tool_invocations))
                }
            }
        },
    ))
}

async fn approve_task(
    State(state): State<AppState>,
    Path(approval_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user);
    let task = state
        .daemon
        .resolve_approval(user.clone(), approval_id, true)
        .await?;
    Ok(Redirect::to(&task_link(&task.task_id, user.as_deref())))
}

async fn deny_task(
    State(state): State<AppState>,
    Path(approval_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user);
    let task = state
        .daemon
        .resolve_approval(user.clone(), approval_id, false)
        .await?;
    Ok(Redirect::to(&task_link(&task.task_id, user.as_deref())))
}

async fn reset_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    state
        .daemon
        .reset_session(normalize_user(form.user.clone()), session_id)
        .await?;
    Ok(Redirect::to(&dashboard_link(
        normalize_user(form.user).as_deref(),
        false,
    )))
}

async fn archive_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    state
        .daemon
        .archive_session(normalize_user(form.user.clone()), session_id)
        .await?;
    Ok(Redirect::to(&dashboard_link(
        normalize_user(form.user).as_deref(),
        true,
    )))
}

async fn new_workflow(
    State(state): State<AppState>,
    Query(query): Query<UserQuery>,
) -> Result<Markup, AppError> {
    let user = normalize_user(query.user.clone());
    let users = state.daemon.list_users().await?;
    let catalog = state.daemon.automation_catalog(user.clone()).await?;
    Ok(render_workflow_editor(
        &users,
        user.as_deref(),
        &catalog,
        None,
        None,
    ))
}

async fn edit_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Query(query): Query<ScheduleEditQuery>,
) -> Result<Markup, AppError> {
    let user = normalize_user(query.user.clone());
    let users = state.daemon.list_users().await?;
    let catalog = state.daemon.automation_catalog(user.clone()).await?;
    let workflow = state
        .daemon
        .show_workflow(user.clone(), workflow_id)
        .await?;
    let editing_schedule = query
        .schedule_id
        .as_deref()
        .and_then(|schedule_id| {
            workflow
                .schedules
                .iter()
                .find(|row| row.schedule_id == schedule_id)
        })
        .cloned();
    Ok(render_workflow_editor(
        &users,
        user.as_deref(),
        &catalog,
        Some(&workflow),
        editing_schedule.as_ref(),
    ))
}

async fn save_workflow(
    State(state): State<AppState>,
    Form(form): Form<WorkflowForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    let definition = workflow_definition_from_form(&form)?;
    let workflow = state
        .daemon
        .upsert_workflow(
            user.clone(),
            normalize_user(form.workflow_id.clone()),
            definition,
            non_empty(form.intended_behavior_summary).unwrap_or_else(|| {
                "Review this workflow for local household automation.".to_owned()
            }),
        )
        .await?;
    Ok(Redirect::to(&workflow_link(
        &workflow.workflow.workflow_id,
        user.as_deref(),
    )))
}

async fn show_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Query(query): Query<UserQuery>,
) -> Result<Markup, AppError> {
    let user = normalize_user(query.user.clone());
    let workflow = state
        .daemon
        .show_workflow(user.clone(), workflow_id)
        .await?;
    Ok(render_workflow_detail(&workflow, user.as_deref()))
}

async fn activate_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    state
        .daemon
        .activate_workflow(user.clone(), workflow_id.clone())
        .await?;
    Ok(Redirect::to(&workflow_link(&workflow_id, user.as_deref())))
}

async fn disable_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    state
        .daemon
        .disable_workflow(user.clone(), workflow_id.clone())
        .await?;
    Ok(Redirect::to(&workflow_link(&workflow_id, user.as_deref())))
}

async fn replay_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    let task = state
        .daemon
        .replay_workflow(user.clone(), workflow_id)
        .await?;
    Ok(Redirect::to(&task_link(&task.task_id, user.as_deref())))
}

async fn delete_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    state
        .daemon
        .delete_workflow(user.clone(), workflow_id)
        .await?;
    Ok(Redirect::to(&dashboard_link(user.as_deref(), false)))
}

async fn save_workflow_schedule(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
    Form(form): Form<ScheduleFormInput>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    let cron = schedule_cron(&form)?;
    state
        .daemon
        .upsert_workflow_schedule(
            user.clone(),
            normalize_user(form.schedule_id.clone()),
            workflow_id.clone(),
            cron,
            form.enabled.is_some(),
        )
        .await?;
    Ok(Redirect::to(&workflow_link(&workflow_id, user.as_deref())))
}

async fn toggle_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    let catalog = state.daemon.automation_catalog(user.clone()).await?;
    let schedule = catalog
        .schedules
        .into_iter()
        .find(|schedule| schedule.schedule_id == schedule_id)
        .ok_or_else(|| anyhow!("schedule '{schedule_id}' not found"))?;
    state
        .daemon
        .set_schedule_enabled(
            user.clone(),
            schedule.schedule_id.clone(),
            !schedule_enabled(&schedule),
        )
        .await?;
    let destination = if schedule.target_type == "workflow" {
        workflow_link(&schedule.target_id, user.as_deref())
    } else {
        dashboard_link(user.as_deref(), false)
    };
    Ok(Redirect::to(&destination))
}

async fn delete_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
    Form(form): Form<ActionForm>,
) -> Result<Redirect, AppError> {
    let user = normalize_user(form.user.clone());
    let catalog = state.daemon.automation_catalog(user.clone()).await?;
    let schedule = catalog
        .schedules
        .into_iter()
        .find(|schedule| schedule.schedule_id == schedule_id)
        .ok_or_else(|| anyhow!("schedule '{schedule_id}' not found"))?;
    state
        .daemon
        .delete_schedule(user.clone(), schedule_id)
        .await?;
    let destination = if schedule.target_type == "workflow" {
        workflow_link(&schedule.target_id, user.as_deref())
    } else {
        dashboard_link(user.as_deref(), false)
    };
    Ok(Redirect::to(&destination))
}

fn render_workflow_detail(workflow: &WorkflowInspection, user: Option<&str>) -> Markup {
    page_shell(
        &format!("Workflow {}", workflow.workflow.name),
        user,
        html! {},
        html! {
            section class="hero compact" {
                div class="hero-copy" {
                    p class="eyebrow" { "Workflow" }
                    h1 { (&workflow.workflow.name) }
                    p class="lede" {
                        @if let Some(description) = workflow.workflow.description.as_deref() {
                            (description)
                        } @else {
                            "No workflow description yet."
                        }
                    }
                    p class="hint" {
                        "Status: " (status_chip(&workflow.workflow.status)) " "
                        "Safety: " (status_chip(&workflow.workflow.safety_status))
                    }
                }
                div class="hero-actions" {
                    a class="secondary-link" href=(dashboard_link(user, false)) { "Back to dashboard" }
                    a class="primary-link" href=(workflow_editor_link(Some(&workflow.workflow.workflow_id), user)) { "Edit workflow" }
                }
            }
            section class="toolbar actions" {
                form method="post" action=(format!("/workflows/{}/activate", workflow.workflow.workflow_id)) {
                    (hidden_user_input(user))
                    button type="submit" class="primary" { "Activate" }
                }
                form method="post" action=(format!("/workflows/{}/disable", workflow.workflow.workflow_id)) {
                    (hidden_user_input(user))
                    button type="submit" { "Disable" }
                }
                form method="post" action=(format!("/workflows/{}/replay", workflow.workflow.workflow_id)) {
                    (hidden_user_input(user))
                    button type="submit" { "Run now" }
                }
                form method="post" action=(format!("/workflows/{}/delete", workflow.workflow.workflow_id)) onsubmit="return confirm('Delete this workflow and its schedules?');" {
                    (hidden_user_input(user))
                    button type="submit" class="danger" { "Delete" }
                }
            }
            section class="grid two-up" {
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Stages" }
                        p { "The ordered workflow plan currently active in the daemon." }
                    }
                    @for stage in &workflow.stages {
                        div class="stage-card" {
                            div class="stage-index" { (stage.stage_index + 1) }
                            div class="stage-copy" {
                                h3 { (stage.stage_label.as_deref().unwrap_or(stage.stage_kind.as_str())) }
                                p class="hint" { "Kind: " code { (&stage.stage_kind) } }
                                @if let Some(artifact_ref) = stage.artifact_ref.as_deref() {
                                    p class="hint" { "Artifact: " code { (artifact_ref) } }
                                }
                                pre class="code-block small" { (pretty_json(&stage.stage_config_json)) }
                            }
                        }
                    }
                }
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Schedules" }
                        p { "Schedules are stored on the daemon and will materialize future workflow runs." }
                    }
                    (workflow_schedules_panel(&workflow.workflow.workflow_id, &workflow.schedules, user))
                }
            }
            section class="grid two-up" {
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Safety reviews" }
                        p { "Latest safety reviews for the current and previous versions." }
                    }
                    @if workflow.reviews.is_empty() {
                        p class="empty" { "No reviews recorded yet." }
                    } @else {
                        @for review in &workflow.reviews {
                            article class="list-item" {
                                div class="list-title" {
                                    strong { (&review.verdict) } " "
                                    span class="muted" { "risk " (&review.risk_level) }
                                }
                                p { (&review.summary_text) }
                                p class="hint" {
                                    "Version " (review.version_number) " at " (&review.created_at)
                                }
                            }
                        }
                    }
                }
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Recent runs" }
                        p { "Latest persisted workflow executions." }
                    }
                    @if workflow.runs.is_empty() {
                        p class="empty" { "No runs recorded yet." }
                    } @else {
                        @for run in &workflow.runs {
                            article class="list-item" {
                                div class="list-title" {
                                    strong { (&run.state) }
                                    span class="muted" { "run " (&run.workflow_run_id) }
                                }
                                p class="hint" {
                                    "Started " (&run.started_at)
                                    @if let Some(completed_at) = run.completed_at.as_deref() {
                                        " • Completed " (completed_at)
                                    }
                                }
                                @if let Some(error) = run.last_error.as_deref() {
                                    p class="hint danger-text" { (error) }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}

fn render_workflow_editor(
    users: &[UserSummary],
    user: Option<&str>,
    catalog: &AutomationCatalog,
    workflow: Option<&WorkflowInspection>,
    editing_schedule: Option<&ScheduleRow>,
) -> Markup {
    let workflow_id = workflow.map(|workflow| workflow.workflow.workflow_id.clone());
    let workflow_name = workflow
        .map(|workflow| workflow.workflow.name.clone())
        .unwrap_or_default();
    let workflow_description = workflow
        .and_then(|workflow| workflow.workflow.description.clone())
        .unwrap_or_default();
    let review_summary = workflow
        .and_then(|workflow| workflow.reviews.first())
        .map(|review| review.summary_text.clone())
        .unwrap_or_else(|| {
            "Review this workflow for intent matching, stage safety, and local household usability."
                .to_owned()
        });
    let stages = workflow
        .map(|workflow| workflow.stages.clone())
        .unwrap_or_default();
    page_shell(
        if workflow.is_some() {
            "Edit workflow"
        } else {
            "Create workflow"
        },
        user,
        html! {},
        html! {
            section class="hero compact" {
                div class="hero-copy" {
                    p class="eyebrow" { "Workflow editor" }
                    h1 {
                        @if workflow.is_some() { "Edit workflow" } @else { "Create workflow" }
                    }
                    p class="lede" {
                        "This editor keeps the current workflow model, but presents it in plain fields instead of raw JSON."
                    }
                }
                div class="hero-actions" {
                    @if let Some(workflow_id) = workflow_id.as_deref() {
                        a class="secondary-link" href=(workflow_link(workflow_id, user)) { "Back to workflow" }
                    } @else {
                        a class="secondary-link" href=(dashboard_link(user, false)) { "Back to dashboard" }
                    }
                }
            }
            section class="grid two-up" {
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Workflow details" }
                        p { "Use clear labels and plain-language instructions so the next operator understands what this workflow is meant to do." }
                    }
                    form method="post" action="/workflows/save" class="stacked-form" {
                        (hidden_user_input(user))
                        @if let Some(workflow_id) = workflow_id.as_deref() {
                            input type="hidden" name="workflow_id" value=(workflow_id);
                        }
                        label {
                            span { "User view" }
                            select name="user" {
                                option value="" selected[user.is_none()] { "Default user" }
                                @for entry in users {
                                    option value=(entry.user.user_id) selected[user == Some(entry.user.user_id.as_str())] {
                                        (&entry.user.display_name) " (" (entry.role_ids.join(", ")) ")"
                                    }
                                }
                            }
                        }
                        label {
                            span { "Workflow name" }
                            input type="text" name="name" value=(workflow_name) placeholder="Morning briefing" required;
                        }
                        label {
                            span { "Description" }
                            textarea name="description" rows="3" placeholder="What this workflow is for and what it should produce." {
                                (workflow_description)
                            }
                        }
                        label {
                            span { "Safety review summary" }
                            textarea name="intended_behavior_summary" rows="3" {
                                (review_summary)
                            }
                        }
                        div class="panel-subheader" {
                            h3 { "Stages" }
                            p { "Mix reusable scripts or Lua tools with plain-language agent and notification steps." }
                        }
                        div id="stage-list" class="stage-list" {
                            @if stages.is_empty() {
                                (workflow_stage_editor_row(None))
                            } @else {
                                @for stage in &stages {
                                    (workflow_stage_editor_row(Some(stage)))
                                }
                            }
                        }
                        div class="toolbar" {
                            button type="button" id="add-agent-stage" { "Add agent step" }
                            button type="button" id="add-notify-stage" { "Add notify step" }
                            button type="button" id="add-script-stage" { "Add script step" }
                            button type="button" id="add-tool-stage" { "Add Lua tool step" }
                        }
                        button type="submit" class="primary" { "Save workflow" }
                    }
                }
                div class="panel" {
                    div class="panel-header" {
                        h2 { "Available automation assets" }
                        p { "These are the current scripts, Lua tools, and schedules already known to the daemon." }
                    }
                    (catalog_panel(catalog))
                    @if let Some(workflow) = workflow {
                        div class="panel-subheader" {
                            h3 { "Schedule this workflow" }
                            p { "Use a friendly preset when possible. Custom cron is still available for advanced cases." }
                        }
                        (schedule_editor_form(&workflow.workflow.workflow_id, user, editing_schedule))
                    }
                }
            }
            (workflow_editor_script())
        },
    )
}

fn workflow_editor_script() -> Markup {
    html! {
        template id="stage-template" {
            (workflow_stage_editor_row(None))
        }
        script {
            (PreEscaped(r#"
const stageList = document.getElementById('stage-list');
const template = document.getElementById('stage-template');
function syncStageCard(card) {
  const kind = card.querySelector('select[name=\"stage_kind\"]').value;
  const artifact = card.querySelector('[data-field=\"artifact\"]');
  const prompt = card.querySelector('[data-field=\"prompt\"]');
  const notify = card.querySelector('[data-field=\"notify\"]');
  artifact.hidden = !(kind === 'lua_script' || kind === 'lua_tool');
  prompt.hidden = kind !== 'agent_task';
  notify.hidden = kind !== 'user_notify';
}
function wireStageCard(card) {
  const select = card.querySelector('select[name=\"stage_kind\"]');
  select.addEventListener('change', () => syncStageCard(card));
  card.querySelector('[data-remove-stage]').addEventListener('click', () => {
    if (stageList.children.length > 1) {
      card.remove();
    }
  });
  syncStageCard(card);
}
document.querySelectorAll('.stage-editor').forEach(wireStageCard);
function addStage(kind) {
  const fragment = template.content.cloneNode(true);
  const card = fragment.querySelector('.stage-editor');
  const select = card.querySelector('select[name=\"stage_kind\"]');
  select.value = kind;
  stageList.appendChild(fragment);
  wireStageCard(stageList.lastElementChild);
}
document.getElementById('add-agent-stage').addEventListener('click', () => addStage('agent_task'));
document.getElementById('add-notify-stage').addEventListener('click', () => addStage('user_notify'));
document.getElementById('add-script-stage').addEventListener('click', () => addStage('lua_script'));
document.getElementById('add-tool-stage').addEventListener('click', () => addStage('lua_tool'));
"#))
        }
    }
}

fn workflow_stage_editor_row(stage: Option<&beaverki_db::WorkflowStageRow>) -> Markup {
    let kind = stage
        .map(|stage| stage.stage_kind.as_str())
        .unwrap_or("agent_task");
    let config = stage
        .and_then(|stage| serde_json::from_str::<Value>(&stage.stage_config_json).ok())
        .unwrap_or_else(|| json!({}));
    html! {
        div class="stage-editor" {
            div class="stage-editor-head" {
                h4 { "Stage" }
                button type="button" class="ghost" data-remove-stage="true" { "Remove" }
            }
            label {
                span { "Stage kind" }
                select name="stage_kind" {
                    option value="agent_task" selected[kind == "agent_task"] { "Agent step" }
                    option value="user_notify" selected[kind == "user_notify"] { "Notify a user" }
                    option value="lua_script" selected[kind == "lua_script"] { "Run a Lua script" }
                    option value="lua_tool" selected[kind == "lua_tool"] { "Run a Lua tool" }
                }
            }
            label {
                span { "Label" }
                input type="text" name="stage_label" value=(stage.and_then(|stage| stage.stage_label.as_deref()).unwrap_or("")) placeholder="Draft digest";
            }
            div data-field="artifact" hidden[(kind != "lua_script" && kind != "lua_tool")] {
                label {
                    span { "Artifact ID" }
                    input type="text" name="stage_artifact_ref" list="artifact-options" value=(stage.and_then(|stage| stage.artifact_ref.as_deref()).unwrap_or("")) placeholder="script_daily_briefing";
                }
            }
            @if kind != "lua_script" && kind != "lua_tool" {
                input type="hidden" name="stage_artifact_ref" value=(stage.and_then(|stage| stage.artifact_ref.as_deref()).unwrap_or(""));
            }
            div data-field="prompt" hidden[kind != "agent_task"] {
                label {
                    span { "Agent prompt" }
                    textarea name="stage_prompt" rows="4" placeholder="Write a short morning digest from the workflow artifacts." {
                        (config.get("prompt").and_then(Value::as_str).unwrap_or(""))
                    }
                }
            }
            @if kind != "agent_task" {
                input type="hidden" name="stage_prompt" value=(config.get("prompt").and_then(Value::as_str).unwrap_or(""));
            }
            div data-field="notify" hidden[kind != "user_notify"] {
                label {
                    span { "Recipient" }
                    input type="text" name="stage_recipient" value=(config.get("recipient").and_then(Value::as_str).unwrap_or("")) placeholder="Leave empty to notify the workflow owner";
                }
                label {
                    span { "Message" }
                    textarea name="stage_message" rows="3" placeholder="Optional fixed message. Leave empty to reuse the last workflow result." {
                        (config.get("message").and_then(Value::as_str).unwrap_or(""))
                    }
                }
                label {
                    span { "Message template" }
                    textarea name="stage_message_template" rows="3" placeholder="Use {{last_result}} or {{stages[0].result}} placeholders." {
                        (config.get("message_template").and_then(Value::as_str).unwrap_or(""))
                    }
                }
            }
            @if kind != "user_notify" {
                input type="hidden" name="stage_recipient" value=(config.get("recipient").and_then(Value::as_str).unwrap_or(""));
                input type="hidden" name="stage_message" value=(config.get("message").and_then(Value::as_str).unwrap_or(""));
                input type="hidden" name="stage_message_template" value=(config.get("message_template").and_then(Value::as_str).unwrap_or(""));
            }
        }
    }
}

fn workflow_schedules_panel(
    workflow_id: &str,
    schedules: &[ScheduleRow],
    user: Option<&str>,
) -> Markup {
    html! {
        @if schedules.is_empty() {
            p class="empty" { "This workflow is not scheduled yet." }
        } @else {
            @for schedule in schedules {
                article class="list-item" {
                    div class="list-title" {
                        strong { (&schedule.schedule_id) }
                        (status_chip(if schedule_enabled(schedule) { "enabled" } else { "disabled" }))
                    }
                    p { "Cron: " code { (&schedule.cron_expr) } }
                    p class="hint" {
                        "Next run: " (&schedule.next_run_at)
                        @if let Some(last_run_at) = schedule.last_run_at.as_deref() {
                            " • Last run: " (last_run_at)
                        }
                    }
                    div class="inline-actions" {
                        form method="post" action=(format!("/schedules/{}/toggle", schedule.schedule_id)) {
                            (hidden_user_input(user))
                            button type="submit" {
                                @if schedule_enabled(schedule) { "Disable" } @else { "Enable" }
                            }
                        }
                        a class="secondary-link" href=(schedule_edit_link(workflow_id, &schedule.schedule_id, user)) { "Edit" }
                        form method="post" action=(format!("/schedules/{}/delete", schedule.schedule_id)) onsubmit="return confirm('Delete this schedule?');" {
                            (hidden_user_input(user))
                            button type="submit" class="danger" { "Delete" }
                        }
                    }
                }
            }
        }
    }
}

fn schedule_editor_form(
    workflow_id: &str,
    user: Option<&str>,
    schedule: Option<&ScheduleRow>,
) -> Markup {
    let (mode, schedule_time, custom_cron) = match schedule {
        Some(schedule) => ("custom", String::new(), schedule.cron_expr.clone()),
        None => ("daily", "08:00".to_owned(), String::new()),
    };
    html! {
        form method="post" action=(format!("/workflows/{workflow_id}/schedules/save")) class="stacked-form" {
            (hidden_user_input(user))
            @if let Some(schedule) = schedule {
                input type="hidden" name="schedule_id" value=(schedule.schedule_id);
            }
            label {
                span { "Schedule preset" }
                select name="schedule_mode" {
                    option value="daily" selected[mode == "daily"] { "Every day at a time" }
                    option value="weekdays" selected[mode == "weekdays"] { "Weekdays at a time" }
                    option value="hourly" selected[mode == "hourly"] { "Every hour" }
                    option value="custom" selected[mode == "custom"] { "Custom cron" }
                }
            }
            label {
                span { "Time" }
                input type="time" name="schedule_time" value=(schedule_time);
            }
            label {
                span { "Custom cron" }
                input type="text" name="custom_cron" value=(custom_cron) placeholder="0 8 * * *";
            }
            label class="check" {
                input type="checkbox" name="enabled" value="true" checked[schedule.map(schedule_enabled).unwrap_or(true)];
                span { "Enable immediately" }
            }
            button type="submit" class="primary" {
                @if schedule.is_some() { "Update schedule" } @else { "Create schedule" }
            }
        }
    }
}

fn catalog_panel(catalog: &AutomationCatalog) -> Markup {
    html! {
        datalist id="artifact-options" {
            @for script in &catalog.scripts {
                option value=(script.script_id) {}
            }
            @for tool in &catalog.lua_tools {
                option value=(tool.tool_id) {}
            }
        }
        div class="catalog-grid" {
            div {
                h3 { "Scripts" }
                @if catalog.scripts.is_empty() {
                    p class="empty" { "No scripts yet." }
                } @else {
                    @for script in &catalog.scripts {
                        p class="catalog-item" {
                            code { (&script.script_id) }
                            " "
                            (status_chip(&script.status))
                        }
                    }
                }
            }
            div {
                h3 { "Lua tools" }
                @if catalog.lua_tools.is_empty() {
                    p class="empty" { "No Lua tools yet." }
                } @else {
                    @for tool in &catalog.lua_tools {
                        p class="catalog-item" {
                            code { (&tool.tool_id) }
                            " "
                            (status_chip(&tool.status))
                        }
                    }
                }
            }
        }
    }
}

fn tasks_panel(tasks: &[beaverki_db::TaskRow], user: Option<&str>) -> Markup {
    html! {
        div class="list" {
            @if tasks.is_empty() {
                p class="empty" { "No recent tasks yet." }
            } @else {
                @for task in tasks {
                    article class="list-item" {
                        div class="list-title" {
                            a href=(task_link(&task.task_id, user)) { (&task.objective) }
                        }
                        p class="hint" {
                            "State: " (status_chip(&task.state))
                            " • Created " (&task.created_at)
                        }
                        @if let Some(result) = task.result_text.as_deref() {
                            p class="truncate" { (result) }
                        }
                    }
                }
            }
        }
    }
}

fn approvals_panel(approvals: &[ApprovalRow], user: Option<&str>) -> Markup {
    html! {
        @if approvals.is_empty() {
            p class="empty" { "No pending approvals." }
        } @else {
            @for approval in approvals {
                article class="list-item approval-card" {
                    div class="list-title" {
                        strong { (approval.action_summary.as_deref().unwrap_or(approval.action_type.as_str())) }
                        span class="muted" { (&approval.approval_id) }
                    }
                    p class="hint" {
                        "Risk: " (approval.risk_level.as_deref().unwrap_or("unknown"))
                        " • Task " a href=(task_link(&approval.task_id, user)) { (&approval.task_id) }
                    }
                    @if let Some(rationale) = approval.rationale_text.as_deref() {
                        p { (rationale) }
                    }
                    div class="inline-actions" {
                        form method="post" action=(format!("/approvals/{}/approve", approval.approval_id)) {
                            (hidden_user_input(user))
                            button type="submit" class="primary" { "Approve" }
                        }
                        form method="post" action=(format!("/approvals/{}/deny", approval.approval_id)) {
                            (hidden_user_input(user))
                            button type="submit" class="danger" { "Deny" }
                        }
                    }
                }
            }
        }
    }
}

fn sessions_panel(sessions: &[SessionSummary], user: Option<&str>) -> Markup {
    html! {
        @if sessions.is_empty() {
            p class="empty" { "No sessions matched this filter." }
        } @else {
            @for summary in sessions {
                article class="list-item session-card" {
                    div class="list-title" {
                        strong { (&summary.session.session_kind) }
                        span class="muted" { (&summary.session.session_id) }
                    }
                    p class="hint" {
                        "Owners: " (summary.owner_user_ids.join(", "))
                        " • Tasks: " (summary.task_count)
                    }
                    p class="hint" {
                        "Last activity: " (&summary.session.last_activity_at)
                        @if let Some(policy_id) = summary.matching_policy_id.as_deref() {
                            " • Policy " code { (policy_id) }
                        }
                    }
                    div class="inline-actions" {
                        form method="post" action=(format!("/sessions/{}/reset", summary.session.session_id)) onsubmit="return confirm('Reset this session? Durable memory stays intact.');" {
                            (hidden_user_input(user))
                            button type="submit" { "Reset" }
                        }
                        form method="post" action=(format!("/sessions/{}/archive", summary.session.session_id)) onsubmit="return confirm('Archive this session?');" {
                            (hidden_user_input(user))
                            button type="submit" class="danger" { "Archive" }
                        }
                    }
                }
            }
        }
    }
}

fn users_panel(users: &[UserSummary], active_user: Option<&str>) -> Markup {
    html! {
        @if users.is_empty() {
            p class="empty" { "No users found." }
        } @else {
            div class="list" {
                @for entry in users {
                    article class={(if active_user == Some(entry.user.user_id.as_str()) { "list-item user-card is-active" } else { "list-item user-card" })} {
                        div class="list-title" {
                            div {
                                strong { (&entry.user.display_name) }
                                p class="muted inline-detail" { (&entry.user.user_id) }
                            }
                            (status_chip(&entry.user.status))
                        }
                        (role_tokens(&entry.role_ids))
                        @if let Some(primary_agent_id) = entry.user.primary_agent_id.as_deref() {
                            p class="hint" { "Primary agent: " code { (primary_agent_id) } }
                        }
                    }
                }
            }
        }
    }
}

fn user_create_panel(roles: &[RoleRow]) -> Markup {
    html! {
        form method="post" action="/users" class="stacked-form" {
            label {
                span { "Display name" }
                input type="text" name="display_name" placeholder="Casey" required;
            }
            div class="role-picker" {
                p class="hint" { "Roles" }
                @for role in roles {
                    label class="role-option" {
                        input type="checkbox" name="roles" value=(role.role_id);
                        span class="role-copy" {
                            strong { (&role.role_id) }
                            span class="hint" { (&role.description) }
                        }
                    }
                }
            }
            button type="submit" class="primary" { "Create user" }
        }
    }
}

fn memories_panel(memories: &[MemoryRow]) -> Markup {
    html! {
        @if memories.is_empty() {
            p class="empty" { "No memories found." }
        } @else {
            @for memory in memories {
                article class="list-item" {
                    div class="list-title" {
                        strong { (&memory.subject_type) }
                        span class="muted" { (&memory.memory_kind) " • " (&memory.scope) }
                    }
                    p class="truncate" { (&memory.content_text) }
                    p class="hint" { (&memory.updated_at) }
                }
            }
        }
    }
}

fn automation_panel(catalog: &AutomationCatalog, user: Option<&str>) -> Markup {
    html! {
        div class="automation-layout" {
            div class="automation-support" {
                div class="panel inset" {
                    div class="panel-header" {
                        h3 { "Scripts" }
                        p { (catalog.scripts.len()) " items" }
                    }
                    @if catalog.scripts.is_empty() {
                        p class="empty" { "No scripts yet." }
                    } @else {
                        @for script in &catalog.scripts {
                            p class="catalog-item" {
                                code { (&script.script_id) }
                                " "
                                (status_chip(&script.status))
                            }
                        }
                    }
                }
                div class="panel inset" {
                    div class="panel-header" {
                        h3 { "Lua tools" }
                        p { (catalog.lua_tools.len()) " items" }
                    }
                    @if catalog.lua_tools.is_empty() {
                        p class="empty" { "No Lua tools yet." }
                    } @else {
                        @for tool in &catalog.lua_tools {
                            p class="catalog-item" {
                                code { (&tool.tool_id) }
                                " "
                                (status_chip(&tool.status))
                            }
                        }
                    }
                }
            }
            div class="panel inset automation-workflows" {
                div class="panel-header" {
                    h3 { "Workflows" }
                    p { (catalog.workflows.len()) " items" }
                }
                @if catalog.workflows.is_empty() {
                    p class="empty" { "No workflows yet." }
                } @else {
                    div class="list" {
                        @for workflow in &catalog.workflows {
                            article class="list-item compact-item" {
                                div class="list-title" {
                                    div {
                                        a href=(workflow_link(&workflow.workflow_id, user)) { (&workflow.name) }
                                        p class="muted inline-detail" { (&workflow.workflow_id) }
                                    }
                                }
                                p class="hint" {
                                    (status_chip(&workflow.status)) " "
                                    (status_chip(&workflow.safety_status))
                                }
                            }
                        }
                    }
                }
            }
        }
        div class="panel inset automation-schedules" {
            div class="panel-header" {
                h3 { "Schedules" }
                p { (catalog.schedules.len()) " items across all target types" }
            }
            @if catalog.schedules.is_empty() {
                p class="empty" { "No schedules yet. Create a workflow and schedule it from the workflow detail page." }
            } @else {
                div class="list" {
                    @for schedule in &catalog.schedules {
                        article class="list-item compact-item" {
                            div class="list-title" {
                                div {
                                    @if schedule.target_type == "workflow" {
                                        a href=(workflow_link(&schedule.target_id, user)) { (&schedule.target_id) }
                                    } @else {
                                        strong { (&schedule.target_id) }
                                    }
                                    p class="muted inline-detail" {
                                        code { (&schedule.target_type) }
                                        " · " (&schedule.schedule_id)
                                    }
                                }
                                (status_chip(if schedule_enabled(schedule) { "enabled" } else { "disabled" }))
                            }
                            p class="hint" {
                                "Cron: " code { (&schedule.cron_expr) }
                                " · Next: " (&schedule.next_run_at)
                                @if let Some(last_run_at) = schedule.last_run_at.as_deref() {
                                    " · Last: " (last_run_at)
                                }
                            }
                            div class="inline-actions" {
                                @if schedule.target_type == "workflow" {
                                    a class="secondary-link" href=(schedule_edit_link(&schedule.target_id, &schedule.schedule_id, user)) { "Edit" }
                                }
                                form method="post" action=(format!("/schedules/{}/toggle", schedule.schedule_id)) {
                                    (hidden_user_input(user))
                                    button type="submit" {
                                        @if schedule_enabled(schedule) { "Disable" } @else { "Enable" }
                                    }
                                }
                                form method="post" action=(format!("/schedules/{}/delete", schedule.schedule_id)) onsubmit="return confirm('Delete this schedule?');" {
                                    (hidden_user_input(user))
                                    button type="submit" class="danger" { "Delete" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn task_events_panel(events: &[TaskEventRow]) -> Markup {
    html! {
        @if events.is_empty() {
            p class="empty" { "No events recorded." }
        } @else {
            @for event in events {
                article class="list-item" {
                    div class="list-title" {
                        strong { (&event.event_type) }
                        span class="muted" { (&event.created_at) }
                    }
                    pre class="code-block small" { (pretty_json(&event.payload_json)) }
                }
            }
        }
    }
}

fn tool_invocations_panel(invocations: &[ToolInvocationRow]) -> Markup {
    html! {
        @if invocations.is_empty() {
            p class="empty" { "No tool invocations recorded." }
        } @else {
            @for invocation in invocations {
                article class="list-item" {
                    div class="list-title" {
                        strong { (&invocation.tool_name) }
                        span class="muted" { (&invocation.status) }
                    }
                    pre class="code-block small" { (pretty_json(&invocation.request_json)) }
                    @if let Some(response_json) = invocation.response_json.as_deref() {
                        pre class="code-block small" { (pretty_json(response_json)) }
                    }
                }
            }
        }
    }
}

fn workflow_definition_from_form(form: &WorkflowForm) -> Result<WorkflowDefinitionInput> {
    let max_len = [
        form.stage_kind.len(),
        form.stage_label.len(),
        form.stage_artifact_ref.len(),
        form.stage_prompt.len(),
        form.stage_recipient.len(),
        form.stage_message.len(),
        form.stage_message_template.len(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let mut stages = Vec::new();
    for index in 0..max_len {
        let kind = form
            .stage_kind
            .get(index)
            .map(|value| value.trim())
            .unwrap_or("");
        if kind.is_empty() {
            continue;
        }
        let label = form
            .stage_label
            .get(index)
            .and_then(|value| non_empty(value.clone()));
        let artifact_ref = form
            .stage_artifact_ref
            .get(index)
            .and_then(|value| non_empty(value.clone()));
        let prompt = form
            .stage_prompt
            .get(index)
            .and_then(|value| non_empty(value.clone()));
        let recipient = form
            .stage_recipient
            .get(index)
            .and_then(|value| non_empty(value.clone()));
        let message = form
            .stage_message
            .get(index)
            .and_then(|value| non_empty(value.clone()));
        let message_template = form
            .stage_message_template
            .get(index)
            .and_then(|value| non_empty(value.clone()));
        let config = match kind {
            "agent_task" => json!({
                "prompt": prompt.unwrap_or_default(),
            }),
            "user_notify" => {
                let mut config = serde_json::Map::new();
                if let Some(recipient) = recipient {
                    config.insert("recipient".to_owned(), Value::String(recipient));
                }
                if let Some(message) = message {
                    config.insert("message".to_owned(), Value::String(message));
                }
                if let Some(message_template) = message_template {
                    config.insert(
                        "message_template".to_owned(),
                        Value::String(message_template),
                    );
                }
                Value::Object(config)
            }
            "lua_script" | "lua_tool" => json!({}),
            other => bail!("unsupported stage kind '{other}'"),
        };
        stages.push(WorkflowStageInput {
            kind: kind.to_owned(),
            label,
            artifact_ref,
            config,
        });
    }
    if stages.is_empty() {
        bail!("the workflow must contain at least one stage");
    }
    Ok(WorkflowDefinitionInput {
        name: form.name.trim().to_owned(),
        description: non_empty(form.description.clone()),
        stages,
    })
}

fn schedule_cron(form: &ScheduleFormInput) -> Result<String> {
    match form.schedule_mode.as_str() {
        "hourly" => Ok("0 * * * *".to_owned()),
        "daily" => {
            let (hour, minute) = parse_time_of_day(form.schedule_time.as_deref())?;
            Ok(format!("{minute} {hour} * * *"))
        }
        "weekdays" => {
            let (hour, minute) = parse_time_of_day(form.schedule_time.as_deref())?;
            Ok(format!("{minute} {hour} * * 1-5"))
        }
        "custom" => form
            .custom_cron
            .clone()
            .and_then(non_empty)
            .ok_or_else(|| anyhow!("custom cron cannot be empty")),
        other => bail!("unsupported schedule mode '{other}'"),
    }
}

fn parse_time_of_day(input: Option<&str>) -> Result<(u8, u8)> {
    let value = input
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("a schedule time is required"))?;
    let (hour, minute) = value
        .split_once(':')
        .ok_or_else(|| anyhow!("time must use HH:MM"))?;
    let hour: u8 = hour.parse().context("invalid hour")?;
    let minute: u8 = minute.parse().context("invalid minute")?;
    if hour > 23 || minute > 59 {
        bail!("time must use a 24-hour HH:MM value");
    }
    Ok((hour, minute))
}

fn page_shell(title: &str, user: Option<&str>, topbar_extra: Markup, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                style {
                    (PreEscaped(r#"
:root {
  --bg: #eef2f6;
  --bg-2: #f6f8fb;
  --panel: #ffffff;
  --panel-2: #f8fafc;
  --ink: #14202b;
  --muted: #5e6d7d;
  --accent: #2563eb;
  --accent-strong: #1d4ed8;
  --accent-soft: #dbeafe;
  --accent-soft-2: #eff6ff;
  --line: #d8e1ea;
  --line-strong: #b8c6d6;
  --danger: #b42318;
  --danger-soft: #fef3f2;
  --shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
  --radius: 12px;
  --radius-sm: 8px;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--ink);
  font-family: "IBM Plex Sans", "Segoe UI Variable", "Avenir Next", sans-serif;
  background: linear-gradient(180deg, var(--bg-2) 0%, var(--bg) 100%);
  line-height: 1.55;
  -webkit-font-smoothing: antialiased;
}
a { color: inherit; }
.page {
  max-width: 1280px;
  margin: 0 auto;
  padding: 24px 20px 56px;
}
.topbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  margin-bottom: 24px;
  padding: 14px 18px;
  background: rgba(255, 255, 255, 0.72);
  border: 1px solid rgba(216, 225, 234, 0.9);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
}
.brand {
  font-family: "IBM Plex Sans", "Segoe UI Variable", sans-serif;
  font-size: 1.35rem;
  letter-spacing: 0.04em;
  font-weight: 800;
  text-decoration: none;
}
.brand span { color: var(--accent); }
.topnav {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}
.topnav a {
  text-decoration: none;
  color: var(--muted);
  padding: 9px 13px;
  border-radius: var(--radius-sm);
  background: var(--panel);
  border: 1px solid var(--line);
  transition: background 0.12s, color 0.12s, border-color 0.12s;
}
.hero, .panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
}
.hero {
  display: grid;
  grid-template-columns: 1.4fr .8fr;
  gap: 22px;
  padding: 18px 24px;
  margin-bottom: 14px;
  border-left: 4px solid var(--accent);
}
.hero.compact { grid-template-columns: 1fr auto; }
.hero-copy h1, .panel h2, .panel h3, .panel h4 {
  margin: 0;
  font-family: "IBM Plex Sans", "Segoe UI Variable", sans-serif;
  letter-spacing: 0.02em;
}
.eyebrow {
  margin: 0 0 10px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  font-size: 0.77rem;
  color: var(--accent);
  font-weight: 700;
}
.lede {
  font-size: 1.05rem;
  line-height: 1.6;
}
.hint, .muted {
  color: var(--muted);
}
.danger-text { color: var(--danger); }
.hero-stats {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}
.stat-card {
  padding: 14px 16px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--line);
  background: var(--panel-2);
}
.stat-card-label {
  display: block;
  font-size: 0.8rem;
  color: var(--muted);
  margin-bottom: 6px;
}
.stat-card-value {
  font-size: 1.4rem;
  font-weight: 700;
}
.grid {
  display: grid;
  gap: 18px;
}
.two-up { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.three-up { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.panel {
  padding: 22px;
  background: var(--panel);
}
.panel.inset {
  margin: 0;
  background: var(--panel-2);
  box-shadow: none;
}
.section-block {
  margin-bottom: 28px;
}
.section-heading {
  margin: 0 0 16px;
  padding-left: 14px;
  border-left: 3px solid var(--accent);
}
.section-heading h2,
.section-heading p {
  margin: 0;
}
.section-heading h2 {
  margin-bottom: 6px;
}
.panel-header, .panel-subheader {
  margin-bottom: 16px;
}
.panel-header h2, .panel-header h3 { margin: 0 0 4px; }
.panel-subheader h3 { margin: 0 0 4px; }
.panel-header p, .panel-subheader p { margin: 0; color: var(--muted); font-size: 0.9rem; }
.toolbar, .inline-actions, .hero-actions, .toolbar-form {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.toolbar.actions { margin-bottom: 18px; }
.toolbar-form { align-items: end; }
.workspace-panel {
  margin-bottom: 28px;
}
.workspace-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(280px, 0.8fr);
  gap: 18px;
}
.workspace-form {
  margin-bottom: 14px;
}
.workspace-controls,
.workspace-summary {
  border: 1px solid var(--line);
  border-radius: var(--radius-sm);
  padding: 18px;
  background: var(--panel-2);
}
.workspace-summary h3 {
  margin: 0 0 6px;
}
.quick-links {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.summary-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 12px 0;
}
.summary-list {
  display: grid;
  gap: 10px;
  margin: 16px 0 0;
}
.summary-list div {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  padding-top: 10px;
  border-top: 1px solid var(--line);
}
.summary-list dt,
.summary-list dd {
  margin: 0;
}
.toolbar-form label, .stacked-form label {
  display: grid;
  gap: 8px;
  color: var(--muted);
}
.stacked-form {
  display: grid;
  gap: 14px;
}
textarea, input, select, button {
  font: inherit;
}
textarea, input[type=text], input[type=time], select {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: var(--radius-sm);
  padding: 12px 14px;
  background: #fff;
  color: var(--ink);
}
textarea::placeholder, input::placeholder {
  color: #8b98a8;
}
button, .primary-link, .secondary-link {
  border-radius: var(--radius-sm);
  padding: 11px 16px;
  border: 1px solid transparent;
  text-decoration: none;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.82rem;
  font-weight: 700;
  transition: background 0.12s, border-color 0.12s, color 0.12s;
}
button, .secondary-link {
  background: #fff;
  border-color: var(--line);
  color: var(--ink);
}
.primary, .primary-link {
  background: var(--accent);
  border-color: var(--accent-strong);
  color: #fff;
}
.danger {
  background: var(--danger-soft);
  border-color: #f7c6c2;
  color: var(--danger);
}
.ghost {
  background: transparent;
  color: var(--muted);
}
.check {
  display: inline-flex !important;
  align-items: center;
  gap: 10px;
}
.role-picker {
  display: grid;
  gap: 10px;
}
.role-option {
  display: flex !important;
  gap: 12px;
  align-items: flex-start;
  padding: 12px;
  border: 1px solid var(--line);
  border-radius: var(--radius-sm);
  background: var(--panel-2);
}
.role-copy {
  display: grid;
  gap: 4px;
}
.list {
  display: grid;
  gap: 12px;
}
.list-item, .stage-editor, .stage-card {
  border: 1px solid var(--line);
  border-radius: var(--radius-sm);
  padding: 16px;
  background: var(--panel-2);
  transition: border-color 0.12s, box-shadow 0.12s;
}
.list-item:hover {
  border-color: var(--line-strong);
  box-shadow: 0 2px 8px rgba(15,23,42,0.06);
}
.list-item.compact-item {
  padding: 12px 14px;
}
.user-card.is-active {
  border-color: var(--accent);
  box-shadow: inset 0 0 0 1px rgba(37, 99, 235, 0.12);
}
.list-title {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: baseline;
  margin-bottom: 8px;
}
.inline-detail {
  margin: 4px 0 0;
}
.empty {
  color: var(--muted);
  margin: 8px 0 0;
}
.truncate {
  margin: 0;
  overflow: hidden;
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
}
.status-chip {
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: 999px;
  background: var(--accent-soft-2);
  border: 1px solid #c7ddff;
  color: var(--accent-strong);
  font-size: 0.8rem;
  text-transform: capitalize;
}
.token-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
}
.token {
  display: inline-flex;
  align-items: center;
  padding: 5px 10px;
  border-radius: 999px;
  background: #eef4ff;
  border: 1px solid #d7e4ff;
  color: var(--accent-strong);
  font-size: 0.82rem;
  font-weight: 600;
}
.token-muted {
  color: var(--muted);
  background: #f3f6f9;
  border-color: var(--line);
}
.code-block {
  margin: 0;
  padding: 14px;
  border-radius: var(--radius-sm);
  border: 1px solid #203246;
  background: #0f172a;
  color: #e2e8f0;
  overflow-x: auto;
  white-space: pre-wrap;
}
.code-block.small {
  font-size: 0.85rem;
}
.catalog-grid,
.automation-layout {
  display: grid;
  gap: 18px;
}
.catalog-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}
.automation-layout {
  grid-template-columns: minmax(240px, 0.8fr) minmax(0, 1.2fr);
}
.automation-support {
  display: grid;
  gap: 18px;
}
.automation-schedules {
  margin-top: 18px;
}
.catalog-item {
  margin: 0 0 10px;
}
.stage-list {
  display: grid;
  gap: 14px;
}
.stage-editor-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}
.stage-card {
  display: grid;
  grid-template-columns: 52px 1fr;
  gap: 14px;
}
.stage-index {
  width: 52px;
  height: 52px;
  border-radius: var(--radius-sm);
  background: #eff6ff;
  border: 1px solid #cfe0ff;
  display: grid;
  place-items: center;
  font-weight: 700;
  color: var(--accent-strong);
}
code {
  font-family: "SFMono-Regular", "Consolas", monospace;
  font-size: 0.87em;
  background: #f1f4f8;
  padding: 1px 5px;
  border-radius: 4px;
  border: 1px solid var(--line);
}
.code-block code {
  background: transparent;
  border: none;
  padding: 0;
}
/* Topbar user form */
.topbar-userform {
  display: flex;
  align-items: center;
  gap: 10px;
}
.topbar-userform select {
  width: auto;
  min-width: 150px;
  padding: 7px 10px;
  font-size: 0.88rem;
  border-radius: var(--radius-sm);
  border: 1px solid var(--line);
  background: var(--panel);
  color: var(--ink);
}
.topbar-check {
  display: inline-flex !important;
  align-items: center;
  gap: 6px;
  font-size: 0.85rem;
  color: var(--muted);
  cursor: pointer;
  white-space: nowrap;
}
.topbar-check input[type=checkbox] {
  width: auto;
  border: none;
  padding: 0;
  margin: 0;
}
/* Tab bar */
.tab-bar {
  display: flex;
  gap: 2px;
  margin-bottom: 20px;
  border-bottom: 2px solid var(--line);
}
.tab-btn {
  border: none;
  border-radius: 0;
  background: transparent;
  color: var(--muted);
  font-size: 0.9rem;
  font-weight: 600;
  letter-spacing: 0.02em;
  text-transform: none;
  padding: 10px 20px;
  border-bottom: 2px solid transparent;
  margin-bottom: -2px;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  transition: color 0.12s, border-color 0.12s;
}
.tab-btn:hover:not(:disabled) {
  background: transparent;
  border-color: transparent;
  border-bottom-color: var(--line-strong);
  color: var(--ink);
}
.tab-btn.active {
  color: var(--accent);
  border-bottom-color: var(--accent);
}
.tab-btn.active:hover:not(:disabled) {
  background: transparent;
  border-bottom-color: var(--accent-strong);
}
.tab-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 20px;
  height: 20px;
  padding: 0 6px;
  border-radius: 999px;
  background: #f59e0b;
  color: #fff;
  font-size: 0.75rem;
  font-weight: 700;
}
.tab-badge-muted {
  background: var(--line);
  color: var(--muted);
}
/* Status chip color variants */
.chip-success { background: #f0fdf4; border-color: #86efac; color: #15803d; }
.chip-info { background: var(--accent-soft-2); border-color: #c7ddff; color: var(--accent-strong); }
.chip-warn { background: #fffbeb; border-color: #fcd34d; color: #b45309; }
.chip-danger { background: var(--danger-soft); border-color: #fca5a5; color: #b91c1c; }
.chip-muted { background: #f3f6f9; border-color: var(--line); color: var(--muted); }
/* Approval card urgency */
.approval-card { border-left: 3px solid #f59e0b; }
/* Button hover states */
button:hover:not(:disabled),
.secondary-link:hover { background: var(--panel-2); border-color: var(--line-strong); }
.primary:hover:not(:disabled),
.primary-link:hover { background: var(--accent-strong); border-color: var(--accent-strong); }
.danger:hover:not(:disabled) { background: #fee2e0; border-color: #fca5a5; }
/* Keyboard focus ring */
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
/* Topnav hover and active page */
.topnav a:hover, .topnav a.active {
  background: var(--accent-soft);
  color: var(--accent);
  border-color: #93c5fd;
}
/* Clickable list item links */
.list-title a { text-decoration: none; font-weight: 600; color: var(--ink); }
.list-title a:hover { color: var(--accent); }
/* Hint text */
.hint { font-size: 0.88rem; }
@media (max-width: 920px) {
  .hero, .hero.compact, .two-up, .three-up, .catalog-grid, .automation-layout, .workspace-layout {
    grid-template-columns: 1fr;
  }
  .page {
    padding: 18px 14px 40px;
  }
  .topbar {
    padding: 12px 14px;
  }
}
"#))
                }
            }
            body {
                div class="page" {
                    header class="topbar" {
                        a class="brand" href=(dashboard_link(user, false)) { span { "Beaver" } "Ki UI" }
                        (topbar_extra)
                        nav class="topnav" {
                            a href=(dashboard_link(user, false)) { "Dashboard" }
                            a href=(workflow_editor_link(None, user)) { "New workflow" }
                        }
                    }
                    script {
                        (PreEscaped(r#"
(function(){
  var path = window.location.pathname;
  document.querySelectorAll('.topnav a').forEach(function(a) {
    var ap = new URL(a.href).pathname;
    if (ap === path || (ap !== '/' && path.startsWith(ap))) {
      a.classList.add('active');
    }
  });
})();
"#))
                    }
                    (body)
                }
            }
        }
    }
}

fn stat_card(label: &str, value: &str) -> Markup {
    html! {
        div class="stat-card" {
            span class="stat-card-label" { (label) }
            strong class="stat-card-value" { (value) }
        }
    }
}

fn role_tokens(role_ids: &[String]) -> Markup {
    html! {
        div class="token-row" {
            @if role_ids.is_empty() {
                span class="token token-muted" { "No roles" }
            } @else {
                @for role_id in role_ids {
                    span class="token" { (role_id) }
                }
            }
        }
    }
}

fn status_chip(label: &str) -> Markup {
    let modifier = match label.to_lowercase().as_str() {
        "completed" | "active" | "approved" | "enabled" | "safe" => "chip-success",
        "running" => "chip-info",
        "pending" | "waiting_approval" | "pending_review" | "waiting" => "chip-warn",
        "failed" | "error" | "denied" | "unsafe" | "blocked" => "chip-danger",
        "disabled" | "inactive" | "archived" | "draft" => "chip-muted",
        _ => "",
    };
    let class = if modifier.is_empty() {
        "status-chip".to_owned()
    } else {
        format!("status-chip {modifier}")
    };
    html! { span class=(class) { (label.replace('_', " ")) } }
}

fn hidden_user_input(user: Option<&str>) -> Markup {
    html! { input type="hidden" name="user" value=(user.unwrap_or("")); }
}

fn schedule_enabled(schedule: &ScheduleRow) -> bool {
    schedule.enabled != 0
}

fn normalize_user(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn dashboard_link(user: Option<&str>, include_archived: bool) -> String {
    let mut path = "/".to_owned();
    let mut params = Vec::new();
    if let Some(user) = user {
        params.push(format!("user={user}"));
    }
    if include_archived {
        params.push("include_archived=true".to_owned());
    }
    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }
    path
}

fn user_query_suffix(user: Option<&str>) -> String {
    match user {
        Some(user) => format!("?user={user}"),
        None => String::new(),
    }
}

fn task_link(task_id: &str, user: Option<&str>) -> String {
    format!("/tasks/{task_id}{}", user_query_suffix(user))
}

fn workflow_link(workflow_id: &str, user: Option<&str>) -> String {
    format!("/workflows/{workflow_id}{}", user_query_suffix(user))
}

fn schedule_edit_link(workflow_id: &str, schedule_id: &str, user: Option<&str>) -> String {
    let user_param = user.map(|u| format!("&user={u}")).unwrap_or_default();
    format!("/workflows/{workflow_id}/edit?schedule_id={schedule_id}{user_param}")
}

fn workflow_editor_link(workflow_id: Option<&str>, user: Option<&str>) -> String {
    match workflow_id {
        Some(workflow_id) => format!("/workflows/{workflow_id}/edit{}", user_query_suffix(user)),
        None => format!("/workflows/new{}", user_query_suffix(user)),
    }
}

fn pretty_json(raw: &str) -> String {
    match serde_json::from_str::<Value>(raw) {
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| raw.to_owned()),
        Err(_) => raw.to_owned(),
    }
}
