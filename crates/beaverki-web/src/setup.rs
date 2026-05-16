use std::fs;
use std::future;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Form,
    extract::State,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
};
use beaverki_config::{
    DEFAULT_LM_STUDIO_BASE_URL, DEFAULT_LM_STUDIO_MODEL, DEFAULT_OPENAI_EXECUTOR_MODEL,
    DEFAULT_OPENAI_PLANNER_MODEL, DEFAULT_OPENAI_SAFETY_REVIEW_MODEL,
    DEFAULT_OPENAI_SUMMARIZER_MODEL, LoadedConfig, SetupAnswers, SetupProviderKind,
    default_app_paths, default_lm_studio_provider, default_openai_provider, default_workspace_root,
    normalize_provider_base_url, write_setup_files,
};
use beaverki_db::Database;
use beaverki_models::{LmStudioProvider, ModelProvider, OpenAiProvider};
use beaverki_runtime::{DaemonClient, Runtime, RuntimeDaemon};
use maud::{DOCTYPE, Markup, PreEscaped, html};
use serde::{Deserialize, Serialize};

use crate::{AppError, AppState};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct SetupDraft {
    instance_id: String,
    owner_display_name: String,
    workspace_root: PathBuf,
    data_dir: PathBuf,
    state_dir: PathBuf,
    log_dir: PathBuf,
    secret_dir: PathBuf,
    provider: SetupProviderKind,
    openai_planner_model: String,
    openai_executor_model: String,
    openai_summarizer_model: String,
    openai_safety_review_model: String,
    lm_studio_base_url: String,
    lm_studio_planner_model: String,
    lm_studio_executor_model: String,
    lm_studio_summarizer_model: String,
    lm_studio_safety_review_model: String,
    lm_studio_tool_calling: bool,
    advanced_paths: bool,
}

impl Default for SetupDraft {
    fn default() -> Self {
        let app_paths = default_app_paths().ok();
        let workspace_root = default_workspace_root().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            instance_id: "beaverki-local".to_owned(),
            owner_display_name: String::new(),
            workspace_root,
            data_dir: app_paths
                .as_ref()
                .map(|paths| paths.data_dir.clone())
                .unwrap_or_else(|| PathBuf::from("./data")),
            state_dir: app_paths
                .as_ref()
                .map(|paths| paths.state_dir.clone())
                .unwrap_or_else(|| PathBuf::from("./state")),
            log_dir: app_paths
                .as_ref()
                .map(|paths| paths.log_dir.clone())
                .unwrap_or_else(|| PathBuf::from("./state/logs")),
            secret_dir: app_paths
                .as_ref()
                .map(|paths| paths.secret_dir.clone())
                .unwrap_or_else(|| PathBuf::from("./state/secrets")),
            provider: SetupProviderKind::OpenAi,
            openai_planner_model: DEFAULT_OPENAI_PLANNER_MODEL.to_owned(),
            openai_executor_model: DEFAULT_OPENAI_EXECUTOR_MODEL.to_owned(),
            openai_summarizer_model: DEFAULT_OPENAI_SUMMARIZER_MODEL.to_owned(),
            openai_safety_review_model: DEFAULT_OPENAI_SAFETY_REVIEW_MODEL.to_owned(),
            lm_studio_base_url: DEFAULT_LM_STUDIO_BASE_URL.to_owned(),
            lm_studio_planner_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_executor_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_summarizer_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_safety_review_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_tool_calling: false,
            advanced_paths: false,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SetupForm {
    instance_id: String,
    owner_display_name: String,
    workspace_root: String,
    data_dir: String,
    state_dir: String,
    log_dir: String,
    secret_dir: String,
    provider: String,
    openai_api_token: Option<String>,
    master_passphrase: Option<String>,
    master_passphrase_confirm: Option<String>,
    openai_planner_model: String,
    openai_executor_model: String,
    openai_summarizer_model: String,
    openai_safety_review_model: String,
    lm_studio_base_url: String,
    lm_studio_planner_model: String,
    lm_studio_executor_model: String,
    lm_studio_summarizer_model: String,
    lm_studio_safety_review_model: String,
    lm_studio_tool_calling: Option<String>,
    advanced_paths: Option<String>,
}

pub(crate) fn router() -> axum::Router<AppState> {
    axum::Router::new()
        .route("/setup", get(setup_page))
        .route("/setup/save", post(save_setup))
        .route("/setup/complete", post(complete_setup))
}

pub(crate) async fn setup_required(config_dir: &Path) -> Result<bool> {
    if !config_dir.join("runtime.yaml").exists() || !config_dir.join("providers.yaml").exists() {
        return Ok(true);
    }

    let config = LoadedConfig::load_from_dir(config_dir)
        .with_context(|| format!("failed to load {}", config_dir.display()))?;
    let db = Database::connect(&config.runtime.database_path).await?;
    Ok(db.default_user().await?.is_none())
}

async fn setup_page(State(state): State<AppState>) -> Result<Markup, AppError> {
    let draft = load_setup_draft(&state.config_dir)?;
    Ok(render_setup_page(&state.config_dir, &draft, None))
}

async fn save_setup(
    State(state): State<AppState>,
    Form(form): Form<SetupForm>,
) -> Result<Response, AppError> {
    let draft = draft_from_form(&form)?;
    write_setup_draft(&state.config_dir, &draft)?;
    Ok(Redirect::to("/setup").into_response())
}

async fn complete_setup(
    State(state): State<AppState>,
    Form(form): Form<SetupForm>,
) -> Result<Response, AppError> {
    let draft = draft_from_form(&form)?;
    write_setup_draft(&state.config_dir, &draft)?;

    let app_paths = default_app_paths()?;
    let passphrase = required_secret(form.master_passphrase.as_deref(), "master passphrase")?;
    let passphrase_confirm = required_secret(
        form.master_passphrase_confirm.as_deref(),
        "master passphrase confirmation",
    )?;
    if passphrase != passphrase_confirm {
        return Err(anyhow!("master passphrases do not match").into());
    }

    let provider_kind = SetupProviderKind::parse(&form.provider)?;
    let (provider, provider_secret_value) = match provider_kind {
        SetupProviderKind::OpenAi => {
            let api_token = required_secret(form.openai_api_token.as_deref(), "OpenAI API token")?;
            (
                default_openai_provider(
                    required_text(&form.openai_planner_model, "planner model")?,
                    required_text(&form.openai_executor_model, "executor model")?,
                    required_text(&form.openai_summarizer_model, "summarizer model")?,
                    required_text(&form.openai_safety_review_model, "safety review model")?,
                ),
                Some(api_token),
            )
        }
        SetupProviderKind::LmStudio => (
            default_lm_studio_provider(
                &form.lm_studio_base_url,
                required_text(&form.lm_studio_planner_model, "planner model")?,
                required_text(&form.lm_studio_executor_model, "executor model")?,
                required_text(&form.lm_studio_summarizer_model, "summarizer model")?,
                required_text(&form.lm_studio_safety_review_model, "safety review model")?,
                form.lm_studio_tool_calling.is_some(),
            )?,
            None,
        ),
    };

    verify_provider_entry(&provider, provider_secret_value.as_deref()).await?;

    let data_dir = path_from_input(&form.data_dir, "data dir")?;
    let state_dir = path_from_input(&form.state_dir, "state dir")?;
    let log_dir = path_from_input(&form.log_dir, "log dir")?;
    let secret_dir = path_from_input(&form.secret_dir, "secret dir")?;
    let answers = SetupAnswers {
        config_dir: state.config_dir.clone(),
        instance_id: required_text(&form.instance_id, "instance ID")?,
        owner_display_name: required_text(&form.owner_display_name, "owner display name")?,
        data_dir,
        state_dir: state_dir.clone(),
        log_dir,
        secret_dir,
        database_path: state_dir.join("runtime.db"),
        workspace_root: path_from_input(&form.workspace_root, "workspace root")?,
        provider,
        provider_secret_value,
        master_passphrase: Some(passphrase.clone()),
    };

    write_setup_files(&answers)?;
    let db = Database::connect(&answers.database_path).await?;
    db.bootstrap_single_user(&answers.owner_display_name)
        .await?;
    remove_setup_draft(&state.config_dir)?;
    start_daemon_for_setup(
        &state,
        &app_paths.config_dir,
        &state.config_dir,
        &passphrase,
    )
    .await?;
    Ok(Redirect::to("/").into_response())
}

fn render_setup_page(config_dir: &Path, draft: &SetupDraft, error: Option<&str>) -> Markup {
    setup_shell(
        "BeaverKi Setup",
        html! {
            main class="setup-page" {
                section class="setup-hero" {
                    div {
                        p class="eyebrow" { "First run" }
                        h1 { "Set up BeaverKi" }
                        p class="lede" {
                            "Create the local configuration, choose a model provider, and start the daemon."
                        }
                    }
                    div class="setup-meta" {
                        span { "Loopback only" }
                        code { (config_dir.display()) }
                    }
                }
                @if let Some(message) = error {
                    div class="error" { (message) }
                }
                form method="post" action="/setup/complete" class="setup-form" {
                    section class="panel" {
                        h2 { "Owner" }
                        div class="field-grid" {
                            label {
                                span { "Owner display name" }
                                input type="text" name="owner_display_name" value=(&draft.owner_display_name) required;
                            }
                            label {
                                span { "Instance ID" }
                                input type="text" name="instance_id" value=(&draft.instance_id) required;
                            }
                        }
                    }
                    section class="panel" {
                        div class="section-title" {
                            h2 { "Local paths" }
                            label class="checkline" {
                                input class="advanced-toggle" type="checkbox" name="advanced_paths" checked[draft.advanced_paths];
                                span { "Advanced" }
                            }
                        }
                        div class="field-grid" {
                            label {
                                span { "Workspace root" }
                                input type="text" name="workspace_root" value=(draft.workspace_root.display()) required;
                            }
                        }
                        div class="field-grid advanced-path-fields" {
                            label {
                                span { "Data directory" }
                                input type="text" name="data_dir" value=(draft.data_dir.display()) required;
                            }
                            label {
                                span { "State directory" }
                                input type="text" name="state_dir" value=(draft.state_dir.display()) required;
                            }
                            label {
                                span { "Log directory" }
                                input type="text" name="log_dir" value=(draft.log_dir.display()) required;
                            }
                            label {
                                span { "Secret directory" }
                                input type="text" name="secret_dir" value=(draft.secret_dir.display()) required;
                            }
                        }
                    }
                    section class="panel" {
                        h2 { "Secret passphrase" }
                        div class="field-grid" {
                            label {
                                span { "Master passphrase" }
                                input type="password" name="master_passphrase" autocomplete="new-password" required;
                            }
                            label {
                                span { "Confirm passphrase" }
                                input type="password" name="master_passphrase_confirm" autocomplete="new-password" required;
                            }
                        }
                    }
                    section class="panel" {
                        h2 { "Provider" }
                        div class="choice-row" {
                            label class="choice" {
                                input type="radio" name="provider" value="openai" checked[draft.provider == SetupProviderKind::OpenAi];
                                span { "OpenAI" }
                            }
                            label class="choice" {
                                input type="radio" name="provider" value="lm_studio" checked[draft.provider == SetupProviderKind::LmStudio];
                                span { "LM Studio" }
                            }
                        }
                        div class="provider-grid" {
                            div class="provider-panel" {
                                h3 { "OpenAI" }
                                label {
                                    span { "API token" }
                                    input type="password" name="openai_api_token" autocomplete="off";
                                }
                                (model_inputs(
                                    "openai",
                                    &draft.openai_planner_model,
                                    &draft.openai_executor_model,
                                    &draft.openai_summarizer_model,
                                    &draft.openai_safety_review_model,
                                ))
                            }
                            div class="provider-panel" {
                                h3 { "LM Studio" }
                                label {
                                    span { "Base URL" }
                                    input type="text" name="lm_studio_base_url" value=(&draft.lm_studio_base_url) required;
                                }
                                label class="checkline spaced" {
                                    input type="checkbox" name="lm_studio_tool_calling" checked[draft.lm_studio_tool_calling];
                                    span { "Enable tool calling" }
                                }
                                (model_inputs(
                                    "lm_studio",
                                    &draft.lm_studio_planner_model,
                                    &draft.lm_studio_executor_model,
                                    &draft.lm_studio_summarizer_model,
                                    &draft.lm_studio_safety_review_model,
                                ))
                            }
                        }
                    }
                    div class="actions" {
                        button type="submit" formaction="/setup/save" class="secondary" { "Save progress" }
                        button type="submit" { "Finish setup" }
                    }
                }
            }
        },
    )
}

fn model_inputs(
    prefix: &str,
    planner: &str,
    executor: &str,
    summarizer: &str,
    safety_review: &str,
) -> Markup {
    html! {
        div class="field-grid compact" {
            label {
                span { "Planner model" }
                input type="text" name=(format!("{prefix}_planner_model")) value=(planner) required;
            }
            label {
                span { "Executor model" }
                input type="text" name=(format!("{prefix}_executor_model")) value=(executor) required;
            }
            label {
                span { "Summarizer model" }
                input type="text" name=(format!("{prefix}_summarizer_model")) value=(summarizer) required;
            }
            label {
                span { "Safety review model" }
                input type="text" name=(format!("{prefix}_safety_review_model")) value=(safety_review) required;
            }
        }
    }
}

fn setup_shell(title: &str, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                style { (PreEscaped(SETUP_CSS)) }
            }
            body { (body) }
        }
    }
}

fn load_setup_draft(config_dir: &Path) -> Result<SetupDraft> {
    let path = draft_path(config_dir);
    if !path.exists() {
        return Ok(SetupDraft::default());
    }
    let text =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))
}

fn write_setup_draft(config_dir: &Path, draft: &SetupDraft) -> Result<()> {
    fs::create_dir_all(config_dir)
        .with_context(|| format!("failed to create {}", config_dir.display()))?;
    let path = draft_path(config_dir);
    fs::write(&path, serde_yaml::to_string(draft)?)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn remove_setup_draft(config_dir: &Path) -> Result<()> {
    let path = draft_path(config_dir);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn draft_path(config_dir: &Path) -> PathBuf {
    config_dir.join("onboarding-draft.yaml")
}

fn draft_from_form(form: &SetupForm) -> Result<SetupDraft> {
    Ok(SetupDraft {
        instance_id: required_text(&form.instance_id, "instance ID")?,
        owner_display_name: form.owner_display_name.trim().to_owned(),
        workspace_root: path_from_input(&form.workspace_root, "workspace root")?,
        data_dir: path_from_input(&form.data_dir, "data dir")?,
        state_dir: path_from_input(&form.state_dir, "state dir")?,
        log_dir: path_from_input(&form.log_dir, "log dir")?,
        secret_dir: path_from_input(&form.secret_dir, "secret dir")?,
        provider: SetupProviderKind::parse(&form.provider)?,
        openai_planner_model: required_text(&form.openai_planner_model, "planner model")?,
        openai_executor_model: required_text(&form.openai_executor_model, "executor model")?,
        openai_summarizer_model: required_text(&form.openai_summarizer_model, "summarizer model")?,
        openai_safety_review_model: required_text(
            &form.openai_safety_review_model,
            "safety review model",
        )?,
        lm_studio_base_url: normalize_provider_base_url(&form.lm_studio_base_url)?,
        lm_studio_planner_model: required_text(&form.lm_studio_planner_model, "planner model")?,
        lm_studio_executor_model: required_text(&form.lm_studio_executor_model, "executor model")?,
        lm_studio_summarizer_model: required_text(
            &form.lm_studio_summarizer_model,
            "summarizer model",
        )?,
        lm_studio_safety_review_model: required_text(
            &form.lm_studio_safety_review_model,
            "safety review model",
        )?,
        lm_studio_tool_calling: form.lm_studio_tool_calling.is_some(),
        advanced_paths: form.advanced_paths.is_some(),
    })
}

async fn verify_provider_entry(
    provider: &beaverki_config::ProviderEntry,
    openai_api_token: Option<&str>,
) -> Result<()> {
    match provider.kind.as_str() {
        "openai" => {
            let api_token =
                openai_api_token.ok_or_else(|| anyhow!("OpenAI setup requires an API token"))?;
            OpenAiProvider::from_entry(provider, api_token.to_owned())?
                .verify_configuration()
                .await
        }
        "lm_studio" => {
            LmStudioProvider::from_entry(provider)?
                .verify_configuration()
                .await
        }
        other => bail!("unsupported provider kind '{}'", other),
    }
}

async fn start_daemon_for_setup(
    state: &AppState,
    default_config_dir: &Path,
    config_dir: &Path,
    passphrase: &str,
) -> Result<()> {
    let config = LoadedConfig::load_from_dir(config_dir)?;
    let socket_path = config.runtime.state_dir.join("daemon.sock");
    state.set_daemon_socket_path(socket_path.clone()).await;
    if DaemonClient::is_reachable(socket_path.clone()).await {
        return Ok(());
    }

    if let Some(cli_path) = sibling_cli_path()? {
        spawn_cli_daemon(&cli_path, &config, config_dir, passphrase)?;
        DaemonClient::wait_until_ready(socket_path, Duration::from_secs(10)).await?;
        return Ok(());
    }

    let config_dir_for_task = config_dir.to_path_buf();
    let passphrase_for_task = passphrase.to_owned();
    tokio::spawn(async move {
        match Runtime::load(&config_dir_for_task, &passphrase_for_task).await {
            Ok(runtime) => {
                let daemon = RuntimeDaemon::new(runtime)
                    .with_config_access(config_dir_for_task, passphrase_for_task);
                if let Err(error) = daemon.run_until(future::pending::<()>()).await {
                    eprintln!("BeaverKi setup daemon exited: {error:#}");
                }
            }
            Err(error) => eprintln!("failed to load BeaverKi runtime after setup: {error:#}"),
        }
    });

    if default_config_dir != config_dir {
        eprintln!(
            "BeaverKi setup used {}; no sibling beaverki-cli binary was found, so the daemon is attached to this web process.",
            config_dir.display()
        );
    }
    DaemonClient::wait_until_ready(socket_path, Duration::from_secs(10)).await?;
    Ok(())
}

fn sibling_cli_path() -> Result<Option<PathBuf>> {
    let current_exe = std::env::current_exe().context("failed to locate current executable")?;
    let Some(parent) = current_exe.parent() else {
        return Ok(None);
    };
    let binary_name = if cfg!(windows) {
        "beaverki-cli.exe"
    } else {
        "beaverki-cli"
    };
    let path = parent.join(binary_name);
    Ok(path.exists().then_some(path))
}

fn spawn_cli_daemon(
    cli_path: &Path,
    config: &LoadedConfig,
    config_dir: &Path,
    passphrase: &str,
) -> Result<()> {
    fs::create_dir_all(&config.runtime.log_dir)
        .with_context(|| format!("failed to create {}", config.runtime.log_dir.display()))?;
    let log_path = config.runtime.log_dir.join("daemon.log");
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let log_file_err = log_file
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;
    Command::new(cli_path)
        .arg("daemon")
        .arg("serve")
        .arg("--config-dir")
        .arg(config_dir)
        .arg("--passphrase-env")
        .arg("BEAVERKI_MASTER_PASSPHRASE")
        .env("BEAVERKI_MASTER_PASSPHRASE", passphrase)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .spawn()
        .with_context(|| format!("failed to spawn {}", cli_path.display()))?;
    Ok(())
}

fn required_text(value: &str, label: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{label} cannot be empty");
    }
    Ok(trimmed.to_owned())
}

fn required_secret(value: Option<&str>, label: &str) -> Result<String> {
    required_text(value.unwrap_or_default(), label)
}

fn path_from_input(value: &str, label: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_text(value, label)?))
}

const SETUP_CSS: &str = r#"
:root {
  --bg: #f6f8fb;
  --panel: #ffffff;
  --ink: #07111f;
  --muted: #5d6b7d;
  --accent: #0a5bff;
  --line: #dbe5ef;
  --danger: #b42318;
  --radius: 8px;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  background: var(--bg);
  color: var(--ink);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  line-height: 1.5;
}
.setup-page { max-width: 1040px; margin: 0 auto; padding: 32px 20px 56px; }
.setup-hero {
  display: flex;
  justify-content: space-between;
  gap: 24px;
  align-items: flex-end;
  padding-bottom: 22px;
  border-bottom: 1px solid var(--line);
  margin-bottom: 22px;
}
.eyebrow { margin: 0 0 6px; text-transform: uppercase; letter-spacing: .08em; font-size: .78rem; color: var(--accent); font-weight: 800; }
h1 { margin: 0; font-size: clamp(2rem, 5vw, 3.8rem); line-height: 1; letter-spacing: 0; }
.lede { margin: 12px 0 0; color: var(--muted); max-width: 620px; font-size: 1.05rem; }
.setup-meta { display: grid; gap: 8px; justify-items: end; color: var(--muted); font-size: .9rem; }
code { max-width: min(420px, 80vw); overflow-wrap: anywhere; color: var(--ink); background: #eef4ff; padding: 5px 8px; border-radius: 6px; }
.setup-form { display: grid; gap: 16px; }
.panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  padding: 18px;
  box-shadow: 0 10px 24px rgba(3,14,31,.05);
}
h2 { margin: 0 0 14px; font-size: 1.05rem; }
h3 { margin: 0 0 12px; font-size: .95rem; }
.section-title { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 12px; }
.section-title h2 { margin: 0; }
.field-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
.field-grid.compact { grid-template-columns: 1fr; gap: 10px; }
.panel:has(.advanced-toggle:not(:checked)) .advanced-path-fields { display: none; }
label { display: grid; gap: 6px; color: var(--muted); font-size: .88rem; font-weight: 700; }
input[type="text"], input[type="password"] {
  width: 100%;
  min-height: 40px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 8px 10px;
  color: var(--ink);
  font: inherit;
  background: #fff;
}
.choice-row, .actions { display: flex; gap: 10px; flex-wrap: wrap; }
.choice, .checkline { display: inline-flex; grid-template-columns: none; align-items: center; gap: 8px; color: var(--ink); }
.spaced { margin: 10px 0 14px; }
.provider-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; margin-top: 14px; }
.provider-panel { border: 1px solid var(--line); border-radius: var(--radius); padding: 14px; background: #f9fbfd; }
button {
  border: 0;
  border-radius: 6px;
  background: var(--accent);
  color: white;
  min-height: 42px;
  padding: 0 16px;
  font: inherit;
  font-weight: 800;
  cursor: pointer;
}
button.secondary { background: #e8eef7; color: var(--ink); }
.error { border: 1px solid #f4b4ad; background: #fff4f2; color: var(--danger); border-radius: var(--radius); padding: 12px 14px; margin-bottom: 14px; font-weight: 700; }
@media (max-width: 760px) {
  .setup-hero, .field-grid, .provider-grid { grid-template-columns: 1fr; display: grid; }
  .setup-meta { justify-items: start; }
}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use beaverki_config::{
        ProviderAuth, ProviderCapabilities, ProviderEntry, ProviderModels, SetupAnswers,
        write_setup_files,
    };

    #[test]
    fn draft_does_not_store_secrets() {
        let form = SetupForm {
            instance_id: "beaverki-local".to_owned(),
            owner_display_name: "Alex".to_owned(),
            workspace_root: "/tmp/workspace".to_owned(),
            data_dir: "/tmp/data".to_owned(),
            state_dir: "/tmp/state".to_owned(),
            log_dir: "/tmp/state/logs".to_owned(),
            secret_dir: "/tmp/state/secrets".to_owned(),
            provider: "openai".to_owned(),
            openai_api_token: Some("sk-secret".to_owned()),
            master_passphrase: Some("passphrase".to_owned()),
            master_passphrase_confirm: Some("passphrase".to_owned()),
            openai_planner_model: DEFAULT_OPENAI_PLANNER_MODEL.to_owned(),
            openai_executor_model: DEFAULT_OPENAI_EXECUTOR_MODEL.to_owned(),
            openai_summarizer_model: DEFAULT_OPENAI_SUMMARIZER_MODEL.to_owned(),
            openai_safety_review_model: DEFAULT_OPENAI_SAFETY_REVIEW_MODEL.to_owned(),
            lm_studio_base_url: DEFAULT_LM_STUDIO_BASE_URL.to_owned(),
            lm_studio_planner_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_executor_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_summarizer_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_safety_review_model: DEFAULT_LM_STUDIO_MODEL.to_owned(),
            lm_studio_tool_calling: None,
            advanced_paths: None,
        };

        let draft = draft_from_form(&form).expect("draft");
        let serialized = serde_yaml::to_string(&draft).expect("serialize");
        assert!(!serialized.contains("sk-secret"));
        assert!(!serialized.contains("passphrase"));
    }

    #[tokio::test]
    async fn setup_required_detects_fresh_and_configured_state() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config_dir = tempdir.path().join("config");
        assert!(setup_required(&config_dir).await.expect("fresh"));

        let state_dir = tempdir.path().join("state");
        let database_path = state_dir.join("runtime.db");
        write_setup_files(&SetupAnswers {
            config_dir: config_dir.clone(),
            instance_id: "test".to_owned(),
            owner_display_name: "Alex".to_owned(),
            data_dir: tempdir.path().join("data"),
            state_dir: state_dir.clone(),
            log_dir: state_dir.join("logs"),
            secret_dir: state_dir.join("secrets"),
            database_path: database_path.clone(),
            workspace_root: tempdir.path().join("workspace"),
            provider: ProviderEntry {
                provider_id: "lm_studio_local".to_owned(),
                kind: "lm_studio".to_owned(),
                base_url: Some("http://127.0.0.1:1234".to_owned()),
                auth: ProviderAuth {
                    mode: "none".to_owned(),
                    secret_ref: None,
                },
                models: ProviderModels {
                    planner: "local".to_owned(),
                    executor: "local".to_owned(),
                    summarizer: "local".to_owned(),
                    safety_review: "local".to_owned(),
                },
                capabilities: ProviderCapabilities {
                    tool_calling: false,
                },
            },
            provider_secret_value: None,
            master_passphrase: None,
        })
        .expect("write setup");
        assert!(setup_required(&config_dir).await.expect("no user"));

        let db = Database::connect(&database_path).await.expect("db");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");
        assert!(!setup_required(&config_dir).await.expect("configured"));
    }
}
