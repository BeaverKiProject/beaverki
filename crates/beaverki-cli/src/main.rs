use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use beaverki_config::{
    LoadedConfig, SetupAnswers, default_app_paths, prompt_passphrase_from_env,
    write_integrations_config, write_providers_config, write_setup_files,
};
use beaverki_core::{MemoryKind, MemoryScope};
use beaverki_db::{Database, MemoryRow, UserRow};
use beaverki_models::OpenAiProvider;
use beaverki_policy::{is_builtin_role, visible_memory_scopes};
use beaverki_runtime::{DaemonClient, Runtime, RuntimeDaemon, latest_daemon_status};
use clap::{Args, Parser, Subcommand};
use dialoguer::{Input, Password};
use tokio::time::{self, Duration};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "beaverki")]
#[command(about = "BeaverKI M1 CLI runtime")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Automation {
        #[command(subcommand)]
        command: Box<AutomationCommand>,
    },
    Connector {
        #[command(subcommand)]
        command: Box<ConnectorCommand>,
    },
    Daemon {
        #[command(subcommand)]
        command: Box<DaemonCommand>,
    },
    Setup {
        #[command(subcommand)]
        command: Box<SetupCommand>,
    },
    Task {
        #[command(subcommand)]
        command: Box<TaskCommand>,
    },
    Memory {
        #[command(subcommand)]
        command: Box<MemoryCommand>,
    },
    User {
        #[command(subcommand)]
        command: Box<UserCommand>,
    },
    Approval {
        #[command(subcommand)]
        command: Box<ApprovalCommand>,
    },
    Role {
        #[command(subcommand)]
        command: Box<RoleCommand>,
    },
}

#[derive(Subcommand)]
enum AutomationCommand {
    Script {
        #[command(subcommand)]
        command: Box<ScriptCommand>,
    },
    Schedule {
        #[command(subcommand)]
        command: Box<ScheduleCommand>,
    },
}

#[derive(Subcommand)]
enum ScriptCommand {
    Create(ScriptCreateArgs),
    List(UserConfigArgs),
    Show(ScriptShowArgs),
    Review(ScriptReviewArgs),
    Activate(ScriptActionArgs),
    Disable(ScriptActionArgs),
}

#[derive(Subcommand)]
enum ScheduleCommand {
    Add(ScheduleAddArgs),
    List(UserConfigArgs),
    Enable(ScheduleToggleArgs),
    Disable(ScheduleToggleArgs),
}

#[derive(Subcommand)]
enum ConnectorCommand {
    Discord {
        #[command(subcommand)]
        command: Box<DiscordCommand>,
    },
}

#[derive(Subcommand)]
enum DiscordCommand {
    Show(ConfigDirArgs),
    Configure(DiscordConfigureArgs),
    MapUser(DiscordMapUserArgs),
    ListMappings(ConfigDirArgs),
}

#[derive(Subcommand)]
enum SetupCommand {
    Init(Box<SetupInitArgs>),
    VerifyOpenai(VerifyOpenAiArgs),
    ShowModels(ConfigDirArgs),
    SetModels(Box<SetModelsArgs>),
}

#[derive(Subcommand)]
enum DaemonCommand {
    Start(DaemonStartArgs),
    Run(DaemonRunArgs),
    Status(ConfigDirArgs),
    Stop(ConfigDirArgs),
    #[command(hide = true)]
    Serve(DaemonServeArgs),
}

#[derive(Subcommand)]
enum TaskCommand {
    Run(TaskRunArgs),
    Show(TaskShowArgs),
}

#[derive(Subcommand)]
enum MemoryCommand {
    List(MemoryListArgs),
    Show(MemoryShowArgs),
    History(MemoryHistoryArgs),
    Forget(MemoryForgetArgs),
}

#[derive(Subcommand)]
enum UserCommand {
    Add(UserAddArgs),
    List(ConfigDirArgs),
}

#[derive(Subcommand)]
enum ApprovalCommand {
    List(ApprovalListArgs),
    Approve(ApprovalResolveArgs),
    Deny(ApprovalResolveArgs),
}

#[derive(Subcommand)]
enum RoleCommand {
    List(ConfigDirArgs),
}

#[derive(Args, Clone)]
struct SetupInitArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    instance_id: Option<String>,
    #[arg(long)]
    owner_name: Option<String>,
    #[arg(long)]
    workspace_root: Option<PathBuf>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long)]
    log_dir: Option<PathBuf>,
    #[arg(long)]
    secret_dir: Option<PathBuf>,
    #[arg(long, default_value = "gpt-5.4")]
    planner_model: String,
    #[arg(long, default_value = "gpt-5.4-mini")]
    executor_model: String,
    #[arg(long, default_value = "gpt-5.4-mini")]
    summarizer_model: String,
    #[arg(long, default_value = "gpt-5.4-mini")]
    safety_review_model: String,
    #[arg(long, default_value = "OPENAI_API_KEY")]
    openai_api_token_env: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
    #[arg(long, default_value_t = false)]
    skip_openai_check: bool,
}

#[derive(Args, Clone)]
struct TaskRunArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    objective: String,
    #[arg(long, default_value = "private")]
    scope: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct TaskShowArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    task_id: String,
}

#[derive(Args, Clone)]
struct MemoryListArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    scope: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long, default_value_t = false)]
    include_superseded: bool,
    #[arg(long, default_value_t = false)]
    include_forgotten: bool,
    #[arg(long, default_value_t = 20)]
    limit: i64,
}

#[derive(Args, Clone)]
struct MemoryShowArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    memory_id: String,
}

#[derive(Args, Clone)]
struct MemoryHistoryArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    subject_key: String,
    #[arg(long)]
    scope: Option<String>,
    #[arg(long)]
    subject_type: Option<String>,
    #[arg(long, default_value_t = 20)]
    limit: i64,
}

#[derive(Args, Clone)]
struct MemoryForgetArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    memory_id: String,
    #[arg(long)]
    reason: String,
}

#[derive(Args, Clone)]
struct UserAddArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    display_name: String,
    #[arg(long = "role", required = true)]
    roles: Vec<String>,
}

#[derive(Args, Clone)]
struct ApprovalListArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    status: Option<String>,
}

#[derive(Args, Clone)]
struct ApprovalResolveArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    approval_id: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct VerifyOpenAiArgs {
    #[arg(long, default_value = "OPENAI_API_KEY")]
    openai_api_token_env: String,
}

#[derive(Args, Clone)]
struct ConfigDirArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
}

#[derive(Args, Clone)]
struct UserConfigArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
}

#[derive(Args, Clone)]
struct SetModelsArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    planner_model: Option<String>,
    #[arg(long)]
    executor_model: Option<String>,
    #[arg(long)]
    summarizer_model: Option<String>,
    #[arg(long)]
    safety_review_model: Option<String>,
    #[arg(long, default_value = "OPENAI_API_KEY")]
    openai_api_token_env: String,
    #[arg(long, default_value_t = false)]
    skip_openai_check: bool,
}

#[derive(Args, Clone)]
struct DiscordConfigureArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    enable: bool,
    #[arg(long, default_value_t = false)]
    disable: bool,
    #[arg(long)]
    command_prefix: Option<String>,
    #[arg(long = "allow-channel")]
    allowed_channel_ids: Vec<String>,
    #[arg(long, default_value = "DISCORD_BOT_TOKEN")]
    discord_token_env: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct DiscordMapUserArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    external_user_id: String,
    #[arg(long)]
    mapped_user_id: String,
    #[arg(long)]
    external_channel_id: Option<String>,
    #[arg(long, default_value = "authenticated_message")]
    trust_level: String,
}

#[derive(Args, Clone)]
struct ScriptCreateArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    script_id: Option<String>,
    #[arg(long)]
    source_file: Option<PathBuf>,
    #[arg(long)]
    source_text: Option<String>,
    #[arg(long)]
    capability_profile_file: Option<PathBuf>,
    #[arg(long)]
    summary: String,
    #[arg(long)]
    created_from_task_id: Option<String>,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct ScriptShowArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    script_id: String,
}

#[derive(Args, Clone)]
struct ScriptReviewArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    script_id: String,
    #[arg(long)]
    summary: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct ScriptActionArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    script_id: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct ScheduleAddArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    schedule_id: Option<String>,
    #[arg(long)]
    script_id: String,
    #[arg(long)]
    cron: String,
    #[arg(long, default_value_t = true)]
    enabled: bool,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct ScheduleToggleArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    schedule_id: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct DaemonStartArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
    #[arg(long, default_value_t = 10)]
    startup_timeout_secs: u64,
}

#[derive(Args, Clone)]
struct DaemonRunArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct DaemonServeArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Automation { command } => match *command {
            AutomationCommand::Script { command } => match *command {
                ScriptCommand::Create(args) => script_create(args).await,
                ScriptCommand::List(args) => script_list(args).await,
                ScriptCommand::Show(args) => script_show(args).await,
                ScriptCommand::Review(args) => script_review(args).await,
                ScriptCommand::Activate(args) => script_activate(args).await,
                ScriptCommand::Disable(args) => script_disable(args).await,
            },
            AutomationCommand::Schedule { command } => match *command {
                ScheduleCommand::Add(args) => schedule_add(args).await,
                ScheduleCommand::List(args) => schedule_list(args).await,
                ScheduleCommand::Enable(args) => schedule_toggle(args, true).await,
                ScheduleCommand::Disable(args) => schedule_toggle(args, false).await,
            },
        },
        Commands::Connector { command } => match *command {
            ConnectorCommand::Discord { command } => match *command {
                DiscordCommand::Show(args) => discord_show(args).await,
                DiscordCommand::Configure(args) => discord_configure(args).await,
                DiscordCommand::MapUser(args) => discord_map_user(args).await,
                DiscordCommand::ListMappings(args) => discord_list_mappings(args).await,
            },
        },
        Commands::Daemon { command } => match *command {
            DaemonCommand::Start(args) => daemon_start(args).await,
            DaemonCommand::Run(args) => daemon_run(args).await,
            DaemonCommand::Status(args) => daemon_status(args).await,
            DaemonCommand::Stop(args) => daemon_stop(args).await,
            DaemonCommand::Serve(args) => daemon_serve(args).await,
        },
        Commands::Setup { command } => match *command {
            SetupCommand::Init(args) => setup_init(*args).await,
            SetupCommand::VerifyOpenai(args) => verify_openai(args).await,
            SetupCommand::ShowModels(args) => show_models(args).await,
            SetupCommand::SetModels(args) => set_models(*args).await,
        },
        Commands::Task { command } => match *command {
            TaskCommand::Run(args) => task_run(args).await,
            TaskCommand::Show(args) => task_show(args).await,
        },
        Commands::Memory { command } => match *command {
            MemoryCommand::List(args) => memory_list(args).await,
            MemoryCommand::Show(args) => memory_show(args).await,
            MemoryCommand::History(args) => memory_history(args).await,
            MemoryCommand::Forget(args) => memory_forget(args).await,
        },
        Commands::User { command } => match *command {
            UserCommand::Add(args) => user_add(args).await,
            UserCommand::List(args) => user_list(args).await,
        },
        Commands::Approval { command } => match *command {
            ApprovalCommand::List(args) => approval_list(args).await,
            ApprovalCommand::Approve(args) => approval_resolve(args, true).await,
            ApprovalCommand::Deny(args) => approval_resolve(args, false).await,
        },
        Commands::Role { command } => match *command {
            RoleCommand::List(args) => role_list(args).await,
        },
    }
}

async fn setup_init(args: SetupInitArgs) -> Result<()> {
    let app_paths = default_app_paths()?;
    let config_dir = args
        .config_dir
        .unwrap_or_else(|| app_paths.config_dir.clone());
    let instance_id = match args.instance_id {
        Some(value) => value,
        None => Input::new()
            .with_prompt("Instance ID")
            .default("beaverki-local".to_owned())
            .interact_text()
            .context("failed to read instance ID")?,
    };
    let owner_name = match args.owner_name {
        Some(value) => value,
        None => Input::new()
            .with_prompt("Owner display name")
            .interact_text()
            .context("failed to read owner display name")?,
    };
    let workspace_root = match args.workspace_root {
        Some(path) => path,
        None => PathBuf::from(
            Input::<String>::new()
                .with_prompt("Workspace root")
                .default(default_workspace_root()?.display().to_string())
                .interact_text()
                .context("failed to read workspace root")?,
        ),
    };
    let data_dir = args.data_dir.unwrap_or_else(|| app_paths.data_dir.clone());
    let state_dir = args
        .state_dir
        .unwrap_or_else(|| app_paths.state_dir.clone());
    let log_dir = args.log_dir.unwrap_or_else(|| app_paths.log_dir.clone());
    let secret_dir = args
        .secret_dir
        .unwrap_or_else(|| app_paths.secret_dir.clone());
    let database_path = state_dir.join("runtime.db");
    let api_token = std::env::var(&args.openai_api_token_env).unwrap_or_else(|_| {
        Password::new()
            .with_prompt("OpenAI API token")
            .interact()
            .expect("failed to read OpenAI API token")
    });
    let master_passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase for encrypted local secrets")
            .with_confirmation("Confirm master passphrase", "passphrases do not match")
            .interact()
            .expect("failed to read master passphrase")
    });

    if !args.skip_openai_check {
        verify_openai_api_token(
            &api_token,
            &args.planner_model,
            &args.executor_model,
            &args.summarizer_model,
            &args.safety_review_model,
        )
        .await?;
    }

    let answers = SetupAnswers {
        config_dir: config_dir.clone(),
        instance_id,
        owner_display_name: owner_name.clone(),
        data_dir: data_dir.clone(),
        state_dir: state_dir.clone(),
        log_dir: log_dir.clone(),
        secret_dir: secret_dir.clone(),
        database_path: database_path.clone(),
        workspace_root,
        planner_model: args.planner_model,
        executor_model: args.executor_model,
        summarizer_model: args.summarizer_model,
        safety_review_model: args.safety_review_model,
        openai_api_token: api_token,
        master_passphrase,
    };

    let artifacts = write_setup_files(&answers)?;
    let db = Database::connect(&database_path).await?;
    let bootstrap = db.bootstrap_single_user(&owner_name).await?;

    println!("BeaverKI setup complete.");
    println!("Runtime config: {}", artifacts.runtime_path.display());
    println!("Providers config: {}", artifacts.providers_path.display());
    println!(
        "Integrations config: {}",
        artifacts.integrations_path.display()
    );
    println!("Encrypted secret ref: {}", artifacts.secret_ref);
    println!("Data dir: {}", data_dir.display());
    println!("State dir: {}", state_dir.display());
    println!("Log dir: {}", log_dir.display());
    println!("Secret dir: {}", secret_dir.display());
    println!("User ID: {}", bootstrap.user_id);
    println!("Primary agent ID: {}", bootstrap.primary_agent_id);
    println!("Assigned role: owner");

    Ok(())
}

async fn daemon_start(args: DaemonStartArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let config = LoadedConfig::load_from_dir(&config_dir)?;
    let socket_path = config.runtime.state_dir.join("daemon.sock");
    if DaemonClient::is_reachable(socket_path.clone()).await {
        println!("Daemon already running at {}", socket_path.display());
        return Ok(());
    }

    std::fs::create_dir_all(&config.runtime.log_dir)
        .with_context(|| format!("failed to create {}", config.runtime.log_dir.display()))?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let log_path = config.runtime.log_dir.join("daemon.log");
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let log_file_err = log_file
        .try_clone()
        .with_context(|| format!("failed to clone {}", log_path.display()))?;

    let current_exe = std::env::current_exe().context("failed to determine current executable")?;
    let mut child = std::process::Command::new(current_exe);
    child
        .arg("daemon")
        .arg("serve")
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--passphrase-env")
        .arg(&args.passphrase_env)
        .env(&args.passphrase_env, passphrase)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err));

    child.spawn().context("failed to spawn daemon process")?;

    let _client = DaemonClient::wait_until_ready(
        socket_path.clone(),
        Duration::from_secs(args.startup_timeout_secs),
    )
    .await?;
    println!("Daemon started.");
    println!("Socket: {}", socket_path.display());
    println!("Log: {}", log_path.display());
    Ok(())
}

async fn daemon_run(args: DaemonRunArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let daemon = RuntimeDaemon::new(runtime);
    println!("Daemon listening on {}", daemon.socket_path().display());
    daemon
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}

async fn daemon_serve(args: DaemonServeArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env)
        .with_context(|| format!("missing environment variable {}", args.passphrase_env))?;
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    RuntimeDaemon::new(runtime)
        .run_until(std::future::pending::<()>())
        .await
}

async fn daemon_status(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let config = LoadedConfig::load_from_dir(&config_dir)?;
    let client = DaemonClient::new(config.runtime.state_dir.join("daemon.sock"));

    if let Ok(status) = client.status().await {
        print_daemon_status(&status, true);
        return Ok(());
    }

    if let Some(status) = latest_daemon_status(&config_dir).await? {
        print_daemon_status(&status, false);
        return Ok(());
    }

    println!("Daemon is not running.");
    Ok(())
}

async fn daemon_stop(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let config = LoadedConfig::load_from_dir(&config_dir)?;
    let socket_path = config.runtime.state_dir.join("daemon.sock");
    let client = DaemonClient::new(socket_path.clone());
    let status = client
        .shutdown()
        .await
        .with_context(|| format!("daemon not reachable at {}", socket_path.display()))?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if !DaemonClient::is_reachable(socket_path.clone()).await {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("daemon did not stop within the timeout");
        }
        time::sleep(Duration::from_millis(150)).await;
    }

    println!("Daemon stop requested for session {}.", status.session_id);
    Ok(())
}

async fn task_run(args: TaskRunArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let client = require_daemon_client(&config_dir).await?;
    let task = client
        .run_task(args.user, args.objective, args.scope, true)
        .await?;

    println!("Task ID: {}", task.task_id);
    println!("State: {}", task.state);
    if let Some(result_text) = task.result_text {
        println!("\n{result_text}");
    }

    Ok(())
}

async fn task_show(args: TaskShowArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let inspection = if let Ok(client) = try_daemon_client(&config_dir).await {
        client
            .show_task(args.user.clone(), args.task_id.clone())
            .await?
    } else {
        let (_, db) = load_db(&config_dir).await?;
        let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
        let task = db
            .fetch_task_for_owner(&user.user_id, &args.task_id)
            .await?
            .with_context(|| format!("task '{}' not found", args.task_id))?;
        let events = db
            .fetch_task_events_for_owner(&user.user_id, &args.task_id)
            .await?;
        let tool_invocations = db
            .fetch_tool_invocations_for_owner(&user.user_id, &args.task_id)
            .await?;
        beaverki_runtime::TaskInspection {
            task,
            events,
            tool_invocations,
        }
    };

    println!("Task: {}", inspection.task.task_id);
    println!("Owner: {}", inspection.task.owner_user_id);
    println!("State: {}", inspection.task.state);
    println!("Kind: {}", inspection.task.kind);
    println!("Objective: {}", inspection.task.objective);
    if let Some(result_text) = inspection.task.result_text {
        println!("Result: {result_text}");
    }

    println!("\nEvents:");
    for event in inspection.events {
        println!(
            "- {} {} {} {}",
            event.created_at, event.actor_type, event.event_type, event.payload_json
        );
    }

    println!("\nTool Invocations:");
    for invocation in inspection.tool_invocations {
        println!(
            "- {} {} {}",
            invocation.started_at, invocation.tool_name, invocation.status
        );
        if let Some(response) = invocation.response_json {
            println!("  response: {response}");
        }
    }

    Ok(())
}

async fn memory_list(args: MemoryListArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let memories = if let Ok(client) = try_daemon_client(&config_dir).await {
        client
            .list_memories(
                args.user.clone(),
                args.scope.clone(),
                args.kind.clone(),
                args.include_superseded,
                args.include_forgotten,
                args.limit,
            )
            .await?
    } else {
        let (_, db) = load_db(&config_dir).await?;
        let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
        visible_memories_for_user(
            &db,
            &user,
            args.scope.as_deref(),
            args.kind.as_deref(),
            args.include_superseded,
            args.include_forgotten,
            None,
            None,
            args.limit,
        )
        .await?
    };

    if memories.is_empty() {
        println!("No memories matched the requested filters.");
        return Ok(());
    }

    for memory in memories {
        print_memory_summary(&memory);
    }

    Ok(())
}

async fn memory_show(args: MemoryShowArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let memory = if let Ok(client) = try_daemon_client(&config_dir).await {
        client
            .show_memory(args.user.clone(), args.memory_id.clone())
            .await?
            .memory
    } else {
        let (_, db) = load_db(&config_dir).await?;
        let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
        visible_memory_by_id(&db, &user, &args.memory_id).await?
    };

    print_memory_detail(&memory);
    Ok(())
}

async fn memory_history(args: MemoryHistoryArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let memories = if let Ok(client) = try_daemon_client(&config_dir).await {
        client
            .memory_history(
                args.user.clone(),
                args.subject_key.clone(),
                args.scope.clone(),
                args.subject_type.clone(),
                args.limit,
            )
            .await?
    } else {
        let (_, db) = load_db(&config_dir).await?;
        let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
        visible_memories_for_user(
            &db,
            &user,
            args.scope.as_deref(),
            None,
            true,
            true,
            args.subject_type.as_deref(),
            Some(&args.subject_key),
            args.limit,
        )
        .await?
    };

    if memories.is_empty() {
        println!(
            "No memory history found for subject key '{}'.",
            args.subject_key
        );
        return Ok(());
    }

    for memory in memories {
        print_memory_summary(&memory);
    }

    Ok(())
}

async fn memory_forget(args: MemoryForgetArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let memory = if let Ok(client) = try_daemon_client(&config_dir).await {
        client
            .forget_memory(
                args.user.clone(),
                args.memory_id.clone(),
                args.reason.clone(),
            )
            .await?
            .memory
    } else {
        let (_, db) = load_db(&config_dir).await?;
        let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
        forget_memory_for_user(&db, &user, &args.memory_id, &args.reason).await?
    };

    println!("Forgot memory {}.", memory.memory_id);
    print_memory_detail(&memory);
    Ok(())
}

async fn user_add(args: UserAddArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    let roles = args.roles;
    for role in &roles {
        if !is_builtin_role(role) {
            bail!("unsupported role '{role}'");
        }
    }

    let bootstrap = db.create_user(&args.display_name, &roles).await?;
    println!("User created.");
    println!("User ID: {}", bootstrap.user_id);
    println!("Primary agent ID: {}", bootstrap.primary_agent_id);
    println!("Roles: {}", roles.join(", "));
    Ok(())
}

async fn user_list(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    let users = db.list_users().await?;

    for user in users {
        let roles = db
            .list_user_roles(&user.user_id)
            .await?
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();
        println!(
            "- {} ({}) roles=[{}] primary_agent={}",
            user.user_id,
            user.display_name,
            roles.join(", "),
            user.primary_agent_id.unwrap_or_else(|| "<none>".to_owned())
        );
    }

    Ok(())
}

async fn approval_list(args: ApprovalListArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let approvals = if let Ok(client) = try_daemon_client(&config_dir).await {
        client
            .list_approvals(args.user.clone(), args.status.clone())
            .await?
    } else {
        let (_, db) = load_db(&config_dir).await?;
        let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
        db.list_approvals_for_user(&user.user_id, args.status.as_deref())
            .await?
    };

    for approval in approvals {
        println!(
            "- {} task={} status={} action={} risk={} summary={} target={}",
            approval.approval_id,
            approval.task_id,
            approval.status,
            approval.action_type,
            approval.risk_level.unwrap_or_else(|| "<none>".to_owned()),
            approval
                .action_summary
                .unwrap_or_else(|| "<none>".to_owned()),
            approval.target_ref.unwrap_or_else(|| "<none>".to_owned())
        );
    }

    Ok(())
}

async fn approval_resolve(args: ApprovalResolveArgs, approve: bool) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let client = require_daemon_client(&config_dir).await?;
    let task = client
        .resolve_approval(args.user, args.approval_id.clone(), approve)
        .await?;

    println!(
        "Approval {} {}. Task {} is now {}.",
        args.approval_id,
        if approve { "approved" } else { "denied" },
        task.task_id,
        task.state
    );
    if let Some(result_text) = task.result_text {
        println!("\n{result_text}");
    }

    Ok(())
}

async fn role_list(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    let roles = db.list_roles().await?;
    for role in roles {
        println!("- {}: {}", role.role_id, role.description);
    }
    Ok(())
}

async fn script_create(args: ScriptCreateArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let source_text = match (args.source_text, args.source_file) {
        (Some(text), None) => text,
        (None, Some(path)) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?,
        (Some(_), Some(_)) => bail!("provide either --source-text or --source-file, not both"),
        (None, None) => bail!("provide one of --source-text or --source-file"),
    };
    let capability_profile = match args.capability_profile_file {
        Some(path) => serde_json::from_str::<serde_json::Value>(
            &std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", path.display()))?,
        None => serde_json::json!({}),
    };
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let inspection = runtime
        .create_lua_script(
            args.user.as_deref(),
            args.script_id.as_deref(),
            &source_text,
            capability_profile,
            args.created_from_task_id.as_deref(),
            &args.summary,
        )
        .await?;

    println!("Script created.");
    println!("Script ID: {}", inspection.script.script_id);
    println!("Status: {}", inspection.script.status);
    println!("Safety status: {}", inspection.script.safety_status);
    if let Some(summary) = inspection.script.safety_summary.as_deref() {
        println!("Safety summary: {summary}");
    }
    Ok(())
}

async fn script_list(args: UserConfigArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
    let scripts = db.list_scripts_for_owner(&user.user_id).await?;
    for script in scripts {
        println!(
            "- {} kind={} status={} safety={}",
            script.script_id, script.kind, script.status, script.safety_status
        );
    }
    Ok(())
}

async fn script_show(args: ScriptShowArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env("BEAVERKI_MASTER_PASSPHRASE");
    if let Some(passphrase) = passphrase {
        let runtime = Runtime::load(&config_dir, &passphrase).await?;
        let inspection = runtime
            .inspect_script(args.user.as_deref(), &args.script_id)
            .await?;
        print_script_inspection(&inspection);
        return Ok(());
    }

    let (_, db) = load_db(&config_dir).await?;
    let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
    let script = db
        .fetch_script_for_owner(&user.user_id, &args.script_id)
        .await?
        .ok_or_else(|| anyhow!("script '{}' not found", args.script_id))?;
    println!("Script ID: {}", script.script_id);
    println!("Status: {}", script.status);
    println!("Safety status: {}", script.safety_status);
    if let Some(summary) = script.safety_summary {
        println!("Safety summary: {summary}");
    }
    println!("\n{}", script.source_text);
    Ok(())
}

async fn script_review(args: ScriptReviewArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let review = runtime
        .review_lua_script(args.user.as_deref(), &args.script_id, &args.summary)
        .await?;
    println!("Script reviewed.");
    println!("Review ID: {}", review.review_id);
    println!("Verdict: {}", review.verdict);
    println!("Risk level: {}", review.risk_level);
    println!("Summary: {}", review.summary_text);
    print_script_review_details(&review);
    Ok(())
}

async fn script_activate(args: ScriptActionArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let script = runtime
        .activate_script(args.user.as_deref(), &args.script_id)
        .await?;
    println!("Script {} is now {}.", script.script_id, script.status);
    Ok(())
}

async fn script_disable(args: ScriptActionArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let script = runtime
        .disable_script(args.user.as_deref(), &args.script_id)
        .await?;
    println!("Script {} is now {}.", script.script_id, script.status);
    Ok(())
}

async fn schedule_add(args: ScheduleAddArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let schedule = runtime
        .create_schedule(
            args.user.as_deref(),
            args.schedule_id.as_deref(),
            &args.script_id,
            &args.cron,
            args.enabled,
        )
        .await?;
    println!("Schedule created.");
    println!("Schedule ID: {}", schedule.schedule_id);
    println!("Target: {}", schedule.target_id);
    println!("Cron: {}", schedule.cron_expr);
    println!(
        "Enabled: {}",
        if schedule.enabled != 0 { "yes" } else { "no" }
    );
    println!("Next run at: {}", schedule.next_run_at);
    Ok(())
}

async fn schedule_list(args: UserConfigArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    let user = resolve_user_for_db(&db, args.user.as_deref()).await?;
    let schedules = db.list_schedules_for_owner(&user.user_id).await?;
    for schedule in schedules {
        println!(
            "- {} target={} enabled={} next_run_at={}",
            schedule.schedule_id,
            schedule.target_id,
            if schedule.enabled != 0 { "yes" } else { "no" },
            schedule.next_run_at
        );
    }
    Ok(())
}

async fn schedule_toggle(args: ScheduleToggleArgs, enabled: bool) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let schedule = runtime
        .set_schedule_enabled(args.user.as_deref(), &args.schedule_id, enabled)
        .await?;
    println!(
        "Schedule {} is now {}. Next run at: {}",
        schedule.schedule_id,
        if schedule.enabled != 0 {
            "enabled"
        } else {
            "disabled"
        },
        schedule.next_run_at
    );
    Ok(())
}

fn print_script_inspection(inspection: &beaverki_runtime::ScriptInspection) {
    println!("Script ID: {}", inspection.script.script_id);
    println!("Status: {}", inspection.script.status);
    println!("Safety status: {}", inspection.script.safety_status);
    if let Some(summary) = inspection.script.safety_summary.as_deref() {
        println!("Safety summary: {summary}");
    }
    if let Some(task_id) = inspection.script.created_from_task_id.as_deref() {
        println!("Created from task: {task_id}");
    }

    println!("\nSource:\n{}", inspection.script.source_text);

    if !inspection.reviews.is_empty() {
        println!("\nReviews:");
        for review in &inspection.reviews {
            println!(
                "- {} verdict={} risk={} summary={}",
                review.review_id, review.verdict, review.risk_level, review.summary_text
            );
            print_script_review_details(review);
        }
    }

    if !inspection.schedules.is_empty() {
        println!("\nSchedules:");
        for schedule in &inspection.schedules {
            println!(
                "- {} cron={} enabled={} next_run_at={}",
                schedule.schedule_id,
                schedule.cron_expr,
                if schedule.enabled != 0 { "yes" } else { "no" },
                schedule.next_run_at
            );
        }
    }
}

fn print_script_review_details(review: &beaverki_db::ScriptReviewRow) {
    let findings_json = match serde_json::from_str::<serde_json::Value>(&review.findings_json) {
        Ok(value) => value,
        Err(_) => return,
    };

    if let Some(findings) = findings_json
        .get("findings")
        .and_then(serde_json::Value::as_array)
        && !findings.is_empty()
    {
        println!("  Findings:");
        for finding in findings.iter().filter_map(serde_json::Value::as_str) {
            println!("    - {finding}");
        }
    }

    if let Some(required_changes) = findings_json
        .get("required_changes")
        .and_then(serde_json::Value::as_array)
        && !required_changes.is_empty()
    {
        println!("  Required changes:");
        for required_change in required_changes
            .iter()
            .filter_map(serde_json::Value::as_str)
        {
            println!("    - {required_change}");
        }
    }
}

async fn verify_openai(args: VerifyOpenAiArgs) -> Result<()> {
    let api_token = std::env::var(&args.openai_api_token_env)
        .with_context(|| format!("missing environment variable {}", args.openai_api_token_env))?;
    verify_openai_api_token(
        &api_token,
        "gpt-5.4",
        "gpt-5.4-mini",
        "gpt-5.4-mini",
        "gpt-5.4-mini",
    )
    .await?;
    println!("OpenAI API key verification succeeded.");
    Ok(())
}

async fn show_models(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let config = LoadedConfig::load_from_dir(&config_dir)?;
    let provider = config.providers.active_provider()?;

    println!("Config dir: {}", config.config_dir.display());
    println!("Active provider: {}", provider.provider_id);
    println!("Provider kind: {}", provider.kind);
    println!("Planner model: {}", provider.models.planner);
    println!("Executor model: {}", provider.models.executor);
    println!("Summarizer model: {}", provider.models.summarizer);
    println!("Safety review model: {}", provider.models.safety_review);

    Ok(())
}

async fn discord_show(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let config = LoadedConfig::load_from_dir(&config_dir)?;
    let discord = &config.integrations.discord;

    println!("Config dir: {}", config.config_dir.display());
    println!("Discord enabled: {}", discord.enabled);
    println!("Command prefix: {}", discord.command_prefix);
    println!(
        "Bot token secret ref: {}",
        discord
            .bot_token_secret_ref
            .as_deref()
            .unwrap_or("<not configured>")
    );
    if discord.allowed_channel_ids.is_empty() {
        println!("Allowed channels: <none>");
    } else {
        println!(
            "Allowed channels: {}",
            discord.allowed_channel_ids.join(", ")
        );
    }
    println!("Task wait timeout: {}s", discord.task_wait_timeout_secs);
    Ok(())
}

async fn discord_configure(args: DiscordConfigureArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    if args.enable && args.disable {
        bail!("--enable and --disable cannot be used together");
    }

    let mut config = LoadedConfig::load_from_dir(&config_dir)?;
    let mut discord = config.integrations.discord.clone();

    if args.enable {
        discord.enabled = true;
    }
    if args.disable {
        discord.enabled = false;
    }
    if let Some(command_prefix) = args.command_prefix {
        let command_prefix = command_prefix.trim();
        if command_prefix.is_empty() {
            bail!("command prefix cannot be empty");
        }
        discord.command_prefix = command_prefix.to_owned();
    }
    if !args.allowed_channel_ids.is_empty() {
        discord.allowed_channel_ids = args.allowed_channel_ids;
    }

    if discord.enabled {
        let secret_ref = discord
            .bot_token_secret_ref
            .clone()
            .unwrap_or_else(|| "secret://local/discord_bot_token".to_owned());
        let token = std::env::var(&args.discord_token_env).unwrap_or_else(|_| {
            Password::new()
                .with_prompt("Discord bot token")
                .interact()
                .expect("failed to read Discord bot token")
        });
        if token.trim().is_empty() {
            bail!("Discord bot token cannot be empty when enabling the connector");
        }

        let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
            Password::new()
                .with_prompt("Master passphrase")
                .interact()
                .expect("failed to read master passphrase")
        });
        let secret_store = beaverki_config::SecretStore::new(&config.runtime.secret_dir);
        secret_store.write_secret(&secret_ref, token.trim(), &passphrase)?;
        discord.bot_token_secret_ref = Some(secret_ref);
    }

    config.integrations.discord = discord.clone();
    let path = write_integrations_config(&config_dir, &config.integrations)?;
    println!("Updated Discord integration config in {}", path.display());
    println!("Discord enabled: {}", discord.enabled);
    println!("Command prefix: {}", discord.command_prefix);
    println!(
        "Allowed channels: {}",
        if discord.allowed_channel_ids.is_empty() {
            "<none>".to_owned()
        } else {
            discord.allowed_channel_ids.join(", ")
        }
    );

    Ok(())
}

async fn discord_map_user(args: DiscordMapUserArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    db.fetch_user(&args.mapped_user_id)
        .await?
        .ok_or_else(|| anyhow!("user '{}' not found", args.mapped_user_id))?;

    let mapping = db
        .upsert_connector_identity(
            "discord",
            &args.external_user_id,
            args.external_channel_id.as_deref(),
            &args.mapped_user_id,
            &args.trust_level,
        )
        .await?;

    println!("Discord mapping updated.");
    println!("Identity ID: {}", mapping.identity_id);
    println!("External user ID: {}", mapping.external_user_id);
    println!("Mapped user ID: {}", mapping.mapped_user_id);
    println!("Trust level: {}", mapping.trust_level);
    Ok(())
}

async fn discord_list_mappings(args: ConfigDirArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let (_, db) = load_db(&config_dir).await?;
    let mappings = db.list_connector_identities(Some("discord")).await?;
    if mappings.is_empty() {
        println!("No Discord mappings configured.");
        return Ok(());
    }

    for mapping in mappings {
        println!(
            "- {} => {} ({})",
            mapping.external_user_id, mapping.mapped_user_id, mapping.trust_level
        );
    }
    Ok(())
}

async fn set_models(args: SetModelsArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let mut config = LoadedConfig::load_from_dir(&config_dir)?;
    let active_id = config.providers.active.clone();
    let provider = config
        .providers
        .entries
        .iter_mut()
        .find(|entry| entry.provider_id == active_id)
        .with_context(|| format!("active provider '{}' not found", active_id))?;

    let planner_model = args
        .planner_model
        .unwrap_or_else(|| provider.models.planner.clone());
    let executor_model = args
        .executor_model
        .unwrap_or_else(|| provider.models.executor.clone());
    let summarizer_model = args
        .summarizer_model
        .unwrap_or_else(|| provider.models.summarizer.clone());
    let safety_review_model = args
        .safety_review_model
        .unwrap_or_else(|| provider.models.safety_review.clone());

    if !args.skip_openai_check {
        let api_token = std::env::var(&args.openai_api_token_env).with_context(|| {
            format!("missing environment variable {}", args.openai_api_token_env)
        })?;
        verify_openai_api_token(
            &api_token,
            &planner_model,
            &executor_model,
            &summarizer_model,
            &safety_review_model,
        )
        .await?;
    }

    provider.models.planner = planner_model;
    provider.models.executor = executor_model;
    provider.models.summarizer = summarizer_model;
    provider.models.safety_review = safety_review_model;
    let planner = provider.models.planner.clone();
    let executor = provider.models.executor.clone();
    let summarizer = provider.models.summarizer.clone();
    let safety_review = provider.models.safety_review.clone();

    let providers_path = write_providers_config(&config_dir, &config.providers)?;
    println!("Updated provider models in {}", providers_path.display());
    println!("Planner model: {}", planner);
    println!("Executor model: {}", executor);
    println!("Summarizer model: {}", summarizer);
    println!("Safety review model: {}", safety_review);

    Ok(())
}

async fn verify_openai_api_token(
    api_token: &str,
    planner_model: &str,
    executor_model: &str,
    summarizer_model: &str,
    safety_review_model: &str,
) -> Result<()> {
    let provider = OpenAiProvider::from_entry(
        &beaverki_config::ProviderEntry {
            provider_id: "openai_main".to_owned(),
            kind: "openai".to_owned(),
            auth: beaverki_config::ProviderAuth {
                mode: "api_token".to_owned(),
                secret_ref: "secret://local/openai_main_api_token".to_owned(),
            },
            models: beaverki_config::ProviderModels {
                planner: planner_model.to_owned(),
                executor: executor_model.to_owned(),
                summarizer: summarizer_model.to_owned(),
                safety_review: safety_review_model.to_owned(),
            },
        },
        api_token.to_owned(),
    )?;
    provider.verify_credentials().await
}

async fn load_db(config_dir: &PathBuf) -> Result<(LoadedConfig, Database)> {
    let config = LoadedConfig::load_from_dir(config_dir)?;
    let db = Database::connect(&config.runtime.database_path).await?;
    Ok((config, db))
}

async fn visible_memories_for_user(
    db: &Database,
    user: &UserRow,
    scope: Option<&str>,
    kind: Option<&str>,
    include_superseded: bool,
    include_forgotten: bool,
    subject_type: Option<&str>,
    subject_key: Option<&str>,
    limit: i64,
) -> Result<Vec<MemoryRow>> {
    let role_ids = db
        .list_user_roles(&user.user_id)
        .await?
        .into_iter()
        .map(|row| row.role_id)
        .collect::<Vec<_>>();
    let scopes = resolve_visible_scope_filter(&role_ids, scope)?;
    let kind = parse_memory_kind_filter(kind)?;
    db.query_memories(
        Some(&user.user_id),
        &scopes,
        kind,
        subject_type,
        subject_key,
        include_superseded,
        include_forgotten,
        limit,
    )
    .await
}

async fn visible_memory_by_id(db: &Database, user: &UserRow, memory_id: &str) -> Result<MemoryRow> {
    let role_ids = db
        .list_user_roles(&user.user_id)
        .await?
        .into_iter()
        .map(|row| row.role_id)
        .collect::<Vec<_>>();
    let visible_scopes = visible_memory_scopes(&role_ids);
    let memory = db
        .fetch_memory(memory_id)
        .await?
        .ok_or_else(|| anyhow!("memory '{memory_id}' not found"))?;
    ensure_memory_visible_to_user(&memory, &user.user_id, &visible_scopes)?;
    Ok(memory)
}

async fn forget_memory_for_user(
    db: &Database,
    user: &UserRow,
    memory_id: &str,
    reason: &str,
) -> Result<MemoryRow> {
    let role_ids = db
        .list_user_roles(&user.user_id)
        .await?
        .into_iter()
        .map(|row| row.role_id)
        .collect::<Vec<_>>();
    let visible_scopes = visible_memory_scopes(&role_ids);
    let memory = db
        .fetch_memory(memory_id)
        .await?
        .ok_or_else(|| anyhow!("memory '{memory_id}' not found"))?;
    ensure_memory_visible_to_user(&memory, &user.user_id, &visible_scopes)?;
    let scope = memory.scope.parse::<MemoryScope>().map_err(|_| {
        anyhow!(
            "memory '{}' has unsupported scope '{}'",
            memory.memory_id,
            memory.scope
        )
    })?;
    if matches!(scope, MemoryScope::Household)
        && !beaverki_policy::can_write_household_memory(&role_ids)
    {
        bail!(
            "user '{}' is not allowed to forget household memory",
            user.user_id
        );
    }
    if memory.forgotten_at.is_some() {
        bail!("memory '{memory_id}' is already forgotten");
    }

    db.forget_memory(memory_id, reason).await?;
    db.record_audit_event(
        "user",
        &user.user_id,
        "memory_forgotten",
        serde_json::json!({
            "memory_id": memory_id,
            "scope": memory.scope,
            "memory_kind": memory.memory_kind,
            "subject_type": memory.subject_type,
            "subject_key": memory.subject_key,
            "reason": reason,
        }),
    )
    .await?;
    db.fetch_memory(memory_id)
        .await?
        .ok_or_else(|| anyhow!("memory '{memory_id}' disappeared after forget"))
}

fn resolve_visible_scope_filter(
    role_ids: &[String],
    scope: Option<&str>,
) -> Result<Vec<MemoryScope>> {
    let visible_scopes = visible_memory_scopes(role_ids);
    match scope {
        None | Some("all") => Ok(visible_scopes),
        Some(scope_value) => {
            let scope = scope_value.parse::<MemoryScope>().map_err(|_| {
                anyhow!("unsupported scope '{scope_value}', expected private or household")
            })?;
            if !visible_scopes.contains(&scope) {
                bail!("scope '{scope}' is not visible to the selected user");
            }
            Ok(vec![scope])
        }
    }
}

fn parse_memory_kind_filter(kind: Option<&str>) -> Result<Option<MemoryKind>> {
    match kind {
        None | Some("all") => Ok(None),
        Some(kind_value) => kind_value.parse::<MemoryKind>().map(Some).map_err(|_| {
            anyhow!("unsupported memory kind '{kind_value}', expected semantic or episodic")
        }),
    }
}

fn ensure_memory_visible_to_user(
    memory: &MemoryRow,
    user_id: &str,
    visible_scopes: &[MemoryScope],
) -> Result<()> {
    let scope = memory.scope.parse::<MemoryScope>().map_err(|_| {
        anyhow!(
            "memory '{}' has unsupported scope '{}'",
            memory.memory_id,
            memory.scope
        )
    })?;
    if !visible_scopes.contains(&scope) {
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

fn print_memory_summary(memory: &MemoryRow) {
    println!(
        "- {} scope={} kind={} subject={} key={} owner={} updated={} superseded_by={} forgotten_at={} value={}",
        memory.memory_id,
        memory.scope,
        memory.memory_kind,
        memory.subject_type,
        memory.subject_key.as_deref().unwrap_or("<none>"),
        memory.owner_user_id.as_deref().unwrap_or("<shared>"),
        memory.updated_at,
        memory
            .superseded_by_memory_id
            .as_deref()
            .unwrap_or("<active>"),
        memory.forgotten_at.as_deref().unwrap_or("<active>"),
        memory.content_text
    );
}

fn print_memory_detail(memory: &MemoryRow) {
    println!("Memory: {}", memory.memory_id);
    println!(
        "Owner: {}",
        memory.owner_user_id.as_deref().unwrap_or("<shared>")
    );
    println!("Scope: {}", memory.scope);
    println!("Kind: {}", memory.memory_kind);
    println!("Subject Type: {}", memory.subject_type);
    println!(
        "Subject Key: {}",
        memory.subject_key.as_deref().unwrap_or("<none>")
    );
    println!("Source Type: {}", memory.source_type);
    println!(
        "Source Ref: {}",
        memory.source_ref.as_deref().unwrap_or("<none>")
    );
    println!("Sensitivity: {}", memory.sensitivity);
    println!("Task ID: {}", memory.task_id.as_deref().unwrap_or("<none>"));
    println!("Created At: {}", memory.created_at);
    println!("Updated At: {}", memory.updated_at);
    println!(
        "Last Accessed: {}",
        memory.last_accessed_at.as_deref().unwrap_or("<none>")
    );
    println!(
        "Superseded By: {}",
        memory
            .superseded_by_memory_id
            .as_deref()
            .unwrap_or("<active>")
    );
    println!(
        "Forgotten At: {}",
        memory.forgotten_at.as_deref().unwrap_or("<active>")
    );
    println!(
        "Forgotten Reason: {}",
        memory.forgotten_reason.as_deref().unwrap_or("<none>")
    );
    println!("Content: {}", memory.content_text);
    println!(
        "Content JSON: {}",
        memory.content_json.as_deref().unwrap_or("<none>")
    );
}

async fn try_daemon_client(config_dir: &PathBuf) -> Result<DaemonClient> {
    let config = LoadedConfig::load_from_dir(config_dir)?;
    let client = DaemonClient::new(config.runtime.state_dir.join("daemon.sock"));
    client
        .ping()
        .await
        .with_context(|| format!("daemon not reachable at {}", client.socket_path().display()))?;
    Ok(client)
}

async fn require_daemon_client(config_dir: &PathBuf) -> Result<DaemonClient> {
    try_daemon_client(config_dir).await.with_context(
        || "daemon is not running. Start it with 'beaverki daemon start' or 'beaverki daemon run'",
    )
}

async fn resolve_user_for_db(db: &Database, user_id: Option<&str>) -> Result<UserRow> {
    match user_id {
        Some(user_id) => db
            .fetch_user(user_id)
            .await?
            .ok_or_else(|| anyhow!("user '{user_id}' not found")),
        None => db
            .default_user()
            .await?
            .ok_or_else(|| anyhow!("runtime database has no bootstrap user; run setup first")),
    }
}

fn resolve_config_dir(config_dir: Option<PathBuf>) -> Result<PathBuf> {
    Ok(config_dir.unwrap_or(default_app_paths()?.config_dir))
}

fn default_workspace_root() -> Result<PathBuf> {
    std::env::current_dir().context("failed to determine current working directory")
}

fn print_daemon_status(status: &beaverki_runtime::DaemonStatus, reachable: bool) {
    println!("Daemon reachable: {}", if reachable { "yes" } else { "no" });
    println!("Session: {}", status.session_id);
    println!("State: {}", status.state);
    println!("PID: {}", status.pid);
    println!("Socket: {}", status.socket_path);
    println!("Queue depth: {}", status.queue_depth);
    println!(
        "Automation planning enabled: {}",
        if status.automation_planning_enabled {
            "yes"
        } else {
            "no"
        }
    );
    println!("Started at: {}", status.started_at);
    if let Some(last_heartbeat_at) = &status.last_heartbeat_at {
        println!("Last heartbeat: {last_heartbeat_at}");
    }
    if let Some(active_task_id) = &status.active_task_id {
        println!("Active task: {active_task_id}");
    }
    if let Some(stopped_at) = &status.stopped_at {
        println!("Stopped at: {stopped_at}");
    }
    if let Some(last_error) = &status.last_error {
        println!("Last error: {last_error}");
    }
}
