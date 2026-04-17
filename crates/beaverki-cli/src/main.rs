use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use beaverki_config::{
    LoadedConfig, SetupAnswers, default_app_paths, prompt_passphrase_from_env,
    write_integrations_config, write_providers_config, write_setup_files,
};
use beaverki_db::{Database, UserRow};
use beaverki_models::OpenAiProvider;
use beaverki_policy::is_builtin_role;
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
struct SetModelsArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    planner_model: Option<String>,
    #[arg(long)]
    executor_model: Option<String>,
    #[arg(long)]
    summarizer_model: Option<String>,
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
            "- {} task={} status={} action={} target={}",
            approval.approval_id,
            approval.task_id,
            approval.status,
            approval.action_type,
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

async fn verify_openai(args: VerifyOpenAiArgs) -> Result<()> {
    let api_token = std::env::var(&args.openai_api_token_env)
        .with_context(|| format!("missing environment variable {}", args.openai_api_token_env))?;
    verify_openai_api_token(&api_token, "gpt-5.4", "gpt-5.4-mini", "gpt-5.4-mini").await?;
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

    if !args.skip_openai_check {
        let api_token = std::env::var(&args.openai_api_token_env).with_context(|| {
            format!("missing environment variable {}", args.openai_api_token_env)
        })?;
        verify_openai_api_token(
            &api_token,
            &planner_model,
            &executor_model,
            &summarizer_model,
        )
        .await?;
    }

    provider.models.planner = planner_model;
    provider.models.executor = executor_model;
    provider.models.summarizer = summarizer_model;
    let planner = provider.models.planner.clone();
    let executor = provider.models.executor.clone();
    let summarizer = provider.models.summarizer.clone();

    let providers_path = write_providers_config(&config_dir, &config.providers)?;
    println!("Updated provider models in {}", providers_path.display());
    println!("Planner model: {}", planner);
    println!("Executor model: {}", executor);
    println!("Summarizer model: {}", summarizer);

    Ok(())
}

async fn verify_openai_api_token(
    api_token: &str,
    planner_model: &str,
    executor_model: &str,
    summarizer_model: &str,
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
