use std::path::PathBuf;

use anyhow::{Context, Result};
use beaverki_config::{
    LoadedConfig, SetupAnswers, default_app_paths, prompt_passphrase_from_env,
    write_providers_config, write_setup_files,
};
use beaverki_db::Database;
use beaverki_models::OpenAiProvider;
use beaverki_runtime::Runtime;
use clap::{Args, Parser, Subcommand};
use dialoguer::{Input, Password};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "beaverki")]
#[command(about = "BeaverKI M0 CLI runtime")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Setup {
        #[command(subcommand)]
        command: Box<SetupCommand>,
    },
    Task {
        #[command(subcommand)]
        command: Box<TaskCommand>,
    },
}

#[derive(Subcommand)]
enum SetupCommand {
    Init(Box<SetupInitArgs>),
    VerifyOpenai(VerifyOpenAiArgs),
    ShowModels(ConfigDirArgs),
    SetModels(Box<SetModelsArgs>),
}

#[derive(Subcommand)]
enum TaskCommand {
    Run(TaskRunArgs),
    Show(TaskShowArgs),
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
    objective: String,
    #[arg(long, default_value = "BEAVERKI_MASTER_PASSPHRASE")]
    passphrase_env: String,
}

#[derive(Args, Clone)]
struct TaskShowArgs {
    #[arg(long)]
    config_dir: Option<PathBuf>,
    #[arg(long)]
    task_id: String,
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
    println!("Encrypted secret ref: {}", artifacts.secret_ref);
    println!("Data dir: {}", data_dir.display());
    println!("State dir: {}", state_dir.display());
    println!("Log dir: {}", log_dir.display());
    println!("Secret dir: {}", secret_dir.display());
    println!("User ID: {}", bootstrap.user_id);
    println!("Primary agent ID: {}", bootstrap.primary_agent_id);

    Ok(())
}

async fn task_run(args: TaskRunArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let passphrase = prompt_passphrase_from_env(&args.passphrase_env).unwrap_or_else(|| {
        Password::new()
            .with_prompt("Master passphrase")
            .interact()
            .expect("failed to read master passphrase")
    });
    let runtime = Runtime::load(&config_dir, &passphrase).await?;
    let result = runtime.run_objective(&args.objective).await?;

    println!("Task ID: {}", result.task.task_id);
    println!("State: {}", result.task.state);
    if let Some(result_text) = result.task.result_text {
        println!("\n{result_text}");
    }

    Ok(())
}

async fn task_show(args: TaskShowArgs) -> Result<()> {
    let config_dir = resolve_config_dir(args.config_dir)?;
    let config = LoadedConfig::load_from_dir(&config_dir)?;
    let db = Database::connect(&config.runtime.database_path).await?;
    let task = db
        .fetch_task(&args.task_id)
        .await?
        .with_context(|| format!("task '{}' not found", args.task_id))?;
    let events = db.fetch_task_events(&args.task_id).await?;
    let invocations = db.fetch_tool_invocations(&args.task_id).await?;

    println!("Task: {}", task.task_id);
    println!("State: {}", task.state);
    println!("Objective: {}", task.objective);
    if let Some(result_text) = task.result_text {
        println!("Result: {result_text}");
    }

    println!("\nEvents:");
    for event in events {
        println!(
            "- {} {} {} {}",
            event.created_at, event.actor_type, event.event_type, event.payload_json
        );
    }

    println!("\nTool Invocations:");
    for invocation in invocations {
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

fn resolve_config_dir(config_dir: Option<PathBuf>) -> Result<PathBuf> {
    Ok(config_dir.unwrap_or(default_app_paths()?.config_dir))
}

fn default_workspace_root() -> Result<PathBuf> {
    std::env::current_dir().context("failed to determine current working directory")
}
