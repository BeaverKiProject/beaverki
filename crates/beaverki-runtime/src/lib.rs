use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use beaverki_agent::{AgentRequest, AgentResult, PrimaryAgentRunner};
use beaverki_config::{LoadedConfig, SecretStore};
use beaverki_core::MemoryScope;
use beaverki_db::{Database, TaskEventRow, TaskRow, ToolInvocationRow, UserRow};
use beaverki_memory::MemoryStore;
use beaverki_models::{ModelProvider, OpenAiProvider};
use beaverki_tools::{ToolContext, builtin_registry};

#[derive(Debug, Clone)]
pub struct TaskInspection {
    pub task: TaskRow,
    pub events: Vec<TaskEventRow>,
    pub tool_invocations: Vec<ToolInvocationRow>,
}

pub struct Runtime {
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
        let memory = MemoryStore::new(db.clone());
        let mut allowed_roots = vec![config.runtime.workspace_root.clone()];
        if !allowed_roots.contains(&config.runtime.data_dir) {
            allowed_roots.push(config.runtime.data_dir.clone());
        }
        let tools = builtin_registry();
        let runner = PrimaryAgentRunner::new(
            db.clone(),
            memory,
            provider,
            tools,
            ToolContext::new(config.runtime.workspace_root.clone(), allowed_roots),
            usize::from(config.runtime.defaults.max_agent_steps),
        );

        Ok(Self {
            db,
            default_user,
            runner,
        })
    }

    pub async fn run_objective(&self, objective: &str) -> Result<AgentResult> {
        let primary_agent_id = self
            .default_user
            .primary_agent_id
            .clone()
            .ok_or_else(|| anyhow!("default user has no primary agent"))?;
        self.runner
            .run_task(AgentRequest {
                owner_user_id: self.default_user.user_id.clone(),
                primary_agent_id,
                objective: objective.to_owned(),
                scope: MemoryScope::Private,
            })
            .await
    }

    pub async fn inspect_task(&self, task_id: &str) -> Result<TaskInspection> {
        let task = self
            .db
            .fetch_task_for_owner(&self.default_user.user_id, task_id)
            .await?
            .ok_or_else(|| anyhow!("task '{task_id}' not found"))?;
        let events = self
            .db
            .fetch_task_events_for_owner(&self.default_user.user_id, task_id)
            .await?;
        let tool_invocations = self
            .db
            .fetch_tool_invocations_for_owner(&self.default_user.user_id, task_id)
            .await?;

        Ok(TaskInspection {
            task,
            events,
            tool_invocations,
        })
    }

    pub fn default_user(&self) -> &UserRow {
        &self.default_user
    }
}

fn load_provider(config: &LoadedConfig, passphrase: &str) -> Result<OpenAiProvider> {
    let provider_entry = config.providers.active_provider()?;
    if provider_entry.auth.mode != "api_token" {
        bail!(
            "provider auth mode '{}' is not implemented in M0",
            provider_entry.auth.mode
        );
    }

    let secret_store = SecretStore::new(&config.runtime.secret_dir);
    let api_token = secret_store.read_secret(&provider_entry.auth.secret_ref, passphrase)?;
    OpenAiProvider::from_entry(provider_entry, api_token)
}
