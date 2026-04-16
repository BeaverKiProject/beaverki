use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use beaverki_agent::{AgentMemoryMode, AgentRequest, AgentResult, PrimaryAgentRunner};
use beaverki_config::{LoadedConfig, SecretStore};
use beaverki_core::MemoryScope;
use beaverki_db::{
    ApprovalRow, BootstrapState, Database, RoleRow, TaskEventRow, TaskRow, ToolInvocationRow,
    UserRoleRow, UserRow,
};
use beaverki_memory::MemoryStore;
use beaverki_models::{ModelProvider, OpenAiProvider};
use beaverki_policy::{can_grant_approvals, visible_memory_scopes};
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
        if matches!(scope, MemoryScope::Household) && !visible_scopes.contains(&MemoryScope::Household)
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
