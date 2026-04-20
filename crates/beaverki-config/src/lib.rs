use std::fs;
use std::io::{Cursor, Read, Write};
use std::iter;
use std::path::{Path, PathBuf};

use age::Decryptor;
use age::Encryptor;
use age::secrecy::SecretString;
use anyhow::{Context, Result, anyhow};
use directories::BaseDirs;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const CURRENT_CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,
    pub instance_id: String,
    pub mode: String,
    pub data_dir: PathBuf,
    pub state_dir: PathBuf,
    pub log_dir: PathBuf,
    pub secret_dir: PathBuf,
    pub database_path: PathBuf,
    pub workspace_root: PathBuf,
    pub default_timezone: String,
    pub features: RuntimeFeatures,
    pub defaults: RuntimeDefaults,
    #[serde(default)]
    pub session_management: SessionManagementConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeFeatures {
    pub markdown_exports: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDefaults {
    pub max_agent_steps: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionManagementConfig {
    pub cleanup_interval_secs: u64,
    pub policies: Vec<SessionLifecyclePolicy>,
}

impl Default for SessionManagementConfig {
    fn default() -> Self {
        Self {
            cleanup_interval_secs: 5 * 60,
            policies: vec![
                SessionLifecyclePolicy {
                    policy_id: "cli_inactive_reset".to_owned(),
                    enabled: true,
                    action: SessionLifecycleAction::Reset,
                    inactivity_after_secs: 12 * 60 * 60,
                    session_kind: Some("cli".to_owned()),
                    connector_type: None,
                    connector_target_prefix: None,
                    audience_policy: None,
                    max_memory_scope: None,
                },
                SessionLifecyclePolicy {
                    policy_id: "direct_message_inactive_reset".to_owned(),
                    enabled: true,
                    action: SessionLifecycleAction::Reset,
                    inactivity_after_secs: 6 * 60 * 60,
                    session_kind: Some("direct_message".to_owned()),
                    connector_type: None,
                    connector_target_prefix: None,
                    audience_policy: None,
                    max_memory_scope: None,
                },
                SessionLifecyclePolicy {
                    policy_id: "group_room_inactive_archive".to_owned(),
                    enabled: true,
                    action: SessionLifecycleAction::Archive,
                    inactivity_after_secs: 7 * 24 * 60 * 60,
                    session_kind: Some("group_room".to_owned()),
                    connector_type: None,
                    connector_target_prefix: None,
                    audience_policy: None,
                    max_memory_scope: None,
                },
                SessionLifecyclePolicy {
                    policy_id: "cron_run_inactive_archive".to_owned(),
                    enabled: true,
                    action: SessionLifecycleAction::Archive,
                    inactivity_after_secs: 24 * 60 * 60,
                    session_kind: Some("cron_run".to_owned()),
                    connector_type: None,
                    connector_target_prefix: None,
                    audience_policy: None,
                    max_memory_scope: None,
                },
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionLifecycleAction {
    Reset,
    Archive,
}

impl SessionLifecycleAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reset => "reset",
            Self::Archive => "archive",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionLifecyclePolicy {
    pub policy_id: String,
    pub enabled: bool,
    pub action: SessionLifecycleAction,
    pub inactivity_after_secs: u64,
    pub session_kind: Option<String>,
    pub connector_type: Option<String>,
    pub connector_target_prefix: Option<String>,
    pub audience_policy: Option<String>,
    pub max_memory_scope: Option<String>,
}

impl Default for SessionLifecyclePolicy {
    fn default() -> Self {
        Self {
            policy_id: String::new(),
            enabled: true,
            action: SessionLifecycleAction::Reset,
            inactivity_after_secs: 0,
            session_kind: None,
            connector_type: None,
            connector_target_prefix: None,
            audience_policy: None,
            max_memory_scope: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SessionPolicyMatchInput<'a> {
    pub session_kind: &'a str,
    pub connector_type: Option<&'a str>,
    pub connector_target: Option<&'a str>,
    pub audience_policy: &'a str,
    pub max_memory_scope: &'a str,
}

impl SessionLifecyclePolicy {
    pub fn matches(&self, input: &SessionPolicyMatchInput<'_>) -> bool {
        if !self.enabled {
            return false;
        }
        if let Some(session_kind) = self.session_kind.as_deref()
            && session_kind != input.session_kind
        {
            return false;
        }
        if let Some(connector_type) = self.connector_type.as_deref()
            && Some(connector_type) != input.connector_type
        {
            return false;
        }
        if let Some(prefix) = self.connector_target_prefix.as_deref() {
            let Some(target) = input.connector_target else {
                return false;
            };
            if !target.starts_with(prefix) {
                return false;
            }
        }
        if let Some(audience_policy) = self.audience_policy.as_deref()
            && audience_policy != input.audience_policy
        {
            return false;
        }
        if let Some(max_memory_scope) = self.max_memory_scope.as_deref()
            && max_memory_scope != input.max_memory_scope
        {
            return false;
        }
        true
    }
}

pub fn select_session_lifecycle_policy<'a>(
    policies: &'a [SessionLifecyclePolicy],
    input: &SessionPolicyMatchInput<'_>,
) -> Option<&'a SessionLifecyclePolicy> {
    policies.iter().find(|policy| policy.matches(input))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvidersConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,
    pub active: String,
    pub entries: Vec<ProviderEntry>,
}

impl ProvidersConfig {
    pub fn active_provider(&self) -> Result<&ProviderEntry> {
        self.entries
            .iter()
            .find(|entry| entry.provider_id == self.active)
            .ok_or_else(|| anyhow!("active provider '{}' not found", self.active))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderEntry {
    pub provider_id: String,
    pub kind: String,
    pub auth: ProviderAuth,
    pub models: ProviderModels,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAuth {
    pub mode: String,
    pub secret_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderModels {
    pub planner: String,
    pub executor: String,
    pub summarizer: String,
    #[serde(default = "default_safety_review_model")]
    pub safety_review: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct IntegrationsConfig {
    #[serde(default = "default_config_version")]
    pub version: u32,
    pub browser: BrowserConfig,
    pub discord: DiscordConfig,
    pub notion: NotionConfig,
}

impl Default for IntegrationsConfig {
    fn default() -> Self {
        Self {
            version: CURRENT_CONFIG_VERSION,
            browser: BrowserConfig::default(),
            discord: DiscordConfig::default(),
            notion: NotionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct BrowserConfig {
    pub interactive_launcher: Option<String>,
    pub headless_browser: Option<String>,
    pub headless_args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscordConfig {
    pub enabled: bool,
    pub bot_token_secret_ref: Option<String>,
    pub command_prefix: String,
    pub allowed_channels: Vec<DiscordAllowedChannel>,
    pub task_wait_timeout_secs: u64,
    pub approval_action_ttl_secs: u64,
    pub approval_dm_only: bool,
    pub critical_confirmation_ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscordAllowedChannel {
    pub channel_id: String,
    #[serde(default)]
    pub mode: DiscordChannelMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DiscordChannelMode {
    #[default]
    Household,
    Guest,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token_secret_ref: None,
            command_prefix: "!bk".to_owned(),
            allowed_channels: Vec::new(),
            task_wait_timeout_secs: 5,
            approval_action_ttl_secs: 15 * 60,
            approval_dm_only: true,
            critical_confirmation_ttl_secs: 5 * 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NotionConfig {
    pub enabled: bool,
    pub api_token_secret_ref: Option<String>,
    pub api_base_url: String,
    pub api_version: String,
}

impl Default for NotionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_token_secret_ref: None,
            api_base_url: "https://api.notion.com/v1".to_owned(),
            api_version: "2026-03-11".to_owned(),
        }
    }
}

impl DiscordConfig {
    pub fn is_allowed_channel(&self, channel_id: &str) -> bool {
        self.allowed_channels
            .iter()
            .any(|channel| channel.channel_id == channel_id)
    }

    pub fn channel_mode(&self, channel_id: &str) -> Option<DiscordChannelMode> {
        self.allowed_channels
            .iter()
            .find(|channel| channel.channel_id == channel_id)
            .map(|channel| channel.mode)
    }
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config_dir: PathBuf,
    pub base_dir: PathBuf,
    pub runtime: RuntimeConfig,
    pub providers: ProvidersConfig,
    pub integrations: IntegrationsConfig,
}

impl LoadedConfig {
    pub fn load_from_dir(config_dir: impl AsRef<Path>) -> Result<Self> {
        let config_dir = config_dir.as_ref().to_path_buf();
        let base_dir = config_base_dir(&config_dir);
        let runtime_path = config_dir.join("runtime.yaml");
        let providers_path = config_dir.join("providers.yaml");
        let integrations_path = config_dir.join("integrations.yaml");

        let mut runtime: RuntimeConfig =
            load_versioned_yaml(&runtime_path, migrate_runtime_config_value)?;
        let providers: ProvidersConfig =
            load_versioned_yaml(&providers_path, migrate_providers_config_value)?;
        let integrations = if integrations_path.exists() {
            load_versioned_yaml(&integrations_path, migrate_integrations_config_value)?
        } else {
            IntegrationsConfig::default()
        };

        runtime.data_dir = resolve_path(&base_dir, &runtime.data_dir);
        runtime.state_dir = resolve_path(&base_dir, &runtime.state_dir);
        runtime.log_dir = resolve_path(&base_dir, &runtime.log_dir);
        runtime.secret_dir = resolve_path(&base_dir, &runtime.secret_dir);
        runtime.database_path = resolve_path(&base_dir, &runtime.database_path);
        runtime.workspace_root = resolve_path(&base_dir, &runtime.workspace_root);

        Ok(Self {
            config_dir,
            base_dir,
            runtime,
            providers,
            integrations,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SetupAnswers {
    pub config_dir: PathBuf,
    pub instance_id: String,
    pub owner_display_name: String,
    pub data_dir: PathBuf,
    pub state_dir: PathBuf,
    pub log_dir: PathBuf,
    pub secret_dir: PathBuf,
    pub database_path: PathBuf,
    pub workspace_root: PathBuf,
    pub planner_model: String,
    pub executor_model: String,
    pub summarizer_model: String,
    pub safety_review_model: String,
    pub openai_api_token: String,
    pub master_passphrase: String,
}

#[derive(Debug, Clone)]
pub struct SetupArtifacts {
    pub runtime_path: PathBuf,
    pub providers_path: PathBuf,
    pub integrations_path: PathBuf,
    pub secret_ref: String,
}

pub fn write_setup_files(answers: &SetupAnswers) -> Result<SetupArtifacts> {
    fs::create_dir_all(&answers.config_dir)
        .with_context(|| format!("failed to create {}", answers.config_dir.display()))?;
    fs::create_dir_all(&answers.data_dir)
        .with_context(|| format!("failed to create {}", answers.data_dir.display()))?;
    fs::create_dir_all(&answers.state_dir)
        .with_context(|| format!("failed to create {}", answers.state_dir.display()))?;
    fs::create_dir_all(&answers.log_dir)
        .with_context(|| format!("failed to create {}", answers.log_dir.display()))?;
    fs::create_dir_all(&answers.secret_dir)
        .with_context(|| format!("failed to create {}", answers.secret_dir.display()))?;

    let provider_id = "openai_main";
    let secret_ref = format!("secret://local/{provider_id}_api_token");
    let runtime = RuntimeConfig {
        version: CURRENT_CONFIG_VERSION,
        instance_id: answers.instance_id.clone(),
        mode: "cli".to_owned(),
        data_dir: answers.data_dir.clone(),
        state_dir: answers.state_dir.clone(),
        log_dir: answers.log_dir.clone(),
        secret_dir: answers.secret_dir.clone(),
        database_path: answers.database_path.clone(),
        workspace_root: answers.workspace_root.clone(),
        default_timezone: "UTC".to_owned(),
        features: RuntimeFeatures {
            markdown_exports: true,
        },
        defaults: RuntimeDefaults { max_agent_steps: 8 },
        session_management: SessionManagementConfig::default(),
    };
    let providers = ProvidersConfig {
        version: CURRENT_CONFIG_VERSION,
        active: provider_id.to_owned(),
        entries: vec![ProviderEntry {
            provider_id: provider_id.to_owned(),
            kind: "openai".to_owned(),
            auth: ProviderAuth {
                mode: "api_token".to_owned(),
                secret_ref: secret_ref.clone(),
            },
            models: ProviderModels {
                planner: answers.planner_model.clone(),
                executor: answers.executor_model.clone(),
                summarizer: answers.summarizer_model.clone(),
                safety_review: answers.safety_review_model.clone(),
            },
        }],
    };
    let integrations = IntegrationsConfig::default();

    let runtime_path = answers.config_dir.join("runtime.yaml");
    let providers_path = answers.config_dir.join("providers.yaml");
    let integrations_path = answers.config_dir.join("integrations.yaml");

    fs::write(&runtime_path, serde_yaml::to_string(&runtime)?)
        .with_context(|| format!("failed to write {}", runtime_path.display()))?;
    write_providers_config_path(&providers_path, &providers)?;
    write_integrations_config_path(&integrations_path, &integrations)?;

    let secret_store = SecretStore::new(&answers.secret_dir);
    secret_store.write_secret(
        &secret_ref,
        &answers.openai_api_token,
        &answers.master_passphrase,
    )?;

    Ok(SetupArtifacts {
        runtime_path,
        providers_path,
        integrations_path,
        secret_ref,
    })
}

fn default_safety_review_model() -> String {
    "gpt-5.4-mini".to_owned()
}

fn default_config_version() -> u32 {
    CURRENT_CONFIG_VERSION
}

pub fn write_providers_config(
    config_dir: impl AsRef<Path>,
    providers: &ProvidersConfig,
) -> Result<PathBuf> {
    let path = config_dir.as_ref().join("providers.yaml");
    write_providers_config_path(&path, providers)?;
    Ok(path)
}

pub fn write_runtime_config(
    config_dir: impl AsRef<Path>,
    runtime: &RuntimeConfig,
) -> Result<PathBuf> {
    let path = config_dir.as_ref().join("runtime.yaml");
    write_runtime_config_path(&path, runtime)?;
    Ok(path)
}

pub fn write_integrations_config(
    config_dir: impl AsRef<Path>,
    integrations: &IntegrationsConfig,
) -> Result<PathBuf> {
    let path = config_dir.as_ref().join("integrations.yaml");
    write_integrations_config_path(&path, integrations)?;
    Ok(path)
}

fn write_providers_config_path(path: &Path, providers: &ProvidersConfig) -> Result<()> {
    fs::write(path, serde_yaml::to_string(providers)?)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn write_runtime_config_path(path: &Path, runtime: &RuntimeConfig) -> Result<()> {
    fs::write(path, serde_yaml::to_string(runtime)?)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn write_integrations_config_path(path: &Path, integrations: &IntegrationsConfig) -> Result<()> {
    fs::write(path, serde_yaml::to_string(integrations)?)
        .with_context(|| format!("failed to write {}", path.display()))
}

fn load_versioned_yaml<T>(
    path: &Path,
    migrate: fn(serde_yaml::Value) -> Result<(serde_yaml::Value, bool)>,
) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    let original =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let raw: serde_yaml::Value = serde_yaml::from_str(&original)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let (migrated, changed) =
        migrate(raw).with_context(|| format!("failed to migrate {}", path.display()))?;
    let parsed: T = serde_yaml::from_value(migrated)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    if changed {
        write_yaml_atomically_with_backup(path, &original, &serde_yaml::to_string(&parsed)?)?;
    }

    Ok(parsed)
}

fn migrate_runtime_config_value(value: serde_yaml::Value) -> Result<(serde_yaml::Value, bool)> {
    migrate_root_mapping(value, |mapping, version| {
        let mut changed = false;
        if version < CURRENT_CONFIG_VERSION {
            let session_management_key = serde_yaml::Value::String("session_management".to_owned());
            if !mapping.contains_key(&session_management_key) {
                mapping.insert(
                    session_management_key,
                    serde_yaml::to_value(SessionManagementConfig::default())?,
                );
                changed = true;
            }
        }
        Ok(changed)
    })
}

fn migrate_providers_config_value(value: serde_yaml::Value) -> Result<(serde_yaml::Value, bool)> {
    migrate_root_mapping(value, |mapping, version| {
        let mut changed = false;
        if version < CURRENT_CONFIG_VERSION
            && let Some(entries) = mapping
                .get_mut(serde_yaml::Value::String("entries".to_owned()))
                .and_then(serde_yaml::Value::as_sequence_mut)
        {
            for entry in entries {
                let Some(entry_mapping) = entry.as_mapping_mut() else {
                    continue;
                };
                let Some(models) = entry_mapping
                    .get_mut(serde_yaml::Value::String("models".to_owned()))
                    .and_then(serde_yaml::Value::as_mapping_mut)
                else {
                    continue;
                };
                let safety_key = serde_yaml::Value::String("safety_review".to_owned());
                if !models.contains_key(&safety_key) {
                    models.insert(
                        safety_key,
                        serde_yaml::Value::String(default_safety_review_model()),
                    );
                    changed = true;
                }
            }
        }
        Ok(changed)
    })
}

fn migrate_integrations_config_value(
    value: serde_yaml::Value,
) -> Result<(serde_yaml::Value, bool)> {
    migrate_root_mapping(value, |mapping, _| {
        let mut changed = false;
        let discord_key = serde_yaml::Value::String("discord".to_owned());
        let allowed_channels_key = serde_yaml::Value::String("allowed_channels".to_owned());
        let allowed_channel_ids_key = serde_yaml::Value::String("allowed_channel_ids".to_owned());

        if let Some(discord) = mapping
            .get_mut(&discord_key)
            .and_then(serde_yaml::Value::as_mapping_mut)
        {
            if !discord.contains_key(&allowed_channels_key) {
                let migrated_channels = discord
                    .remove(&allowed_channel_ids_key)
                    .and_then(|value| value.as_sequence().cloned())
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                    .map(|channel_id| {
                        serde_yaml::to_value(DiscordAllowedChannel {
                            channel_id,
                            mode: DiscordChannelMode::Household,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !migrated_channels.is_empty() {
                    discord.insert(
                        allowed_channels_key.clone(),
                        serde_yaml::Value::Sequence(migrated_channels),
                    );
                    changed = true;
                }
            } else if discord.remove(&allowed_channel_ids_key).is_some() {
                changed = true;
            }
        }

        Ok(changed)
    })
}

fn migrate_root_mapping(
    value: serde_yaml::Value,
    migrate_body: impl FnOnce(&mut serde_yaml::Mapping, u32) -> Result<bool>,
) -> Result<(serde_yaml::Value, bool)> {
    let mut changed = false;
    let mut mapping = value
        .as_mapping()
        .cloned()
        .ok_or_else(|| anyhow!("expected YAML mapping at document root"))?;

    let version_key = serde_yaml::Value::String("version".to_owned());
    let version = match mapping
        .get(&version_key)
        .and_then(serde_yaml::Value::as_u64)
    {
        Some(version) => u32::try_from(version).context("config version out of range")?,
        None => {
            mapping.insert(
                version_key.clone(),
                serde_yaml::Value::Number(serde_yaml::Number::from(CURRENT_CONFIG_VERSION)),
            );
            changed = true;
            0
        }
    };

    if version > CURRENT_CONFIG_VERSION {
        return Err(anyhow!(
            "unsupported config version {version}; this build supports up to {CURRENT_CONFIG_VERSION}"
        ));
    }

    changed |= migrate_body(&mut mapping, version)?;

    if version < CURRENT_CONFIG_VERSION {
        mapping.insert(
            version_key,
            serde_yaml::Value::Number(serde_yaml::Number::from(CURRENT_CONFIG_VERSION)),
        );
        changed = true;
    }

    Ok((serde_yaml::Value::Mapping(mapping), changed))
}

fn write_yaml_atomically_with_backup(path: &Path, original: &str, updated: &str) -> Result<()> {
    if original == updated {
        return Ok(());
    }

    let backup_path = backup_path_for(path);
    fs::write(&backup_path, original)
        .with_context(|| format!("failed to write {}", backup_path.display()))?;

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid config path {}", path.display()))?;
    let temp_path = path.with_file_name(format!("{file_name}.tmp"));
    fs::write(&temp_path, updated)
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    fs::rename(&temp_path, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

fn backup_path_for(path: &Path) -> PathBuf {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if !ext.is_empty() => path.with_extension(format!("{ext}.bak")),
        _ => path.with_extension("bak"),
    }
}

pub struct SecretStore {
    secret_dir: PathBuf,
}

impl SecretStore {
    pub fn new(secret_dir: impl Into<PathBuf>) -> Self {
        Self {
            secret_dir: secret_dir.into(),
        }
    }

    pub fn write_secret(
        &self,
        secret_ref: &str,
        secret_value: &str,
        passphrase: &str,
    ) -> Result<()> {
        let path = self.secret_path(secret_ref)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let encryptor = Encryptor::with_user_passphrase(SecretString::from(passphrase.to_owned()));
        let mut encrypted = Vec::new();
        {
            let mut writer = encryptor
                .wrap_output(&mut encrypted)
                .context("failed to start secret encryption")?;
            writer
                .write_all(secret_value.as_bytes())
                .context("failed to encrypt secret")?;
            writer.finish().context("failed to finalize secret")?;
        }

        fs::write(&path, encrypted)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    pub fn read_secret(&self, secret_ref: &str, passphrase: &str) -> Result<String> {
        let path = self.secret_path(secret_ref)?;
        let encrypted =
            fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let decryptor = Decryptor::new(Cursor::new(encrypted)).context("invalid secret file")?;
        let mut decrypted = String::new();
        let passphrase_identity =
            age::scrypt::Identity::new(SecretString::from(passphrase.to_owned()));
        let mut reader = decryptor
            .decrypt(iter::once(&passphrase_identity as &dyn age::Identity))
            .context("failed to decrypt secret")?;
        reader
            .read_to_string(&mut decrypted)
            .context("failed to decode decrypted secret")?;

        Ok(decrypted)
    }

    pub fn path_for_ref(&self, secret_ref: &str) -> Result<PathBuf> {
        self.secret_path(secret_ref)
    }

    fn secret_path(&self, secret_ref: &str) -> Result<PathBuf> {
        const PREFIX: &str = "secret://local/";
        let name = secret_ref
            .strip_prefix(PREFIX)
            .ok_or_else(|| anyhow!("unsupported secret ref: {secret_ref}"))?;
        Ok(self.secret_dir.join(format!("{name}.age")))
    }
}

pub fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn config_base_dir(config_dir: &Path) -> PathBuf {
    config_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn prompt_passphrase_from_env(env_var: &str) -> Option<String> {
    std::env::var(env_var)
        .ok()
        .filter(|value| !value.is_empty())
}

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub state_dir: PathBuf,
    pub log_dir: PathBuf,
    pub secret_dir: PathBuf,
    pub database_path: PathBuf,
}

pub fn default_app_paths() -> Result<AppPaths> {
    let base_dirs = BaseDirs::new().ok_or_else(|| anyhow!("could not determine home directory"))?;
    let config_dir = base_dirs.config_dir().join("beaverki");
    let data_dir = base_dirs.data_dir().join("beaverki");
    let state_root = base_dirs
        .state_dir()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| data_dir.clone());
    let state_dir = state_root.join("beaverki");
    let log_dir = state_dir.join("logs");
    let secret_dir = state_dir.join("secrets");
    let database_path = state_dir.join("runtime.db");

    Ok(AppPaths {
        config_dir,
        data_dir,
        state_dir,
        log_dir,
        secret_dir,
        database_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_relative_paths_against_parent_of_config_dir() {
        let config_dir = PathBuf::from("/tmp/beaverki/config");
        let loaded = LoadedConfig {
            config_dir: config_dir.clone(),
            base_dir: config_base_dir(&config_dir),
            runtime: RuntimeConfig {
                version: CURRENT_CONFIG_VERSION,
                instance_id: "test".to_owned(),
                mode: "cli".to_owned(),
                data_dir: PathBuf::from("./.beaverki"),
                state_dir: PathBuf::from("./.beaverki"),
                log_dir: PathBuf::from("./logs"),
                secret_dir: PathBuf::from("./secrets"),
                database_path: PathBuf::from("./.beaverki/runtime.db"),
                workspace_root: PathBuf::from("."),
                default_timezone: "UTC".to_owned(),
                features: RuntimeFeatures {
                    markdown_exports: true,
                },
                defaults: RuntimeDefaults { max_agent_steps: 8 },
                session_management: SessionManagementConfig::default(),
            },
            providers: ProvidersConfig {
                version: CURRENT_CONFIG_VERSION,
                active: "openai_main".to_owned(),
                entries: vec![],
            },
            integrations: IntegrationsConfig::default(),
        };

        assert_eq!(loaded.base_dir, PathBuf::from("/tmp/beaverki"));
    }

    #[test]
    fn uses_current_directory_when_config_dir_has_no_parent() {
        assert_eq!(config_base_dir(Path::new("config")), PathBuf::from("."));
    }

    #[test]
    fn encrypts_and_decrypts_secrets() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let store = SecretStore::new(tempdir.path().join("secrets"));
        let passphrase = "test-passphrase";
        let secret_ref = "secret://local/openai_main_api_token";

        store
            .write_secret(secret_ref, "sk-test", passphrase)
            .expect("write secret");
        let recovered = store
            .read_secret(secret_ref, passphrase)
            .expect("read secret");

        assert_eq!(recovered, "sk-test");
    }

    #[test]
    fn default_app_paths_are_named_for_beaverki() {
        let paths = default_app_paths().expect("paths");
        assert!(paths.config_dir.ends_with("beaverki"));
        assert!(paths.data_dir.ends_with("beaverki"));
        assert!(paths.state_dir.ends_with("beaverki"));
        assert!(paths.secret_dir.ends_with("secrets"));
    }

    #[test]
    fn load_from_dir_migrates_legacy_configs_and_creates_backups() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config_dir = tempdir.path().join("config");
        fs::create_dir_all(&config_dir).expect("config dir");

        let runtime_path = config_dir.join("runtime.yaml");
        let providers_path = config_dir.join("providers.yaml");
        let integrations_path = config_dir.join("integrations.yaml");

        fs::write(
            &runtime_path,
            r#"instance_id: test-instance
mode: cli
data_dir: ./data
state_dir: ./state
log_dir: ./logs
secret_dir: ./secrets
database_path: ./state/runtime.db
workspace_root: .
default_timezone: UTC
features:
  markdown_exports: true
defaults:
  max_agent_steps: 8
"#,
        )
        .expect("write runtime");
        fs::write(
            &providers_path,
            r#"active: openai_main
entries:
  - provider_id: openai_main
    kind: openai
    auth:
      mode: api_token
      secret_ref: secret://local/openai_main_api_token
    models:
      planner: gpt-5.4
      executor: gpt-5.4-mini
      summarizer: gpt-5.4-mini
"#,
        )
        .expect("write providers");
        fs::write(
            &integrations_path,
            "discord:\n  enabled: true\n  command_prefix: \"!bk-test\"\n  allowed_channel_ids: [\"111\", \"222\"]\n",
        )
        .expect("write integrations");

        let loaded = LoadedConfig::load_from_dir(&config_dir).expect("load");
        assert_eq!(loaded.runtime.version, CURRENT_CONFIG_VERSION);
        assert_eq!(
            loaded.runtime.session_management.cleanup_interval_secs,
            SessionManagementConfig::default().cleanup_interval_secs
        );
        assert_eq!(
            loaded.runtime.session_management.policies.len(),
            SessionManagementConfig::default().policies.len()
        );
        assert_eq!(loaded.providers.version, CURRENT_CONFIG_VERSION);
        assert_eq!(loaded.integrations.version, CURRENT_CONFIG_VERSION);
        assert_eq!(
            loaded.providers.entries[0].models.safety_review,
            default_safety_review_model()
        );
        assert_eq!(loaded.integrations.discord.task_wait_timeout_secs, 5);
        assert_eq!(
            loaded.integrations.discord.approval_action_ttl_secs,
            15 * 60
        );
        assert_eq!(loaded.integrations.discord.allowed_channels.len(), 2);
        assert_eq!(
            loaded.integrations.discord.allowed_channels[0].channel_id,
            "111"
        );
        assert_eq!(
            loaded.integrations.discord.allowed_channels[0].mode,
            DiscordChannelMode::Household
        );
        assert!(loaded.integrations.discord.approval_dm_only);
        assert_eq!(
            loaded.integrations.discord.critical_confirmation_ttl_secs,
            5 * 60
        );

        let runtime_rewritten = fs::read_to_string(&runtime_path).expect("runtime content");
        let providers_rewritten = fs::read_to_string(&providers_path).expect("providers content");
        let integrations_rewritten =
            fs::read_to_string(&integrations_path).expect("integrations content");

        assert!(runtime_rewritten.contains("version: 1"));
        assert!(runtime_rewritten.contains("session_management:"));
        assert!(providers_rewritten.contains("version: 1"));
        assert!(providers_rewritten.contains("safety_review: gpt-5.4-mini"));
        assert!(integrations_rewritten.contains("version: 1"));
        assert!(integrations_rewritten.contains("allowed_channels:"));
        assert!(integrations_rewritten.contains("channel_id:"));
        assert!(integrations_rewritten.contains("111"));
        assert!(integrations_rewritten.contains("mode: household"));
        assert!(!integrations_rewritten.contains("allowed_channel_ids:"));

        let runtime_backup =
            fs::read_to_string(backup_path_for(&runtime_path)).expect("runtime backup");
        let providers_backup =
            fs::read_to_string(backup_path_for(&providers_path)).expect("providers backup");
        let integrations_backup =
            fs::read_to_string(backup_path_for(&integrations_path)).expect("integrations backup");

        assert!(!runtime_backup.contains("version:"));
        assert!(!providers_backup.contains("safety_review:"));
        assert!(!integrations_backup.contains("version:"));
    }

    #[test]
    fn load_from_dir_rejects_future_config_version() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let config_dir = tempdir.path().join("config");
        fs::create_dir_all(&config_dir).expect("config dir");

        fs::write(
            config_dir.join("runtime.yaml"),
            r#"version: 999
instance_id: test-instance
mode: cli
data_dir: ./data
state_dir: ./state
log_dir: ./logs
secret_dir: ./secrets
database_path: ./state/runtime.db
workspace_root: .
default_timezone: UTC
features:
  markdown_exports: true
defaults:
  max_agent_steps: 8
"#,
        )
        .expect("write runtime");
        fs::write(
            config_dir.join("providers.yaml"),
            r#"version: 1
active: openai_main
entries:
  - provider_id: openai_main
    kind: openai
    auth:
      mode: api_token
      secret_ref: secret://local/openai_main_api_token
    models:
      planner: gpt-5.4
      executor: gpt-5.4-mini
      summarizer: gpt-5.4-mini
      safety_review: gpt-5.4-mini
"#,
        )
        .expect("write providers");

        let error = LoadedConfig::load_from_dir(&config_dir).expect_err("future version error");
        assert!(format!("{error:#}").contains("unsupported config version 999"));
    }

    #[test]
    fn session_policy_matching_respects_optional_filters() {
        let policy = SessionLifecyclePolicy {
            policy_id: "discord-room".to_owned(),
            enabled: true,
            action: SessionLifecycleAction::Archive,
            inactivity_after_secs: 600,
            session_kind: Some("group_room".to_owned()),
            connector_type: Some("discord".to_owned()),
            connector_target_prefix: Some("room-".to_owned()),
            audience_policy: Some("shared_room".to_owned()),
            max_memory_scope: Some("household".to_owned()),
        };
        let matching = SessionPolicyMatchInput {
            session_kind: "group_room",
            connector_type: Some("discord"),
            connector_target: Some("room-42"),
            audience_policy: "shared_room",
            max_memory_scope: "household",
        };
        let non_matching = SessionPolicyMatchInput {
            session_kind: "group_room",
            connector_type: Some("discord"),
            connector_target: Some("dm-42"),
            audience_policy: "shared_room",
            max_memory_scope: "household",
        };

        assert!(policy.matches(&matching));
        assert!(!policy.matches(&non_matching));
        assert_eq!(
            select_session_lifecycle_policy(&[policy], &matching)
                .expect("policy")
                .policy_id,
            "discord-room"
        );
    }
}
