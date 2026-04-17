use std::fs;
use std::io::{Cursor, Read, Write};
use std::iter;
use std::path::{Path, PathBuf};

use age::Decryptor;
use age::Encryptor;
use age::secrecy::SecretString;
use anyhow::{Context, Result, anyhow};
use directories::BaseDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
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
pub struct ProvidersConfig {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct IntegrationsConfig {
    pub browser: BrowserConfig,
    pub discord: DiscordConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BrowserConfig {
    pub interactive_launcher: Option<String>,
    pub headless_browser: Option<String>,
    pub headless_args: Vec<String>,
}

impl Default for BrowserConfig {
    fn default() -> Self {
        Self {
            interactive_launcher: None,
            headless_browser: None,
            headless_args: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscordConfig {
    pub enabled: bool,
    pub bot_token_secret_ref: Option<String>,
    pub command_prefix: String,
    pub allowed_channel_ids: Vec<String>,
    pub task_wait_timeout_secs: u64,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_token_secret_ref: None,
            command_prefix: "!bk".to_owned(),
            allowed_channel_ids: Vec::new(),
            task_wait_timeout_secs: 5,
        }
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

        let mut runtime: RuntimeConfig = serde_yaml::from_str(
            &fs::read_to_string(&runtime_path)
                .with_context(|| format!("failed to read {}", runtime_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", runtime_path.display()))?;
        let providers: ProvidersConfig = serde_yaml::from_str(
            &fs::read_to_string(&providers_path)
                .with_context(|| format!("failed to read {}", providers_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", providers_path.display()))?;
        let integrations = if integrations_path.exists() {
            serde_yaml::from_str::<IntegrationsConfig>(
                &fs::read_to_string(&integrations_path)
                    .with_context(|| format!("failed to read {}", integrations_path.display()))?,
            )
            .with_context(|| format!("failed to parse {}", integrations_path.display()))?
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
    };
    let providers = ProvidersConfig {
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

pub fn write_providers_config(
    config_dir: impl AsRef<Path>,
    providers: &ProvidersConfig,
) -> Result<PathBuf> {
    let path = config_dir.as_ref().join("providers.yaml");
    write_providers_config_path(&path, providers)?;
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

fn write_integrations_config_path(path: &Path, integrations: &IntegrationsConfig) -> Result<()> {
    fs::write(path, serde_yaml::to_string(integrations)?)
        .with_context(|| format!("failed to write {}", path.display()))
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
            },
            providers: ProvidersConfig {
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
}
