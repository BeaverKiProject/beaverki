use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use beaverki_config::{LoadedConfig, SecretStore};
use beaverki_models::{LmStudioProvider, ModelProvider, OpenAiProvider};

pub(crate) fn load_provider(
    config: &LoadedConfig,
    passphrase: &str,
) -> Result<Arc<dyn ModelProvider>> {
    let provider_entry = config.providers.active_provider()?;
    match provider_entry.kind.as_str() {
        "openai" => {
            if provider_entry.auth.mode != "api_token" {
                bail!(
                    "OpenAI provider auth mode '{}' is not implemented",
                    provider_entry.auth.mode
                );
            }

            let secret_ref = provider_entry.auth.secret_ref.as_deref().ok_or_else(|| {
                anyhow!(
                    "OpenAI provider '{}' requires auth.secret_ref",
                    provider_entry.provider_id
                )
            })?;
            let secret_store = SecretStore::new(&config.runtime.secret_dir);
            let api_token = secret_store.read_secret(secret_ref, passphrase)?;
            Ok(Arc::new(OpenAiProvider::from_entry(
                provider_entry,
                api_token,
            )?))
        }
        "lm_studio" => Ok(Arc::new(LmStudioProvider::from_entry(provider_entry)?)),
        other => bail!("unsupported provider kind '{}'", other),
    }
}

pub(crate) fn load_discord_bot_token(
    config: &LoadedConfig,
    passphrase: &str,
) -> Result<Option<String>> {
    if !config.integrations.discord.enabled {
        return Ok(None);
    }

    let Some(secret_ref) = config.integrations.discord.bot_token_secret_ref.as_deref() else {
        bail!("Discord integration is enabled but bot_token_secret_ref is not configured");
    };

    let secret_store = SecretStore::new(&config.runtime.secret_dir);
    Ok(Some(secret_store.read_secret(secret_ref, passphrase)?))
}

pub(crate) fn load_notion_api_token(
    config: &LoadedConfig,
    passphrase: &str,
) -> Result<Option<String>> {
    if !config.integrations.notion.enabled {
        return Ok(None);
    }

    let Some(secret_ref) = config.integrations.notion.api_token_secret_ref.as_deref() else {
        bail!("Notion integration is enabled but api_token_secret_ref is not configured");
    };

    let secret_store = SecretStore::new(&config.runtime.secret_dir);
    Ok(Some(secret_store.read_secret(secret_ref, passphrase)?))
}
