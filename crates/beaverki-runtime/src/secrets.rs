use anyhow::{Result, bail};
use beaverki_config::{LoadedConfig, SecretStore};
use beaverki_models::OpenAiProvider;

pub(crate) fn load_provider(config: &LoadedConfig, passphrase: &str) -> Result<OpenAiProvider> {
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