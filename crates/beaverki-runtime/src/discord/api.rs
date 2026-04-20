use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use beaverki_db::ConnectorIdentityRow;
use serde_json::{Value, json};
use tokio::time::{self, Instant, MissedTickBehavior};
use tracing::warn;

use crate::RuntimeDaemon;

use super::models::{
    DiscordCreateDmResponse, DiscordCreatedMessage, DiscordCurrentUser,
    DiscordDirectDeliveryResult, DiscordRateLimitBody, DiscordRegisteredCommand,
    DiscordRegisteredCommandOption,
};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_MAX_MESSAGE_LEN: usize = 1_900;
const DISCORD_TYPING_REFRESH_INTERVAL: Duration = Duration::from_secs(8);

#[cfg(test)]
static TEST_DIRECT_SEND_CAPTURE: OnceLock<Mutex<Option<Arc<Mutex<Vec<String>>>>>> = OnceLock::new();

fn discord_api_base() -> String {
    DISCORD_API_BASE.to_owned()
}

#[cfg(test)]
pub(crate) fn set_test_direct_send_capture(value: Option<Arc<Mutex<Vec<String>>>>) {
    *TEST_DIRECT_SEND_CAPTURE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("discord direct send capture lock") = value;
}

pub(crate) fn build_discord_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("beaverki/0.1")
        .build()
        .context("failed to build Discord HTTP client")
}

pub(super) async fn send_discord_message(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    content: &str,
) -> Result<String> {
    let mut last_message_id = None;
    for message_chunk in split_discord_message_chunks(content) {
        let response = http_client
            .post(format!(
                "{}/channels/{channel_id}/messages",
                discord_api_base()
            ))
            .header("Authorization", format!("Bot {token}"))
            .json(&json!({ "content": message_chunk }))
            .send()
            .await
            .with_context(|| format!("failed to send Discord message to channel {channel_id}"))?
            .error_for_status()
            .with_context(|| format!("Discord rejected message for channel {channel_id}"))?;
        let created = response
            .json::<DiscordCreatedMessage>()
            .await
            .with_context(|| {
                format!("failed to decode Discord message response for channel {channel_id}")
            })?;
        last_message_id = Some(created.id);
    }

    Ok(last_message_id.unwrap_or_default())
}

pub(crate) async fn send_direct_household_message(
    http_client: &reqwest::Client,
    token: &str,
    identity: &ConnectorIdentityRow,
    content: &str,
) -> Result<DiscordDirectDeliveryResult> {
    #[cfg(test)]
    if let Some(capture) = TEST_DIRECT_SEND_CAPTURE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("discord direct send capture lock")
        .clone()
    {
        capture.lock().expect("capture direct send").push(format!(
            "recipient={} content={}",
            identity.external_user_id, content
        ));
        return Ok(DiscordDirectDeliveryResult {
            target_kind: "discord_dm",
            target_ref: "dm-casey".to_owned(),
            message_id: "msg-casey-1".to_owned(),
        });
    }

    match create_discord_dm_channel(http_client, token, &identity.external_user_id).await {
        Ok(channel_id) => {
            let message_id = send_discord_message(http_client, token, &channel_id, content).await?;
            Ok(DiscordDirectDeliveryResult {
                target_kind: "discord_dm",
                target_ref: channel_id,
                message_id,
            })
        }
        Err(dm_error) => {
            let Some(channel_id) = identity.external_channel_id.as_deref() else {
                return Err(dm_error);
            };
            let message_id = send_discord_message(http_client, token, channel_id, content)
                .await
                .with_context(|| {
                    format!(
                        "failed direct Discord delivery after DM fallback from user {} to channel {}",
                        identity.external_user_id, channel_id
                    )
                })?;
            Ok(DiscordDirectDeliveryResult {
                target_kind: "discord_channel_fallback",
                target_ref: channel_id.to_owned(),
                message_id,
            })
        }
    }
}

pub(super) async fn sync_discord_global_commands(
    daemon: &RuntimeDaemon,
    http_client: &reqwest::Client,
    token: &str,
    application_id: &str,
) -> Result<()> {
    let commands = list_discord_global_commands(http_client, token, application_id).await?;
    let desired_commands = discord_global_command_payloads();
    if discord_command_sets_match(&commands, &desired_commands) {
        daemon
            .runtime
            .db
            .record_audit_event(
                "connector",
                "discord",
                "discord_command_registration_checked",
                json!({
                    "scope": "global",
                    "status": "already_synchronized",
                    "command_count": commands.len(),
                }),
            )
            .await?;
        return Ok(());
    }

    overwrite_discord_global_commands(http_client, token, application_id, &desired_commands)
        .await?;
    daemon
        .runtime
        .db
        .record_audit_event(
            "connector",
            "discord",
            "discord_command_registered",
            json!({
                "scope": "global",
                "status": "synchronized",
                "previous_command_count": commands.len(),
                "command_names": desired_commands
                    .iter()
                    .filter_map(|command| command.get("name").and_then(Value::as_str))
                    .collect::<Vec<_>>(),
            }),
        )
        .await?;
    Ok(())
}

pub(super) async fn fetch_discord_current_user_id(
    http_client: &reqwest::Client,
    token: &str,
) -> Result<String> {
    let current_user = http_client
        .get(format!("{}/users/@me", discord_api_base()))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .context("failed to fetch Discord bot identity")?
        .error_for_status()
        .context("Discord rejected bot identity lookup")?
        .json::<DiscordCurrentUser>()
        .await
        .context("failed to decode Discord bot identity response")?;
    Ok(current_user.id)
}

pub(super) async fn send_discord_interaction_callback(
    http_client: &reqwest::Client,
    interaction_id: &str,
    interaction_token: &str,
    payload: Value,
) -> Result<()> {
    http_client
        .post(format!(
            "{}/interactions/{interaction_id}/{interaction_token}/callback",
            discord_api_base()
        ))
        .json(&payload)
        .send()
        .await
        .with_context(|| {
            format!("failed to send Discord interaction callback for interaction {interaction_id}")
        })?
        .error_for_status()
        .with_context(|| {
            format!("Discord rejected interaction callback for interaction {interaction_id}")
        })?;
    Ok(())
}

pub(super) async fn edit_discord_interaction_response(
    http_client: &reqwest::Client,
    application_id: &str,
    interaction_token: &str,
    content: &str,
) -> Result<()> {
    let message = truncate_reply(content);
    http_client
        .patch(format!(
            "{}/webhooks/{application_id}/{interaction_token}/messages/@original",
            discord_api_base()
        ))
        .json(&json!({ "content": message }))
        .send()
        .await
        .with_context(|| {
            format!("failed to edit Discord interaction response for application {application_id}")
        })?
        .error_for_status()
        .with_context(|| {
            format!("Discord rejected interaction response edit for application {application_id}")
        })?;
    Ok(())
}

pub(super) async fn add_discord_reaction(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    message_id: &str,
    emoji_path: &str,
) -> Result<()> {
    http_client
        .put(format!(
            "{}/channels/{channel_id}/messages/{message_id}/reactions/{emoji_path}/@me",
            discord_api_base()
        ))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .with_context(|| {
            format!(
                "failed to add Discord reaction to message {message_id} in channel {channel_id}"
            )
        })?
        .error_for_status()
        .with_context(|| {
            format!(
                "Discord rejected reaction add for message {message_id} in channel {channel_id}"
            )
        })?;
    Ok(())
}

pub(super) async fn delete_discord_reaction(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    message_id: &str,
    emoji_path: &str,
) -> Result<()> {
    http_client
        .delete(format!(
            "{}/channels/{channel_id}/messages/{message_id}/reactions/{emoji_path}/@me",
            discord_api_base()
        ))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .with_context(|| {
            format!(
                "failed to delete Discord reaction from message {message_id} in channel {channel_id}"
            )
        })?
        .error_for_status()
        .with_context(|| {
            format!(
                "Discord rejected reaction delete for message {message_id} in channel {channel_id}"
            )
        })?;
    Ok(())
}

pub(super) async fn with_discord_typing<F, T>(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let stop_signal = Arc::new(tokio::sync::Notify::new());
    let typing_http_client = http_client.clone();
    let typing_token = token.to_owned();
    let typing_channel_id = channel_id.to_owned();
    let typing_stop_signal = Arc::clone(&stop_signal);
    let typing_task = tokio::spawn(async move {
        if let Err(error) =
            send_discord_typing(&typing_http_client, &typing_token, &typing_channel_id).await
        {
            warn!(
                "failed to send Discord typing indicator for channel {}: {error:#}",
                typing_channel_id
            );
        }

        let mut typing_refresh = time::interval_at(
            Instant::now() + DISCORD_TYPING_REFRESH_INTERVAL,
            DISCORD_TYPING_REFRESH_INTERVAL,
        );
        typing_refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = typing_stop_signal.notified() => break,
                _ = typing_refresh.tick() => {
                    if let Err(error) = send_discord_typing(
                        &typing_http_client,
                        &typing_token,
                        &typing_channel_id,
                    )
                    .await
                    {
                        warn!(
                            "failed to refresh Discord typing indicator for channel {}: {error:#}",
                            typing_channel_id
                        );
                    }
                }
            }
        }
    });

    let result = future.await;
    stop_signal.notify_one();
    if let Err(error) = typing_task.await {
        warn!("Discord typing task join failed for channel {channel_id}: {error}");
    }
    result
}

pub(super) fn discord_global_command_payloads() -> Vec<Value> {
    vec![
        json!({
            "name": "new",
            "type": 1,
            "description": "Start a new BeaverKI conversation",
            "contexts": [0, 1],
        }),
        json!({
            "name": "status",
            "type": 1,
            "description": "Show the current BeaverKI session status",
            "contexts": [0, 1],
        }),
        json!({
            "name": "approval",
            "type": 1,
            "description": "Inspect or resolve BeaverKI approvals",
            "contexts": [0, 1],
            "options": [
                {
                    "type": 1,
                    "name": "list",
                    "description": "Show pending BeaverKI approvals"
                },
                {
                    "type": 1,
                    "name": "inspect",
                    "description": "Inspect a BeaverKI approval token",
                    "options": [
                        {
                            "type": 3,
                            "name": "token",
                            "description": "The BeaverKI approval token",
                            "required": true
                        }
                    ]
                },
                {
                    "type": 1,
                    "name": "approve",
                    "description": "Approve a BeaverKI approval token",
                    "options": [
                        {
                            "type": 3,
                            "name": "token",
                            "description": "The BeaverKI approval token",
                            "required": true
                        }
                    ]
                },
                {
                    "type": 1,
                    "name": "deny",
                    "description": "Deny a BeaverKI approval token",
                    "options": [
                        {
                            "type": 3,
                            "name": "token",
                            "description": "The BeaverKI approval token",
                            "required": true
                        }
                    ]
                },
                {
                    "type": 1,
                    "name": "confirm",
                    "description": "Confirm a critical BeaverKI approval token",
                    "options": [
                        {
                            "type": 3,
                            "name": "token",
                            "description": "The BeaverKI approval token",
                            "required": true
                        }
                    ]
                }
            ]
        }),
    ]
}

pub(super) fn discord_command_sets_match(
    commands: &[DiscordRegisteredCommand],
    desired_commands: &[Value],
) -> bool {
    if commands.len() != desired_commands.len() {
        return false;
    }

    commands.iter().all(|command| {
        desired_commands
            .iter()
            .any(|desired| discord_registered_command_matches_payload(command, desired))
    })
}

pub(super) fn discord_registered_command_matches_payload(
    command: &DiscordRegisteredCommand,
    desired_command: &Value,
) -> bool {
    command.name
        == desired_command
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
        && command.command_type
            == desired_command
                .get("type")
                .and_then(Value::as_i64)
                .unwrap_or_default()
        && command.description
            == desired_command
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
        && normalized_discord_command_contexts(command.contexts.as_deref())
            == normalized_discord_command_contexts_from_value(desired_command.get("contexts"))
        && command.options
            == normalized_discord_command_options_from_value(desired_command.get("options"))
}

pub(super) fn normalized_discord_command_options_from_value(
    options: Option<&Value>,
) -> Vec<DiscordRegisteredCommandOption> {
    options
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<DiscordRegisteredCommandOption>>(value).ok())
        .unwrap_or_default()
}

pub(super) async fn parse_retry_after_secs(response: reqwest::Response) -> Option<u64> {
    if let Some(header_value) = response.headers().get("retry-after")
        && let Ok(header_value) = header_value.to_str()
    {
        if let Ok(seconds) = header_value.parse::<u64>() {
            return Some(seconds.max(1));
        }
        if let Ok(seconds) = header_value.parse::<f64>() {
            return Some(seconds.ceil().max(1.0) as u64);
        }
    }

    let body = response.text().await.ok()?;
    let parsed = serde_json::from_str::<DiscordRateLimitBody>(&body).ok()?;
    parsed
        .retry_after
        .map(|seconds| seconds.ceil().max(1.0) as u64)
}

pub(super) fn retry_delay_for_error(error: &anyhow::Error) -> Option<Duration> {
    let text = error.to_string();
    let marker = "retry_after_secs=";
    let start = text.find(marker)? + marker.len();
    let end = text[start..]
        .find(|ch: char| !ch.is_ascii_digit())
        .map(|offset| start + offset)
        .unwrap_or(text.len());
    let seconds = text[start..end].parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds.max(1)))
}

pub(super) fn truncate_reply(reply: &str) -> String {
    if reply.chars().count() <= DISCORD_MAX_MESSAGE_LEN {
        reply.to_owned()
    } else {
        let shortened: String = reply.chars().take(DISCORD_MAX_MESSAGE_LEN).collect();
        format!("{shortened}...")
    }
}

fn split_discord_message_chunks(message: &str) -> Vec<String> {
    if message.is_empty() {
        return vec![String::new()];
    }

    let mut chunks = Vec::new();
    let mut remaining = message;
    while !remaining.is_empty() {
        if remaining.chars().count() <= DISCORD_MAX_MESSAGE_LEN {
            chunks.push(remaining.to_owned());
            break;
        }

        let split_at = discord_chunk_boundary(remaining);
        let (chunk, rest) = remaining.split_at(split_at);
        chunks.push(chunk.to_owned());
        remaining = rest;
    }

    chunks
}

fn discord_chunk_boundary(message: &str) -> usize {
    let mut last_boundary = 0usize;
    let mut last_paragraph_boundary = None;
    let mut last_line_boundary = None;
    let mut last_space_boundary = None;

    for (idx, ch) in message.char_indices() {
        let next_idx = idx + ch.len_utf8();
        if next_idx > DISCORD_MAX_MESSAGE_LEN {
            break;
        }
        last_boundary = next_idx;

        if ch == '\n' {
            last_line_boundary = Some(next_idx);
        } else if ch.is_whitespace() {
            last_space_boundary = Some(next_idx);
        }

        if message[..next_idx].ends_with("\n\n") {
            last_paragraph_boundary = Some(next_idx);
        }
    }

    last_paragraph_boundary
        .or(last_line_boundary)
        .or(last_space_boundary)
        .unwrap_or(last_boundary)
}

async fn create_discord_dm_channel(
    http_client: &reqwest::Client,
    token: &str,
    recipient_user_id: &str,
) -> Result<String> {
    let response = http_client
        .post(format!("{}/users/@me/channels", discord_api_base()))
        .header("Authorization", format!("Bot {token}"))
        .json(&json!({ "recipient_id": recipient_user_id }))
        .send()
        .await
        .with_context(|| {
            format!("failed to create Discord DM channel for user {recipient_user_id}")
        })?
        .error_for_status()
        .with_context(|| {
            format!("Discord rejected DM channel creation for user {recipient_user_id}")
        })?;
    let created = response
        .json::<DiscordCreateDmResponse>()
        .await
        .with_context(|| {
            format!("failed to decode Discord DM channel response for user {recipient_user_id}")
        })?;
    Ok(created.id)
}

async fn list_discord_global_commands(
    http_client: &reqwest::Client,
    token: &str,
    application_id: &str,
) -> Result<Vec<DiscordRegisteredCommand>> {
    http_client
        .get(format!(
            "{}/applications/{application_id}/commands",
            discord_api_base()
        ))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .with_context(|| {
            format!("failed to list Discord commands for application {application_id}")
        })?
        .error_for_status()
        .with_context(|| {
            format!("Discord rejected command listing for application {application_id}")
        })?
        .json::<Vec<DiscordRegisteredCommand>>()
        .await
        .with_context(|| {
            format!("failed to decode Discord command list for application {application_id}")
        })
}

async fn overwrite_discord_global_commands(
    http_client: &reqwest::Client,
    token: &str,
    application_id: &str,
    payload: &[Value],
) -> Result<()> {
    http_client
        .put(format!(
            "{}/applications/{application_id}/commands",
            discord_api_base()
        ))
        .header("Authorization", format!("Bot {token}"))
        .json(&payload)
        .send()
        .await
        .with_context(|| {
            format!("failed to overwrite Discord commands for application {application_id}")
        })?
        .error_for_status()
        .with_context(|| {
            format!("Discord rejected command overwrite for application {application_id}")
        })?;
    Ok(())
}

fn normalized_discord_command_contexts(contexts: Option<&[i64]>) -> Vec<i64> {
    let mut contexts = contexts.unwrap_or(&[]).to_vec();
    contexts.sort_unstable();
    contexts.dedup();
    contexts
}

fn normalized_discord_command_contexts_from_value(contexts: Option<&Value>) -> Vec<i64> {
    let mut contexts = contexts
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_i64)
        .collect::<Vec<_>>();
    contexts.sort_unstable();
    contexts.dedup();
    contexts
}

async fn send_discord_typing(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
) -> Result<()> {
    http_client
        .post(format!(
            "{}/channels/{channel_id}/typing",
            discord_api_base()
        ))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .with_context(|| {
            format!("failed to send Discord typing indicator for channel {channel_id}")
        })?
        .error_for_status()
        .with_context(|| format!("Discord rejected typing indicator for channel {channel_id}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DISCORD_MAX_MESSAGE_LEN, split_discord_message_chunks, truncate_reply};

    #[test]
    fn truncate_reply_shortens_oversized_interaction_content() {
        let reply = "x".repeat(DISCORD_MAX_MESSAGE_LEN + 20);
        let truncated = truncate_reply(&reply);
        assert_eq!(truncated.chars().count(), DISCORD_MAX_MESSAGE_LEN + 3);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn split_discord_message_chunks_keeps_short_messages_intact() {
        let message = "Short Discord message.";
        assert_eq!(
            split_discord_message_chunks(message),
            vec![message.to_owned()]
        );
    }

    #[test]
    fn split_discord_message_chunks_splits_long_messages_without_losing_text() {
        let part_a = "a".repeat(DISCORD_MAX_MESSAGE_LEN - 10);
        let part_b = "b".repeat(DISCORD_MAX_MESSAGE_LEN - 20);
        let part_c = "c".repeat(100);
        let message = format!("{part_a}\n\n{part_b}\n{part_c}");

        let chunks = split_discord_message_chunks(&message);
        assert!(chunks.len() >= 2);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.chars().count() <= DISCORD_MAX_MESSAGE_LEN)
        );
        assert_eq!(chunks.concat(), message);
    }

    #[test]
    fn split_discord_message_chunks_handles_unbroken_text() {
        let message = "x".repeat((DISCORD_MAX_MESSAGE_LEN * 2) + 17);
        let chunks = split_discord_message_chunks(&message);

        assert_eq!(chunks.len(), 3);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.chars().count() <= DISCORD_MAX_MESSAGE_LEN)
        );
        assert_eq!(chunks.concat(), message);
    }
}
