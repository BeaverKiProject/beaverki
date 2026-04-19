use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use beaverki_db::{
    ApprovalActionRow, ApprovalActionSet, ApprovalRow, ConnectorIdentityRow,
    ConversationSessionRow, Database, TaskEventRow, TaskRow,
};
use beaverki_policy::can_grant_approvals;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::{self, Instant, MissedTickBehavior};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;

use crate::{
    ConnectorMessageReply, ConnectorMessageRequest, RemoteApprovalActionOutcome, RuntimeDaemon,
};
use beaverki_core::{MemoryScope, TaskState};

const DISCORD_GATEWAY_URL: &str = "https://discord.com/api/v10/gateway/bot";
const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_INTENTS: i64 = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);
const DISCORD_MAX_MESSAGE_LEN: usize = 1_900;
const DISCORD_CONVERSATION_HISTORY_LIMIT: i64 = 4;
const DISCORD_ACTIVE_CONVERSATION_WINDOW_SECS: i64 = 45 * 60;
const DISCORD_TYPING_REFRESH_INTERVAL: Duration = Duration::from_secs(8);
const DISCORD_REACTION_WORKING: &str = "%E2%8F%B3";
const CONNECTOR_MESSAGE_CONTEXT_EVENT: &str = "connector_message_context";
const CONNECTOR_FOLLOW_UP_REQUESTED_EVENT: &str = "connector_follow_up_requested";
const CONNECTOR_FOLLOW_UP_SENT_EVENT: &str = "connector_follow_up_sent";
const APPROVAL_ACTION_TOKEN_PREFIX: &str = "approval_token_";

#[cfg(test)]
static DISCORD_API_BASE_OVERRIDE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[cfg(test)]
static TEST_DIRECT_SEND_CAPTURE: OnceLock<Mutex<Option<Arc<Mutex<Vec<String>>>>>> = OnceLock::new();

pub(crate) struct DiscordDirectDeliveryResult {
    pub target_kind: &'static str,
    pub target_ref: String,
    pub message_id: String,
}

#[derive(Debug, Deserialize)]
struct DiscordGatewayInfo {
    url: String,
}

#[derive(Debug, Deserialize)]
struct DiscordRateLimitBody {
    retry_after: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DiscordMessageCreate {
    id: String,
    channel_id: String,
    guild_id: Option<String>,
    content: String,
    author: DiscordAuthor,
}

#[derive(Debug, Deserialize)]
struct DiscordAuthor {
    id: String,
    bot: Option<bool>,
    username: Option<String>,
    global_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiscordCurrentUser {
    id: String,
}

#[derive(Debug, Deserialize)]
struct DiscordCreateDmResponse {
    id: String,
}

#[derive(Debug, Deserialize)]
struct DiscordCreatedMessage {
    id: String,
}

#[derive(Debug, Deserialize)]
struct DiscordRegisteredCommand {
    name: String,
    description: String,
    #[serde(rename = "type")]
    command_type: i64,
    contexts: Option<Vec<i64>>,
    #[serde(default)]
    options: Vec<DiscordRegisteredCommandOption>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct DiscordRegisteredCommandOption {
    #[serde(rename = "type")]
    option_type: i64,
    name: String,
    description: String,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    options: Vec<DiscordRegisteredCommandOption>,
}

#[derive(Debug, Deserialize)]
struct DiscordInteractionCreate {
    id: String,
    application_id: String,
    #[serde(rename = "type")]
    interaction_type: i64,
    token: String,
    channel_id: Option<String>,
    guild_id: Option<String>,
    data: Option<DiscordInteractionData>,
    member: Option<DiscordInteractionMember>,
    user: Option<DiscordAuthor>,
}

#[derive(Debug, Deserialize)]
struct DiscordInteractionMember {
    user: Option<DiscordAuthor>,
}

#[derive(Debug, Deserialize)]
struct DiscordInteractionData {
    name: String,
    #[serde(default)]
    options: Vec<DiscordInteractionOption>,
}

#[derive(Debug, Deserialize)]
struct DiscordInteractionOption {
    name: String,
    value: Option<Value>,
    #[serde(default)]
    options: Vec<DiscordInteractionOption>,
}

pub(crate) async fn run_discord_loop(daemon: Arc<RuntimeDaemon>) -> Result<()> {
    let token = daemon
        .runtime
        .discord_bot_token()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("Discord connector is enabled but no bot token is loaded"))?;
    let http_client = build_discord_http_client()?;
    let bot_user_id = fetch_discord_current_user_id(&http_client, &token).await?;

    if let Err(error) =
        sync_discord_global_commands(&daemon, &http_client, &token, &bot_user_id).await
    {
        warn!("failed to sync Discord global commands: {error:#}");
        daemon
            .runtime
            .db
            .record_audit_event(
                "connector",
                "discord",
                "discord_command_registration_error",
                json!({
                    "scope": "global",
                    "error": error.to_string(),
                }),
            )
            .await?;
    }

    daemon
        .runtime
        .db
        .record_audit_event(
            "connector",
            "discord",
            "discord_connector_started",
            json!({
                "allowed_channels": daemon.runtime.config.integrations.discord.allowed_channels,
                "command_prefix": daemon.runtime.config.integrations.discord.command_prefix,
            }),
        )
        .await?;

    let mut retry_delay = Duration::from_secs(1);
    loop {
        if daemon.shutdown_notified().await {
            return Ok(());
        }

        match run_gateway_session(Arc::clone(&daemon), &http_client, &token, &bot_user_id).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                warn!("discord connector session failed: {error:#}");
                daemon
                    .runtime
                    .db
                    .record_audit_event(
                        "connector",
                        "discord",
                        "discord_connector_error",
                        json!({ "error": error.to_string() }),
                    )
                    .await?;
                if daemon.shutdown_notified().await {
                    return Ok(());
                }
                retry_delay = retry_delay_for_error(&error).unwrap_or(retry_delay);
                time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

async fn run_gateway_session(
    daemon: Arc<RuntimeDaemon>,
    http_client: &reqwest::Client,
    token: &str,
    bot_user_id: &str,
) -> Result<()> {
    let gateway_response = http_client
        .get(DISCORD_GATEWAY_URL)
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .context("failed to fetch Discord gateway URL")?;
    if gateway_response.status() == StatusCode::TOO_MANY_REQUESTS {
        let retry_after = parse_retry_after_secs(gateway_response).await;
        if let Some(retry_after) = retry_after {
            bail!("Discord gateway URL request rate limited; retry_after_secs={retry_after}");
        }
        bail!("Discord gateway URL request rate limited");
    }
    let gateway_info = gateway_response
        .error_for_status()
        .context("Discord gateway URL request failed")?
        .json::<DiscordGatewayInfo>()
        .await
        .context("failed to decode Discord gateway URL response")?;
    let gateway_url = build_gateway_websocket_url(&gateway_info.url);
    let (mut socket, _) = connect_async(&gateway_url)
        .await
        .with_context(|| format!("failed to connect to Discord gateway at {gateway_url}"))?;

    let mut sequence_number: Option<i64> = None;
    let mut heartbeat = time::interval(Duration::from_secs(60));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut identified = false;

    loop {
        tokio::select! {
            _ = daemon.shutdown.notified() => {
                let _ = socket.close(None).await;
                return Ok(());
            }
            _ = heartbeat.tick(), if identified => {
                let heartbeat_payload = json!({
                    "op": 1,
                    "d": sequence_number,
                });
                socket
                    .send(Message::Text(heartbeat_payload.to_string()))
                    .await
                    .context("failed to send Discord heartbeat")?;
            }
            next_message = socket.next() => {
                let Some(next_message) = next_message else {
                    bail!("Discord gateway stream ended unexpectedly");
                };
                let next_message = next_message.context("Discord gateway message error")?;
                match next_message {
                    Message::Text(text) => {
                        let payload: Value = serde_json::from_str(&text)
                            .context("failed to decode Discord gateway payload")?;
                        if let Some(sequence) = payload.get("s").and_then(Value::as_i64) {
                            sequence_number = Some(sequence);
                        }

                        match payload.get("op").and_then(Value::as_i64).unwrap_or_default() {
                            0 => {
                                if let Some(event_type) = payload.get("t").and_then(Value::as_str) {
                                    match event_type {
                                        "READY" => {
                                            daemon.runtime.db.record_audit_event(
                                                "connector",
                                                "discord",
                                                "discord_connector_ready",
                                                json!({ "session": payload.get("d") }),
                                            ).await?;
                                        }
                                        "MESSAGE_CREATE" => {
                                            let message = serde_json::from_value::<DiscordMessageCreate>(
                                                payload.get("d").cloned().unwrap_or(Value::Null),
                                            )
                                            .context("failed to decode Discord message event")?;
                                            if message.author.bot.unwrap_or(false) {
                                                continue;
                                            }
                                            let external_display_name = discord_author_display_name(&message.author);
                                            let reply = daemon
                                                .handle_discord_gateway_message(
                                                    http_client,
                                                    token,
                                                    bot_user_id,
                                                    ConnectorMessageRequest {
                                                        connector_type: "discord".to_owned(),
                                                        external_user_id: message.author.id,
                                                        external_display_name,
                                                        channel_id: message.channel_id.clone(),
                                                        message_id: message.id,
                                                        content: rewrite_channel_message_for_bot_mention(
                                                            &message.content,
                                                            message.guild_id.is_none(),
                                                            &daemon.runtime.config.integrations.discord.command_prefix,
                                                            bot_user_id,
                                                        ),
                                                        is_direct_message: message.guild_id.is_none(),
                                                    },
                                                )
                                                .await?;
                                            if let Some(reply_text) = reply.reply {
                                                let _ = send_discord_message(
                                                    http_client,
                                                    token,
                                                    &message.channel_id,
                                                    &reply_text,
                                                )
                                                .await?;
                                            }
                                        }
                                        "INTERACTION_CREATE" => {
                                            let interaction = serde_json::from_value::<DiscordInteractionCreate>(
                                                payload.get("d").cloned().unwrap_or(Value::Null),
                                            )
                                            .context("failed to decode Discord interaction event")?;
                                            daemon
                                                .handle_discord_interaction(http_client, interaction)
                                                .await?;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            1 => {
                                let heartbeat_payload = json!({
                                    "op": 1,
                                    "d": sequence_number,
                                });
                                socket
                                    .send(Message::Text(heartbeat_payload.to_string()))
                                    .await
                                    .context("failed to send Discord heartbeat request response")?;
                            }
                            7 => bail!("Discord gateway requested reconnect"),
                            9 => bail!("Discord gateway invalidated the session"),
                            10 => {
                                let heartbeat_interval_ms = payload
                                    .get("d")
                                    .and_then(|value| value.get("heartbeat_interval"))
                                    .and_then(Value::as_u64)
                                    .ok_or_else(|| anyhow!("Discord gateway hello payload missing heartbeat interval"))?;
                                heartbeat = time::interval(Duration::from_millis(heartbeat_interval_ms));
                                heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                                let identify_payload = json!({
                                    "op": 2,
                                    "d": {
                                        "token": token,
                                        "intents": DISCORD_INTENTS,
                                        "properties": {
                                            "os": std::env::consts::OS,
                                            "browser": "beaverki",
                                            "device": "beaverki"
                                        }
                                    }
                                });
                                socket
                                    .send(Message::Text(identify_payload.to_string()))
                                    .await
                                    .context("failed to send Discord identify payload")?;
                                identified = true;
                            }
                            11 => {}
                            _ => {}
                        }
                    }
                    Message::Ping(payload) => {
                        socket
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to respond to Discord ping")?;
                    }
                    Message::Close(frame) => {
                        bail!("Discord gateway closed: {frame:?}");
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn discord_api_base() -> String {
    #[cfg(test)]
    if let Some(override_base) = DISCORD_API_BASE_OVERRIDE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("discord api base override lock")
        .clone()
    {
        return override_base;
    }

    DISCORD_API_BASE.to_owned()
}

#[cfg(test)]
pub(crate) fn set_test_discord_api_base(value: Option<String>) {
    *DISCORD_API_BASE_OVERRIDE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("discord api base override lock") = value;
}

#[cfg(test)]
pub(crate) fn set_test_direct_send_capture(value: Option<Arc<Mutex<Vec<String>>>>) {
    *TEST_DIRECT_SEND_CAPTURE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("discord direct send capture lock") = value;
}

async fn send_discord_message(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    content: &str,
) -> Result<String> {
    let message = truncate_reply(content);
    let response = http_client
        .post(format!("{}/channels/{channel_id}/messages", discord_api_base()))
        .header("Authorization", format!("Bot {token}"))
        .json(&json!({ "content": message }))
        .send()
        .await
        .with_context(|| format!("failed to send Discord message to channel {channel_id}"))?
        .error_for_status()
        .with_context(|| format!("Discord rejected message for channel {channel_id}"))?;
    let created = response
        .json::<DiscordCreatedMessage>()
        .await
        .with_context(|| format!("failed to decode Discord message response for channel {channel_id}"))?;
    Ok(created.id)
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

async fn sync_discord_global_commands(
    daemon: &RuntimeDaemon,
    http_client: &reqwest::Client,
    token: &str,
    application_id: &str,
) -> Result<()> {
    let commands = list_discord_global_commands(http_client, token, &application_id).await?;
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

    overwrite_discord_global_commands(http_client, token, &application_id, &desired_commands)
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

async fn fetch_discord_current_user_id(
    http_client: &reqwest::Client,
    token: &str,
) -> Result<String> {
    let current_user = http_client
        .get(format!("{DISCORD_API_BASE}/users/@me"))
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

async fn list_discord_global_commands(
    http_client: &reqwest::Client,
    token: &str,
    application_id: &str,
) -> Result<Vec<DiscordRegisteredCommand>> {
    http_client
        .get(format!(
            "{DISCORD_API_BASE}/applications/{application_id}/commands"
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
            "{DISCORD_API_BASE}/applications/{application_id}/commands"
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

fn discord_global_command_payloads() -> Vec<Value> {
    vec![
        json!({
            "name": "new",
            "type": 1,
            "description": "Start a new BeaverKI conversation",
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

fn discord_command_sets_match(
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

fn discord_registered_command_matches_payload(
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

fn normalized_discord_command_options_from_value(
    options: Option<&Value>,
) -> Vec<DiscordRegisteredCommandOption> {
    options
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<DiscordRegisteredCommandOption>>(value).ok())
        .unwrap_or_default()
}

async fn send_discord_interaction_callback(
    http_client: &reqwest::Client,
    interaction_id: &str,
    interaction_token: &str,
    payload: Value,
) -> Result<()> {
    http_client
        .post(format!(
            "{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
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

async fn edit_discord_interaction_response(
    http_client: &reqwest::Client,
    application_id: &str,
    interaction_token: &str,
    content: &str,
) -> Result<()> {
    let message = truncate_reply(content);
    http_client
        .patch(format!(
            "{DISCORD_API_BASE}/webhooks/{application_id}/{interaction_token}/messages/@original"
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

async fn add_discord_reaction(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    message_id: &str,
    emoji_path: &str,
) -> Result<()> {
    http_client
        .put(format!(
            "{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{emoji_path}/@me"
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

async fn delete_discord_reaction(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    message_id: &str,
    emoji_path: &str,
) -> Result<()> {
    http_client
        .delete(format!(
            "{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{emoji_path}/@me"
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

pub(crate) fn build_discord_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("beaverki/0.1")
        .build()
        .context("failed to build Discord HTTP client")
}

async fn send_discord_typing(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
) -> Result<()> {
    http_client
        .post(format!("{DISCORD_API_BASE}/channels/{channel_id}/typing"))
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

async fn with_discord_typing<F, T>(
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

async fn wait_for_discord_task_completion(
    daemon: &RuntimeDaemon,
    owner_user_id: &str,
    task_id: &str,
    timeout_secs: u64,
    typing_context: Option<(&reqwest::Client, &str, &str)>,
) -> Result<Option<TaskRow>> {
    let wait_for_task = async {
        match time::timeout(
            Duration::from_secs(timeout_secs),
            daemon.wait_for_task_state(owner_user_id, task_id),
        )
        .await
        {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    };

    match typing_context {
        Some((http_client, token, channel_id)) => {
            with_discord_typing(http_client, token, channel_id, wait_for_task).await
        }
        None => wait_for_task.await,
    }
}

pub(super) async fn maybe_send_task_follow_up(
    daemon: &RuntimeDaemon,
    task: &TaskRow,
    events: &[TaskEventRow],
) -> Result<()> {
    let Some(channel_id) = connector_follow_up_channel(events, "discord", &task.state) else {
        return Ok(());
    };
    let Some(token) = daemon.runtime.discord_bot_token() else {
        return Ok(());
    };

    let http_client = build_discord_http_client()?;
    let reply = format_task_reply(task);
    let _ = send_discord_message(&http_client, token, &channel_id, &reply).await?;
    if let Some(context) = connector_context_from_events(events)
        && context.connector_type == "discord"
        && let Some(message_id) = context.message_id.as_deref()
        && let Err(error) = delete_discord_reaction(
            &http_client,
            token,
            &channel_id,
            message_id,
            DISCORD_REACTION_WORKING,
        )
        .await
    {
        warn!(
            "failed to clear Discord working reaction from message {} in channel {}: {error:#}",
            message_id, channel_id
        );
    }
    daemon
        .runtime
        .db
        .append_task_event(
            &task.task_id,
            CONNECTOR_FOLLOW_UP_SENT_EVENT,
            "connector",
            "discord",
            json!({
                "connector_type": "discord",
                "channel_id": channel_id,
                "task_state": task.state,
            }),
        )
        .await?;
    daemon
        .runtime
        .db
        .record_audit_event(
            "connector",
            &task.task_id,
            "connector_follow_up_sent",
            json!({
                "connector_type": "discord",
                "channel_id": channel_id,
                "task_state": task.state,
            }),
        )
        .await?;

    Ok(())
}

impl RuntimeDaemon {
    async fn handle_discord_interaction(
        &self,
        http_client: &reqwest::Client,
        interaction: DiscordInteractionCreate,
    ) -> Result<()> {
        match interaction.interaction_type {
            1 => {
                send_discord_interaction_callback(
                    http_client,
                    &interaction.id,
                    &interaction.token,
                    json!({ "type": 1 }),
                )
                .await?;
                Ok(())
            }
            2 => {
                let interaction_id = interaction.id.clone();
                let application_id = interaction.application_id.clone();
                let interaction_token = interaction.token.clone();
                send_discord_interaction_callback(
                    http_client,
                    &interaction_id,
                    &interaction_token,
                    json!({ "type": 5 }),
                )
                .await?;

                let reply_text = match self.handle_discord_application_command(interaction).await {
                    Ok(Some(reply_text)) => reply_text,
                    Ok(None) => "This Discord command is not enabled in this context.".to_owned(),
                    Err(error) => {
                        warn!("discord interaction handling failed: {error:#}");
                        format!("Discord command failed: {error}")
                    }
                };

                edit_discord_interaction_response(
                    http_client,
                    &application_id,
                    &interaction_token,
                    &reply_text,
                )
                .await
            }
            _ => Ok(()),
        }
    }

    async fn handle_discord_application_command(
        &self,
        interaction: DiscordInteractionCreate,
    ) -> Result<Option<String>> {
        let request = self.discord_interaction_to_message_request(&interaction)?;
        let reply = self.handle_connector_message(request).await?;
        Ok(reply.reply)
    }

    fn discord_interaction_to_message_request(
        &self,
        interaction: &DiscordInteractionCreate,
    ) -> Result<ConnectorMessageRequest> {
        let channel_id = interaction
            .channel_id
            .clone()
            .ok_or_else(|| anyhow!("Discord interaction missing channel_id"))?;
        let author = interaction
            .user
            .as_ref()
            .or_else(|| {
                interaction
                    .member
                    .as_ref()
                    .and_then(|member| member.user.as_ref())
            })
            .ok_or_else(|| anyhow!("Discord interaction missing user payload"))?;
        let data = interaction
            .data
            .as_ref()
            .ok_or_else(|| anyhow!("Discord application command missing data payload"))?;
        let command_text = interaction_command_text(data)?;
        let is_direct_message = interaction.guild_id.is_none();
        let content = if is_direct_message {
            command_text
        } else {
            let prefix = self
                .runtime
                .config
                .integrations
                .discord
                .command_prefix
                .trim();
            if prefix.is_empty() {
                command_text
            } else {
                format!("{prefix} {command_text}")
            }
        };

        Ok(ConnectorMessageRequest {
            connector_type: "discord".to_owned(),
            external_user_id: author.id.clone(),
            external_display_name: discord_author_display_name(author),
            channel_id,
            message_id: String::new(),
            content,
            is_direct_message,
        })
    }

    async fn handle_discord_gateway_message(
        &self,
        http_client: &reqwest::Client,
        token: &str,
        _bot_user_id: &str,
        message: ConnectorMessageRequest,
    ) -> Result<ConnectorMessageReply> {
        self.handle_connector_message_internal(message, Some((http_client, token)))
            .await
    }

    pub(crate) async fn handle_connector_message(
        &self,
        message: ConnectorMessageRequest,
    ) -> Result<ConnectorMessageReply> {
        self.handle_connector_message_internal(message, None).await
    }

    async fn handle_connector_message_internal(
        &self,
        message: ConnectorMessageRequest,
        typing_context: Option<(&reqwest::Client, &str)>,
    ) -> Result<ConnectorMessageReply> {
        if message.connector_type != "discord" {
            bail!("unsupported connector type '{}'", message.connector_type);
        }

        self.runtime
            .db
            .record_audit_event(
                "connector",
                &format!("discord:{}", message.external_user_id),
                "connector_message_received",
                json!({
                    "message_id": message.message_id,
                    "channel_id": message.channel_id,
                    "is_direct_message": message.is_direct_message,
                }),
            )
            .await?;

        let discord_config = &self.runtime.config.integrations.discord;
        if !message.is_direct_message && !discord_config.is_allowed_channel(&message.channel_id) {
            let attempted_trigger = is_channel_trigger_attempt(
                message.content.as_str(),
                discord_config.command_prefix.as_str(),
            );
            if attempted_trigger {
                warn!(
                    channel_id = %message.channel_id,
                    external_user_id = %message.external_user_id,
                    message_id = %message.message_id,
                    "Discord connector ignored a command-like message from a non-allowlisted channel"
                );
            }
            self.runtime
                .db
                .record_audit_event(
                    "connector",
                    &format!("discord:{}", message.external_user_id),
                    "connector_message_ignored",
                    json!({
                        "reason": "channel_not_allowlisted",
                        "channel_id": message.channel_id,
                        "attempted_trigger": attempted_trigger,
                    }),
                )
                .await?;
            return Ok(ConnectorMessageReply {
                accepted: false,
                reply: None,
            });
        }

        let command_text = match normalize_message_text(&message, &discord_config.command_prefix) {
            Some(command_text) => command_text,
            None => {
                self.runtime
                    .db
                    .record_audit_event(
                        "connector",
                        &format!("discord:{}", message.external_user_id),
                        "connector_message_ignored",
                        json!({
                            "reason": "missing_command_prefix",
                            "channel_id": message.channel_id,
                        }),
                    )
                    .await?;
                return Ok(ConnectorMessageReply {
                    accepted: false,
                    reply: None,
                });
            }
        };

        let Some(identity) = self
            .runtime
            .fetch_connector_identity("discord", &message.external_user_id)
            .await?
        else {
            return Ok(ConnectorMessageReply {
                accepted: true,
                reply: Some(
                    "Your Discord account is not mapped to a BeaverKI user yet. Use the CLI to add a Discord mapping first.".to_owned(),
                ),
            });
        };

        let role_ids = self
            .runtime
            .db
            .list_user_roles(&identity.mapped_user_id)
            .await?
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<_>>();

        if let Some(approval_command) = parse_approval_command(&command_text) {
            if discord_config.approval_dm_only && !message.is_direct_message {
                self.runtime
                    .db
                    .record_audit_event(
                        "connector",
                        &identity.identity_id,
                        "connector_approval_redirected_to_dm",
                        json!({
                            "channel_id": message.channel_id,
                            "command": command_text,
                        }),
                    )
                    .await?;
                return Ok(ConnectorMessageReply {
                    accepted: true,
                    reply: Some(
                        "Approvals can only be handled in a Discord DM. Open a DM with the bot and send approvals."
                            .to_owned(),
                    ),
                });
            }

            if !can_grant_approvals(&role_ids) {
                self.runtime
                    .db
                    .record_audit_event(
                        "connector",
                        &identity.identity_id,
                        "connector_approval_denied",
                        json!({
                            "reason": "missing_approval_authority",
                            "channel_id": message.channel_id,
                        }),
                    )
                    .await?;
                return Ok(ConnectorMessageReply {
                    accepted: true,
                    reply: Some(
                        "This mapped account can submit tasks, but it is not allowed to approve requests."
                            .to_owned(),
                    ),
                });
            }

            return self
                .handle_discord_approval_command(
                    &identity,
                    &message,
                    approval_command,
                    discord_config.approval_action_ttl_secs as i64,
                    discord_config.critical_confirmation_ttl_secs as i64,
                )
                .await;
        }

        let scope = if message.is_direct_message {
            MemoryScope::Private
        } else {
            MemoryScope::Household
        };
        let mapped_user = self
            .runtime
            .resolve_user(Some(&identity.mapped_user_id))
            .await?;
        let session = self
            .runtime
            .resolve_connector_conversation_session(
                &identity.mapped_user_id,
                &message.connector_type,
                &message.channel_id,
                message.is_direct_message,
            )
            .await?;
        if crate::is_session_reset_command(&command_text) {
            self.runtime
                .reset_connector_conversation_session(
                    &mapped_user,
                    &identity.identity_id,
                    &command_text,
                    &session,
                    scope,
                )
                .await?;
            return Ok(ConnectorMessageReply {
                accepted: true,
                reply: Some(
                    "Started a new conversation. Durable memory and audit history were left intact."
                        .to_owned(),
                ),
            });
        }
        let user_label = message
            .external_display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(mapped_user.display_name.as_str());
        let recent_exchanges = load_recent_conversation_exchanges(
            &self.runtime.db,
            &session,
            mapped_user.display_name.as_str(),
        )
        .await?;
        let current_source_label = discord_source_label(message.is_direct_message);
        let task_context = build_discord_task_context(
            &session,
            &recent_exchanges,
            current_source_label,
            user_label,
            &message.channel_id,
        );
        let task = self
            .runtime
            .enqueue_objective_from_connector(
                &mapped_user,
                &identity.identity_id,
                &command_text,
                &session,
                Some(&task_context),
                scope,
            )
            .await?;
        self.runtime
            .db
            .append_task_event(
                &task.task_id,
                CONNECTOR_MESSAGE_CONTEXT_EVENT,
                "connector",
                &identity.identity_id,
                json!({
                    "connector_type": &message.connector_type,
                    "external_user_id": &message.external_user_id,
                    "external_display_name": &message.external_display_name,
                    "channel_id": &message.channel_id,
                    "message_id": &message.message_id,
                    "is_direct_message": message.is_direct_message,
                    "source_label": current_source_label,
                }),
            )
            .await?;
        self.runtime
            .db
            .record_audit_event(
                "connector",
                &identity.identity_id,
                "connector_task_enqueued",
                json!({
                    "task_id": task.task_id,
                    "scope": task.scope,
                    "channel_id": message.channel_id,
                    "is_direct_message": message.is_direct_message,
                }),
            )
            .await?;
        self.wake_worker.notify_one();

        let waited_task = wait_for_discord_task_completion(
            self,
            &task.owner_user_id,
            &task.task_id,
            discord_config.task_wait_timeout_secs,
            typing_context
                .map(|(http_client, token)| (http_client, token, message.channel_id.as_str())),
        )
        .await?;

        let reply = if let Some(waited_task) = waited_task {
            if waited_task.state == TaskState::WaitingApproval.as_str() {
                Some(
                    render_waiting_approval_reply(
                        self,
                        &identity,
                        &message,
                        &waited_task,
                        discord_config.approval_action_ttl_secs as i64,
                    )
                    .await?,
                )
            } else {
                Some(format_task_reply(&waited_task))
            }
        } else {
            self.runtime
                .db
                .append_task_event(
                    &task.task_id,
                    CONNECTOR_FOLLOW_UP_REQUESTED_EVENT,
                    "connector",
                    &identity.identity_id,
                    json!({
                        "connector_type": &message.connector_type,
                        "channel_id": &message.channel_id,
                        "reason": "task_wait_timeout",
                    }),
                )
                .await?;
            if let Some((http_client, token)) = typing_context {
                if let Err(error) = add_discord_reaction(
                    http_client,
                    token,
                    &message.channel_id,
                    &message.message_id,
                    DISCORD_REACTION_WORKING,
                )
                .await
                {
                    warn!(
                        "failed to add Discord working reaction to message {} in channel {}: {error:#}",
                        message.message_id, message.channel_id
                    );
                }
                None
            } else {
                Some(format!(
                    "Request accepted for {}. The daemon is still working on it.",
                    identity.mapped_user_id
                ))
            }
        };
        self.runtime
            .db
            .record_audit_event(
                "connector",
                &identity.identity_id,
                "connector_reply_prepared",
                json!({
                    "task_id": task.task_id,
                    "reply": reply,
                }),
            )
            .await?;
        Ok(ConnectorMessageReply {
            accepted: true,
            reply,
        })
    }

    async fn handle_discord_approval_command(
        &self,
        identity: &ConnectorIdentityRow,
        message: &ConnectorMessageRequest,
        command: ApprovalCommand<'_>,
        action_ttl_secs: i64,
        confirmation_ttl_secs: i64,
    ) -> Result<ConnectorMessageReply> {
        match command {
            ApprovalCommand::Inbox => {
                let approvals = self
                    .runtime
                    .db
                    .list_approvals_for_user(&identity.mapped_user_id, Some("pending"))
                    .await?;
                if approvals.is_empty() {
                    return Ok(ConnectorMessageReply {
                        accepted: true,
                        reply: Some(
                            "You have no pending approvals in BeaverKI right now.".to_owned(),
                        ),
                    });
                }

                let mut sections = Vec::with_capacity(approvals.len());
                for approval in approvals {
                    let action_set = issue_connector_approval_actions(
                        self,
                        identity,
                        message,
                        &approval,
                        action_ttl_secs,
                    )
                    .await?;
                    sections.push(render_approval_overview(&approval, &action_set));
                }
                let reply = format!("Pending approvals:\n\n{}", sections.join("\n\n"));
                self.runtime
                    .db
                    .record_audit_event(
                        "connector",
                        &identity.identity_id,
                        "approval_prompt_rendered",
                        json!({
                            "kind": "approval_inbox",
                            "approval_count": sections.len(),
                            "channel_id": message.channel_id,
                        }),
                    )
                    .await?;
                Ok(ConnectorMessageReply {
                    accepted: true,
                    reply: Some(truncate_reply(&reply)),
                })
            }
            ApprovalCommand::Inspect(token) => {
                let outcome = self
                    .runtime
                    .resolve_approval_action(
                        Some(&identity.mapped_user_id),
                        token,
                        "inspect",
                        Some("discord"),
                        Some(&identity.identity_id),
                        Some(&message.channel_id),
                        confirmation_ttl_secs,
                    )
                    .await;
                match outcome {
                    Ok(RemoteApprovalActionOutcome::Inspection { approval, .. }) => {
                        let action_set = issue_connector_approval_actions(
                            self,
                            identity,
                            message,
                            &approval,
                            action_ttl_secs,
                        )
                        .await?;
                        let reply = render_approval_detail(&approval, &action_set);
                        self.runtime
                            .db
                            .record_audit_event(
                                "connector",
                                &identity.identity_id,
                                "approval_prompt_rendered",
                                json!({
                                    "kind": "approval_detail",
                                    "approval_id": approval.approval_id,
                                    "channel_id": message.channel_id,
                                }),
                            )
                            .await?;
                        Ok(ConnectorMessageReply {
                            accepted: true,
                            reply: Some(truncate_reply(&reply)),
                        })
                    }
                    Ok(_) => bail!("unexpected approval inspect outcome"),
                    Err(error) => {
                        self.runtime
                            .db
                            .record_audit_event(
                                "connector",
                                &identity.identity_id,
                                "connector_approval_resolution_failed",
                                json!({
                                    "requested_action": "inspect",
                                    "channel_id": message.channel_id,
                                    "error": error.to_string(),
                                }),
                            )
                            .await?;
                        Ok(ConnectorMessageReply {
                            accepted: true,
                            reply: Some(format!("Could not inspect that approval token: {error}")),
                        })
                    }
                }
            }
            ApprovalCommand::Approve(token)
            | ApprovalCommand::Deny(token)
            | ApprovalCommand::Confirm(token) => {
                let requested_action = match command {
                    ApprovalCommand::Approve(_) => "approve",
                    ApprovalCommand::Deny(_) => "deny",
                    ApprovalCommand::Confirm(_) => "confirm",
                    ApprovalCommand::Inbox | ApprovalCommand::Inspect(_) => unreachable!(),
                };
                let outcome = self
                    .runtime
                    .resolve_approval_action(
                        Some(&identity.mapped_user_id),
                        token,
                        requested_action,
                        Some("discord"),
                        Some(&identity.identity_id),
                        Some(&message.channel_id),
                        confirmation_ttl_secs,
                    )
                    .await;
                match outcome {
                    Ok(RemoteApprovalActionOutcome::Resolved { action, task }) => {
                        self.runtime
                            .db
                            .record_audit_event(
                                "connector",
                                &identity.identity_id,
                                "connector_approval_resolved",
                                json!({
                                    "approval_id": action.approval_id,
                                    "action_kind": action.action_kind,
                                    "resolution_channel": message.channel_id,
                                    "task_id": task.task_id,
                                    "task_state": task.state,
                                }),
                            )
                            .await?;
                        Ok(ConnectorMessageReply {
                            accepted: true,
                            reply: Some(format_task_reply(&task)),
                        })
                    }
                    Ok(RemoteApprovalActionOutcome::StepUpRequired {
                        approval,
                        confirm_action,
                        ..
                    }) => {
                        self.runtime
                            .db
                            .record_audit_event(
                                "connector",
                                &identity.identity_id,
                                "connector_approval_step_up_required",
                                json!({
                                    "approval_id": approval.approval_id,
                                    "resolution_channel": message.channel_id,
                                    "confirm_action_id": confirm_action.action_id,
                                }),
                            )
                            .await?;
                        let reply = render_critical_confirmation_prompt(&approval, &confirm_action);
                        Ok(ConnectorMessageReply {
                            accepted: true,
                            reply: Some(truncate_reply(&reply)),
                        })
                    }
                    Ok(RemoteApprovalActionOutcome::Inspection { .. }) => {
                        bail!("unexpected approval resolution inspect outcome")
                    }
                    Err(error) => {
                        self.runtime
                            .db
                            .record_audit_event(
                                "connector",
                                &identity.identity_id,
                                "connector_approval_resolution_failed",
                                json!({
                                    "requested_action": requested_action,
                                    "channel_id": message.channel_id,
                                    "error": error.to_string(),
                                }),
                            )
                            .await?;
                        Ok(ConnectorMessageReply {
                            accepted: true,
                            reply: Some(format!(
                                "Could not {} that approval token: {}",
                                requested_action, error
                            )),
                        })
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApprovalCommand<'a> {
    Inbox,
    Inspect(&'a str),
    Approve(&'a str),
    Deny(&'a str),
    Confirm(&'a str),
}

async fn render_waiting_approval_reply(
    daemon: &RuntimeDaemon,
    identity: &ConnectorIdentityRow,
    message: &ConnectorMessageRequest,
    task: &TaskRow,
    action_ttl_secs: i64,
) -> Result<String> {
    if daemon.runtime.config.integrations.discord.approval_dm_only && !message.is_direct_message {
        daemon
            .runtime
            .db
            .record_audit_event(
                "connector",
                &identity.identity_id,
                "approval_prompt_redirected_to_dm",
                json!({
                    "task_id": task.task_id,
                    "channel_id": message.channel_id,
                }),
            )
            .await?;
        return Ok(
            "Approval required for this request. For safety, open a Discord DM with the bot and send approvals."
                .to_owned(),
        );
    }

    let Some(approval) = daemon
        .runtime
        .db
        .pending_approval_for_task(&identity.mapped_user_id, &task.task_id)
        .await?
    else {
        return Ok(format_task_reply(task));
    };

    let action_set =
        issue_connector_approval_actions(daemon, identity, message, &approval, action_ttl_secs)
            .await?;
    daemon
        .runtime
        .db
        .record_audit_event(
            "connector",
            &identity.identity_id,
            "approval_prompt_rendered",
            json!({
                "kind": "task_waiting_approval",
                "approval_id": approval.approval_id,
                "task_id": task.task_id,
                "channel_id": message.channel_id,
            }),
        )
        .await?;
    Ok(render_approval_detail(&approval, &action_set))
}

async fn issue_connector_approval_actions(
    daemon: &RuntimeDaemon,
    identity: &ConnectorIdentityRow,
    message: &ConnectorMessageRequest,
    approval: &ApprovalRow,
    action_ttl_secs: i64,
) -> Result<ApprovalActionSet> {
    let action_set = daemon
        .runtime
        .issue_approval_actions(
            Some(&identity.mapped_user_id),
            &approval.approval_id,
            Some("discord"),
            Some(&identity.identity_id),
            Some(&message.channel_id),
            action_ttl_secs,
            false,
        )
        .await?;
    daemon
        .runtime
        .db
        .record_audit_event(
            "connector",
            &identity.identity_id,
            "approval_token_issued",
            json!({
                "approval_id": approval.approval_id,
                "channel_id": message.channel_id,
                "connector_type": "discord",
                "actions": [
                    {"action_id": action_set.approve.action_id, "action_kind": action_set.approve.action_kind, "expires_at": action_set.approve.expires_at},
                    {"action_id": action_set.deny.action_id, "action_kind": action_set.deny.action_kind, "expires_at": action_set.deny.expires_at},
                    {"action_id": action_set.inspect.action_id, "action_kind": action_set.inspect.action_kind, "expires_at": action_set.inspect.expires_at}
                ],
            }),
        )
        .await?;
    Ok(action_set)
}

fn parse_approval_command(command_text: &str) -> Option<ApprovalCommand<'_>> {
    let mut parts = command_text.split_whitespace();
    let command = parts.next()?;
    match command {
        "approvals" => {
            if parts.next().is_some() {
                return None;
            }
            Some(ApprovalCommand::Inbox)
        }
        "inspect" | "approve" | "deny" | "confirm" => {
            let token = parts.next()?;
            if parts.next().is_some() || !token.starts_with(APPROVAL_ACTION_TOKEN_PREFIX) {
                return None;
            }
            match command {
                "inspect" => Some(ApprovalCommand::Inspect(token)),
                "approve" => Some(ApprovalCommand::Approve(token)),
                "deny" => Some(ApprovalCommand::Deny(token)),
                "confirm" => Some(ApprovalCommand::Confirm(token)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn render_approval_overview(approval: &ApprovalRow, action_set: &ApprovalActionSet) -> String {
    format!(
        "{}\nRisk: {}\nRequester: {}\nApprove: approve {}\nDeny: deny {}\nInspect: inspect {}",
        approval_title(approval),
        approval_risk_label(approval),
        approval_requester_label(approval),
        action_set.approve.action_token,
        action_set.deny.action_token,
        action_set.inspect.action_token,
    )
}

fn render_approval_detail(approval: &ApprovalRow, action_set: &ApprovalActionSet) -> String {
    let mut lines = vec![
        "Approval required".to_owned(),
        format!("Action: {}", approval_title(approval)),
        format!("Risk: {}", approval_risk_label(approval)),
        format!("Requester: {}", approval_requester_label(approval)),
    ];
    if let Some(target_details) = approval_target_label(approval) {
        lines.push(format!("Target: {}", target_details));
    }
    if let Some(rationale) = approval.rationale_text.as_deref() {
        lines.push(format!("Why: {}", rationale));
    }
    lines.push(format!(
        "Approve: approve {}",
        action_set.approve.action_token
    ));
    lines.push(format!("Deny: deny {}", action_set.deny.action_token));
    lines.push(format!(
        "Inspect again: inspect {}",
        action_set.inspect.action_token
    ));
    lines.join("\n")
}

fn render_critical_confirmation_prompt(
    approval: &ApprovalRow,
    confirm_action: &ApprovalActionRow,
) -> String {
    let expires_at = format_expiry_label(&confirm_action.expires_at);
    format!(
        "Critical approval needs a second confirmation.\nAction: {}\nRisk: {}\nConfirm: confirm {}\nExpires: {}",
        approval_title(approval),
        approval_risk_label(approval),
        confirm_action.action_token,
        expires_at,
    )
}

fn approval_title(approval: &ApprovalRow) -> String {
    approval
        .action_summary
        .clone()
        .or_else(|| approval.target_details.clone())
        .or_else(|| approval.target_ref.clone())
        .unwrap_or_else(|| approval.action_type.clone())
}

fn approval_requester_label(approval: &ApprovalRow) -> String {
    approval
        .requester_display_name
        .clone()
        .unwrap_or_else(|| approval.requested_by_agent_id.clone())
}

fn approval_target_label(approval: &ApprovalRow) -> Option<String> {
    approval
        .target_details
        .clone()
        .or_else(|| approval.target_ref.clone())
}

fn approval_risk_label(approval: &ApprovalRow) -> &'static str {
    match approval.risk_level.as_deref() {
        Some("critical") => "Critical",
        Some("high") => "High",
        Some("medium") => "Medium",
        Some("low") => "Low",
        _ => "Unspecified",
    }
}

fn format_expiry_label(expires_at: &str) -> String {
    DateTime::parse_from_rfc3339(expires_at)
        .map(|value| value.with_timezone(&Utc).to_rfc3339())
        .unwrap_or_else(|_| expires_at.to_owned())
}

fn normalize_message_text(
    message: &ConnectorMessageRequest,
    command_prefix: &str,
) -> Option<String> {
    let trimmed = message.content.trim();
    if trimmed.is_empty() {
        return None;
    }

    if message.is_direct_message {
        return Some(trimmed.to_owned());
    }

    let stripped = trimmed.strip_prefix(command_prefix)?.trim();
    if stripped.is_empty() {
        None
    } else {
        Some(stripped.to_owned())
    }
}

fn is_channel_trigger_attempt(content: &str, command_prefix: &str) -> bool {
    let trimmed = content.trim();
    let command_prefix = command_prefix.trim();
    !trimmed.is_empty() && !command_prefix.is_empty() && trimmed.starts_with(command_prefix)
}

fn rewrite_channel_message_for_bot_mention(
    content: &str,
    is_direct_message: bool,
    command_prefix: &str,
    bot_user_id: &str,
) -> String {
    if is_direct_message {
        return content.to_owned();
    }

    let trimmed = content.trim();
    if trimmed.is_empty() {
        return content.to_owned();
    }

    let command_prefix = command_prefix.trim();
    if command_prefix.is_empty() || trimmed.starts_with(command_prefix) {
        return content.to_owned();
    }

    let Some(stripped) = strip_bot_mention(trimmed, bot_user_id) else {
        return content.to_owned();
    };
    let stripped = strip_channel_trigger_delimiters(&stripped);
    if stripped.is_empty() {
        return content.to_owned();
    }

    if stripped.starts_with(command_prefix) {
        stripped.to_owned()
    } else {
        format!("{command_prefix} {stripped}")
    }
}

fn strip_bot_mention(content: &str, bot_user_id: &str) -> Option<String> {
    remove_bot_mention(content, &format!("<@{bot_user_id}>"))
        .or_else(|| remove_bot_mention(content, &format!("<@!{bot_user_id}>")))
}

fn remove_bot_mention(content: &str, mention: &str) -> Option<String> {
    let (before, after_with_mention) = content.split_once(mention)?;
    let before = before.trim_end();
    let after = strip_channel_trigger_delimiters(after_with_mention);

    let combined = match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (true, false) => after.to_owned(),
        (false, true) => before.to_owned(),
        (false, false) => format!("{before} {after}"),
    };

    Some(combined.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn strip_channel_trigger_delimiters(content: &str) -> &str {
    content.trim_start_matches(|ch: char| ch.is_ascii_whitespace() || matches!(ch, ':' | ','))
}

fn discord_author_display_name(author: &DiscordAuthor) -> Option<String> {
    author
        .global_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            author
                .username
                .as_deref()
                .filter(|value| !value.trim().is_empty())
        })
        .map(ToOwned::to_owned)
}

fn interaction_command_text(data: &DiscordInteractionData) -> Result<String> {
    let mut path = vec![data.name.clone()];
    let mut values = Vec::new();
    let mut preferred_prompt = None;

    flatten_interaction_options(&data.options, &mut path, &mut values, &mut preferred_prompt);

    if let Some(approval_command) = normalize_interaction_approval_command(&path, &values) {
        return Ok(approval_command);
    }

    if values.is_empty() && path.len() == 1 && path[0] == "new" {
        return Ok("/new".to_owned());
    }

    if path.len() == 1 {
        if let Some(prompt) = preferred_prompt.or_else(|| {
            if values.is_empty() {
                None
            } else {
                Some(values.join(" "))
            }
        }) {
            let trimmed = prompt.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_owned());
            }
        }

        let trimmed = path[0].trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_owned());
        }
    }

    let mut parts = path;
    parts.extend(values);
    let command = parts
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if command.is_empty() {
        bail!("Discord application command did not contain usable input")
    }
    Ok(command)
}

fn flatten_interaction_options(
    options: &[DiscordInteractionOption],
    path: &mut Vec<String>,
    values: &mut Vec<String>,
    preferred_prompt: &mut Option<String>,
) {
    if let Some(nested_option) = options.iter().find(|option| !option.options.is_empty()) {
        path.push(nested_option.name.clone());
        flatten_interaction_options(&nested_option.options, path, values, preferred_prompt);
        return;
    }

    if options.len() == 1 && options[0].value.is_none() {
        path.push(options[0].name.clone());
        return;
    }

    for option in options {
        let Some(value) = option.value.as_ref() else {
            continue;
        };
        let rendered = render_interaction_option_value(value);
        if rendered.trim().is_empty() {
            continue;
        }
        if preferred_prompt.is_none() && is_prompt_option_name(&option.name) {
            *preferred_prompt = Some(rendered.clone());
        }
        values.push(rendered);
    }
}

fn normalize_interaction_approval_command(path: &[String], values: &[String]) -> Option<String> {
    let last = path.last()?.as_str();
    match last {
        "approvals" => Some("approvals".to_owned()),
        "approve" | "deny" | "inspect" | "confirm" => {
            let token = values.first()?;
            Some(format!("{last} {token}"))
        }
        "inbox" | "list" | "pending"
            if path
                .first()
                .is_some_and(|segment| segment == "approval" || segment == "approvals") =>
        {
            Some("approvals".to_owned())
        }
        _ => None,
    }
}

fn render_interaction_option_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Array(items) => items
            .iter()
            .map(render_interaction_option_value)
            .collect::<Vec<_>>()
            .join(" "),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn is_prompt_option_name(name: &str) -> bool {
    matches!(
        name,
        "prompt" | "request" | "message" | "text" | "objective" | "input" | "query"
    )
}

#[derive(Debug, Clone)]
struct ConversationExchange {
    created_at: String,
    state: String,
    source_label: String,
    channel_id: Option<String>,
    user_label: String,
    user_text: String,
    assistant_text: String,
}

fn build_discord_task_context(
    session: &ConversationSessionRow,
    recent_exchanges: &[ConversationExchange],
    current_source_label: &str,
    user_label: &str,
    channel_id: &str,
) -> String {
    let mut context = String::new();
    context.push_str("Connector context:\n");
    context.push_str(&format!("- Source: {}.\n", current_source_label));
    context.push_str(&format!("- Channel ID: {}.\n", channel_id));
    context.push_str(&format!("- Current user label: {}.\n", user_label));
    context.push_str(&format!("- Session kind: {}.\n", session.session_kind));
    match session.session_kind.as_str() {
        "direct_message" => context.push_str(
            "- Conversation history is shared across direct-message entrypoints for this mapped BeaverKI user.\n",
        ),
        "group_room" => context.push_str(
            "- Conversation history is scoped to this shared connector room rather than the user's wider personal history.\n",
        ),
        _ => context.push_str(
            "- Conversation history is scoped to this connector session.\n",
        ),
    }

    let Some(latest_exchange) = recent_exchanges.first() else {
        context.push_str(
            "- Conversation status: new conversation. No recent prior exchange is available in this connector context.\n",
        );
        context.push_str(
            "- Treat the current message as a new conversation unless the user explicitly refers to older context.\n",
        );
        return context;
    };

    let status = conversation_status(&latest_exchange.state, &latest_exchange.created_at);
    let age_text = latest_exchange_age_text(latest_exchange.created_at.as_str())
        .unwrap_or_else(|| "at an unknown time".to_owned());
    match status {
        ConversationStatus::FollowUp => {
            context.push_str(&format!(
                "- Conversation status: active follow-up. The latest exchange with this user was {}.\n",
                age_text
            ));
            context.push_str(
                "- Continue the prior topic only if the new message depends on it. If the user starts a new topic, answer the new topic directly.\n",
            );
            context.push_str("Recent attributed exchanges:\n");
            for exchange in recent_exchanges.iter().rev() {
                context.push_str(&format!(
                    "- [{}] User ({}): {}\n",
                    source_descriptor(&exchange.source_label, exchange.channel_id.as_deref()),
                    exchange.user_label,
                    compact_message_text(&exchange.user_text)
                ));
                context.push_str(&format!(
                    "- [{}] Assistant: {}\n",
                    source_descriptor(&exchange.source_label, exchange.channel_id.as_deref()),
                    compact_message_text(&exchange.assistant_text)
                ));
            }
        }
        ConversationStatus::FreshStart => {
            context.push_str(&format!(
                "- Conversation status: fresh conversation. The latest exchange with this user was {}.\n",
                age_text
            ));
            context.push_str(
                "- Treat the current message as a new conversation unless the user explicitly refers to the earlier topic. Keep stable facts and preferences if they are reliable.\n",
            );
        }
    }

    context
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConversationStatus {
    FollowUp,
    FreshStart,
}

fn conversation_status(state: &str, created_at: &str) -> ConversationStatus {
    if matches!(
        state.parse::<TaskState>(),
        Ok(TaskState::Pending | TaskState::Running | TaskState::WaitingApproval)
    ) {
        return ConversationStatus::FollowUp;
    }

    let Some(created_at) = parse_timestamp(created_at) else {
        return ConversationStatus::FreshStart;
    };
    let age = Utc::now().signed_duration_since(created_at).num_seconds();
    if age <= DISCORD_ACTIVE_CONVERSATION_WINDOW_SECS {
        ConversationStatus::FollowUp
    } else {
        ConversationStatus::FreshStart
    }
}

fn latest_exchange_age_text(created_at: &str) -> Option<String> {
    let created_at = parse_timestamp(created_at)?;
    let age = Utc::now().signed_duration_since(created_at);
    if age.num_minutes() < 1 {
        Some("less than a minute ago".to_owned())
    } else if age.num_hours() < 1 {
        Some(format!("{} minutes ago", age.num_minutes()))
    } else if age.num_days() < 1 {
        Some(format!("{} hours ago", age.num_hours()))
    } else {
        Some(format!("{} days ago", age.num_days()))
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn assistant_reply_for_context(task: &TaskRow) -> String {
    match task.state.parse::<TaskState>() {
        Ok(TaskState::Completed) => task
            .result_text
            .clone()
            .unwrap_or_else(|| "Task completed without a recorded reply.".to_owned()),
        Ok(TaskState::WaitingApproval) => task.result_text.clone().unwrap_or_else(|| {
            "The assistant is waiting for approval before it can continue.".to_owned()
        }),
        Ok(TaskState::Pending | TaskState::Running) => {
            "The assistant is still working on that request.".to_owned()
        }
        Ok(TaskState::Failed) => task
            .result_text
            .clone()
            .unwrap_or_else(|| "The assistant failed without a recorded explanation.".to_owned()),
        Ok(TaskState::Blocked) | Err(_) => task
            .result_text
            .clone()
            .unwrap_or_else(|| format!("Task state: {}.", task.state)),
    }
}

fn compact_message_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn source_descriptor(source_label: &str, channel_id: Option<&str>) -> String {
    match channel_id {
        Some(channel_id) => format!("{source_label} | channel {channel_id}"),
        None => source_label.to_owned(),
    }
}

fn discord_source_label(is_direct_message: bool) -> &'static str {
    if is_direct_message {
        "Discord direct message"
    } else {
        "Discord channel"
    }
}

async fn load_recent_conversation_exchanges(
    db: &Database,
    session: &ConversationSessionRow,
    fallback_user_label: &str,
) -> Result<Vec<ConversationExchange>> {
    let tasks = db
        .list_recent_interactive_tasks_for_session(
            &session.session_id,
            session.last_reset_at.as_deref(),
            DISCORD_CONVERSATION_HISTORY_LIMIT,
        )
        .await?;
    let mut exchanges = Vec::with_capacity(tasks.len());
    for task in tasks {
        let events = db.fetch_task_events(&task.task_id).await?;
        exchanges.push(build_conversation_exchange(
            &task,
            &events,
            fallback_user_label,
        ));
    }
    Ok(exchanges)
}

fn build_conversation_exchange(
    task: &TaskRow,
    events: &[TaskEventRow],
    fallback_user_label: &str,
) -> ConversationExchange {
    let connector_context = connector_context_from_events(events);
    let (source_label, channel_id, user_label) = if let Some(context) = connector_context {
        (
            context.source_label,
            Some(context.channel_id),
            context
                .user_label
                .unwrap_or_else(|| fallback_user_label.to_owned()),
        )
    } else if task.initiating_identity_id.starts_with("cli:") {
        ("CLI".to_owned(), None, fallback_user_label.to_owned())
    } else {
        (
            "Unknown source".to_owned(),
            None,
            fallback_user_label.to_owned(),
        )
    };

    ConversationExchange {
        created_at: task.created_at.clone(),
        state: task.state.clone(),
        source_label,
        channel_id,
        user_label,
        user_text: task.objective.clone(),
        assistant_text: assistant_reply_for_context(task),
    }
}

#[derive(Debug, Clone)]
struct ConnectorEventContext {
    connector_type: String,
    source_label: String,
    channel_id: String,
    message_id: Option<String>,
    user_label: Option<String>,
}

fn connector_context_from_events(events: &[TaskEventRow]) -> Option<ConnectorEventContext> {
    let event = events
        .iter()
        .find(|event| event.event_type == CONNECTOR_MESSAGE_CONTEXT_EVENT)?;
    let payload: Value = serde_json::from_str(&event.payload_json).ok()?;
    let connector_type = payload
        .get("connector_type")
        .and_then(Value::as_str)
        .unwrap_or("connector")
        .to_owned();
    let source_label = payload
        .get("source_label")
        .and_then(Value::as_str)
        .unwrap_or("Connector")
        .to_owned();
    let channel_id = payload
        .get("channel_id")
        .and_then(Value::as_str)?
        .to_owned();
    let message_id = payload
        .get("message_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned);
    let user_label = payload
        .get("external_display_name")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    Some(ConnectorEventContext {
        connector_type,
        source_label,
        channel_id,
        message_id,
        user_label,
    })
}

fn connector_follow_up_channel(
    events: &[TaskEventRow],
    connector_type: &str,
    task_state: &str,
) -> Option<String> {
    let context = connector_context_from_events(events)?;
    if context.connector_type != connector_type {
        return None;
    }

    let follow_up_requested = events
        .iter()
        .filter(|event| event.event_type == CONNECTOR_FOLLOW_UP_REQUESTED_EVENT)
        .any(|event| {
            serde_json::from_str::<Value>(&event.payload_json)
                .ok()
                .and_then(|payload| {
                    payload
                        .get("connector_type")
                        .and_then(Value::as_str)
                        .map(|value| value == connector_type)
                })
                .unwrap_or(false)
        });
    if !follow_up_requested {
        return None;
    }

    let already_sent = events
        .iter()
        .filter(|event| event.event_type == CONNECTOR_FOLLOW_UP_SENT_EVENT)
        .any(|event| {
            serde_json::from_str::<Value>(&event.payload_json)
                .ok()
                .map(|payload| {
                    payload.get("connector_type").and_then(Value::as_str) == Some(connector_type)
                        && payload.get("task_state").and_then(Value::as_str) == Some(task_state)
                })
                .unwrap_or(false)
        });
    if already_sent {
        return None;
    }

    Some(context.channel_id)
}

fn format_task_reply(task: &beaverki_db::TaskRow) -> String {
    match task.state.parse::<TaskState>() {
        Ok(TaskState::Completed) => task
            .result_text
            .clone()
            .unwrap_or_else(|| "Task completed.".to_owned()),
        Ok(TaskState::WaitingApproval) => format!(
            "Task {} is waiting for approval. {}",
            task.task_id,
            task.result_text
                .as_deref()
                .unwrap_or("Approval details were not recorded.")
        ),
        Ok(TaskState::Failed) => format!(
            "Task {} failed. {}",
            task.task_id,
            task.result_text
                .as_deref()
                .unwrap_or("No failure details were recorded.")
        ),
        _ => format!("Task is now {}.", task.state),
    }
}

fn truncate_reply(reply: &str) -> String {
    if reply.chars().count() <= DISCORD_MAX_MESSAGE_LEN {
        reply.to_owned()
    } else {
        let shortened: String = reply.chars().take(DISCORD_MAX_MESSAGE_LEN).collect();
        format!("{shortened}...")
    }
}

fn build_gateway_websocket_url(base_url: &str) -> String {
    if base_url.contains('?') {
        return base_url.to_owned();
    }

    let trimmed = base_url.trim_end_matches('/');
    format!("{trimmed}/?v=10&encoding=json")
}

async fn parse_retry_after_secs(response: reqwest::Response) -> Option<u64> {
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

fn retry_delay_for_error(error: &anyhow::Error) -> Option<Duration> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use beaverki_db::{ConversationSessionRow, TaskEventRow, TaskRow};
    use chrono::{Duration as ChronoDuration, Utc};

    fn test_task(task_id: &str, state: &str, result_text: Option<&str>) -> TaskRow {
        TaskRow {
            task_id: task_id.to_owned(),
            owner_user_id: "user_alex".to_owned(),
            initiating_identity_id: "identity_discord".to_owned(),
            primary_agent_id: "agent_alex".to_owned(),
            assigned_agent_id: "agent_alex".to_owned(),
            parent_task_id: None,
            session_id: None,
            kind: "interactive".to_owned(),
            state: state.to_owned(),
            objective: "test objective".to_owned(),
            context_summary: None,
            result_text: result_text.map(ToOwned::to_owned),
            scope: "private".to_owned(),
            wake_at: None,
            created_at: "2026-04-17T00:00:00Z".to_owned(),
            updated_at: "2026-04-17T00:00:00Z".to_owned(),
            completed_at: None,
        }
    }

    fn test_session(session_kind: &str) -> ConversationSessionRow {
        ConversationSessionRow {
            session_id: "conversation_session_123".to_owned(),
            session_kind: session_kind.to_owned(),
            session_key: format!("{session_kind}:test"),
            audience_policy: "test".to_owned(),
            max_memory_scope: "private".to_owned(),
            originating_connector_type: Some("discord".to_owned()),
            originating_connector_target: Some("dm-1".to_owned()),
            last_activity_at: "2026-04-17T00:00:00Z".to_owned(),
            last_reset_at: None,
            archived_at: None,
            lifecycle_reason: None,
            created_at: "2026-04-17T00:00:00Z".to_owned(),
            updated_at: "2026-04-17T00:00:00Z".to_owned(),
        }
    }

    fn test_task_event(event_type: &str, payload: Value) -> TaskEventRow {
        TaskEventRow {
            event_id: format!("event_{event_type}"),
            task_id: "task_123".to_owned(),
            event_type: event_type.to_owned(),
            actor_type: "connector".to_owned(),
            actor_id: "connector-test".to_owned(),
            payload_json: payload.to_string(),
            created_at: "2026-04-17T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn gateway_url_gets_root_path_when_missing() {
        assert_eq!(
            build_gateway_websocket_url("wss://gateway.discord.gg"),
            "wss://gateway.discord.gg/?v=10&encoding=json"
        );
    }

    #[test]
    fn gateway_url_preserves_existing_query() {
        assert_eq!(
            build_gateway_websocket_url("wss://gateway.discord.gg/?v=10&encoding=json"),
            "wss://gateway.discord.gg/?v=10&encoding=json"
        );
    }

    #[test]
    fn retry_delay_can_be_extracted_from_error_text() {
        let error = anyhow!("Discord gateway URL request rate limited; retry_after_secs=42");
        let delay = retry_delay_for_error(&error).expect("retry delay");
        assert_eq!(delay, Duration::from_secs(42));
    }

    #[test]
    fn channel_trigger_attempt_matches_prefix_command() {
        assert!(is_channel_trigger_attempt(
            "!bk summarize the latest task",
            "!bk"
        ));
    }

    #[test]
    fn channel_trigger_attempt_matches_rewritten_mention_command() {
        let rewritten = rewrite_channel_message_for_bot_mention(
            "please <@1234567890> summarize the latest task",
            false,
            "!bk",
            "1234567890",
        );

        assert!(is_channel_trigger_attempt(&rewritten, "!bk"));
    }

    #[test]
    fn channel_trigger_attempt_rejects_plain_channel_chat() {
        assert!(!is_channel_trigger_attempt(
            "hello everyone, general update here",
            "!bk"
        ));
    }

    #[test]
    fn mention_trigger_rewrites_to_existing_prefix_flow() {
        let rewritten = rewrite_channel_message_for_bot_mention(
            "<@1234567890> summarize the latest task activity",
            false,
            "!bk",
            "1234567890",
        );

        assert_eq!(rewritten, "!bk summarize the latest task activity");
    }

    #[test]
    fn mention_trigger_handles_nickname_mentions_and_punctuation() {
        let rewritten = rewrite_channel_message_for_bot_mention(
            "<@!1234567890>, summarize the latest task activity",
            false,
            "!bk",
            "1234567890",
        );

        assert_eq!(rewritten, "!bk summarize the latest task activity");
    }

    #[test]
    fn mention_trigger_matches_mentions_mid_message() {
        let rewritten = rewrite_channel_message_for_bot_mention(
            "please <@1234567890> summarize the latest task activity",
            false,
            "!bk",
            "1234567890",
        );

        assert_eq!(rewritten, "!bk please summarize the latest task activity");
    }

    #[test]
    fn mention_trigger_does_not_double_apply_prefix() {
        let rewritten = rewrite_channel_message_for_bot_mention(
            "<@1234567890> !bk summarize the latest task activity",
            false,
            "!bk",
            "1234567890",
        );

        assert_eq!(rewritten, "!bk summarize the latest task activity");
    }

    #[test]
    fn mention_trigger_ignores_other_mentions() {
        let rewritten = rewrite_channel_message_for_bot_mention(
            "<@9999999999> summarize the latest task activity",
            false,
            "!bk",
            "1234567890",
        );

        assert_eq!(
            rewritten,
            "<@9999999999> summarize the latest task activity"
        );
    }

    #[test]
    fn completed_reply_omits_task_id() {
        let reply = format_task_reply(&test_task("task_123", "completed", Some("done")));

        assert_eq!(reply, "done");
        assert!(!reply.contains("task_123"));
    }

    #[test]
    fn waiting_approval_reply_keeps_task_id() {
        let reply = format_task_reply(&test_task(
            "task_123",
            "waiting_approval",
            Some("Approval ID: approval_123"),
        ));

        assert!(reply.contains("task_123"));
        assert!(reply.contains("waiting for approval"));
    }

    #[test]
    fn failed_reply_keeps_task_id() {
        let reply = format_task_reply(&test_task("task_123", "failed", Some("boom")));

        assert!(reply.contains("task_123"));
        assert!(reply.contains("failed"));
    }

    #[test]
    fn discord_context_marks_recent_exchange_as_follow_up() {
        let recent_exchange = ConversationExchange {
            created_at: (Utc::now() - ChronoDuration::minutes(5)).to_rfc3339(),
            state: "completed".to_owned(),
            source_label: "Discord direct message".to_owned(),
            channel_id: Some("dm-1".to_owned()),
            user_label: "Torlenor".to_owned(),
            user_text: "Who am I?".to_owned(),
            assistant_text: "You said your name is Joe.".to_owned(),
        };

        let context = build_discord_task_context(
            &test_session("direct_message"),
            &[recent_exchange],
            "Discord direct message",
            "Torlenor",
            "dm-1",
        );

        assert!(context.contains("Conversation status: active follow-up"));
        assert!(
            context.contains("[Discord direct message | channel dm-1] User (Torlenor): Who am I?")
        );
        assert!(context.contains(
            "[Discord direct message | channel dm-1] Assistant: You said your name is Joe."
        ));
    }

    #[test]
    fn discord_context_marks_stale_exchange_as_fresh_conversation() {
        let stale_exchange = ConversationExchange {
            created_at: (Utc::now() - ChronoDuration::hours(8)).to_rfc3339(),
            state: "completed".to_owned(),
            source_label: "Discord direct message".to_owned(),
            channel_id: Some("dm-1".to_owned()),
            user_label: "Torlenor".to_owned(),
            user_text: "Old question".to_owned(),
            assistant_text: "old reply".to_owned(),
        };

        let context = build_discord_task_context(
            &test_session("direct_message"),
            &[stale_exchange],
            "Discord direct message",
            "Torlenor",
            "dm-1",
        );

        assert!(context.contains("Conversation status: fresh conversation"));
        assert!(!context.contains("Recent attributed exchanges"));
    }

    #[test]
    fn connector_follow_up_requires_timeout_marker() {
        let events = vec![test_task_event(
            CONNECTOR_MESSAGE_CONTEXT_EVENT,
            json!({
                "connector_type": "discord",
                "channel_id": "dm-1",
                "message_id": "msg-1",
                "source_label": "Discord direct message",
            }),
        )];

        assert_eq!(
            connector_follow_up_channel(&events, "discord", "completed"),
            None
        );
    }

    #[test]
    fn connector_follow_up_uses_context_channel() {
        let events = vec![
            test_task_event(
                CONNECTOR_MESSAGE_CONTEXT_EVENT,
                json!({
                    "connector_type": "discord",
                    "channel_id": "dm-1",
                    "message_id": "msg-1",
                    "source_label": "Discord direct message",
                }),
            ),
            test_task_event(
                CONNECTOR_FOLLOW_UP_REQUESTED_EVENT,
                json!({
                    "connector_type": "discord",
                    "channel_id": "dm-1",
                    "reason": "task_wait_timeout",
                }),
            ),
        ];

        assert_eq!(
            connector_follow_up_channel(&events, "discord", "completed"),
            Some("dm-1".to_owned())
        );
    }

    #[test]
    fn connector_follow_up_skips_duplicate_state() {
        let events = vec![
            test_task_event(
                CONNECTOR_MESSAGE_CONTEXT_EVENT,
                json!({
                    "connector_type": "discord",
                    "channel_id": "dm-1",
                    "message_id": "msg-1",
                    "source_label": "Discord direct message",
                }),
            ),
            test_task_event(
                CONNECTOR_FOLLOW_UP_REQUESTED_EVENT,
                json!({
                    "connector_type": "discord",
                    "channel_id": "dm-1",
                    "reason": "task_wait_timeout",
                }),
            ),
            test_task_event(
                CONNECTOR_FOLLOW_UP_SENT_EVENT,
                json!({
                    "connector_type": "discord",
                    "channel_id": "dm-1",
                    "task_state": "completed",
                }),
            ),
        ];

        assert_eq!(
            connector_follow_up_channel(&events, "discord", "completed"),
            None
        );
    }

    #[test]
    fn connector_follow_up_ignores_other_connectors() {
        let events = vec![
            test_task_event(
                CONNECTOR_MESSAGE_CONTEXT_EVENT,
                json!({
                    "connector_type": "slack",
                    "channel_id": "C123",
                    "message_id": "msg-1",
                    "source_label": "Slack DM",
                }),
            ),
            test_task_event(
                CONNECTOR_FOLLOW_UP_REQUESTED_EVENT,
                json!({
                    "connector_type": "slack",
                    "channel_id": "C123",
                    "reason": "task_wait_timeout",
                }),
            ),
        ];

        assert_eq!(
            connector_follow_up_channel(&events, "discord", "completed"),
            None
        );
    }

    #[test]
    fn connector_context_includes_message_id() {
        let events = vec![test_task_event(
            CONNECTOR_MESSAGE_CONTEXT_EVENT,
            json!({
                "connector_type": "discord",
                "channel_id": "dm-1",
                "message_id": "msg-42",
                "source_label": "Discord direct message",
            }),
        )];

        let context = connector_context_from_events(&events).expect("context");
        assert_eq!(context.message_id.as_deref(), Some("msg-42"));
    }

    #[test]
    fn connector_context_ignores_empty_message_id() {
        let events = vec![test_task_event(
            CONNECTOR_MESSAGE_CONTEXT_EVENT,
            json!({
                "connector_type": "discord",
                "channel_id": "dm-1",
                "message_id": "",
                "source_label": "Discord direct message",
            }),
        )];

        let context = connector_context_from_events(&events).expect("context");
        assert_eq!(context.message_id, None);
    }

    #[test]
    fn interaction_command_prefers_prompt_option_text() {
        let command = interaction_command_text(&DiscordInteractionData {
            name: "beaverki".to_owned(),
            options: vec![DiscordInteractionOption {
                name: "prompt".to_owned(),
                value: Some(Value::String("summarize the latest task".to_owned())),
                options: Vec::new(),
            }],
        })
        .expect("command text");

        assert_eq!(command, "summarize the latest task");
    }

    #[test]
    fn interaction_command_maps_approval_subcommands() {
        let command = interaction_command_text(&DiscordInteractionData {
            name: "approval".to_owned(),
            options: vec![DiscordInteractionOption {
                name: "inspect".to_owned(),
                value: None,
                options: vec![DiscordInteractionOption {
                    name: "token".to_owned(),
                    value: Some(Value::String("approval_token_123".to_owned())),
                    options: Vec::new(),
                }],
            }],
        })
        .expect("command text");

        assert_eq!(command, "inspect approval_token_123");
    }

    #[test]
    fn interaction_command_maps_approval_list_subcommand() {
        let command = interaction_command_text(&DiscordInteractionData {
            name: "approval".to_owned(),
            options: vec![DiscordInteractionOption {
                name: "list".to_owned(),
                value: None,
                options: Vec::new(),
            }],
        })
        .expect("command text");

        assert_eq!(command, "approvals");
    }

    #[test]
    fn interaction_command_maps_new_to_session_reset() {
        let command = interaction_command_text(&DiscordInteractionData {
            name: "new".to_owned(),
            options: Vec::new(),
        })
        .expect("command text");

        assert_eq!(command, "/new");
    }

    #[test]
    fn registered_command_match_requires_expected_new_shape() {
        let command = DiscordRegisteredCommand {
            name: "new".to_owned(),
            description: "Start a new BeaverKI conversation".to_owned(),
            command_type: 1,
            contexts: Some(vec![1, 0, 1]),
            options: Vec::new(),
        };

        assert!(discord_registered_command_matches_payload(
            &command,
            &discord_global_command_payloads()[0]
        ));
    }

    #[test]
    fn registered_command_match_rejects_wrong_description() {
        let command = DiscordRegisteredCommand {
            name: "new".to_owned(),
            description: "Reset things".to_owned(),
            command_type: 1,
            contexts: Some(vec![0, 1]),
            options: Vec::new(),
        };

        assert!(!discord_registered_command_matches_payload(
            &command,
            &discord_global_command_payloads()[0]
        ));
    }

    #[test]
    fn command_set_match_rejects_extra_existing_commands() {
        let commands = vec![
            DiscordRegisteredCommand {
                name: "new".to_owned(),
                description: "Start a new BeaverKI conversation".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: Vec::new(),
            },
            DiscordRegisteredCommand {
                name: "old-test".to_owned(),
                description: "Old test command".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: Vec::new(),
            },
        ];

        assert!(!discord_command_sets_match(
            &commands,
            &discord_global_command_payloads()
        ));
    }

    #[test]
    fn command_set_match_accepts_expected_command_list() {
        let commands = vec![
            DiscordRegisteredCommand {
                name: "new".to_owned(),
                description: "Start a new BeaverKI conversation".to_owned(),
                command_type: 1,
                contexts: Some(vec![1, 0]),
                options: Vec::new(),
            },
            DiscordRegisteredCommand {
                name: "approval".to_owned(),
                description: "Inspect or resolve BeaverKI approvals".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: normalized_discord_command_options_from_value(
                    discord_global_command_payloads()[1].get("options"),
                ),
            },
        ];

        assert!(discord_command_sets_match(
            &commands,
            &discord_global_command_payloads()
        ));
    }

    #[test]
    fn command_set_match_rejects_option_drift() {
        let commands = vec![
            DiscordRegisteredCommand {
                name: "new".to_owned(),
                description: "Start a new BeaverKI conversation".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: Vec::new(),
            },
            DiscordRegisteredCommand {
                name: "approval".to_owned(),
                description: "Inspect or resolve BeaverKI approvals".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: vec![DiscordRegisteredCommandOption {
                    option_type: 1,
                    name: "approve".to_owned(),
                    description: "Approve a BeaverKI approval token".to_owned(),
                    required: false,
                    options: Vec::new(),
                }],
            },
        ];

        assert!(!discord_command_sets_match(
            &commands,
            &discord_global_command_payloads()
        ));
    }

    #[test]
    fn approval_parser_accepts_exact_token_commands_and_inbox() {
        assert_eq!(
            parse_approval_command("approvals"),
            Some(ApprovalCommand::Inbox)
        );
        assert_eq!(
            parse_approval_command("approve approval_token_123"),
            Some(ApprovalCommand::Approve("approval_token_123"))
        );
        assert_eq!(
            parse_approval_command("deny approval_token_123"),
            Some(ApprovalCommand::Deny("approval_token_123"))
        );
        assert_eq!(
            parse_approval_command("inspect approval_token_123"),
            Some(ApprovalCommand::Inspect("approval_token_123"))
        );
        assert_eq!(
            parse_approval_command("confirm approval_token_123"),
            Some(ApprovalCommand::Confirm("approval_token_123"))
        );
    }

    #[test]
    fn approval_parser_rejects_ambiguous_or_non_token_messages() {
        assert_eq!(
            parse_approval_command("please approve approval_token_123"),
            None
        );
        assert_eq!(parse_approval_command("approve approval_123"), None);
        assert_eq!(
            parse_approval_command("approve approval_token_123 now"),
            None
        );
        assert_eq!(parse_approval_command("inspect the folder"), None);
    }
}
