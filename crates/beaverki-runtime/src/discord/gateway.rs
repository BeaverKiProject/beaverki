use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{self, MissedTickBehavior};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;

use crate::{ConnectorMessageRequest, RuntimeDaemon};

use super::api::{
    build_discord_http_client, fetch_discord_current_user_id, parse_retry_after_secs,
    retry_delay_for_error, send_discord_message, sync_discord_global_commands,
};
use super::interactions::{discord_author_display_name, rewrite_channel_message_for_bot_mention};
use super::models::{
    DiscordGatewayConnectMode, DiscordGatewayInfo, DiscordGatewaySessionOutcome,
    DiscordGatewaySessionState, DiscordInteractionCreate, DiscordMessageCreate,
    DiscordReadyPayload,
};

const DISCORD_GATEWAY_URL: &str = "https://discord.com/api/v10/gateway/bot";
const DISCORD_INTENTS: i64 = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);

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
    let mut session_state = DiscordGatewaySessionState::default();
    loop {
        if daemon.shutdown_notified().await {
            return Ok(());
        }

        match run_gateway_session(
            Arc::clone(&daemon),
            &http_client,
            &token,
            &bot_user_id,
            &mut session_state,
        )
        .await
        {
            Ok(DiscordGatewaySessionOutcome::Shutdown) => return Ok(()),
            Ok(DiscordGatewaySessionOutcome::Reconnect) => {
                retry_delay = Duration::from_secs(1);
                continue;
            }
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
    session_state: &mut DiscordGatewaySessionState,
) -> Result<DiscordGatewaySessionOutcome> {
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
    let gateway_url = build_gateway_websocket_url(session_state.websocket_url(&gateway_info.url));
    let (mut socket, _) = connect_async(&gateway_url)
        .await
        .with_context(|| format!("failed to connect to Discord gateway at {gateway_url}"))?;

    let mut heartbeat = time::interval(Duration::from_secs(60));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut identified = false;

    loop {
        tokio::select! {
            _ = daemon.shutdown.notified() => {
                let _ = socket.close(None).await;
                return Ok(DiscordGatewaySessionOutcome::Shutdown);
            }
            _ = heartbeat.tick(), if identified => {
                let heartbeat_payload = json!({
                    "op": 1,
                    "d": session_state.sequence_number,
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
                            session_state.sequence_number = Some(sequence);
                        }

                        match payload.get("op").and_then(Value::as_i64).unwrap_or_default() {
                            0 => {
                                if let Some(event_type) = payload.get("t").and_then(Value::as_str) {
                                    match event_type {
                                        "READY" => {
                                            let ready = serde_json::from_value::<DiscordReadyPayload>(
                                                payload.get("d").cloned().unwrap_or(Value::Null),
                                            )
                                            .context("failed to decode Discord ready payload")?;
                                            session_state.update_ready(ready);
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
                                    "d": session_state.sequence_number,
                                });
                                socket
                                    .send(Message::Text(heartbeat_payload.to_string()))
                                    .await
                                    .context("failed to send Discord heartbeat request response")?;
                            }
                            7 => {
                                daemon
                                    .runtime
                                    .db
                                    .record_audit_event(
                                        "connector",
                                        "discord",
                                        "discord_connector_reconnect_requested",
                                        json!({
                                            "session_id": session_state.session_id,
                                            "sequence_number": session_state.sequence_number,
                                        }),
                                    )
                                    .await?;
                                let _ = socket.close(None).await;
                                return Ok(DiscordGatewaySessionOutcome::Reconnect);
                            }
                            9 => {
                                let can_resume = payload
                                    .get("d")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if !can_resume {
                                    session_state.clear_resume_state();
                                }
                                daemon
                                    .runtime
                                    .db
                                    .record_audit_event(
                                        "connector",
                                        "discord",
                                        "discord_connector_session_invalidated",
                                        json!({
                                            "can_resume": can_resume,
                                            "session_id": session_state.session_id,
                                            "sequence_number": session_state.sequence_number,
                                        }),
                                    )
                                    .await?;
                                let _ = socket.close(None).await;
                                return Ok(DiscordGatewaySessionOutcome::Reconnect);
                            }
                            10 => {
                                let heartbeat_interval_ms = payload
                                    .get("d")
                                    .and_then(|value| value.get("heartbeat_interval"))
                                    .and_then(Value::as_u64)
                                    .ok_or_else(|| anyhow!("Discord gateway hello payload missing heartbeat interval"))?;
                                heartbeat = time::interval(Duration::from_millis(heartbeat_interval_ms));
                                heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                                let identify_payload = gateway_connect_payload(token, session_state);
                                socket
                                    .send(Message::Text(identify_payload.to_string()))
                                    .await
                                    .with_context(|| {
                                        format!(
                                            "failed to send Discord {} payload",
                                            gateway_connect_mode_label(session_state.connect_mode())
                                        )
                                    })?;
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

pub(super) fn gateway_connect_payload(
    token: &str,
    session_state: &DiscordGatewaySessionState,
) -> Value {
    match session_state.connect_mode() {
        DiscordGatewayConnectMode::Identify => json!({
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
        }),
        DiscordGatewayConnectMode::Resume => json!({
            "op": 6,
            "d": {
                "token": token,
                "session_id": session_state.session_id.as_deref().unwrap_or_default(),
                "seq": session_state.sequence_number,
            }
        }),
    }
}

pub(super) fn build_gateway_websocket_url(base_url: &str) -> String {
    if base_url.contains('?') {
        return base_url.to_owned();
    }

    let trimmed = base_url.trim_end_matches('/');
    format!("{trimmed}/?v=10&encoding=json")
}

fn gateway_connect_mode_label(mode: DiscordGatewayConnectMode) -> &'static str {
    match mode {
        DiscordGatewayConnectMode::Identify => "identify",
        DiscordGatewayConnectMode::Resume => "resume",
    }
}
