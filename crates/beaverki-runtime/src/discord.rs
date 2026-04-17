use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use beaverki_db::{Database, TaskEventRow, TaskRow};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::{self, Instant, MissedTickBehavior};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::warn;

use crate::{ConnectorMessageReply, ConnectorMessageRequest, RuntimeDaemon};
use beaverki_core::{MemoryScope, TaskState};

const DISCORD_GATEWAY_URL: &str = "https://discord.com/api/v10/gateway/bot";
const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
const DISCORD_INTENTS: i64 = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);
const DISCORD_MAX_MESSAGE_LEN: usize = 1_900;
const DISCORD_CONVERSATION_HISTORY_LIMIT: i64 = 4;
const DISCORD_ACTIVE_CONVERSATION_WINDOW_SECS: i64 = 45 * 60;
const DISCORD_TYPING_REFRESH_INTERVAL: Duration = Duration::from_secs(8);

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

pub(crate) async fn run_discord_loop(daemon: Arc<RuntimeDaemon>) -> Result<()> {
    let token = daemon
        .runtime
        .discord_bot_token()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("Discord connector is enabled but no bot token is loaded"))?;
    let http_client = reqwest::Client::builder()
        .user_agent("beaverki/0.1")
        .build()
        .context("failed to build Discord HTTP client")?;

    daemon
        .runtime
        .db
        .record_audit_event(
            "connector",
            "discord",
            "discord_connector_started",
            json!({
                "allowed_channel_ids": daemon.runtime.config.integrations.discord.allowed_channel_ids,
                "command_prefix": daemon.runtime.config.integrations.discord.command_prefix,
            }),
        )
        .await?;

    let mut retry_delay = Duration::from_secs(1);
    loop {
        if daemon.shutdown_notified().await {
            return Ok(());
        }

        match run_gateway_session(Arc::clone(&daemon), &http_client, &token).await {
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
                                                    ConnectorMessageRequest {
                                                        connector_type: "discord".to_owned(),
                                                        external_user_id: message.author.id,
                                                        external_display_name,
                                                        channel_id: message.channel_id.clone(),
                                                        message_id: message.id,
                                                        content: message.content,
                                                        is_direct_message: message.guild_id.is_none(),
                                                    },
                                                )
                                                .await?;
                                            if let Some(reply_text) = reply.reply {
                                                send_discord_message(
                                                    http_client,
                                                    token,
                                                    &message.channel_id,
                                                    &reply_text,
                                                )
                                                .await?;
                                            }
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

async fn send_discord_message(
    http_client: &reqwest::Client,
    token: &str,
    channel_id: &str,
    content: &str,
) -> Result<()> {
    let message = truncate_reply(content);
    http_client
        .post(format!("{DISCORD_API_BASE}/channels/{channel_id}/messages"))
        .header("Authorization", format!("Bot {token}"))
        .json(&json!({ "content": message }))
        .send()
        .await
        .with_context(|| format!("failed to send Discord message to channel {channel_id}"))?
        .error_for_status()
        .with_context(|| format!("Discord rejected message for channel {channel_id}"))?;
    Ok(())
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

impl RuntimeDaemon {
    async fn handle_discord_gateway_message(
        &self,
        http_client: &reqwest::Client,
        token: &str,
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
        if !message.is_direct_message
            && !discord_config
                .allowed_channel_ids
                .iter()
                .any(|channel_id| channel_id == &message.channel_id)
        {
            self.runtime
                .db
                .record_audit_event(
                    "connector",
                    &format!("discord:{}", message.external_user_id),
                    "connector_message_ignored",
                    json!({
                        "reason": "channel_not_allowlisted",
                        "channel_id": message.channel_id,
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

        if let Some((approve, approval_id)) = parse_approval_command(&command_text) {
            let task = match self
                .runtime
                .resolve_approval(Some(&identity.mapped_user_id), approval_id, approve)
                .await
            {
                Ok(task) => task,
                Err(error) => {
                    return Ok(ConnectorMessageReply {
                        accepted: true,
                        reply: Some(format!(
                            "Could not {} approval {}: {}",
                            if approve { "approve" } else { "deny" },
                            approval_id,
                            error
                        )),
                    });
                }
            };
            self.runtime
                .db
                .record_audit_event(
                    "connector",
                    &identity.identity_id,
                    "connector_approval_resolved",
                    json!({
                        "approval_id": approval_id,
                        "approve": approve,
                        "task_id": task.task_id,
                        "task_state": task.state,
                    }),
                )
                .await?;
            return Ok(ConnectorMessageReply {
                accepted: true,
                reply: Some(format_task_reply(&task)),
            });
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
        let user_label = message
            .external_display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(mapped_user.display_name.as_str());
        let recent_exchanges = load_recent_conversation_exchanges(
            &self.runtime.db,
            &identity.mapped_user_id,
            mapped_user.display_name.as_str(),
        )
        .await?;
        let current_source_label = discord_source_label(message.is_direct_message);
        let task_context = build_discord_task_context(
            &recent_exchanges,
            current_source_label,
            user_label,
            &message.channel_id,
        );
        let task = self
            .runtime
            .enqueue_objective_from_connector(
                &identity.mapped_user_id,
                &identity.identity_id,
                &command_text,
                Some(&task_context),
                scope,
            )
            .await?;
        self.runtime
            .db
            .append_task_event(
                &task.task_id,
                "connector_message_context",
                "connector",
                &identity.identity_id,
                json!({
                    "connector_type": &message.connector_type,
                    "external_user_id": &message.external_user_id,
                    "external_display_name": &message.external_display_name,
                    "channel_id": &message.channel_id,
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
            format_task_reply(&waited_task)
        } else {
            format!(
                "Request accepted for {}. The daemon is still working on it.",
                identity.mapped_user_id
            )
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
            reply: Some(reply),
        })
    }
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
    context.push_str(
        "- Conversation history is shared across this BeaverKI user, even when they switch channels or connectors.\n",
    );

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
    owner_user_id: &str,
    fallback_user_label: &str,
) -> Result<Vec<ConversationExchange>> {
    let tasks = db
        .list_recent_interactive_tasks_for_owner(owner_user_id, DISCORD_CONVERSATION_HISTORY_LIMIT)
        .await?;
    let mut exchanges = Vec::with_capacity(tasks.len());
    for task in tasks {
        let events = db
            .fetch_task_events_for_owner(owner_user_id, &task.task_id)
            .await?;
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
    source_label: String,
    channel_id: String,
    user_label: Option<String>,
}

fn connector_context_from_events(events: &[TaskEventRow]) -> Option<ConnectorEventContext> {
    let event = events
        .iter()
        .find(|event| event.event_type == "connector_message_context")?;
    let payload: Value = serde_json::from_str(&event.payload_json).ok()?;
    let source_label = payload
        .get("source_label")
        .and_then(Value::as_str)
        .unwrap_or("Connector")
        .to_owned();
    let channel_id = payload
        .get("channel_id")
        .and_then(Value::as_str)?
        .to_owned();
    let user_label = payload
        .get("external_display_name")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    Some(ConnectorEventContext {
        source_label,
        channel_id,
        user_label,
    })
}

fn parse_approval_command(command_text: &str) -> Option<(bool, &str)> {
    let mut parts = command_text.split_whitespace();
    let command = parts.next()?;
    let approval_id = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    match command {
        "approve" => Some((true, approval_id)),
        "deny" => Some((false, approval_id)),
        _ => None,
    }
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
    use beaverki_db::TaskRow;
    use chrono::{Duration as ChronoDuration, Utc};

    fn test_task(task_id: &str, state: &str, result_text: Option<&str>) -> TaskRow {
        TaskRow {
            task_id: task_id.to_owned(),
            owner_user_id: "user_alex".to_owned(),
            initiating_identity_id: "identity_discord".to_owned(),
            primary_agent_id: "agent_alex".to_owned(),
            assigned_agent_id: "agent_alex".to_owned(),
            parent_task_id: None,
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
            &[stale_exchange],
            "Discord direct message",
            "Torlenor",
            "dm-1",
        );

        assert!(context.contains("Conversation status: fresh conversation"));
        assert!(!context.contains("Recent attributed exchanges"));
    }
}
