use std::time::Duration;

use anyhow::{Result, bail};
use beaverki_core::{MemoryScope, TaskState};
use beaverki_db::{ConnectorIdentityRow, TaskEventRow, TaskRow};
use beaverki_policy::can_grant_approvals;
use serde_json::json;
use tokio::time;
use tracing::warn;

use crate::connector_support::{
    CONNECTOR_FOLLOW_UP_REQUESTED_EVENT, CONNECTOR_FOLLOW_UP_SENT_EVENT,
    CONNECTOR_MESSAGE_CONTEXT_EVENT, connector_context_from_events, connector_follow_up_channel,
    format_task_reply,
};
use crate::{
    ConnectorMessageReply, ConnectorMessageRequest, RemoteApprovalActionOutcome, RuntimeDaemon,
};

mod api;
mod approvals;
mod context;
mod gateway;
mod interactions;
mod models;

use self::api::{
    add_discord_reaction, delete_discord_reaction, edit_discord_interaction_response,
    send_discord_interaction_callback, send_discord_message, truncate_reply, with_discord_typing,
};
use self::approvals::{
    ApprovalCommand, issue_connector_approval_actions, parse_approval_command,
    render_approval_detail, render_approval_overview, render_critical_confirmation_prompt,
    render_waiting_approval_reply,
};
use self::context::{
    build_discord_task_context, discord_source_label, load_recent_conversation_exchanges,
};
use self::interactions::{
    discord_interaction_to_message_request, is_channel_trigger_attempt, normalize_message_text,
};
use self::models::DiscordInteractionCreate;

pub(crate) use self::api::{build_discord_http_client, send_direct_household_message};
pub(crate) use self::gateway::run_discord_loop;
pub(crate) use self::models::DiscordDirectDeliveryResult;

#[cfg(test)]
pub(crate) use self::api::set_test_direct_send_capture;

const DISCORD_CONVERSATION_HISTORY_LIMIT: i64 = 4;
const DISCORD_ACTIVE_CONVERSATION_WINDOW_SECS: i64 = 45 * 60;
const DISCORD_REACTION_WORKING: &str = "%E2%8F%B3";
const APPROVAL_ACTION_TOKEN_PREFIX: &str = "approval_token_";

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

pub(crate) async fn maybe_send_task_follow_up(
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
        let request = discord_interaction_to_message_request(
            &self.runtime.config.integrations.discord.command_prefix,
            &interaction,
        )?;
        let reply = self.handle_connector_message(request).await?;
        Ok(reply.reply)
    }

    pub(super) async fn handle_discord_gateway_message(
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
        if crate::is_session_status_command(&command_text) {
            let queue_depth = self.runtime.pending_task_count().await?;
            let task = self
                .runtime
                .status_connector_conversation_session(
                    &mapped_user,
                    &identity.identity_id,
                    &command_text,
                    &session,
                    scope,
                    queue_depth,
                )
                .await?;
            self.runtime
                .db
                .record_audit_event(
                    "connector",
                    &identity.identity_id,
                    "connector_session_status_requested",
                    json!({
                        "connector_type": &message.connector_type,
                        "channel_id": &message.channel_id,
                        "message_id": &message.message_id,
                        "session_id": session.session_id,
                        "task_id": task.task_id,
                        "queue_depth": queue_depth,
                    }),
                )
                .await?;
            return Ok(ConnectorMessageReply {
                accepted: true,
                reply: task.result_text,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::anyhow;
    use beaverki_db::{ConversationSessionRow, TaskEventRow, TaskRow};
    use chrono::{Duration as ChronoDuration, Utc};
    use serde_json::{Value, json};

    use super::api::{
        discord_command_sets_match, discord_global_command_payloads,
        discord_registered_command_matches_payload, normalized_discord_command_options_from_value,
        retry_delay_for_error,
    };
    use super::approvals::{ApprovalCommand, parse_approval_command};
    use super::context::{ConversationExchange, build_discord_task_context};
    use super::gateway::{build_gateway_websocket_url, gateway_connect_payload};
    use super::interactions::{
        interaction_command_text, is_channel_trigger_attempt,
        rewrite_channel_message_for_bot_mention,
    };
    use super::models::{
        DiscordGatewayConnectMode, DiscordGatewaySessionState, DiscordInteractionData,
        DiscordInteractionOption, DiscordReadyPayload, DiscordRegisteredCommand,
        DiscordRegisteredCommandOption,
    };
    use crate::connector_support::{
        CONNECTOR_FOLLOW_UP_REQUESTED_EVENT, CONNECTOR_FOLLOW_UP_SENT_EVENT,
        CONNECTOR_MESSAGE_CONTEXT_EVENT, connector_context_from_events,
        connector_follow_up_channel, format_task_reply,
    };

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
    fn gateway_session_identifies_without_resume_state() {
        let session_state = DiscordGatewaySessionState::default();

        assert_eq!(
            session_state.connect_mode(),
            DiscordGatewayConnectMode::Identify
        );
        assert_eq!(
            gateway_connect_payload("token-123", &session_state)["op"],
            2
        );
    }

    #[test]
    fn gateway_session_resumes_with_session_id_and_sequence() {
        let session_state = DiscordGatewaySessionState {
            sequence_number: Some(42),
            session_id: Some("session-123".to_owned()),
            resume_gateway_url: Some("wss://gateway.discord.gg".to_owned()),
        };

        assert_eq!(
            session_state.connect_mode(),
            DiscordGatewayConnectMode::Resume
        );
        assert_eq!(
            gateway_connect_payload("token-123", &session_state)["op"],
            6
        );
        assert_eq!(
            gateway_connect_payload("token-123", &session_state)["d"]["session_id"],
            "session-123"
        );
        assert_eq!(
            gateway_connect_payload("token-123", &session_state)["d"]["seq"],
            42
        );
    }

    #[test]
    fn gateway_session_updates_resume_state_from_ready_payload() {
        let mut session_state = DiscordGatewaySessionState::default();

        session_state.update_ready(DiscordReadyPayload {
            session_id: "session-123".to_owned(),
            resume_gateway_url: Some("wss://resume.discord.gg".to_owned()),
        });

        assert_eq!(session_state.session_id.as_deref(), Some("session-123"));
        assert_eq!(
            session_state.resume_gateway_url.as_deref(),
            Some("wss://resume.discord.gg")
        );
    }

    #[test]
    fn gateway_session_can_clear_resume_state() {
        let mut session_state = DiscordGatewaySessionState {
            sequence_number: Some(42),
            session_id: Some("session-123".to_owned()),
            resume_gateway_url: Some("wss://resume.discord.gg".to_owned()),
        };

        session_state.clear_resume_state();

        assert_eq!(session_state, DiscordGatewaySessionState::default());
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
    fn interaction_command_maps_status_to_session_status() {
        let command = interaction_command_text(&DiscordInteractionData {
            name: "status".to_owned(),
            options: Vec::new(),
        })
        .expect("command text");

        assert_eq!(command, "/status");
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
                name: "status".to_owned(),
                description: "Show the current BeaverKI session status".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: Vec::new(),
            },
            DiscordRegisteredCommand {
                name: "approval".to_owned(),
                description: "Inspect or resolve BeaverKI approvals".to_owned(),
                command_type: 1,
                contexts: Some(vec![0, 1]),
                options: normalized_discord_command_options_from_value(
                    discord_global_command_payloads()[2].get("options"),
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
