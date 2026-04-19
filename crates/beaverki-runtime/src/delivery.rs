use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use beaverki_agent::{
    HouseholdDeliveryDelegate, HouseholdDeliveryOutcome, HouseholdDeliveryRequest,
};
use beaverki_db::{
    ConnectorIdentityRow, Database, HouseholdDeliveryRow, NewHouseholdDelivery, UserRow,
};
use serde_json::json;
use tracing::info;

use crate::discord;

#[derive(Clone)]
pub(crate) struct RuntimeHouseholdDeliveryDelegate {
    pub(crate) db: Database,
    pub(crate) runtime_actor_id: String,
    pub(crate) discord_bot_token: Option<String>,
}

#[async_trait]
impl HouseholdDeliveryDelegate for RuntimeHouseholdDeliveryDelegate {
    async fn deliver_immediate_household_message(
        &self,
        request: HouseholdDeliveryRequest<'_>,
    ) -> std::result::Result<HouseholdDeliveryOutcome, beaverki_tools::ToolError> {
        let requester = self
            .db
            .fetch_user(request.requester_user_id)
            .await
            .map_err(beaverki_tools::ToolError::Failed)?
            .ok_or_else(|| {
                beaverki_tools::ToolError::Failed(anyhow!(
                    "requester '{}' not found for household delivery",
                    request.requester_user_id
                ))
            })?;
        let recipient = self
            .db
            .fetch_user(request.recipient_user_id)
            .await
            .map_err(beaverki_tools::ToolError::Failed)?
            .ok_or_else(|| beaverki_tools::ToolError::Denied {
                message: format!(
                    "recipient '{}' no longer exists for household delivery",
                    request.recipient_user_id
                ),
                detail: json!({
                    "recipient_user_id": request.recipient_user_id,
                }),
            })?;

        let identity = self
            .select_household_delivery_identity(&recipient.user_id)
            .await?;
        let delivery = self
            .db
            .create_or_reuse_household_delivery(NewHouseholdDelivery {
                task_id: request.task_id,
                requester_user_id: request.requester_user_id,
                requester_identity_id: request.requester_identity_id,
                recipient_user_id: request.recipient_user_id,
                message_text: request.message_text,
                delivery_mode: "immediate",
                connector_type: &identity.connector_type,
                connector_identity_id: &identity.identity_id,
                target_ref: &identity.external_user_id,
                fallback_target_ref: identity.external_channel_id.as_deref(),
                dedupe_key: request.dedupe_key,
                initial_status: "dispatching",
                parent_delivery_id: None,
                schedule_id: None,
                scheduled_for_at: None,
                window_starts_at: None,
                window_ends_at: None,
                recurrence_rule: None,
                scheduled_job_state: None,
                materialized_task_id: None,
            })
            .await
            .map_err(beaverki_tools::ToolError::Failed)?;

        if delivery.status == "sent" {
            return Ok(HouseholdDeliveryOutcome {
                delivery_id: delivery.delivery_id,
                recipient_user_id: recipient.user_id,
                recipient_display_name: recipient.display_name,
                connector_type: identity.connector_type,
                connector_identity_id: identity.identity_id,
                target_ref: delivery.target_ref,
                status: delivery.status,
                deduplicated: true,
            });
        }
        if delivery.status == "failed" {
            return Err(beaverki_tools::ToolError::Failed(anyhow!(
                delivery
                    .failure_reason
                    .unwrap_or_else(|| "household delivery previously failed".to_owned())
            )));
        }
        if delivery.status == "dispatching" && delivery.delivered_at.is_none() {
            let send_result = self.send_household_delivery(&delivery, &identity, &requester).await;
            match send_result {
                Ok(sent) => {
                    self.db
                        .mark_household_delivery_sent(
                            &delivery.delivery_id,
                            &identity.connector_type,
                            &identity.identity_id,
                            &sent.target_ref,
                            Some(&sent.message_id),
                        )
                        .await
                        .map_err(beaverki_tools::ToolError::Failed)?;
                    self.db
                        .append_task_event(
                            request.task_id,
                            "household_delivery_sent",
                            "runtime",
                            &self.runtime_actor_id,
                            json!({
                                "delivery_id": delivery.delivery_id,
                                "requester_user_id": requester.user_id,
                                "requester_identity_id": request.requester_identity_id,
                                "recipient_user_id": recipient.user_id,
                                "recipient_display_name": recipient.display_name,
                                "connector_type": identity.connector_type,
                                "connector_identity_id": identity.identity_id,
                                "target_kind": sent.target_kind,
                                "target_ref": sent.target_ref,
                                "deduplicated": false,
                            }),
                        )
                        .await
                        .map_err(beaverki_tools::ToolError::Failed)?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.runtime_actor_id,
                            "household_delivery_sent",
                            json!({
                                "delivery_id": delivery.delivery_id,
                                "task_id": request.task_id,
                                "requester_user_id": requester.user_id,
                                "requester_identity_id": request.requester_identity_id,
                                "requester_display_name": requester.display_name,
                                "recipient_user_id": recipient.user_id,
                                "recipient_display_name": recipient.display_name,
                                "connector_type": identity.connector_type,
                                "connector_identity_id": identity.identity_id,
                                "target_kind": sent.target_kind,
                                "target_ref": sent.target_ref,
                            }),
                        )
                        .await
                        .map_err(beaverki_tools::ToolError::Failed)?;

                    return Ok(HouseholdDeliveryOutcome {
                        delivery_id: delivery.delivery_id,
                        recipient_user_id: recipient.user_id,
                        recipient_display_name: recipient.display_name,
                        connector_type: identity.connector_type,
                        connector_identity_id: identity.identity_id,
                        target_ref: sent.target_ref,
                        status: "sent".to_owned(),
                        deduplicated: false,
                    });
                }
                Err(error) => {
                    let message = error.to_string();
                    self.db
                        .mark_household_delivery_failed(&delivery.delivery_id, &message)
                        .await
                        .map_err(beaverki_tools::ToolError::Failed)?;
                    self.db
                        .append_task_event(
                            request.task_id,
                            "household_delivery_failed",
                            "runtime",
                            &self.runtime_actor_id,
                            json!({
                                "delivery_id": delivery.delivery_id,
                                "requester_user_id": requester.user_id,
                                "recipient_user_id": recipient.user_id,
                                "connector_type": identity.connector_type,
                                "connector_identity_id": identity.identity_id,
                                "error": message,
                            }),
                        )
                        .await
                        .map_err(beaverki_tools::ToolError::Failed)?;
                    self.db
                        .record_audit_event(
                            "runtime",
                            &self.runtime_actor_id,
                            "household_delivery_failed",
                            json!({
                                "delivery_id": delivery.delivery_id,
                                "task_id": request.task_id,
                                "requester_user_id": requester.user_id,
                                "recipient_user_id": recipient.user_id,
                                "connector_type": identity.connector_type,
                                "connector_identity_id": identity.identity_id,
                                "error": message,
                            }),
                        )
                        .await
                        .map_err(beaverki_tools::ToolError::Failed)?;
                    return Err(beaverki_tools::ToolError::Failed(error));
                }
            }
        }

        Ok(HouseholdDeliveryOutcome {
            delivery_id: delivery.delivery_id,
            recipient_user_id: recipient.user_id,
            recipient_display_name: recipient.display_name,
            connector_type: identity.connector_type,
            connector_identity_id: identity.identity_id,
            target_ref: delivery.target_ref,
            status: delivery.status,
            deduplicated: true,
        })
    }
}

impl RuntimeHouseholdDeliveryDelegate {
    pub(crate) async fn select_household_delivery_identity(
        &self,
        recipient_user_id: &str,
    ) -> std::result::Result<ConnectorIdentityRow, beaverki_tools::ToolError> {
        let identities = self
            .db
            .list_connector_identities_for_mapped_user(recipient_user_id)
            .await
            .map_err(beaverki_tools::ToolError::Failed)?;
        let Some(identity) = identities
            .into_iter()
            .find(|identity| identity.connector_type == "discord")
        else {
            return Err(beaverki_tools::ToolError::Denied {
                message: format!(
                    "recipient '{}' does not have a mapped delivery route",
                    recipient_user_id
                ),
                detail: json!({
                    "recipient_user_id": recipient_user_id,
                }),
            });
        };
        Ok(identity)
    }

    pub(crate) async fn send_household_delivery(
        &self,
        delivery: &HouseholdDeliveryRow,
        identity: &ConnectorIdentityRow,
        requester: &UserRow,
    ) -> Result<discord::DiscordDirectDeliveryResult> {
        info!(
            delivery_id = %delivery.delivery_id,
            requester_user_id = %requester.user_id,
            recipient_user_id = %delivery.recipient_user_id,
            connector_type = %identity.connector_type,
            connector_identity_id = %identity.identity_id,
            "attempting household delivery dispatch",
        );
        match identity.connector_type.as_str() {
            "discord" => {
                let token = self.discord_bot_token.as_deref().ok_or_else(|| {
                    anyhow!("Discord direct delivery is not configured with a bot token")
                })?;
                let http_client = discord::build_discord_http_client()?;
                let content = format_household_delivery_message(
                    &requester.display_name,
                    &delivery.message_text,
                );
                let sent =
                    discord::send_direct_household_message(&http_client, token, identity, &content)
                        .await?;
                info!(
                    delivery_id = %delivery.delivery_id,
                    connector_type = %identity.connector_type,
                    connector_identity_id = %identity.identity_id,
                    target_kind = sent.target_kind,
                    target_ref = %sent.target_ref,
                    "household delivery dispatched",
                );
                Ok(sent)
            }
            other => bail!("unsupported household delivery connector '{}'", other),
        }
    }
}

pub(crate) fn format_household_delivery_message(
    requester_display_name: &str,
    message_text: &str,
) -> String {
    format!(
        "Household message from {}:\n\n{}",
        requester_display_name,
        message_text.trim()
    )
}

pub(crate) fn tool_error_to_anyhow(error: beaverki_tools::ToolError) -> anyhow::Error {
    match error {
        beaverki_tools::ToolError::Denied { message, .. } => anyhow!(message),
        beaverki_tools::ToolError::Failed(error) => error,
    }
}