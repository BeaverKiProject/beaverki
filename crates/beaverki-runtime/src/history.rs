use anyhow::anyhow;
use async_trait::async_trait;
use beaverki_agent::{
    ConnectorHistoryDelegate, ConnectorHistoryMessage, ConnectorHistoryOutcome,
    ConnectorHistoryRequest,
};
use serde_json::json;

use crate::connector_support::connector_context_from_events;
use crate::discord;

#[derive(Clone)]
pub(crate) struct RuntimeConnectorHistoryDelegate {
    pub(crate) db: beaverki_db::Database,
    pub(crate) runtime_actor_id: String,
    pub(crate) discord_bot_token: Option<String>,
}

#[async_trait]
impl ConnectorHistoryDelegate for RuntimeConnectorHistoryDelegate {
    async fn read_connector_history(
        &self,
        request: ConnectorHistoryRequest<'_>,
    ) -> std::result::Result<ConnectorHistoryOutcome, beaverki_tools::ToolError> {
        let session_id = request
            .session_id
            .ok_or_else(|| beaverki_tools::ToolError::Denied {
                message: "connector history is only available for connector tasks with a session"
                    .to_owned(),
                detail: json!({
                    "task_id": request.task_id,
                }),
            })?;

        let session = self
            .db
            .fetch_conversation_session(session_id)
            .await
            .map_err(beaverki_tools::ToolError::Failed)?
            .ok_or_else(|| {
                beaverki_tools::ToolError::Failed(anyhow!(
                    "conversation session '{}' not found",
                    session_id
                ))
            })?;

        let events = self
            .db
            .fetch_task_events(request.task_id)
            .await
            .map_err(beaverki_tools::ToolError::Failed)?;
        let context = connector_context_from_events(&events).ok_or_else(|| {
            beaverki_tools::ToolError::Denied {
                message: "connector history is only available from a connector-originated task"
                    .to_owned(),
                detail: json!({
                    "task_id": request.task_id,
                }),
            }
        })?;

        if context.connector_type != "discord" {
            return Err(beaverki_tools::ToolError::Denied {
                message: format!(
                    "connector history is not available for connector '{}'",
                    context.connector_type
                ),
                detail: json!({
                    "connector_type": context.connector_type,
                }),
            });
        }

        let token = self.discord_bot_token.as_deref().ok_or_else(|| {
            beaverki_tools::ToolError::Failed(anyhow!(
                "Discord history inspection is not configured with a bot token"
            ))
        })?;
        let http_client =
            discord::build_discord_http_client().map_err(beaverki_tools::ToolError::Failed)?;
        let query = discord::DiscordHistoryQuery {
            limit: request.limit,
            before_message_id: request.before_message_id,
            after_message_id: request.after_message_id,
            around_message_id: request.around_message_id,
            addressed_to_bot_only: request.addressed_to_bot_only
                && session.session_kind != "direct_message",
        };
        let fetched =
            discord::fetch_discord_messages(&http_client, token, &context.channel_id, &query)
                .await
                .map_err(beaverki_tools::ToolError::Failed)?;

        let messages = fetched
            .messages
            .into_iter()
            .map(|message| {
                let author_display_name = discord_author_label(&message.author);
                let is_bot = message.author.bot.unwrap_or(false);
                ConnectorHistoryMessage {
                    message_id: message.id,
                    channel_id: message.channel_id,
                    author_external_user_id: message.author.id,
                    author_display_name,
                    content: truncate_connector_message(&message.content, 600),
                    created_at: message.timestamp,
                    referenced_message_id: message.referenced_message.map(|message| message.id),
                    is_bot,
                }
            })
            .collect::<Vec<_>>();

        self.db
            .append_task_event(
                request.task_id,
                "connector_history_read",
                "runtime",
                &self.runtime_actor_id,
                json!({
                    "connector_type": context.connector_type,
                    "channel_id": context.channel_id,
                    "session_id": session.session_id,
                    "session_kind": session.session_kind,
                    "limit": request.limit,
                    "before_message_id": request.before_message_id,
                    "after_message_id": request.after_message_id,
                    "around_message_id": request.around_message_id,
                    "addressed_to_bot_only": request.addressed_to_bot_only,
                    "fetched_message_count": messages.len(),
                }),
            )
            .await
            .map_err(beaverki_tools::ToolError::Failed)?;
        self.db
            .record_audit_event(
                "runtime",
                &self.runtime_actor_id,
                "connector_history_read",
                json!({
                    "task_id": request.task_id,
                    "connector_type": context.connector_type,
                    "channel_id": context.channel_id,
                    "session_id": session.session_id,
                    "session_kind": session.session_kind,
                    "limit": request.limit,
                    "before_message_id": request.before_message_id,
                    "after_message_id": request.after_message_id,
                    "around_message_id": request.around_message_id,
                    "addressed_to_bot_only": request.addressed_to_bot_only,
                    "fetched_message_count": messages.len(),
                }),
            )
            .await
            .map_err(beaverki_tools::ToolError::Failed)?;

        Ok(ConnectorHistoryOutcome {
            connector_type: context.connector_type,
            channel_id: context.channel_id,
            session_kind: session.session_kind,
            fetched_message_count: messages.len(),
            messages,
        })
    }
}

fn discord_author_label(author: &discord::DiscordAuthorSummary) -> String {
    author
        .global_name
        .as_deref()
        .or(author.username.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(author.id.as_str())
        .to_owned()
}

fn truncate_connector_message(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::truncate_connector_message;

    #[test]
    fn truncate_connector_message_adds_ellipsis_when_needed() {
        assert_eq!(truncate_connector_message("abcdef", 4), "abcd...");
        assert_eq!(truncate_connector_message("abc", 4), "abc");
    }
}
