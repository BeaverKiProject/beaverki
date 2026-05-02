use anyhow::Result;
use beaverki_core::TaskState;
use beaverki_db::{ConversationSessionRow, Database, TaskEventRow, TaskRow};
use chrono::{DateTime, Utc};

use crate::connector_support::{assistant_reply_for_context, connector_context_from_events};

use super::DISCORD_ACTIVE_CONVERSATION_WINDOW_SECS;

#[derive(Debug, Clone)]
pub(crate) struct ConversationExchange {
    pub(crate) created_at: String,
    pub(crate) state: String,
    pub(crate) source_label: String,
    pub(crate) channel_id: Option<String>,
    pub(crate) user_label: String,
    pub(crate) user_text: String,
    pub(crate) assistant_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConversationStatus {
    FollowUp,
    FreshStart,
}

pub(crate) fn build_discord_task_context(
    session: &ConversationSessionRow,
    summary_text: Option<&str>,
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
        _ => context.push_str("- Conversation history is scoped to this connector session.\n"),
    }

    if let Some(summary_text) = summary_text
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        context.push_str("Older conversation summary:\n");
        for line in summary_text.lines() {
            let line = line.trim();
            if !line.is_empty() {
                context.push_str(&format!("- [Summary] {}\n", line));
            }
        }
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

pub(crate) fn discord_source_label(is_direct_message: bool) -> &'static str {
    if is_direct_message {
        "Discord direct message"
    } else {
        "Discord channel"
    }
}

pub(crate) async fn build_conversation_exchanges(
    db: &Database,
    tasks: &[TaskRow],
    fallback_user_label: &str,
) -> Result<Vec<ConversationExchange>> {
    let mut exchanges = Vec::with_capacity(tasks.len());
    for task in tasks {
        let events = db.fetch_task_events(&task.task_id).await?;
        exchanges.push(build_conversation_exchange(
            task,
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

fn compact_message_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn source_descriptor(source_label: &str, channel_id: Option<&str>) -> String {
    match channel_id {
        Some(channel_id) => format!("{source_label} | channel {channel_id}"),
        None => source_label.to_owned(),
    }
}
