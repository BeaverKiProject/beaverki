use anyhow::{Context, Result, anyhow, bail};
use beaverki_config::{
    DiscordChannelMode, LoadedConfig, SessionLifecycleAction, SessionLifecyclePolicy,
};
use beaverki_core::{MemoryKind, MemoryScope, TaskState};
use beaverki_db::{ConversationSessionRow, MemoryRow};
use beaverki_policy::visible_memory_scopes;
use chrono::{DateTime, Utc};

pub(crate) const CLI_CONVERSATION_HISTORY_LIMIT: i64 = 4;
const CLI_ACTIVE_CONVERSATION_WINDOW_SECS: i64 = 45 * 60;

pub(crate) const SESSION_RESET_COMMAND: &str = "/new";
pub(crate) const SESSION_STATUS_COMMAND: &str = "/status";
pub(crate) const SESSION_KIND_CLI: &str = "cli";
pub(crate) const SESSION_KIND_DIRECT_MESSAGE: &str = "direct_message";
pub(crate) const SESSION_KIND_GROUP_ROOM: &str = "group_room";
pub(crate) const SESSION_KIND_CRON_RUN: &str = "cron_run";
pub(crate) const SESSION_AUDIENCE_DIRECT_USER: &str = "direct_user";
pub(crate) const SESSION_AUDIENCE_GUEST_ROOM: &str = "guest_room";
pub(crate) const SESSION_AUDIENCE_SHARED_ROOM: &str = "shared_room";
pub(crate) const SESSION_AUDIENCE_SCHEDULED_RUN: &str = "scheduled_run";
pub(crate) const SESSION_LIFECYCLE_REASON_MANUAL_RESET: &str = "manual_reset";

pub(crate) fn connector_group_room_policy(
    config: &LoadedConfig,
    connector_type: &str,
    channel_id: &str,
) -> (&'static str, MemoryScope) {
    if connector_type == "discord" {
        match config.integrations.discord.channel_mode(channel_id) {
            Some(DiscordChannelMode::Guest) => {
                return (SESSION_AUDIENCE_GUEST_ROOM, MemoryScope::Private);
            }
            Some(DiscordChannelMode::Household) | None => {}
        }
    }

    (SESSION_AUDIENCE_SHARED_ROOM, MemoryScope::Household)
}

pub(crate) fn session_lifecycle_is_due(
    session: &ConversationSessionRow,
    policy: &SessionLifecyclePolicy,
    now: DateTime<Utc>,
) -> Result<bool> {
    let last_activity_at = DateTime::parse_from_rfc3339(&session.last_activity_at)
        .with_context(|| {
            format!(
                "conversation session '{}' has invalid last_activity_at '{}'",
                session.session_id, session.last_activity_at
            )
        })?
        .with_timezone(&Utc);
    let inactive_for = now.signed_duration_since(last_activity_at);
    let inactivity_threshold = chrono::Duration::seconds(
        i64::try_from(policy.inactivity_after_secs)
            .context("policy inactivity TTL is too large")?,
    );
    if inactive_for < inactivity_threshold {
        return Ok(false);
    }

    let already_applied_at = match policy.action {
        SessionLifecycleAction::Reset => session.last_reset_at.as_deref(),
        SessionLifecycleAction::Archive => session.archived_at.as_deref(),
    };
    let Some(already_applied_at) = already_applied_at else {
        return Ok(true);
    };
    let already_applied_at = DateTime::parse_from_rfc3339(already_applied_at)
        .with_context(|| {
            format!(
                "conversation session '{}' has invalid lifecycle timestamp '{}'",
                session.session_id, already_applied_at
            )
        })?
        .with_timezone(&Utc);
    Ok(already_applied_at < last_activity_at)
}

pub(crate) fn parse_scope(value: &str) -> Result<MemoryScope> {
    value
        .parse::<MemoryScope>()
        .map_err(|_| anyhow!("unsupported scope '{value}', expected private or household"))
}

pub(crate) fn parse_session_max_scope(session: &ConversationSessionRow) -> Result<MemoryScope> {
    parse_scope(&session.max_memory_scope).with_context(|| {
        format!(
            "conversation session '{}' has unsupported max memory scope '{}'",
            session.session_id, session.max_memory_scope
        )
    })
}

pub(crate) fn cap_task_scope_to_session(
    scope: MemoryScope,
    session_max_scope: MemoryScope,
) -> MemoryScope {
    match (scope, session_max_scope) {
        (_, MemoryScope::Private) => MemoryScope::Private,
        _ => scope,
    }
}

pub(crate) fn cap_scopes_to_session(
    scopes: Vec<MemoryScope>,
    session_max_scope: MemoryScope,
) -> Vec<MemoryScope> {
    scopes
        .into_iter()
        .filter(|scope| {
            matches!(session_max_scope, MemoryScope::Household)
                || matches!(scope, MemoryScope::Private)
        })
        .collect()
}

pub(crate) fn is_session_reset_command(objective: &str) -> bool {
    objective.trim() == SESSION_RESET_COMMAND
}

pub(crate) fn is_session_status_command(objective: &str) -> bool {
    objective.trim() == SESSION_STATUS_COMMAND
}

pub(crate) fn parse_memory_kind_filter(value: Option<&str>) -> Result<Option<MemoryKind>> {
    match value {
        None => Ok(None),
        Some("all") => Ok(None),
        Some(value) => value.parse::<MemoryKind>().map(Some).map_err(|_| {
            anyhow!("unsupported memory kind '{value}', expected semantic or episodic")
        }),
    }
}

pub(crate) fn resolve_visible_memory_scopes(
    role_ids: &[String],
    requested_scope: Option<&str>,
) -> Result<Vec<MemoryScope>> {
    let visible_scopes = visible_memory_scopes(role_ids);
    match requested_scope {
        None | Some("all") => Ok(visible_scopes),
        Some(scope_value) => {
            let scope = parse_scope(scope_value)?;
            if !visible_scopes.contains(&scope) {
                bail!("scope '{scope}' is not visible to the selected user");
            }
            Ok(vec![scope])
        }
    }
}

pub(crate) fn ensure_memory_visible_to_user(
    memory: &MemoryRow,
    user_id: &str,
    visible_scopes: &[MemoryScope],
) -> Result<()> {
    let memory_scope = memory.scope.parse::<MemoryScope>().map_err(|_| {
        anyhow!(
            "memory '{}' has unsupported scope '{}'",
            memory.memory_id,
            memory.scope
        )
    })?;
    if !visible_scopes.contains(&memory_scope) {
        bail!(
            "memory '{}' is not visible to the selected user",
            memory.memory_id
        );
    }

    match memory.owner_user_id.as_deref() {
        Some(owner_user_id) if owner_user_id == user_id => Ok(()),
        None => Ok(()),
        _ => bail!(
            "memory '{}' is not visible to the selected user",
            memory.memory_id
        ),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CliConversationExchange {
    pub(crate) created_at: String,
    pub(crate) state: String,
    pub(crate) user_text: String,
    pub(crate) assistant_text: String,
    pub(crate) task_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliConversationStatus {
    FollowUp,
    FreshStart,
}

pub(crate) fn build_cli_task_context(recent_exchanges: &[CliConversationExchange]) -> String {
    let mut context = String::new();
    context.push_str("CLI context:\n");
    context.push_str("- Source: CLI.\n");
    context.push_str(
        "- Conversation history for CLI requests is scoped to this BeaverKI user and entrypoint.\n",
    );

    let Some(latest_exchange) = recent_exchanges.first() else {
        context.push_str(
            "- Conversation status: new conversation. No recent prior CLI exchange is available.\n",
        );
        context.push_str(
            "- Treat the current command as a new conversation unless the user explicitly refers to older context.\n",
        );
        return context;
    };

    let age_text = latest_exchange_age_text(&latest_exchange.created_at)
        .unwrap_or_else(|| "at an unknown time".to_owned());
    match cli_conversation_status(&latest_exchange.state, &latest_exchange.created_at) {
        CliConversationStatus::FollowUp => {
            context.push_str(&format!(
                "- Conversation status: active follow-up. The latest CLI exchange with this user was {}.\n",
                age_text
            ));
            context.push_str(
                "- Continue the prior topic only if the new command depends on it. If the user starts a new topic, answer the new topic directly.\n",
            );
            context.push_str("Recent attributed CLI exchanges:\n");
            for exchange in recent_exchanges.iter().rev() {
                context.push_str(&format!(
                    "- [CLI] User: {}\n",
                    compact_message_text(&exchange.user_text)
                ));
                context.push_str(&format!(
                    "- [CLI] Assistant: {}\n",
                    compact_message_text(&exchange.assistant_text)
                ));
            }
        }
        CliConversationStatus::FreshStart => {
            context.push_str(&format!(
                "- Conversation status: fresh conversation. The latest CLI exchange with this user was {}.\n",
                age_text
            ));
            context.push_str(
                "- Treat the current command as a new conversation unless the user explicitly refers to the earlier topic. Keep stable facts and preferences if they are reliable.\n",
            );
        }
    }

    context
}

pub(crate) fn cli_conversation_status(state: &str, created_at: &str) -> CliConversationStatus {
    if matches!(
        state.parse::<TaskState>(),
        Ok(TaskState::Pending | TaskState::Running | TaskState::WaitingApproval)
    ) {
        return CliConversationStatus::FollowUp;
    }

    let Some(created_at) = parse_timestamp(created_at) else {
        return CliConversationStatus::FreshStart;
    };
    let age = Utc::now().signed_duration_since(created_at).num_seconds();
    if age <= CLI_ACTIVE_CONVERSATION_WINDOW_SECS {
        CliConversationStatus::FollowUp
    } else {
        CliConversationStatus::FreshStart
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
