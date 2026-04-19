use anyhow::{Result, anyhow, bail};
use serde_json::Value;

use crate::ConnectorMessageRequest;

use super::models::{
    DiscordAuthor, DiscordInteractionCreate, DiscordInteractionData, DiscordInteractionOption,
};

pub(super) fn normalize_message_text(
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

pub(super) fn is_channel_trigger_attempt(content: &str, command_prefix: &str) -> bool {
    let trimmed = content.trim();
    let command_prefix = command_prefix.trim();
    !trimmed.is_empty() && !command_prefix.is_empty() && trimmed.starts_with(command_prefix)
}

pub(super) fn rewrite_channel_message_for_bot_mention(
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

pub(super) fn discord_author_display_name(author: &DiscordAuthor) -> Option<String> {
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

pub(super) fn discord_interaction_to_message_request(
    command_prefix: &str,
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
        let prefix = command_prefix.trim();
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

pub(super) fn interaction_command_text(data: &DiscordInteractionData) -> Result<String> {
    let mut path = vec![data.name.clone()];
    let mut values = Vec::new();
    let mut preferred_prompt = None;

    flatten_interaction_options(&data.options, &mut path, &mut values, &mut preferred_prompt);

    if let Some(approval_command) = normalize_interaction_approval_command(&path, &values) {
        return Ok(approval_command);
    }

    if values.is_empty() && path.len() == 1 {
        match path[0].as_str() {
            "new" => return Ok("/new".to_owned()),
            "status" => return Ok("/status".to_owned()),
            _ => {}
        }
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
