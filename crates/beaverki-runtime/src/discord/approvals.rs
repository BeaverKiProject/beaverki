use anyhow::Result;
use beaverki_db::{
    ApprovalActionRow, ApprovalActionSet, ApprovalRow, ConnectorIdentityRow, TaskRow,
};
use chrono::{DateTime, Utc};
use serde_json::json;

use crate::connector_support::format_task_reply;
use crate::{ConnectorMessageRequest, RuntimeDaemon};

use super::APPROVAL_ACTION_TOKEN_PREFIX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ApprovalCommand<'a> {
    Inbox,
    Inspect(&'a str),
    Approve(&'a str),
    Deny(&'a str),
    Confirm(&'a str),
}

pub(super) async fn render_waiting_approval_reply(
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

pub(super) async fn issue_connector_approval_actions(
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

pub(super) fn parse_approval_command(command_text: &str) -> Option<ApprovalCommand<'_>> {
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

pub(super) fn render_approval_overview(
    approval: &ApprovalRow,
    action_set: &ApprovalActionSet,
) -> String {
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

pub(super) fn render_approval_detail(
    approval: &ApprovalRow,
    action_set: &ApprovalActionSet,
) -> String {
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

pub(super) fn render_critical_confirmation_prompt(
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
