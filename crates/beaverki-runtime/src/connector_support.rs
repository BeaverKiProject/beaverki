use beaverki_core::TaskState;
use beaverki_db::{TaskEventRow, TaskRow};
use serde_json::Value;

pub(crate) const CONNECTOR_MESSAGE_CONTEXT_EVENT: &str = "connector_message_context";
pub(crate) const CONNECTOR_FOLLOW_UP_REQUESTED_EVENT: &str = "connector_follow_up_requested";
pub(crate) const CONNECTOR_FOLLOW_UP_SENT_EVENT: &str = "connector_follow_up_sent";

#[derive(Debug, Clone)]
pub(crate) struct ConnectorEventContext {
    pub(crate) connector_type: String,
    pub(crate) source_label: String,
    pub(crate) channel_id: String,
    pub(crate) message_id: Option<String>,
    pub(crate) user_label: Option<String>,
}

pub(crate) fn assistant_reply_for_context(task: &TaskRow) -> String {
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

pub(crate) fn format_task_reply(task: &TaskRow) -> String {
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

pub(crate) fn connector_context_from_events(
    events: &[TaskEventRow],
) -> Option<ConnectorEventContext> {
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

pub(crate) fn connector_type_from_events(events: &[TaskEventRow]) -> Option<String> {
    connector_context_from_events(events).map(|context| context.connector_type)
}

pub(crate) fn connector_follow_up_channel(
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
