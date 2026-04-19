use anyhow::{Context, Result, bail};
use beaverki_db::{WorkflowDefinitionRow, WorkflowStageRow};
use serde_json::{Value, json};

use crate::WorkflowDefinitionInput;

pub(crate) fn parse_json_field(raw: &str, label: &str) -> Result<Value> {
    serde_json::from_str(raw).with_context(|| format!("failed to parse {label}"))
}

pub(crate) fn validate_workflow_definition_input(
    definition: &WorkflowDefinitionInput,
) -> Result<()> {
    if definition.name.trim().is_empty() {
        bail!("workflow name cannot be empty");
    }
    if definition.stages.is_empty() {
        bail!("workflow must include at least one stage");
    }
    for stage in &definition.stages {
        match stage.kind.as_str() {
            "lua_script" | "lua_tool" => {
                if stage
                    .artifact_ref
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    bail!("workflow stage '{}' requires artifact_ref", stage.kind);
                }
            }
            "agent_task" => {
                if stage
                    .config
                    .get("prompt")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    bail!("agent_task stage requires config.prompt");
                }
            }
            "user_notify" => {}
            other => bail!("unsupported workflow stage kind '{other}'"),
        }
    }
    Ok(())
}

pub(crate) fn workflow_definition_json(
    workflow: &WorkflowDefinitionRow,
    stages: &[WorkflowStageRow],
) -> Result<Value> {
    Ok(json!({
        "workflow_id": workflow.workflow_id,
        "owner_user_id": workflow.owner_user_id,
        "name": workflow.name,
        "description": workflow.description,
        "status": workflow.status,
        "safety_status": workflow.safety_status,
        "current_version_number": workflow.current_version_number,
        "stages": stages
            .iter()
            .map(|stage| {
                Ok(json!({
                    "stage_id": stage.stage_id,
                    "version_number": stage.version_number,
                    "stage_index": stage.stage_index,
                    "stage_kind": stage.stage_kind,
                    "stage_label": stage.stage_label,
                    "artifact_ref": stage.artifact_ref,
                    "config": parse_json_field(&stage.stage_config_json, "workflow stage config")?,
                }))
            })
            .collect::<Result<Vec<_>>>()?,
    }))
}

pub(crate) fn apply_stage_result(
    artifacts: &mut Value,
    stage: &WorkflowStageRow,
    result: Value,
    metadata: Value,
) {
    if !artifacts.is_object() {
        *artifacts = json!({});
    }
    let artifact_object = artifacts.as_object_mut().expect("artifacts object");
    let stages = artifact_object
        .entry("stages".to_owned())
        .or_insert_with(|| json!({}));
    if !stages.is_object() {
        *stages = json!({});
    }
    stages.as_object_mut().expect("stages object").insert(
        stage.stage_index.to_string(),
        json!({
            "stage_id": stage.stage_id,
            "stage_kind": stage.stage_kind,
            "result": result.clone(),
            "metadata": metadata,
        }),
    );
    artifact_object.insert("last_result".to_owned(), result);
    artifact_object.insert("last_stage_id".to_owned(), json!(stage.stage_id));
    artifact_object.insert("last_stage_index".to_owned(), json!(stage.stage_index));
}

pub(crate) fn workflow_run_result_text(artifacts: &Value) -> String {
    match artifacts.get("last_result") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Object(map)) => {
            if let Some(Value::String(text)) = map.get("result_text") {
                return text.clone();
            }
            if let Some(Value::String(text)) = map.get("message") {
                return text.clone();
            }
            serde_json::to_string_pretty(&Value::Object(map.clone()))
                .unwrap_or_else(|_| "Workflow completed.".to_owned())
        }
        Some(value) => {
            serde_json::to_string_pretty(value).unwrap_or_else(|_| "Workflow completed.".to_owned())
        }
        None => "Workflow completed.".to_owned(),
    }
}

pub(crate) fn workflow_notify_recipient_exact_match(
    user: &beaverki_db::UserRow,
    normalized_query: &str,
) -> bool {
    let user_id = user.user_id.to_ascii_lowercase();
    let display_name = user.display_name.trim().to_ascii_lowercase();
    let short_user_id = user_id.strip_prefix("user_").unwrap_or(&user_id);
    normalized_query == user_id
        || normalized_query == short_user_id
        || normalized_query == display_name
}

pub(crate) fn workflow_notify_recipient_fuzzy_match(
    user: &beaverki_db::UserRow,
    normalized_query: &str,
) -> bool {
    let display_name = user.display_name.trim().to_ascii_lowercase();
    let user_id = user.user_id.to_ascii_lowercase();
    let short_user_id = user_id.strip_prefix("user_").unwrap_or(&user_id);
    display_name.contains(normalized_query)
        || display_name
            .split_whitespace()
            .any(|word| word.starts_with(normalized_query))
        || short_user_id.starts_with(normalized_query)
}

pub(crate) fn workflow_notify_message(stage_config: &Value, artifacts: &Value) -> String {
    if let Some(template) = stage_config
        .get("message_template")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return render_workflow_notify_template(template, artifacts);
    }

    stage_config
        .get("message")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| workflow_run_result_text(artifacts))
}

fn render_workflow_notify_template(template: &str, artifacts: &Value) -> String {
    let mut rendered = template.replace("{{last_result}}", &workflow_run_result_text(artifacts));

    if let Some(stages) = artifacts.get("stages").and_then(Value::as_object) {
        for (stage_index, stage_artifact) in stages {
            let Some(stage_result) = stage_artifact.get("result") else {
                continue;
            };
            rendered = rendered.replace(
                &format!("{{{{stages[{stage_index}].result}}}}"),
                &workflow_result_value_text(stage_result),
            );
        }
    }

    rendered
}

fn workflow_result_value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get("result_text") {
                return text.clone();
            }
            if let Some(Value::String(text)) = map.get("message") {
                return text.clone();
            }
            serde_json::to_string_pretty(value).unwrap_or_else(|_| "Workflow completed.".to_owned())
        }
        other => {
            serde_json::to_string_pretty(other).unwrap_or_else(|_| "Workflow completed.".to_owned())
        }
    }
}
