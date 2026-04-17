use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use beaverki_core::MemoryScope;
use beaverki_db::{Database, NewMemory, ScheduleRow};
use beaverki_models::{ConversationItem, ModelProvider};
use beaverki_policy::visible_memory_scopes;
use beaverki_tools::{ToolContext, builtin_registry};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use cron::Schedule;
use mlua::{Lua, LuaSerdeExt, Value as LuaValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const SAFETY_AGENT_ID: &str = "agent_safety_builtin";

fn lua_to_anyhow(error: mlua::Error) -> anyhow::Error {
    anyhow!(error.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyReviewOutcome {
    pub verdict: String,
    pub risk_level: String,
    #[serde(default)]
    pub findings: Vec<String>,
    #[serde(default)]
    pub required_changes: Vec<String>,
    pub summary: String,
}

impl SafetyReviewOutcome {
    pub fn approved(&self) -> bool {
        self.verdict == "approved"
    }

    pub fn as_findings_json(&self) -> Value {
        json!({
            "findings": self.findings,
            "required_changes": self.required_changes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LuaExecutionInput {
    pub db: Database,
    pub owner_user_id: String,
    pub task_id: String,
    pub script_id: String,
    pub source_text: String,
    pub capability_profile: Value,
    pub working_dir: PathBuf,
    pub allowed_roots: Vec<PathBuf>,
    pub browser_interactive_launcher: Option<String>,
    pub browser_headless_program: Option<String>,
    pub browser_headless_args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LuaExecutionResult {
    pub result_text: String,
    pub deferred_until: Option<String>,
    pub notifications: Vec<String>,
    pub logs: Vec<String>,
}

pub async fn review_lua_script(
    provider: &Arc<dyn ModelProvider>,
    script_id: &str,
    task_id: Option<&str>,
    owner_user_id: &str,
    source_text: &str,
    capability_profile: &Value,
    intended_behavior_summary: &str,
) -> Result<SafetyReviewOutcome> {
    let request = json!({
        "review_type": "lua_script",
        "script_id": script_id,
        "originating_task_id": task_id,
        "owner_user_id": owner_user_id,
        "source_text": source_text,
        "capability_profile": capability_profile,
        "intended_behavior_summary": intended_behavior_summary,
    });
    review_with_prompt(
        provider,
        provider.model_names().safety_review.as_str(),
        LUA_REVIEW_INSTRUCTIONS,
        &request,
    )
    .await
}

async fn review_with_prompt(
    provider: &Arc<dyn ModelProvider>,
    model_name: &str,
    instructions: &str,
    request: &Value,
) -> Result<SafetyReviewOutcome> {
    let response = provider
        .generate_turn(
            model_name,
            instructions,
            &[ConversationItem::UserText(request.to_string())],
            &[],
        )
        .await?;
    let output_text = response.output_text.trim();
    if output_text.is_empty() {
        bail!("safety review returned an empty response");
    }
    serde_json::from_str(output_text)
        .with_context(|| format!("failed to parse safety review JSON: {output_text}"))
}

pub fn next_run_after(cron_expr: &str, after: &str) -> Result<String> {
    let schedule = Schedule::from_str(cron_expr)
        .with_context(|| format!("invalid cron expression '{cron_expr}'"))?;
    let base = DateTime::parse_from_rfc3339(after)
        .with_context(|| format!("invalid RFC3339 timestamp '{after}'"))?
        .with_timezone(&Utc);
    let next = schedule
        .after(&base)
        .next()
        .ok_or_else(|| anyhow!("cron expression '{cron_expr}' has no future occurrence"))?;
    Ok(next.to_rfc3339())
}

pub async fn execute_lua_script(input: LuaExecutionInput) -> Result<LuaExecutionResult> {
    let allowed_tools = input
        .capability_profile
        .get("allowed_tools")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        });
    let allowed_roots = capability_allowed_roots(
        &input.capability_profile,
        &input.working_dir,
        &input.allowed_roots,
    );
    let role_ids = input
        .db
        .list_user_roles(&input.owner_user_id)
        .await?
        .into_iter()
        .map(|row| row.role_id)
        .collect::<Vec<_>>();
    let visible_scopes = visible_memory_scopes(&role_ids);
    let task_id = input.task_id.clone();
    let script_id = input.script_id.clone();
    let owner_user_id = input.owner_user_id.clone();
    let db = input.db.clone();
    let working_dir = input.working_dir.clone();
    let tool_roots = allowed_roots.clone();
    let deferred_until = Arc::new(Mutex::new(None::<String>));
    let notifications = Arc::new(Mutex::new(Vec::<String>::new()));
    let logs = Arc::new(Mutex::new(Vec::<String>::new()));

    let lua = Lua::new();
    let globals = lua.globals();
    globals.set("os", LuaValue::Nil).map_err(lua_to_anyhow)?;
    globals.set("io", LuaValue::Nil).map_err(lua_to_anyhow)?;
    globals
        .set("package", LuaValue::Nil)
        .map_err(lua_to_anyhow)?;
    globals
        .set("require", LuaValue::Nil)
        .map_err(lua_to_anyhow)?;
    globals
        .set("dofile", LuaValue::Nil)
        .map_err(lua_to_anyhow)?;
    globals
        .set("loadfile", LuaValue::Nil)
        .map_err(lua_to_anyhow)?;
    globals.set("debug", LuaValue::Nil).map_err(lua_to_anyhow)?;

    let ctx = lua.create_table().map_err(lua_to_anyhow)?;

    {
        let db = db.clone();
        let owner_user_id = owner_user_id.clone();
        let visible_scopes = visible_scopes.clone();
        let function = lua
            .create_function(move |lua, limit: Option<u32>| {
                let handle = tokio::runtime::Handle::current();
                let db = db.clone();
                let owner_user_id = owner_user_id.clone();
                let visible_scopes = visible_scopes.clone();
                let rows = tokio::task::block_in_place(|| {
                    handle.block_on(async move {
                        db.retrieve_memories(
                            Some(&owner_user_id),
                            &visible_scopes,
                            i64::from(limit.unwrap_or(8)),
                        )
                        .await
                    })
                })
                .map_err(mlua::Error::external)?;
                let payload = rows
                    .into_iter()
                    .map(|row| {
                        json!({
                            "memory_id": row.memory_id,
                            "scope": row.scope,
                            "subject_type": row.subject_type,
                            "subject_key": row.subject_key,
                            "content_text": row.content_text,
                            "source_type": row.source_type,
                            "source_ref": row.source_ref,
                            "task_id": row.task_id,
                        })
                    })
                    .collect::<Vec<_>>();
                lua.to_value(&payload)
            })
            .map_err(lua_to_anyhow)?;
        ctx.set("memory_read", function).map_err(lua_to_anyhow)?;
    }

    {
        let db = db.clone();
        let owner_user_id = owner_user_id.clone();
        let task_id = task_id.clone();
        let function = lua
            .create_function(move |lua, value: LuaValue| {
                let payload: Value = lua.from_value(value).map_err(mlua::Error::external)?;
                let content_text = payload
                    .get("content_text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        mlua::Error::external(anyhow!("memory_write requires content_text"))
                    })?;
                let scope = payload
                    .get("scope")
                    .and_then(Value::as_str)
                    .unwrap_or("private")
                    .parse::<MemoryScope>()
                    .map_err(mlua::Error::external)?;
                let subject_type = payload
                    .get("subject_type")
                    .and_then(Value::as_str)
                    .unwrap_or("procedure");
                let subject_key = payload.get("subject_key").and_then(Value::as_str);
                let handle = tokio::runtime::Handle::current();
                let db = db.clone();
                let owner_user_id = owner_user_id.clone();
                let task_id = task_id.clone();
                let script_id = script_id.clone();
                tokio::task::block_in_place(|| {
                    handle.block_on(async move {
                        db.insert_memory(NewMemory {
                            owner_user_id: Some(&owner_user_id),
                            scope,
                            subject_type,
                            subject_key,
                            content_text,
                            sensitivity: "normal",
                            source_type: "tool",
                            source_ref: Some(&script_id),
                            task_id: Some(&task_id),
                        })
                        .await
                    })
                })
                .map_err(mlua::Error::external)
            })
            .map_err(lua_to_anyhow)?;
        ctx.set("memory_write", function).map_err(lua_to_anyhow)?;
    }

    {
        let allowed_tools = allowed_tools.clone();
        let working_dir = working_dir.clone();
        let tool_roots = tool_roots.clone();
        let browser_interactive_launcher = input.browser_interactive_launcher.clone();
        let browser_headless_program = input.browser_headless_program.clone();
        let browser_headless_args = input.browser_headless_args.clone();
        let function = lua
            .create_function(move |lua, (name, args): (String, LuaValue)| {
                if let Some(allowed_tools) = &allowed_tools
                    && !allowed_tools.iter().any(|allowed| allowed == &name)
                {
                    return Err(mlua::Error::external(anyhow!(
                        "tool '{name}' is not allowed by the script capability profile"
                    )));
                }

                let args_json: Value = lua.from_value(args).map_err(mlua::Error::external)?;
                let mut tool_context = ToolContext::new(working_dir.clone(), tool_roots.clone());
                tool_context.max_output_chars = 12_000;
                tool_context.browser_interactive_launcher = browser_interactive_launcher.clone();
                tool_context.browser_headless_program = browser_headless_program.clone();
                tool_context.browser_headless_args = browser_headless_args.clone();
                let registry = builtin_registry();
                let handle = tokio::runtime::Handle::current();
                let output = tokio::task::block_in_place(|| {
                    handle.block_on(async move {
                        registry.invoke(&name, args_json, &tool_context).await
                    })
                })
                .map_err(|error| mlua::Error::external(anyhow!(error.as_json().to_string())))?;
                lua.to_value(&output.payload)
            })
            .map_err(lua_to_anyhow)?;
        ctx.set("tool_call", function).map_err(lua_to_anyhow)?;
    }

    {
        let logs = Arc::clone(&logs);
        let function = lua
            .create_function(move |_lua, message: String| {
                logs.lock()
                    .map_err(|_| mlua::Error::external(anyhow!("failed to lock logs")))?
                    .push(message);
                Ok(())
            })
            .map_err(lua_to_anyhow)?;
        ctx.set("log_info", function).map_err(lua_to_anyhow)?;
    }

    {
        let notifications = Arc::clone(&notifications);
        let function = lua
            .create_function(move |_lua, message: String| {
                notifications
                    .lock()
                    .map_err(|_| mlua::Error::external(anyhow!("failed to lock notifications")))?
                    .push(message);
                Ok(())
            })
            .map_err(lua_to_anyhow)?;
        ctx.set("notify_user", function).map_err(lua_to_anyhow)?;
    }

    {
        let deferred_until = Arc::clone(&deferred_until);
        let function = lua
            .create_function(move |_lua, seconds: i64| {
                if seconds <= 0 {
                    return Err(mlua::Error::external(anyhow!(
                        "task_defer requires a positive number of seconds"
                    )));
                }
                let wake_at = (Utc::now() + ChronoDuration::seconds(seconds)).to_rfc3339();
                *deferred_until.lock().map_err(|_| {
                    mlua::Error::external(anyhow!("failed to lock deferred state"))
                })? = Some(wake_at.clone());
                Ok(wake_at)
            })
            .map_err(lua_to_anyhow)?;
        ctx.set("task_defer", function).map_err(lua_to_anyhow)?;
    }

    globals.set("ctx", ctx.clone()).map_err(lua_to_anyhow)?;
    let entry = lua
        .load(&input.source_text)
        .eval::<LuaValue>()
        .map_err(|error| anyhow!("failed to evaluate Lua script: {error}"))?;
    let value = match entry {
        LuaValue::Function(function) => function.call::<LuaValue>(ctx).map_err(lua_to_anyhow)?,
        other => other,
    };
    let result_text = match value {
        LuaValue::Nil => "Lua script completed without returning a value.".to_owned(),
        LuaValue::String(text) => text.to_str().map_err(lua_to_anyhow)?.to_owned(),
        other => {
            let json_value: Value = lua.from_value(other).map_err(lua_to_anyhow)?;
            serde_json::to_string_pretty(&json_value)?
        }
    };

    Ok(LuaExecutionResult {
        result_text,
        deferred_until: deferred_until
            .lock()
            .map_err(|_| anyhow!("failed to lock deferred state"))?
            .clone(),
        notifications: notifications
            .lock()
            .map_err(|_| anyhow!("failed to lock notifications"))?
            .clone(),
        logs: logs
            .lock()
            .map_err(|_| anyhow!("failed to lock logs"))?
            .clone(),
    })
}

pub fn schedule_enabled(schedule: &ScheduleRow) -> bool {
    schedule.enabled != 0
}

fn capability_allowed_roots(
    capability_profile: &Value,
    working_dir: &PathBuf,
    default_roots: &[PathBuf],
) -> Vec<PathBuf> {
    let configured = capability_profile
        .get("allowed_roots")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|path| {
                    let path = PathBuf::from(path);
                    if path.is_absolute() {
                        path
                    } else {
                        working_dir.join(path)
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if configured.is_empty() {
        default_roots.to_vec()
    } else {
        configured
    }
}

const LUA_REVIEW_INSTRUCTIONS: &str = r#"You are BeaverKI's safety review agent.
Review the provided Lua automation script for intent matching, dangerous side effects, privilege escalation, hidden exfiltration, and capability/profile mismatch.
Return only JSON with this exact schema:
{"verdict":"approved|rejected|needs_changes","risk_level":"low|medium|high|critical","findings":["..."],"required_changes":["..."],"summary":"..."}"#;
