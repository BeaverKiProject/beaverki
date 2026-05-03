use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use beaverki_policy::{classify_shell_command, generated_shell_execution_allowed};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio::time::timeout;
use walkdir::WalkDir;

mod notion;

use self::notion::{
    NotionApiRequestTool, NotionAppendBlockChildrenTool, NotionCreateCommentTool,
    NotionCreatePageTool, NotionDeleteBlockTool, NotionFetchTool, NotionSearchTool,
    NotionUpdatePageTool,
};

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

pub fn validate_openai_tool_definitions(definitions: &[ToolDefinition]) -> Result<()> {
    for definition in definitions {
        validate_openai_schema_node(&definition.input_schema, &definition.name)
            .with_context(|| format!("tool '{}' has invalid input schema", definition.name))?;
    }
    Ok(())
}

fn validate_openai_schema_node(schema: &Value, path: &str) -> Result<()> {
    let schema_object = schema
        .as_object()
        .ok_or_else(|| anyhow!("schema path '{}' must be an object", path))?;

    let is_object_schema = schema_object.contains_key("properties")
        || schema_object
            .get("type")
            .map(|types| {
                schema_type_list(types, path)
                    .map(|type_list| type_list.into_iter().any(|entry| entry == "object"))
            })
            .transpose()?
            .unwrap_or(false);

    if is_object_schema {
        match schema_object.get("additionalProperties") {
            Some(Value::Bool(false)) => {}
            _ => {
                bail!(
                    "schema path '{}' must declare additionalProperties as false",
                    path
                )
            }
        }
    }

    if let Some(properties) = schema_object.get("properties") {
        let properties = properties
            .as_object()
            .ok_or_else(|| anyhow!("schema path '{}' has non-object properties", path))?;
        let required = schema_object
            .get("required")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("schema path '{}' must declare required as an array", path))?;
        let mut property_keys = properties.keys().cloned().collect::<Vec<_>>();
        property_keys.sort();

        let mut required_keys = required
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("schema path '{}' has non-string required entry", path))
            })
            .collect::<Result<Vec<_>>>()?;
        required_keys.sort();

        if required_keys != property_keys {
            bail!(
                "schema path '{}' must declare required keys {:?}, found {:?}",
                path,
                property_keys,
                required_keys
            );
        }

        for (name, child_schema) in properties {
            validate_openai_schema_node(child_schema, &format!("{}.{}", path, name))?;
        }
    }

    if let Some(items) = schema_object.get("items") {
        validate_openai_schema_node(items, &format!("{}.items", path))?;
    }

    for keyword in ["anyOf", "allOf", "oneOf"] {
        if let Some(variants) = schema_object.get(keyword) {
            let variants = variants
                .as_array()
                .ok_or_else(|| anyhow!("schema path '{}.{}' must be an array", path, keyword))?;
            for (index, variant) in variants.iter().enumerate() {
                validate_openai_schema_node(variant, &format!("{}.{}[{}]", path, keyword, index))?;
            }
        }
    }

    Ok(())
}

pub fn validate_json_schema_contract(schema: &Value) -> Result<()> {
    validate_json_schema_node(schema, "$")
}

fn validate_json_schema_node(schema: &Value, path: &str) -> Result<()> {
    let schema_object = schema
        .as_object()
        .ok_or_else(|| anyhow!("schema path '{}' must be an object", path))?;

    if let Some(types) = schema_object.get("type") {
        let type_list = schema_type_list(types, path)?;
        for entry in &type_list {
            ensure_supported_schema_type(entry, path)?;
        }
    }

    if let Some(properties) = schema_object.get("properties") {
        let properties = properties
            .as_object()
            .ok_or_else(|| anyhow!("schema path '{}.properties' must be an object", path))?;
        for (name, child_schema) in properties {
            validate_json_schema_node(child_schema, &format!("{path}.properties.{name}"))?;
        }
    }

    if let Some(required) = schema_object.get("required") {
        let required = required
            .as_array()
            .ok_or_else(|| anyhow!("schema path '{}.required' must be an array", path))?;
        for entry in required {
            entry
                .as_str()
                .ok_or_else(|| anyhow!("schema path '{}.required' must contain strings", path))?;
        }
    }

    if let Some(additional_properties) = schema_object.get("additionalProperties")
        && !additional_properties.is_boolean()
    {
        bail!(
            "schema path '{}.additionalProperties' must be a boolean",
            path
        );
    }

    if let Some(items) = schema_object.get("items") {
        validate_json_schema_node(items, &format!("{path}.items"))?;
    }

    if let Some(variants) = schema_object.get("enum")
        && !variants.is_array()
    {
        bail!("schema path '{}.enum' must be an array", path);
    }

    for keyword in ["anyOf", "allOf", "oneOf"] {
        if let Some(variants) = schema_object.get(keyword) {
            let variants = variants
                .as_array()
                .ok_or_else(|| anyhow!("schema path '{}.{}' must be an array", path, keyword))?;
            for (index, variant) in variants.iter().enumerate() {
                validate_json_schema_node(variant, &format!("{path}.{keyword}[{index}]"))?;
            }
        }
    }

    Ok(())
}

pub fn validate_json_value_against_schema(value: &Value, schema: &Value) -> Result<()> {
    validate_json_value_against_schema_at(value, schema, "$")
}

fn validate_json_value_against_schema_at(value: &Value, schema: &Value, path: &str) -> Result<()> {
    let schema_object = schema
        .as_object()
        .ok_or_else(|| anyhow!("schema path '{}' must be an object", path))?;

    if let Some(types) = schema_object.get("type") {
        let type_list = schema_type_list(types, path)?;
        if !type_matches_any(value, &type_list) {
            bail!(
                "value at '{}' does not match expected type(s) {:?}",
                path,
                type_list
            );
        }
    }

    if let Some(variants) = schema_object.get("enum") {
        let variants = variants
            .as_array()
            .ok_or_else(|| anyhow!("schema path '{}.enum' must be an array", path))?;
        if !variants.iter().any(|variant| variant == value) {
            bail!("value at '{}' is not in the allowed enum set", path);
        }
    }

    if let Some(properties) = schema_object.get("properties")
        && value.is_object()
    {
        let value_object = value
            .as_object()
            .ok_or_else(|| anyhow!("value at '{}' must be an object", path))?;
        let properties = properties
            .as_object()
            .ok_or_else(|| anyhow!("schema path '{}.properties' must be an object", path))?;
        let required = schema_object
            .get("required")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for required_key in required {
            let required_key = required_key
                .as_str()
                .ok_or_else(|| anyhow!("schema path '{}.required' must contain strings", path))?;
            if !value_object.contains_key(required_key) {
                bail!(
                    "value at '{}' is missing required property '{}'",
                    path,
                    required_key
                );
            }
        }
        if schema_object
            .get("additionalProperties")
            .and_then(Value::as_bool)
            == Some(false)
        {
            for key in value_object.keys() {
                if !properties.contains_key(key) {
                    bail!("value at '{}' has unsupported property '{}'", path, key);
                }
            }
        }
        for (name, child_schema) in properties {
            if let Some(child_value) = value_object.get(name) {
                validate_json_value_against_schema_at(
                    child_value,
                    child_schema,
                    &format!("{path}.{name}"),
                )?;
            }
        }
    }

    if let Some(items) = schema_object.get("items")
        && let Some(values) = value.as_array()
    {
        for (index, entry) in values.iter().enumerate() {
            validate_json_value_against_schema_at(entry, items, &format!("{path}[{index}]"))?;
        }
    }

    if let Some(variants) = schema_object.get("anyOf") {
        let variants = variants
            .as_array()
            .ok_or_else(|| anyhow!("schema path '{}.anyOf' must be an array", path))?;
        if !variants
            .iter()
            .any(|variant| validate_json_value_against_schema_at(value, variant, path).is_ok())
        {
            bail!(
                "value at '{}' did not match any allowed schema variant",
                path
            );
        }
    }

    if let Some(variants) = schema_object.get("oneOf") {
        let variants = variants
            .as_array()
            .ok_or_else(|| anyhow!("schema path '{}.oneOf' must be an array", path))?;
        let matches = variants
            .iter()
            .filter(|variant| validate_json_value_against_schema_at(value, variant, path).is_ok())
            .count();
        if matches != 1 {
            bail!(
                "value at '{}' must match exactly one schema variant, matched {}",
                path,
                matches
            );
        }
    }

    if let Some(variants) = schema_object.get("allOf") {
        let variants = variants
            .as_array()
            .ok_or_else(|| anyhow!("schema path '{}.allOf' must be an array", path))?;
        for variant in variants {
            validate_json_value_against_schema_at(value, variant, path)?;
        }
    }

    Ok(())
}

fn schema_type_list<'a>(types: &'a Value, path: &str) -> Result<Vec<&'a str>> {
    match types {
        Value::String(value) => Ok(vec![value.as_str()]),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| anyhow!("schema path '{}.type' must contain strings", path))
            })
            .collect(),
        _ => bail!("schema path '{}.type' must be a string or array", path),
    }
}

fn ensure_supported_schema_type(type_name: &str, path: &str) -> Result<()> {
    match type_name {
        "object" | "array" | "string" | "number" | "integer" | "boolean" | "null" => Ok(()),
        other => bail!("schema path '{}' uses unsupported type '{}'", path, other),
    }
}

fn type_matches_any(value: &Value, types: &[&str]) -> bool {
    types.iter().any(|type_name| type_matches(value, type_name))
}

fn type_matches(value: &Value, type_name: &str) -> bool {
    match type_name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct ToolContext {
    pub working_dir: PathBuf,
    pub allowed_roots: Vec<PathBuf>,
    pub skill_search_roots: Vec<PathBuf>,
    pub max_output_chars: usize,
    pub approved_shell_commands: Vec<String>,
    pub default_timezone: Option<String>,
    pub browser_interactive_launcher: Option<String>,
    pub browser_headless_program: Option<String>,
    pub browser_headless_args: Vec<String>,
    pub notion_api_base_url: Option<String>,
    pub notion_api_version: Option<String>,
    pub notion_api_token: Option<String>,
    pub notion_default_parent_kind: Option<String>,
    pub notion_default_parent_ref: Option<String>,
}

impl ToolContext {
    pub fn new(working_dir: PathBuf, allowed_roots: Vec<PathBuf>) -> Self {
        Self {
            working_dir,
            allowed_roots,
            skill_search_roots: Vec::new(),
            max_output_chars: 12_000,
            approved_shell_commands: Vec::new(),
            default_timezone: None,
            browser_interactive_launcher: None,
            browser_headless_program: None,
            browser_headless_args: Vec::new(),
            notion_api_base_url: None,
            notion_api_version: None,
            notion_api_token: None,
            notion_default_parent_kind: None,
            notion_default_parent_ref: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub payload: Value,
}

#[derive(Debug)]
pub enum ToolError {
    Denied { message: String, detail: Value },
    Failed(anyhow::Error),
}

impl ToolError {
    pub fn as_json(&self) -> Value {
        match self {
            Self::Denied { message, detail } => json!({
                "error": {
                    "kind": "denied",
                    "message": message,
                    "detail": detail,
                }
            }),
            Self::Failed(error) => json!({
                "error": {
                    "kind": "failed",
                    "message": error.to_string(),
                }
            }),
        }
    }
}

impl From<anyhow::Error> for ToolError {
    fn from(value: anyhow::Error) -> Self {
        Self::Failed(value)
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError>;
}

#[derive(Default)]
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T>(&mut self, tool: T)
    where
        T: Tool + 'static,
    {
        self.tools
            .insert(tool.definition().name.clone(), Arc::new(tool));
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        let mut definitions = self
            .tools
            .values()
            .map(|tool| tool.definition())
            .collect::<Vec<_>>();
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        definitions
    }

    pub async fn invoke(
        &self,
        name: &str,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| ToolError::Failed(anyhow!("unknown tool: {name}")))?;
        tool.call(input, context).await
    }
}

pub fn builtin_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry.register(ShellExecTool);
    registry.register(ListFilesTool);
    registry.register(FindFilesTool);
    registry.register(ReadTextTool);
    registry.register(WriteTextTool);
    registry.register(SearchFilesTool);
    registry.register(BrowserVisitTool);
    registry.register(NotionSearchTool);
    registry.register(NotionFetchTool);
    registry.register(NotionCreatePageTool);
    registry.register(NotionUpdatePageTool);
    registry.register(NotionAppendBlockChildrenTool);
    registry.register(NotionDeleteBlockTool);
    registry.register(NotionCreateCommentTool);
    registry.register(NotionApiRequestTool);
    registry
}

pub struct BrowserVisitTool;

#[async_trait]
impl Tool for BrowserVisitTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "browser_visit".to_owned(),
            description: "Open a page in an interactive browser session or fetch its DOM through a headless browser session.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "mode": {
                        "type": "string",
                        "enum": ["interactive", "headless"]
                    }
                },
                "required": ["url", "mode"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let url = input
            .get("url")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("browser_visit requires url")))?;
        ensure_browser_url(url).map_err(ToolError::Failed)?;

        let mode = input
            .get("mode")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("browser_visit requires mode")))?;

        match mode {
            "interactive" => launch_interactive_browser(url, context).await,
            "headless" => run_headless_browser(url, context).await,
            other => Err(ToolError::Failed(anyhow!(
                "unsupported browser mode '{other}'"
            ))),
        }
    }
}

pub struct ShellExecTool;

#[async_trait]
impl Tool for ShellExecTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "shell_exec".to_owned(),
            description: "Run a low-risk shell command for local inspection. Only read-only commands are allowed in M0.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("shell_exec requires a non-empty command")))?;

        let risk = classify_shell_command(command);
        let approved = context
            .approved_shell_commands
            .iter()
            .any(|approved| approved.trim() == command);
        if !approved && !generated_shell_execution_allowed(risk) {
            return Err(ToolError::Denied {
                message: "shell command denied by policy".to_owned(),
                detail: json!({
                    "risk": risk.as_str(),
                    "command": command,
                    "approved": approved,
                }),
            });
        }

        let mut child = Command::new("bash");
        child
            .arg("-lc")
            .arg(command)
            .current_dir(&context.working_dir);

        let output = timeout(Duration::from_secs(20), child.output())
            .await
            .context("shell command timed out")
            .map_err(ToolError::Failed)?
            .context("shell command failed to start")
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "risk": risk.as_str(),
                "allowed": true,
                "approved_by_override": approved,
                "command": command,
                "exit_code": output.status.code(),
                "stdout": truncate_text(&String::from_utf8_lossy(&output.stdout), context.max_output_chars),
                "stderr": truncate_text(&String::from_utf8_lossy(&output.stderr), context.max_output_chars),
            }),
        })
    }
}

pub struct ListFilesTool;

#[async_trait]
impl Tool for ListFilesTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "filesystem_list".to_owned(),
            description: "List files and directories within the allowed workspace roots for safe discovery without shell commands.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "recursive": { "type": ["boolean", "null"] },
                    "max_depth": { "type": ["integer", "null"], "minimum": 0, "maximum": 8 }
                },
                "required": ["path", "recursive", "max_depth"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("filesystem_list requires path")))?;
        let recursive = input
            .get("recursive")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let max_depth = input
            .get("max_depth")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(if recursive { 3 } else { 1 });
        let resolved = resolve_allowed_path(path, context, PathAccess::Read)?;
        let metadata = fs::metadata(&resolved)
            .with_context(|| format!("failed to read metadata for '{}'", resolved.display()))
            .map_err(ToolError::Failed)?;

        if metadata.is_file() {
            return Ok(ToolOutput {
                payload: json!({
                    "path": resolved.display().to_string(),
                    "kind": "file",
                    "entries": [json!({
                        "path": resolved.display().to_string(),
                        "kind": "file",
                        "size_bytes": metadata.len(),
                    })],
                }),
            });
        }

        let walk_depth = if recursive { max_depth } else { 1 };
        let mut entries = Vec::new();

        for entry in WalkDir::new(&resolved)
            .follow_links(false)
            .max_depth(walk_depth)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if entry.path() == resolved {
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let kind = if metadata.is_dir() {
                "directory"
            } else if metadata.is_file() {
                "file"
            } else {
                "other"
            };

            entries.push(json!({
                "path": entry.path().display().to_string(),
                "kind": kind,
                "size_bytes": metadata.is_file().then_some(metadata.len()),
            }));

            if entries.len() >= 250 {
                break;
            }
        }

        Ok(ToolOutput {
            payload: json!({
                "path": resolved.display().to_string(),
                "kind": "directory",
                "recursive": recursive,
                "max_depth": walk_depth,
                "entries": entries,
            }),
        })
    }
}

pub struct ReadTextTool;

#[async_trait]
impl Tool for ReadTextTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "filesystem_read_text".to_owned(),
            description: "Read a UTF-8 text file within the allowed workspace roots.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("filesystem_read_text requires path")))?;
        let resolved = resolve_allowed_path(path, context, PathAccess::Read)?;
        let text = tokio::fs::read_to_string(&resolved)
            .await
            .with_context(|| format!("failed to read {}", resolved.display()))
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "path": resolved.display().to_string(),
                "content": truncate_text(&text, 32_000),
            }),
        })
    }
}

pub struct FindFilesTool;

#[async_trait]
impl Tool for FindFilesTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "filesystem_find".to_owned(),
            description:
                "Find files or directories under an allowed root by file name or relative path fragment."
                    .to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name_pattern": { "type": "string" },
                    "root": { "type": ["string", "null"] },
                    "kind": {
                        "type": ["string", "null"],
                        "enum": ["file", "directory", "any", null]
                    }
                },
                "required": ["name_pattern", "root", "kind"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let name_pattern = input
            .get("name_pattern")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("filesystem_find requires name_pattern")))?;
        let root = input.get("root").and_then(Value::as_str).unwrap_or(".");
        let kind = input.get("kind").and_then(Value::as_str).unwrap_or("any");
        let resolved_root = resolve_allowed_path(root, context, PathAccess::Read)?;
        let lowered_pattern = name_pattern.to_ascii_lowercase();
        let mut matches = Vec::new();

        for entry in WalkDir::new(&resolved_root)
            .follow_links(false)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if entry.path() == resolved_root {
                continue;
            }

            let entry_kind = if entry.file_type().is_file() {
                "file"
            } else if entry.file_type().is_dir() {
                "directory"
            } else {
                continue;
            };
            if kind != "any" && kind != entry_kind {
                continue;
            }

            let relative_path = entry
                .path()
                .strip_prefix(&resolved_root)
                .unwrap_or(entry.path())
                .display()
                .to_string();
            let file_name = entry.file_name().to_string_lossy();
            let matches_pattern = file_name.to_ascii_lowercase().contains(&lowered_pattern)
                || relative_path
                    .to_ascii_lowercase()
                    .contains(&lowered_pattern);
            if !matches_pattern {
                continue;
            }

            matches.push(json!({
                "path": entry.path().display().to_string(),
                "relative_path": relative_path,
                "kind": entry_kind,
            }));

            if matches.len() >= 50 {
                break;
            }
        }

        Ok(ToolOutput {
            payload: json!({
                "name_pattern": name_pattern,
                "root": resolved_root.display().to_string(),
                "kind": kind,
                "matches": matches,
            }),
        })
    }
}

pub struct WriteTextTool;

#[async_trait]
impl Tool for WriteTextTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "filesystem_write_text".to_owned(),
            description: "Write or overwrite a UTF-8 text file within the allowed workspace roots."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("filesystem_write_text requires path")))?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("filesystem_write_text requires content")))?;
        let resolved = resolve_allowed_path(path, context, PathAccess::Write)?;

        if let Some(parent) = resolved.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))
                .map_err(ToolError::Failed)?;
        }
        tokio::fs::write(&resolved, content)
            .await
            .with_context(|| format!("failed to write {}", resolved.display()))
            .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "path": resolved.display().to_string(),
                "bytes_written": content.len(),
            }),
        })
    }
}

pub struct SearchFilesTool;

#[async_trait]
impl Tool for SearchFilesTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "filesystem_search".to_owned(),
            description: "Search UTF-8 files under an allowed root for a plain-text pattern."
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "root": { "type": ["string", "null"] }
                },
                "required": ["pattern", "root"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let pattern = input
            .get("pattern")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("filesystem_search requires pattern")))?;
        let root = input.get("root").and_then(Value::as_str).unwrap_or(".");
        let resolved_root = resolve_allowed_path(root, context, PathAccess::Read)?;
        let mut matches = Vec::new();

        for entry in WalkDir::new(&resolved_root)
            .follow_links(false)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_type().is_file())
        {
            if matches.len() >= 25 {
                break;
            }

            let contents = match std::fs::read_to_string(entry.path()) {
                Ok(contents) => contents,
                Err(_) => continue,
            };

            for (line_number, line) in contents.lines().enumerate() {
                if line.contains(pattern) {
                    matches.push(json!({
                        "path": entry.path().display().to_string(),
                        "line_number": line_number + 1,
                        "line": truncate_text(line, 500),
                    }));
                    if matches.len() >= 25 {
                        break;
                    }
                }
            }
        }

        Ok(ToolOutput {
            payload: json!({
                "pattern": pattern,
                "root": resolved_root.display().to_string(),
                "matches": matches,
            }),
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum PathAccess {
    Read,
    Write,
}

fn resolve_allowed_path(input: &str, context: &ToolContext, access: PathAccess) -> Result<PathBuf> {
    let candidate = if Path::new(input).is_absolute() {
        normalize_path(Path::new(input))
    } else {
        normalize_path(&context.working_dir.join(input))
    };
    let candidate = canonicalize_for_access(&candidate, access)
        .with_context(|| format!("failed to resolve path '{}'", Path::new(input).display()))?;

    let allowed_roots = context
        .allowed_roots
        .iter()
        .map(|root| {
            fs::canonicalize(root)
                .with_context(|| format!("failed to resolve allowed root '{}'", root.display()))
        })
        .collect::<Result<Vec<_>>>()?;

    if allowed_roots
        .iter()
        .any(|root| candidate == *root || candidate.starts_with(root))
    {
        Ok(candidate)
    } else {
        bail!(
            "path '{}' is outside allowed roots",
            Path::new(input).display()
        )
    }
}

fn canonicalize_for_access(path: &Path, access: PathAccess) -> Result<PathBuf> {
    match access {
        PathAccess::Read => fs::canonicalize(path)
            .with_context(|| format!("failed to canonicalize '{}'", path.display())),
        PathAccess::Write => {
            if path.exists() {
                return fs::canonicalize(path)
                    .with_context(|| format!("failed to canonicalize '{}'", path.display()));
            }

            let file_name = path
                .file_name()
                .ok_or_else(|| anyhow!("write path '{}' has no file name", path.display()))?
                .to_os_string();
            let parent = path.parent().ok_or_else(|| {
                anyhow!("write path '{}' has no parent directory", path.display())
            })?;
            let canonical_parent = fs::canonicalize(parent)
                .with_context(|| format!("failed to canonicalize '{}'", parent.display()))?;
            Ok(join_normalized(&canonical_parent, &file_name))
        }
    }
}

fn join_normalized(parent: &Path, child: &OsString) -> PathBuf {
    let mut path = parent.to_path_buf();
    path.push(child);
    path
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new("/")),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

pub(crate) fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_owned()
    } else {
        let shortened: String = text.chars().take(max_chars).collect();
        format!("{shortened}\n...[truncated]")
    }
}

fn ensure_browser_url(url: &str) -> Result<()> {
    if url.starts_with("https://") || url.starts_with("http://") {
        Ok(())
    } else {
        bail!("browser_visit only supports http:// and https:// URLs")
    }
}

async fn launch_interactive_browser(
    url: &str,
    context: &ToolContext,
) -> std::result::Result<ToolOutput, ToolError> {
    #[cfg(target_os = "macos")]
    let command_and_args = {
        let launcher = context
            .browser_interactive_launcher
            .clone()
            .unwrap_or_else(|| "open".to_owned());
        (launcher, vec![url.to_owned()])
    };

    #[cfg(target_os = "linux")]
    let command_and_args = {
        let launcher = context
            .browser_interactive_launcher
            .clone()
            .unwrap_or_else(|| "xdg-open".to_owned());
        (launcher, vec![url.to_owned()])
    };

    #[cfg(target_os = "windows")]
    let command_and_args = {
        let launcher = context
            .browser_interactive_launcher
            .clone()
            .unwrap_or_else(|| "cmd".to_owned());
        (
            launcher,
            vec![
                "/C".to_owned(),
                "start".to_owned(),
                "".to_owned(),
                url.to_owned(),
            ],
        )
    };

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let command_and_args = {
        let launcher = context
            .browser_interactive_launcher
            .clone()
            .ok_or_else(|| {
                ToolError::Failed(anyhow!(
                    "interactive browser launcher is not configured for this platform"
                ))
            })?;
        (launcher, vec![url.to_owned()])
    };

    let program = resolve_program(&command_and_args.0).ok_or_else(|| {
        ToolError::Failed(anyhow!(
            "interactive browser launcher '{}' was not found in PATH",
            command_and_args.0
        ))
    })?;

    let mut child = Command::new(&program);
    child
        .args(&command_and_args.1)
        .current_dir(&context.working_dir);
    let spawned = child
        .spawn()
        .with_context(|| {
            format!(
                "failed to launch interactive browser via {}",
                program.display()
            )
        })
        .map_err(ToolError::Failed)?;

    Ok(ToolOutput {
        payload: json!({
            "mode": "interactive",
            "url": url,
            "launcher": program.display().to_string(),
            "pid": spawned.id(),
        }),
    })
}

async fn run_headless_browser(
    url: &str,
    context: &ToolContext,
) -> std::result::Result<ToolOutput, ToolError> {
    let program_name = context
        .browser_headless_program
        .clone()
        .unwrap_or_else(default_headless_browser_program);
    let program = resolve_program(&program_name).ok_or_else(|| {
        ToolError::Failed(anyhow!(
            "headless browser '{}' was not found in PATH",
            program_name
        ))
    })?;

    let mut child = Command::new(&program);
    child.current_dir(&context.working_dir);
    child.args(&context.browser_headless_args);
    child.args(["--headless", "--disable-gpu", "--dump-dom", url]);

    let output = timeout(Duration::from_secs(30), child.output())
        .await
        .context("headless browser command timed out")
        .map_err(ToolError::Failed)?
        .with_context(|| format!("failed to start headless browser via {}", program.display()))
        .map_err(ToolError::Failed)?;

    Ok(ToolOutput {
        payload: json!({
            "mode": "headless",
            "url": url,
            "browser": program.display().to_string(),
            "exit_code": output.status.code(),
            "dom": truncate_text(&String::from_utf8_lossy(&output.stdout), 32_000),
            "stderr": truncate_text(&String::from_utf8_lossy(&output.stderr), context.max_output_chars),
        }),
    })
}

fn default_headless_browser_program() -> String {
    if cfg!(target_os = "windows") {
        "chrome".to_owned()
    } else {
        [
            "chromium",
            "chromium-browser",
            "google-chrome",
            "microsoft-edge",
            "chrome",
        ]
        .into_iter()
        .find_map(resolve_program)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "chromium".to_owned())
    }
}

fn resolve_program(program: &str) -> Option<PathBuf> {
    let candidate = Path::new(program);
    if candidate.is_absolute() || program.contains('/') {
        if candidate.exists() {
            return Some(candidate.to_path_buf());
        }
        return None;
    }

    let paths = env::var_os("PATH")?;
    for path in env::split_paths(&paths) {
        let joined = path.join(program);
        if joined.is_file() {
            return Some(joined);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_tool_schemas_satisfy_openai_requirements() {
        let definitions = builtin_registry().definitions();

        validate_openai_tool_definitions(&definitions).expect("valid tool schemas");
    }

    #[test]
    fn rejects_openai_object_schemas_without_closed_properties() {
        validate_openai_tool_definitions(&[ToolDefinition {
            name: "demo".to_owned(),
            description: "demo".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "input_schema": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                        "additionalProperties": true
                    }
                },
                "required": ["input_schema"],
                "additionalProperties": false
            }),
        }])
        .expect_err("schema should be rejected");
    }

    #[tokio::test]
    async fn rejects_writes_outside_allowed_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let context = ToolContext::new(
            tempdir.path().to_path_buf(),
            vec![tempdir.path().to_path_buf()],
        );
        let tool = WriteTextTool;

        let result = tool
            .call(
                json!({
                    "path": "../outside.txt",
                    "content": "nope"
                }),
                &context,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn filesystem_list_returns_directory_entries() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let root = tempdir.path().join("allowed");
        fs::create_dir_all(root.join("nested")).expect("root dirs");
        fs::write(root.join("alpha.txt"), "alpha").expect("alpha");
        fs::write(root.join("nested").join("beta.txt"), "beta").expect("beta");
        let context = ToolContext::new(root.clone(), vec![root.clone()]);

        let output = ListFilesTool
            .call(
                json!({
                    "path": ".",
                    "recursive": true,
                    "max_depth": 2
                }),
                &context,
            )
            .await
            .expect("list output");

        let entries = output.payload["entries"].as_array().expect("entries array");
        assert!(entries.iter().any(|entry| {
            entry["path"] == json!(root.join("alpha.txt").display().to_string())
                && entry["kind"] == json!("file")
        }));
        assert!(entries.iter().any(|entry| {
            entry["path"] == json!(root.join("nested").display().to_string())
                && entry["kind"] == json!("directory")
        }));
        assert!(entries.iter().any(|entry| {
            entry["path"] == json!(root.join("nested").join("beta.txt").display().to_string())
                && entry["kind"] == json!("file")
        }));
    }

    #[tokio::test]
    async fn filesystem_find_locates_entries_by_name() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let root = tempdir.path().join("allowed");
        fs::create_dir_all(root.join("lists")).expect("root dirs");
        fs::write(root.join("lists").join("subredits.txt"), "r/graz\n").expect("write file");
        let context = ToolContext::new(root.clone(), vec![root.clone()]);

        let output = FindFilesTool
            .call(
                json!({
                    "name_pattern": "subredits.txt",
                    "root": ".",
                    "kind": "file"
                }),
                &context,
            )
            .await
            .expect("find output");

        let matches = output.payload["matches"].as_array().expect("matches array");
        assert!(matches.iter().any(|entry| {
            entry["path"]
                == json!(
                    root.join("lists")
                        .join("subredits.txt")
                        .display()
                        .to_string()
                )
                && entry["kind"] == json!("file")
        }));
    }

    #[tokio::test]
    async fn filesystem_list_rejects_outside_allowed_roots() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let allowed = tempdir.path().join("allowed");
        fs::create_dir_all(&allowed).expect("allowed root");
        let context = ToolContext::new(allowed.clone(), vec![allowed]);

        let result = ListFilesTool
            .call(json!({ "path": "../outside" }), &context)
            .await;

        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlink_escape_for_reads() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("tempdir");
        let allowed_root = tempdir.path().join("allowed");
        let outside_root = tempdir.path().join("outside");
        fs::create_dir_all(&allowed_root).expect("allowed root");
        fs::create_dir_all(&outside_root).expect("outside root");

        let outside_file = outside_root.join("secret.txt");
        fs::write(&outside_file, "secret").expect("outside file");
        symlink(&outside_file, allowed_root.join("link.txt")).expect("symlink");

        let context = ToolContext::new(allowed_root.clone(), vec![allowed_root]);
        let tool = ReadTextTool;

        let result = tool.call(json!({ "path": "link.txt" }), &context).await;

        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlink_escape_for_writes() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("tempdir");
        let allowed_root = tempdir.path().join("allowed");
        let outside_root = tempdir.path().join("outside");
        fs::create_dir_all(&allowed_root).expect("allowed root");
        fs::create_dir_all(&outside_root).expect("outside root");

        let outside_file = outside_root.join("secret.txt");
        fs::write(&outside_file, "secret").expect("outside file");
        symlink(&outside_file, allowed_root.join("link.txt")).expect("symlink");

        let context = ToolContext::new(allowed_root.clone(), vec![allowed_root]);
        let tool = WriteTextTool;

        let result = tool
            .call(
                json!({
                    "path": "link.txt",
                    "content": "new"
                }),
                &context,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rejects_non_http_browser_urls() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let context = ToolContext::new(
            tempdir.path().to_path_buf(),
            vec![tempdir.path().to_path_buf()],
        );

        let result = BrowserVisitTool
            .call(
                json!({
                    "url": "file:///tmp/secret.txt",
                    "mode": "headless"
                }),
                &context,
            )
            .await;

        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interactive_browser_uses_configured_launcher() {
        use std::os::unix::fs::PermissionsExt;
        use tokio::time::{Duration, sleep};

        let tempdir = tempfile::tempdir().expect("tempdir");
        let launcher_path = tempdir.path().join("launcher.sh");
        let output_path = tempdir.path().join("launcher.out");
        fs::write(
            &launcher_path,
            format!(
                "#!/bin/sh\nprintf '%s' \"$1\" > '{}'\n",
                output_path.display()
            ),
        )
        .expect("write launcher");
        let mut permissions = fs::metadata(&launcher_path)
            .expect("metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&launcher_path, permissions).expect("permissions");

        let mut context = ToolContext::new(
            tempdir.path().to_path_buf(),
            vec![tempdir.path().to_path_buf()],
        );
        context.browser_interactive_launcher = Some(launcher_path.display().to_string());

        let output = BrowserVisitTool
            .call(
                json!({
                    "url": "https://example.com",
                    "mode": "interactive"
                }),
                &context,
            )
            .await
            .expect("browser output");

        assert_eq!(output.payload["mode"], "interactive");
        assert_eq!(
            output.payload["launcher"],
            launcher_path.display().to_string()
        );

        for _ in 0..50 {
            if output_path.exists() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            fs::read_to_string(&output_path).expect("launcher output"),
            "https://example.com"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn headless_browser_returns_dom() {
        use std::os::unix::fs::PermissionsExt;

        let tempdir = tempfile::tempdir().expect("tempdir");
        let browser_path = tempdir.path().join("headless-browser.sh");
        fs::write(&browser_path, "#!/bin/sh\necho '<html>ok</html>'\n").expect("write browser");
        let mut permissions = fs::metadata(&browser_path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&browser_path, permissions).expect("permissions");

        let mut context = ToolContext::new(
            tempdir.path().to_path_buf(),
            vec![tempdir.path().to_path_buf()],
        );
        context.browser_headless_program = Some(browser_path.display().to_string());

        let output = BrowserVisitTool
            .call(
                json!({
                    "url": "https://example.com",
                    "mode": "headless"
                }),
                &context,
            )
            .await
            .expect("browser output");

        assert_eq!(output.payload["mode"], "headless");
        assert!(
            output.payload["dom"]
                .as_str()
                .expect("dom")
                .contains("<html>ok</html>")
        );
    }

    #[test]
    fn json_schema_contract_accepts_supported_subset() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "count": { "type": ["integer", "null"] },
                "tags": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            },
            "required": ["name"],
            "additionalProperties": false
        });

        validate_json_schema_contract(&schema).expect("valid schema");
        validate_json_value_against_schema(
            &json!({ "name": "ok", "count": 2, "tags": ["a", "b"] }),
            &schema,
        )
        .expect("matching value");
    }

    #[test]
    fn json_schema_validation_rejects_extra_properties() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" }
            },
            "required": ["name"],
            "additionalProperties": false
        });

        let error =
            validate_json_value_against_schema(&json!({ "name": "ok", "extra": true }), &schema)
                .expect_err("extra property rejected");
        assert!(error.to_string().contains("unsupported property"));
    }
}
