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

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone)]
pub struct ToolContext {
    pub working_dir: PathBuf,
    pub allowed_roots: Vec<PathBuf>,
    pub max_output_chars: usize,
    pub approved_shell_commands: Vec<String>,
    pub browser_interactive_launcher: Option<String>,
    pub browser_headless_program: Option<String>,
    pub browser_headless_args: Vec<String>,
}

impl ToolContext {
    pub fn new(working_dir: PathBuf, allowed_roots: Vec<PathBuf>) -> Self {
        Self {
            working_dir,
            allowed_roots,
            max_output_chars: 12_000,
            approved_shell_commands: Vec::new(),
            browser_interactive_launcher: None,
            browser_headless_program: None,
            browser_headless_args: Vec::new(),
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
    registry.register(ReadTextTool);
    registry.register(WriteTextTool);
    registry.register(SearchFilesTool);
    registry.register(BrowserVisitTool);
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

fn truncate_text(text: &str, max_chars: usize) -> String {
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
}
