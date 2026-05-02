use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use beaverki_config::{ProviderEntry, ProviderModels};
use beaverki_tools::ToolDefinition;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::info;

#[derive(Debug, Clone)]
pub enum ConversationItem {
    UserText(String),
    AssistantOutput(Value),
    FunctionCallOutput { call_id: String, output: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone)]
pub struct ModelTurnResponse {
    pub output_items: Vec<Value>,
    pub tool_calls: Vec<ModelToolCall>,
    pub output_text: String,
    pub usage: Option<ModelTokenUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelTokenUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelProviderCapabilities {
    pub tool_calling: bool,
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    fn provider_id(&self) -> &str;

    fn provider_kind(&self) -> &str;

    fn capabilities(&self) -> ModelProviderCapabilities;

    fn model_names(&self) -> &ProviderModels;

    async fn verify_configuration(&self) -> Result<()>;

    async fn generate_turn(
        &self,
        model_role: &str,
        model_name: &str,
        instructions: &str,
        conversation: &[ConversationItem],
        tools: &[ToolDefinition],
    ) -> Result<ModelTurnResponse>;
}

#[derive(Clone)]
pub struct OpenAiProvider {
    client: Client,
    api_token: String,
    provider_id: String,
    provider_kind: String,
    capabilities: ModelProviderCapabilities,
    models: ProviderModels,
}

#[derive(Clone)]
pub struct LmStudioProvider {
    client: Client,
    provider_id: String,
    provider_kind: String,
    base_url: String,
    capabilities: ModelProviderCapabilities,
    models: ProviderModels,
}

impl OpenAiProvider {
    pub fn from_entry(entry: &ProviderEntry, api_token: String) -> Result<Self> {
        if entry.kind != "openai" {
            return Err(anyhow!("unsupported provider kind '{}'", entry.kind));
        }

        Ok(Self {
            client: Client::new(),
            api_token,
            provider_id: entry.provider_id.clone(),
            provider_kind: entry.kind.clone(),
            capabilities: capabilities_from_entry(entry),
            models: entry.models.clone(),
        })
    }

    pub async fn verify_credentials(&self) -> Result<()> {
        let response = self
            .client
            .get("https://api.openai.com/v1/models")
            .bearer_auth(&self.api_token)
            .send()
            .await
            .context("failed to contact OpenAI while verifying credentials")?;

        if response.status().is_success() {
            return Ok(());
        }

        Err(openai_http_error(
            response.status(),
            response.text().await.ok(),
        ))
    }

    pub async fn list_models(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .get("https://api.openai.com/v1/models")
            .bearer_auth(&self.api_token)
            .send()
            .await
            .context("failed to contact OpenAI while listing models")?;

        let status = response.status();
        if !status.is_success() {
            return Err(openai_http_error(status, response.text().await.ok()));
        }

        let parsed: OpenAiCompatibleModelsResponse = response
            .json()
            .await
            .context("failed to parse OpenAI model list")?;

        let mut ids = parsed
            .data
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }
}

impl LmStudioProvider {
    pub fn from_entry(entry: &ProviderEntry) -> Result<Self> {
        if entry.kind != "lm_studio" {
            return Err(anyhow!("unsupported provider kind '{}'", entry.kind));
        }
        if entry.auth.mode != "none" {
            bail!(
                "LM Studio provider '{}' requires auth.mode 'none', found '{}'",
                entry.provider_id,
                entry.auth.mode
            );
        }

        let base_url = entry.base_url.as_deref().ok_or_else(|| {
            anyhow!(
                "LM Studio provider '{}' is missing base_url",
                entry.provider_id
            )
        })?;

        Ok(Self {
            client: Client::new(),
            provider_id: entry.provider_id.clone(),
            provider_kind: entry.kind.clone(),
            base_url: normalize_openai_compatible_base_url(base_url)?,
            capabilities: capabilities_from_entry(entry),
            models: entry.models.clone(),
        })
    }

    pub async fn list_models(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .get(format!("{}/models", self.base_url))
            .send()
            .await
            .context("failed to contact LM Studio while listing models")?;

        let status = response.status();
        if !status.is_success() {
            return Err(lm_studio_http_error(status, response.text().await.ok()));
        }

        let parsed: OpenAiCompatibleModelsResponse = response
            .json()
            .await
            .context("failed to parse LM Studio model list")?;

        let mut ids = parsed
            .data
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }
}

#[async_trait]
impl ModelProvider for OpenAiProvider {
    fn provider_id(&self) -> &str {
        &self.provider_id
    }

    fn provider_kind(&self) -> &str {
        &self.provider_kind
    }

    fn capabilities(&self) -> ModelProviderCapabilities {
        self.capabilities
    }

    fn model_names(&self) -> &ProviderModels {
        &self.models
    }

    async fn verify_configuration(&self) -> Result<()> {
        self.verify_credentials().await
    }

    async fn generate_turn(
        &self,
        model_role: &str,
        model_name: &str,
        instructions: &str,
        conversation: &[ConversationItem],
        tools: &[ToolDefinition],
    ) -> Result<ModelTurnResponse> {
        let request_body = json!({
            "model": model_name,
            "instructions": instructions,
            "input": render_conversation(conversation),
            "tools": render_tools(tools),
            "parallel_tool_calls": false
        });

        let response = self
            .client
            .post("https://api.openai.com/v1/responses")
            .bearer_auth(&self.api_token)
            .json(&request_body)
            .send()
            .await
            .context("failed to call OpenAI Responses API")?;

        let status = response.status();
        if !status.is_success() {
            return Err(openai_http_error(status, response.text().await.ok()));
        }

        let parsed: ResponsesApiResponse = response
            .json()
            .await
            .context("failed to parse OpenAI response")?;

        let tool_calls = parsed
            .output
            .iter()
            .filter_map(parse_tool_call)
            .collect::<Result<Vec<_>>>()?;

        let usage = parsed.usage.map(|usage| ModelTokenUsage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
        });

        info!(
            provider_id = %self.provider_id,
            provider_kind = %self.provider_kind,
            model_role = %model_role,
            model_name = %model_name,
            input_tokens = ?usage.as_ref().and_then(|item| item.input_tokens),
            output_tokens = ?usage.as_ref().and_then(|item| item.output_tokens),
            "model turn completed"
        );

        Ok(ModelTurnResponse {
            output_text: extract_output_text(&parsed.output),
            output_items: parsed.output,
            tool_calls,
            usage,
        })
    }
}

#[async_trait]
impl ModelProvider for LmStudioProvider {
    fn provider_id(&self) -> &str {
        &self.provider_id
    }

    fn provider_kind(&self) -> &str {
        &self.provider_kind
    }

    fn capabilities(&self) -> ModelProviderCapabilities {
        self.capabilities
    }

    fn model_names(&self) -> &ProviderModels {
        &self.models
    }

    async fn verify_configuration(&self) -> Result<()> {
        let models = self.list_models().await?;
        let configured_models = [
            self.models.planner.as_str(),
            self.models.executor.as_str(),
            self.models.summarizer.as_str(),
            self.models.safety_review.as_str(),
        ];

        let missing = configured_models
            .into_iter()
            .filter(|model| !models.iter().any(|available| available == model))
            .map(str::to_owned)
            .collect::<Vec<_>>();

        if missing.is_empty() {
            return Ok(());
        }

        Err(anyhow!(
            "LM Studio provider '{}' is missing configured models: {}",
            self.provider_id,
            missing.join(", ")
        ))
    }

    async fn generate_turn(
        &self,
        model_role: &str,
        model_name: &str,
        instructions: &str,
        conversation: &[ConversationItem],
        tools: &[ToolDefinition],
    ) -> Result<ModelTurnResponse> {
        let mut request_body = serde_json::Map::new();
        request_body.insert("model".to_owned(), Value::String(model_name.to_owned()));
        request_body.insert(
            "messages".to_owned(),
            Value::Array(render_chat_conversation(instructions, conversation)),
        );
        if !tools.is_empty() {
            request_body.insert("tools".to_owned(), Value::Array(render_chat_tools(tools)));
            request_body.insert("parallel_tool_calls".to_owned(), Value::Bool(false));
        }

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .json(&Value::Object(request_body))
            .send()
            .await
            .context("failed to call LM Studio chat completions API")?;

        let status = response.status();
        if !status.is_success() {
            return Err(lm_studio_http_error(status, response.text().await.ok()));
        }

        let parsed: ChatCompletionsResponse = response
            .json()
            .await
            .context("failed to parse LM Studio response")?;

        let choice = parsed
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("LM Studio returned no choices"))?;

        let normalized = normalize_chat_completion_message(&choice.message)?;
        let usage = parsed.usage.map(|usage| ModelTokenUsage {
            input_tokens: usage.prompt_tokens,
            output_tokens: usage.completion_tokens,
        });

        info!(
            provider_id = %self.provider_id,
            provider_kind = %self.provider_kind,
            model_role = %model_role,
            model_name = %model_name,
            input_tokens = ?usage.as_ref().and_then(|item| item.input_tokens),
            output_tokens = ?usage.as_ref().and_then(|item| item.output_tokens),
            "model turn completed"
        );

        Ok(ModelTurnResponse {
            output_text: normalized.output_text,
            output_items: normalized.output_items,
            tool_calls: normalized.tool_calls,
            usage,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ResponsesApiResponse {
    output: Vec<Value>,
    usage: Option<ResponsesApiUsage>,
}

#[derive(Debug, Deserialize)]
struct ResponsesApiUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleModelsResponse {
    data: Vec<OpenAiCompatibleModel>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleModel {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionsResponse {
    choices: Vec<ChatCompletionChoice>,
    usage: Option<ChatCompletionsUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    message: ChatCompletionMessage,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionsUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionMessage {
    content: Option<Value>,
    #[serde(default)]
    tool_calls: Vec<ChatCompletionToolCall>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionToolCall {
    id: Option<String>,
    #[serde(rename = "type")]
    call_type: Option<String>,
    function: ChatCompletionFunction,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionFunction {
    name: String,
    arguments: String,
}

fn render_conversation(conversation: &[ConversationItem]) -> Vec<Value> {
    conversation
        .iter()
        .map(|item| match item {
            ConversationItem::UserText(text) => json!({
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": text
                    }
                ]
            }),
            ConversationItem::AssistantOutput(value) => value.clone(),
            ConversationItem::FunctionCallOutput { call_id, output } => json!({
                "type": "function_call_output",
                "call_id": call_id,
                "output": output.to_string()
            }),
        })
        .collect()
}

fn render_chat_conversation(instructions: &str, conversation: &[ConversationItem]) -> Vec<Value> {
    let mut rendered = Vec::new();

    if !instructions.trim().is_empty() {
        rendered.push(json!({
            "role": "system",
            "content": instructions,
        }));
    }

    for item in conversation {
        match item {
            ConversationItem::UserText(text) => rendered.push(json!({
                "role": "user",
                "content": text,
            })),
            ConversationItem::AssistantOutput(value) => {
                rendered.extend(render_chat_assistant_output(value));
            }
            ConversationItem::FunctionCallOutput { call_id, output } => rendered.push(json!({
                "role": "tool",
                "tool_call_id": call_id,
                "content": output.to_string(),
            })),
        }
    }

    rendered
}

fn render_tools(tools: &[ToolDefinition]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                "strict": true
            })
        })
        .collect()
}

fn render_chat_tools(tools: &[ToolDefinition]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                }
            })
        })
        .collect()
}

fn render_chat_assistant_output(value: &Value) -> Vec<Value> {
    match value.get("type").and_then(Value::as_str) {
        Some("message") => {
            let text = extract_output_text(std::slice::from_ref(value));
            if text.trim().is_empty() {
                Vec::new()
            } else {
                vec![json!({
                    "role": "assistant",
                    "content": text,
                })]
            }
        }
        Some("function_call") => {
            let Some(name) = value.get("name").and_then(Value::as_str) else {
                return Vec::new();
            };
            let Some(arguments) = value.get("arguments").and_then(Value::as_str) else {
                return Vec::new();
            };
            let Some(call_id) = value.get("call_id").and_then(Value::as_str) else {
                return Vec::new();
            };
            vec![json!({
                "role": "assistant",
                "content": Value::Null,
                "tool_calls": [{
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": arguments,
                    }
                }]
            })]
        }
        _ => Vec::new(),
    }
}

fn parse_tool_call(item: &Value) -> Option<Result<ModelToolCall>> {
    match item.get("type").and_then(Value::as_str) {
        Some("function_call") => Some((|| {
            let arguments = item
                .get("arguments")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("function_call arguments missing"))?;
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("function_call call_id missing"))?;
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("function_call name missing"))?;

            Ok(ModelToolCall {
                call_id: call_id.to_owned(),
                name: name.to_owned(),
                arguments: serde_json::from_str(arguments)
                    .context("failed to decode function arguments")?,
            })
        })()),
        _ => None,
    }
}

fn extract_output_text(output: &[Value]) -> String {
    let mut sections = Vec::new();

    for item in output {
        if item.get("type").and_then(Value::as_str) == Some("message")
            && let Some(content) = item.get("content").and_then(Value::as_array)
        {
            for part in content {
                if part.get("type").and_then(Value::as_str) == Some("output_text")
                    && let Some(text) = part.get("text").and_then(Value::as_str)
                {
                    sections.push(text.to_owned());
                }
            }
        }
    }

    sections.join("\n")
}

fn normalize_chat_completion_message(message: &ChatCompletionMessage) -> Result<ModelTurnResponse> {
    let output_text = extract_chat_message_text(message.content.as_ref());
    let mut output_items = Vec::new();

    if !output_text.trim().is_empty() {
        output_items.push(json!({
            "type": "message",
            "content": [{
                "type": "output_text",
                "text": output_text,
            }]
        }));
    }

    let mut tool_calls = Vec::new();
    for (index, tool_call) in message.tool_calls.iter().enumerate() {
        if let Some(call_type) = tool_call.call_type.as_deref()
            && call_type != "function"
        {
            bail!("unsupported LM Studio tool call type '{call_type}'");
        }

        let call_id = tool_call
            .id
            .clone()
            .unwrap_or_else(|| format!("lm_studio_call_{}", index + 1));
        let arguments = serde_json::from_str(&tool_call.function.arguments)
            .context("failed to decode LM Studio tool arguments")?;

        output_items.push(json!({
            "type": "function_call",
            "call_id": call_id,
            "name": tool_call.function.name,
            "arguments": tool_call.function.arguments,
        }));
        tool_calls.push(ModelToolCall {
            call_id,
            name: tool_call.function.name.clone(),
            arguments,
        });
    }

    Ok(ModelTurnResponse {
        output_items,
        tool_calls,
        output_text,
        usage: None,
    })
}

fn extract_chat_message_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| match part {
                Value::String(text) => Some(text.clone()),
                Value::Object(object) => object
                    .get("text")
                    .and_then(|value| value.as_str().map(str::to_owned)),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn capabilities_from_entry(entry: &ProviderEntry) -> ModelProviderCapabilities {
    ModelProviderCapabilities {
        tool_calling: entry.capabilities.tool_calling,
    }
}

fn normalize_openai_compatible_base_url(base_url: &str) -> Result<String> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("provider base_url cannot be empty");
    }

    if trimmed.ends_with("/v1") {
        return Ok(trimmed.to_owned());
    }

    Ok(format!("{trimmed}/v1"))
}

fn openai_http_error(status: StatusCode, body: Option<String>) -> anyhow::Error {
    let compact_body = compact_response_body(body);

    match status {
        StatusCode::UNAUTHORIZED => {
            let body_text = compact_body.unwrap_or_else(|| "no response body".to_owned());
            anyhow!(
                "OpenAI authentication failed (401 Unauthorized). BeaverKI M0 requires a real OpenAI API key for the OpenAI API; ChatGPT/Codex subscription login credentials do not work here. Verify the API key, then re-run setup or update the encrypted secret. Response body: {body_text}"
            )
        }
        StatusCode::FORBIDDEN => {
            let body_text = compact_body.unwrap_or_else(|| "no response body".to_owned());
            anyhow!(
                "OpenAI rejected the request with 403 Forbidden. The API key may not have access to the requested resource or project. Response body: {body_text}"
            )
        }
        StatusCode::TOO_MANY_REQUESTS => {
            let body_text = compact_body.unwrap_or_else(|| "no response body".to_owned());
            anyhow!(
                "OpenAI rate-limited the request (429 Too Many Requests). Response body: {body_text}"
            )
        }
        other => {
            let body_text = compact_body.unwrap_or_else(|| "no response body".to_owned());
            anyhow!("OpenAI API request failed with status {other}. Response body: {body_text}")
        }
    }
}

fn lm_studio_http_error(status: StatusCode, body: Option<String>) -> anyhow::Error {
    let body_text = compact_response_body(body).unwrap_or_else(|| "no response body".to_owned());
    anyhow!("LM Studio API request failed with status {status}. Response body: {body_text}")
}

fn compact_response_body(body: Option<String>) -> Option<String> {
    body.as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let truncated: String = value.chars().take(800).collect();
            if value.chars().count() > 800 {
                format!("{truncated}...[truncated]")
            } else {
                truncated
            }
        })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    use super::*;
    use beaverki_config::{ProviderAuth, ProviderCapabilities, ProviderEntry, ProviderModels};
    use beaverki_tools::ToolDefinition;

    #[test]
    fn extracts_text_from_message_output() {
        let output = vec![json!({
            "type": "message",
            "content": [
                {
                    "type": "output_text",
                    "text": "hello"
                }
            ]
        })];

        assert_eq!(extract_output_text(&output), "hello");
    }

    #[test]
    fn parses_function_calls() {
        let item = json!({
            "type": "function_call",
            "call_id": "call_123",
            "name": "filesystem_read_text",
            "arguments": "{\"path\":\"README.md\"}"
        });

        let parsed = parse_tool_call(&item)
            .expect("some")
            .expect("valid tool call");

        assert_eq!(parsed.name, "filesystem_read_text");
        assert_eq!(parsed.arguments["path"], "README.md");
    }

    #[test]
    fn deserializes_usage_from_response_payload() {
        let parsed: ResponsesApiResponse = serde_json::from_value(json!({
            "output": [],
            "usage": {
                "input_tokens": 123,
                "output_tokens": 45
            }
        }))
        .expect("response payload");

        assert_eq!(
            parsed.usage.map(|usage| ModelTokenUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
            }),
            Some(ModelTokenUsage {
                input_tokens: Some(123),
                output_tokens: Some(45),
            })
        );
    }

    #[test]
    fn normalizes_chat_completion_tool_calls() {
        let normalized = normalize_chat_completion_message(&ChatCompletionMessage {
            content: Some(Value::String("checking".to_owned())),
            tool_calls: vec![ChatCompletionToolCall {
                id: Some("call_1".to_owned()),
                call_type: Some("function".to_owned()),
                function: ChatCompletionFunction {
                    name: "filesystem_read_text".to_owned(),
                    arguments: "{\"path\":\"README.md\"}".to_owned(),
                },
            }],
        })
        .expect("normalized response");

        assert_eq!(normalized.output_text, "checking");
        assert_eq!(normalized.tool_calls.len(), 1);
        assert_eq!(normalized.tool_calls[0].name, "filesystem_read_text");
        assert_eq!(normalized.tool_calls[0].arguments["path"], "README.md");
        assert_eq!(
            normalized.output_items[1]["type"],
            Value::String("function_call".to_owned())
        );
    }

    #[test]
    fn normalizes_openai_compatible_base_urls() {
        assert_eq!(
            normalize_openai_compatible_base_url("http://127.0.0.1:1234").expect("base url"),
            "http://127.0.0.1:1234/v1"
        );
        assert_eq!(
            normalize_openai_compatible_base_url("http://127.0.0.1:1234/v1/").expect("base url"),
            "http://127.0.0.1:1234/v1"
        );
    }

    #[tokio::test]
    async fn lm_studio_lists_models_from_openai_compatible_endpoint() {
        let (base_url, requests, handle) = spawn_json_server(json!({
            "data": [
                { "id": "qwen3-32b" },
                { "id": "qwen3-14b" }
            ]
        }));
        let provider = lm_studio_provider_for_test(base_url);

        let models = provider.list_models().await.expect("list models");

        let request = requests.recv().expect("captured request");
        handle.join().expect("server thread");

        assert_eq!(models, vec!["qwen3-14b", "qwen3-32b"]);
        assert!(request.starts_with("GET /v1/models HTTP/1.1\r\n"));
    }

    #[tokio::test]
    async fn lm_studio_chat_completions_request_includes_instructions_and_parses_tool_calls() {
        let (base_url, requests, handle) = spawn_json_server(json!({
            "choices": [{
                "message": {
                    "content": "I need to inspect the file.",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "filesystem_read_text",
                            "arguments": "{\"path\":\"README.md\"}"
                        }
                    }]
                }
            }],
            "usage": {
                "prompt_tokens": 123,
                "completion_tokens": 45
            }
        }));
        let provider = lm_studio_provider_for_test(base_url);

        let response = provider
            .generate_turn(
                "planner",
                "qwen3-32b",
                "You are BeaverKI.",
                &[ConversationItem::UserText("Inspect the README".to_owned())],
                &[ToolDefinition {
                    name: "filesystem_read_text".to_owned(),
                    description: "Read a text file".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "path": { "type": "string" }
                        },
                        "required": ["path"],
                        "additionalProperties": false
                    }),
                }],
            )
            .await
            .expect("generate turn");

        let request = requests.recv().expect("captured request");
        handle.join().expect("server thread");

        assert!(request.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        let request_body = parse_request_json_body(&request);
        assert_eq!(request_body["model"], json!("qwen3-32b"));
        assert_eq!(request_body["messages"][0]["role"], json!("system"));
        assert_eq!(
            request_body["messages"][0]["content"],
            json!("You are BeaverKI.")
        );
        assert_eq!(request_body["messages"][1]["role"], json!("user"));
        assert_eq!(
            request_body["messages"][1]["content"],
            json!("Inspect the README")
        );
        assert_eq!(request_body["tools"][0]["type"], json!("function"));
        assert_eq!(response.output_text, "I need to inspect the file.");
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].name, "filesystem_read_text");
        assert_eq!(response.tool_calls[0].arguments["path"], "README.md");
        assert_eq!(response.usage.expect("usage").input_tokens, Some(123));
    }

    fn lm_studio_provider_for_test(base_url: String) -> LmStudioProvider {
        LmStudioProvider::from_entry(&ProviderEntry {
            provider_id: "lm_studio_test".to_owned(),
            kind: "lm_studio".to_owned(),
            base_url: Some(base_url),
            auth: ProviderAuth {
                mode: "none".to_owned(),
                secret_ref: None,
            },
            models: ProviderModels {
                planner: "qwen3-32b".to_owned(),
                executor: "qwen3-14b".to_owned(),
                summarizer: "qwen3-14b".to_owned(),
                safety_review: "qwen3-14b".to_owned(),
            },
            capabilities: ProviderCapabilities { tool_calling: true },
        })
        .expect("lm studio provider")
    }

    fn spawn_json_server(
        response_body: Value,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("listener addr");
        let (request_sender, request_receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let request = read_http_request(&mut stream);
            request_sender.send(request).expect("send request capture");
            let body = response_body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });
        (format!("http://{address}"), request_receiver, handle)
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        let mut header_end = None;
        let mut content_length = 0usize;

        loop {
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);

            if header_end.is_none() {
                header_end = find_header_end(&buffer);
                if let Some(end) = header_end {
                    content_length = parse_content_length(&buffer[..end]);
                }
            }

            if let Some(end) = header_end
                && buffer.len() >= end + 4 + content_length
            {
                break;
            }
        }

        String::from_utf8(buffer).expect("utf8 request")
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn parse_content_length(headers: &[u8]) -> usize {
        String::from_utf8_lossy(headers)
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    fn parse_request_json_body(request: &str) -> Value {
        let (_, body) = request.split_once("\r\n\r\n").expect("request body");
        serde_json::from_str(body).expect("request JSON")
    }
}
