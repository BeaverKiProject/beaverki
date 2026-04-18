use anyhow::{Context, Result, anyhow};
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

#[async_trait]
pub trait ModelProvider: Send + Sync {
    fn model_names(&self) -> &ProviderModels;

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
}

#[async_trait]
impl ModelProvider for OpenAiProvider {
    fn model_names(&self) -> &ProviderModels {
        &self.models
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

fn openai_http_error(status: StatusCode, body: Option<String>) -> anyhow::Error {
    let compact_body = body
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            let truncated: String = value.chars().take(800).collect();
            if value.chars().count() > 800 {
                format!("{truncated}...[truncated]")
            } else {
                truncated
            }
        });

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
