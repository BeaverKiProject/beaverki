use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{Tool, ToolContext, ToolDefinition, ToolError, ToolOutput, truncate_text};

const DEFAULT_API_BASE_URL: &str = "https://api.notion.com/v1";
const DEFAULT_API_VERSION: &str = "2026-03-11";
const DEFAULT_PAGE_SIZE: u64 = 10;
const MAX_PAGE_SIZE: u64 = 100;
const MAX_FETCH_BLOCKS: usize = 100;
const MAX_CREATE_BLOCKS: usize = 50;
const MAX_BLOCK_TEXT_CHARS: usize = 1_800;

#[derive(Clone)]
struct NotionClientConfig {
    api_base_url: String,
    api_version: String,
    api_token: String,
}

pub struct NotionSearchTool;

#[async_trait]
impl Tool for NotionSearchTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_search".to_owned(),
            description: "Search Notion pages or data sources shared with the configured Notion integration. Requires an enabled Notion API token.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "object_kind": {
                        "type": ["string", "null"],
                        "enum": ["page", "data_source", null]
                    },
                    "page_size": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "maximum": 100
                    }
                },
                "required": ["query", "object_kind", "page_size"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let config = notion_client_config(context).map_err(ToolError::Failed)?;
        let query = input
            .get("query")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_search requires query")))?;
        let object_kind = input
            .get("object_kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let page_size = notion_page_size(input.get("page_size").and_then(Value::as_u64));

        let mut body = json!({
            "query": query,
            "page_size": page_size,
            "sort": {
                "direction": "descending",
                "timestamp": "last_edited_time"
            }
        });
        if let Some(kind) = object_kind {
            body["filter"] = json!({
                "property": "object",
                "value": kind
            });
        }

        let response = notion_request_json(Method::POST, "search", Some(body), &config)
            .await
            .map_err(ToolError::Failed)?;
        let results = response
            .get("results")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(normalize_search_result)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(ToolOutput {
            payload: json!({
                "query": query,
                "object_kind": object_kind,
                "page_size": page_size,
                "has_more": response.get("has_more").and_then(Value::as_bool).unwrap_or(false),
                "next_cursor": response.get("next_cursor").cloned().unwrap_or(Value::Null),
                "results": results,
            }),
        })
    }
}

pub struct NotionFetchTool;

#[async_trait]
impl Tool for NotionFetchTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_fetch".to_owned(),
            description: "Fetch a Notion page or data source by ID or URL. For pages, optionally include normalized block content. Requires an enabled Notion API token.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ref": { "type": "string" },
                    "object_kind": {
                        "type": ["string", "null"],
                        "enum": ["page", "data_source", null]
                    },
                    "include_content": { "type": ["boolean", "null"] },
                    "page_size": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "maximum": 100
                    }
                },
                "required": ["ref", "object_kind", "include_content", "page_size"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let config = notion_client_config(context).map_err(ToolError::Failed)?;
        let reference = input
            .get("ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_fetch requires ref")))?;
        let object_kind = input
            .get("object_kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let include_content = input
            .get("include_content")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let page_size = notion_page_size(input.get("page_size").and_then(Value::as_u64));
        let object_id = notion_id_from_ref(reference).map_err(ToolError::Failed)?;

        match object_kind {
            Some("page") => {
                let page =
                    notion_request_json(Method::GET, &format!("pages/{object_id}"), None, &config)
                        .await
                        .map_err(ToolError::Failed)?;
                Ok(ToolOutput {
                    payload: normalize_page_result(
                        &page,
                        include_content,
                        page_size,
                        &config,
                        context.max_output_chars,
                    )
                    .await
                    .map_err(ToolError::Failed)?,
                })
            }
            Some("data_source") => {
                let data_source = notion_request_json(
                    Method::GET,
                    &format!("data_sources/{object_id}"),
                    None,
                    &config,
                )
                .await
                .map_err(ToolError::Failed)?;
                Ok(ToolOutput {
                    payload: normalize_data_source_result(&data_source),
                })
            }
            Some(other) => Err(ToolError::Failed(anyhow!(
                "unsupported notion_fetch object_kind '{other}'"
            ))),
            None => {
                if let Some(page) = notion_request_json_if_status(
                    Method::GET,
                    &format!("pages/{object_id}"),
                    None,
                    &config,
                    StatusCode::NOT_FOUND,
                )
                .await
                .map_err(ToolError::Failed)?
                {
                    return Ok(ToolOutput {
                        payload: normalize_page_result(
                            &page,
                            include_content,
                            page_size,
                            &config,
                            context.max_output_chars,
                        )
                        .await
                        .map_err(ToolError::Failed)?,
                    });
                }

                if let Some(data_source) = notion_request_json_if_status(
                    Method::GET,
                    &format!("data_sources/{object_id}"),
                    None,
                    &config,
                    StatusCode::NOT_FOUND,
                )
                .await
                .map_err(ToolError::Failed)?
                {
                    return Ok(ToolOutput {
                        payload: normalize_data_source_result(&data_source),
                    });
                }

                Err(ToolError::Failed(anyhow!(
                    "Notion object '{reference}' was not found as a page or data source"
                )))
            }
        }
    }
}

pub struct NotionCreatePageTool;

#[async_trait]
impl Tool for NotionCreatePageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_create_page".to_owned(),
            description: "Create a Notion page under a parent page or data source using the configured Notion integration. For data sources, BeaverKi fills the title property automatically when it can infer it from the schema.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "parent_kind": {
                        "type": "string",
                        "enum": ["page", "data_source"]
                    },
                    "parent_ref": { "type": "string" },
                    "title": { "type": "string" },
                    "content": { "type": ["string", "null"] }
                },
                "required": ["parent_kind", "parent_ref", "title", "content"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(
        &self,
        input: Value,
        context: &ToolContext,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let config = notion_client_config(context).map_err(ToolError::Failed)?;
        let parent_kind = input
            .get("parent_kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_create_page requires parent_kind")))?;
        let parent_ref = input
            .get("parent_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_create_page requires parent_ref")))?;
        let title = input
            .get("title")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_create_page requires title")))?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let parent_id = notion_id_from_ref(parent_ref).map_err(ToolError::Failed)?;
        let mut body = json!({
            "parent": {},
            "properties": {},
        });
        match parent_kind {
            "page" => {
                body["parent"] = json!({
                    "type": "page_id",
                    "page_id": parent_id
                });
                body["properties"] = json!({
                    "title": {
                        "title": notion_text_segments(title)
                    }
                });
            }
            "data_source" => {
                let data_source = notion_request_json(
                    Method::GET,
                    &format!("data_sources/{parent_id}"),
                    None,
                    &config,
                )
                .await
                .map_err(ToolError::Failed)?;
                let title_property =
                    data_source_title_property_name(&data_source).ok_or_else(|| {
                        ToolError::Failed(anyhow!(
                            "Notion data source '{parent_ref}' has no title property"
                        ))
                    })?;
                let mut properties = Map::new();
                properties.insert(
                    title_property.clone(),
                    json!({
                        "title": notion_text_segments(title)
                    }),
                );
                body["parent"] = json!({
                    "type": "data_source_id",
                    "data_source_id": parent_id
                });
                body["properties"] = Value::Object(properties);
            }
            other => {
                return Err(ToolError::Failed(anyhow!(
                    "unsupported notion_create_page parent_kind '{other}'"
                )));
            }
        }

        if let Some(content) = content {
            let children = notion_children_from_text(content);
            if !children.is_empty() {
                body["children"] = Value::Array(children);
            }
        }

        let created = notion_request_json(Method::POST, "pages", Some(body), &config)
            .await
            .map_err(ToolError::Failed)?;
        Ok(ToolOutput {
            payload: json!({
                "object": "page",
                "id": created.get("id").cloned().unwrap_or(Value::Null),
                "url": created.get("url").cloned().unwrap_or(Value::Null),
                "title": notion_object_title(&created),
                "parent": created.get("parent").cloned().unwrap_or(Value::Null),
                "created_time": created.get("created_time").cloned().unwrap_or(Value::Null),
                "last_edited_time": created.get("last_edited_time").cloned().unwrap_or(Value::Null),
            }),
        })
    }
}

fn notion_client_config(context: &ToolContext) -> Result<NotionClientConfig> {
    let api_token = context
        .notion_api_token
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!(
                "Notion integration is not configured. Enable it and store a Notion API token first."
            )
        })?;
    Ok(NotionClientConfig {
        api_base_url: context
            .notion_api_base_url
            .clone()
            .unwrap_or_else(|| DEFAULT_API_BASE_URL.to_owned()),
        api_version: context
            .notion_api_version
            .clone()
            .unwrap_or_else(|| DEFAULT_API_VERSION.to_owned()),
        api_token,
    })
}

fn notion_page_size(value: Option<u64>) -> u64 {
    value.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, MAX_PAGE_SIZE)
}

async fn notion_request_json(
    method: Method,
    path: &str,
    body: Option<Value>,
    config: &NotionClientConfig,
) -> Result<Value> {
    notion_request_json_inner(method, path, body, config, None)
        .await?
        .ok_or_else(|| anyhow!("unexpected empty Notion response for '{path}'"))
}

async fn notion_request_json_if_status(
    method: Method,
    path: &str,
    body: Option<Value>,
    config: &NotionClientConfig,
    ignored_status: StatusCode,
) -> Result<Option<Value>> {
    notion_request_json_inner(method, path, body, config, Some(ignored_status)).await
}

async fn notion_request_json_inner(
    method: Method,
    path: &str,
    body: Option<Value>,
    config: &NotionClientConfig,
    ignored_status: Option<StatusCode>,
) -> Result<Option<Value>> {
    let client = notion_http_client()?;
    let url = format!(
        "{}/{}",
        config.api_base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let mut request = client
        .request(method, &url)
        .bearer_auth(&config.api_token)
        .header("Notion-Version", &config.api_version)
        .header("Accept", "application/json");
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("failed to call Notion API '{url}'"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .with_context(|| format!("failed to read Notion API response body for '{url}'"))?;
    if ignored_status == Some(status) {
        return Ok(None);
    }
    if !status.is_success() {
        bail!(
            "Notion API request to '{}' failed with status {}: {}",
            path,
            status,
            truncate_text(&text, 2_000)
        );
    }
    if text.trim().is_empty() {
        return Ok(Some(Value::Null));
    }
    serde_json::from_str(&text)
        .with_context(|| format!("failed to parse Notion API JSON for '{path}'"))
        .map(Some)
}

fn notion_http_client() -> Result<Client> {
    Client::builder()
        .user_agent("BeaverKi/0.1 notion-tools")
        .build()
        .context("failed to build Notion HTTP client")
}

async fn normalize_page_result(
    page: &Value,
    include_content: bool,
    page_size: u64,
    config: &NotionClientConfig,
    max_output_chars: usize,
) -> Result<Value> {
    let id = page
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Notion page response is missing id"))?;
    let blocks = if include_content {
        fetch_page_blocks(id, page_size, config).await?
    } else {
        Vec::new()
    };
    let content_text = blocks
        .iter()
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    Ok(json!({
        "object": "page",
        "id": id,
        "url": page.get("url").cloned().unwrap_or(Value::Null),
        "title": notion_object_title(page),
        "parent": page.get("parent").cloned().unwrap_or(Value::Null),
        "created_time": page.get("created_time").cloned().unwrap_or(Value::Null),
        "last_edited_time": page.get("last_edited_time").cloned().unwrap_or(Value::Null),
        "in_trash": page.get("in_trash").cloned().unwrap_or(Value::Null),
        "properties": summarize_properties(page.get("properties")),
        "content": if include_content {
            json!({
                "block_count": blocks.len(),
                "blocks": blocks,
                "text": truncate_text(&content_text, max_output_chars),
            })
        } else {
            Value::Null
        }
    }))
}

fn normalize_data_source_result(data_source: &Value) -> Value {
    json!({
        "object": "data_source",
        "id": data_source.get("id").cloned().unwrap_or(Value::Null),
        "url": data_source.get("url").cloned().unwrap_or(Value::Null),
        "title": notion_object_title(data_source),
        "description": notion_rich_text_field(data_source.get("description")),
        "parent": data_source.get("parent").cloned().unwrap_or(Value::Null),
        "created_time": data_source.get("created_time").cloned().unwrap_or(Value::Null),
        "last_edited_time": data_source.get("last_edited_time").cloned().unwrap_or(Value::Null),
        "title_property": data_source_title_property_name(data_source),
        "properties": summarize_data_source_properties(data_source.get("properties")),
    })
}

fn normalize_search_result(item: &Value) -> Value {
    json!({
        "object": item.get("object").cloned().unwrap_or(Value::Null),
        "id": item.get("id").cloned().unwrap_or(Value::Null),
        "url": item.get("url").cloned().unwrap_or(Value::Null),
        "title": notion_object_title(item),
        "parent": item.get("parent").cloned().unwrap_or(Value::Null),
        "created_time": item.get("created_time").cloned().unwrap_or(Value::Null),
        "last_edited_time": item.get("last_edited_time").cloned().unwrap_or(Value::Null),
        "in_trash": item.get("in_trash").cloned().unwrap_or(Value::Null),
    })
}

async fn fetch_page_blocks(
    block_id: &str,
    page_size: u64,
    config: &NotionClientConfig,
) -> Result<Vec<Value>> {
    let mut results = Vec::new();
    let mut next_cursor = None::<String>;

    while results.len() < MAX_FETCH_BLOCKS {
        let mut path = format!("blocks/{block_id}/children?page_size={page_size}");
        if let Some(cursor) = next_cursor.as_deref() {
            path.push_str("&start_cursor=");
            path.push_str(cursor);
        }
        let response = notion_request_json(Method::GET, &path, None, config).await?;
        let page_results = response
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in page_results {
            results.push(normalize_block(&item));
            if results.len() >= MAX_FETCH_BLOCKS {
                break;
            }
        }
        let has_more = response
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !has_more {
            break;
        }
        next_cursor = response
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if next_cursor.is_none() {
            break;
        }
    }

    Ok(results)
}

fn normalize_block(block: &Value) -> Value {
    let block_type = block
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mut payload = json!({
        "id": block.get("id").cloned().unwrap_or(Value::Null),
        "type": block_type,
        "has_children": block.get("has_children").cloned().unwrap_or(Value::Bool(false)),
        "text": notion_block_text(block),
    });

    if block_type == "to_do" {
        payload["checked"] = block
            .get("to_do")
            .and_then(|value| value.get("checked"))
            .cloned()
            .unwrap_or(Value::Null);
    }
    if block_type == "code" {
        payload["language"] = block
            .get("code")
            .and_then(|value| value.get("language"))
            .cloned()
            .unwrap_or(Value::Null);
    }

    payload
}

fn notion_block_text(block: &Value) -> String {
    let block_type = block
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let detail = block.get(block_type);
    notion_rich_text_field(detail.and_then(|value| value.get("rich_text")))
}

fn summarize_properties(value: Option<&Value>) -> Value {
    let Some(properties) = value.and_then(Value::as_object) else {
        return Value::Null;
    };
    let mut summary = Map::new();
    for (name, property) in properties {
        summary.insert(name.clone(), summarize_property_value(property));
    }
    Value::Object(summary)
}

fn summarize_property_value(property: &Value) -> Value {
    let property_type = property
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match property_type {
        "title" => Value::String(notion_rich_text_field(property.get("title"))),
        "rich_text" => Value::String(notion_rich_text_field(property.get("rich_text"))),
        "number" => property.get("number").cloned().unwrap_or(Value::Null),
        "select" => property
            .get("select")
            .and_then(|value| value.get("name"))
            .cloned()
            .unwrap_or(Value::Null),
        "multi_select" => Value::Array(
            property
                .get("multi_select")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get("name").cloned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        ),
        "status" => property
            .get("status")
            .and_then(|value| value.get("name"))
            .cloned()
            .unwrap_or(Value::Null),
        "checkbox" => property.get("checkbox").cloned().unwrap_or(Value::Null),
        "date" => property.get("date").cloned().unwrap_or(Value::Null),
        "url" => property.get("url").cloned().unwrap_or(Value::Null),
        "email" => property.get("email").cloned().unwrap_or(Value::Null),
        "phone_number" => property.get("phone_number").cloned().unwrap_or(Value::Null),
        "people" => Value::Array(
            property
                .get("people")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .map(|item| {
                            item.get("name")
                                .cloned()
                                .or_else(|| item.get("id").cloned())
                                .unwrap_or(Value::Null)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        ),
        "relation" => Value::Array(
            property
                .get("relation")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.get("id").cloned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        ),
        "formula" => property.get("formula").cloned().unwrap_or(Value::Null),
        other => json!({
            "type": other,
            "value": property.get(other).cloned().unwrap_or(Value::Null),
        }),
    }
}

fn summarize_data_source_properties(value: Option<&Value>) -> Value {
    let Some(properties) = value.and_then(Value::as_object) else {
        return Value::Null;
    };
    let mut summary = Map::new();
    for (name, property) in properties {
        summary.insert(
            name.clone(),
            json!({
                "type": property.get("type").cloned().unwrap_or(Value::Null),
                "id": property.get("id").cloned().unwrap_or(Value::Null),
            }),
        );
    }
    Value::Object(summary)
}

fn notion_object_title(value: &Value) -> String {
    if let Some(properties) = value.get("properties").and_then(Value::as_object) {
        for property in properties.values() {
            if property.get("type").and_then(Value::as_str) == Some("title") {
                return notion_rich_text_field(property.get("title"));
            }
        }
    }
    if let Some(title) = value.get("title") {
        return notion_rich_text_field(Some(title));
    }
    if let Some(name) = value.get("name").and_then(Value::as_str) {
        return name.to_owned();
    }
    String::new()
}

fn notion_rich_text_field(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("plain_text").and_then(Value::as_str))
                .collect::<String>()
        })
        .unwrap_or_default()
}

fn data_source_title_property_name(data_source: &Value) -> Option<String> {
    data_source
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| {
            properties.iter().find_map(|(name, property)| {
                (property.get("type").and_then(Value::as_str) == Some("title"))
                    .then(|| name.clone())
            })
        })
}

fn notion_children_from_text(content: &str) -> Vec<Value> {
    let mut children = Vec::new();
    for paragraph in content
        .split("\n\n")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        for chunk in split_text_chunks(paragraph, MAX_BLOCK_TEXT_CHARS) {
            children.push(json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": notion_text_segments(&chunk)
                }
            }));
            if children.len() >= MAX_CREATE_BLOCKS {
                return children;
            }
        }
    }
    children
}

fn notion_text_segments(text: &str) -> Vec<Value> {
    split_text_chunks(text, MAX_BLOCK_TEXT_CHARS)
        .into_iter()
        .map(|chunk| {
            json!({
                "type": "text",
                "text": {
                    "content": chunk
                }
            })
        })
        .collect()
}

fn split_text_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        current.push(ch);
        if current.chars().count() >= max_chars {
            chunks.push(current);
            current = String::new();
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn notion_id_from_ref(reference: &str) -> Result<String> {
    let trimmed = reference.trim();
    if trimmed.is_empty() {
        bail!("Notion reference cannot be empty");
    }
    if let Ok(uuid) = Uuid::parse_str(trimmed) {
        return Ok(uuid.to_string());
    }
    for segment in trimmed.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
    {
        if segment.is_empty() {
            continue;
        }
        if let Some(uuid) = notion_uuid_from_segment(segment) {
            return Ok(uuid);
        }
    }
    bail!("could not parse a Notion page or data source ID from '{reference}'")
}

fn notion_uuid_from_segment(segment: &str) -> Option<String> {
    let trimmed = segment.trim_matches(|ch: char| ch == '-' || ch == '_');
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(uuid) = Uuid::parse_str(trimmed) {
        return Some(uuid.to_string());
    }

    let hex_only = trimmed
        .chars()
        .filter(|ch| ch.is_ascii_hexdigit())
        .collect::<String>();
    if hex_only.len() < 32 {
        return None;
    }
    for start in (0..=hex_only.len() - 32).rev() {
        let candidate = &hex_only[start..start + 32];
        let dashed = format!(
            "{}-{}-{}-{}-{}",
            &candidate[0..8],
            &candidate[8..12],
            &candidate[12..16],
            &candidate[16..20],
            &candidate[20..32]
        );
        if let Ok(uuid) = Uuid::parse_str(&dashed) {
            return Some(uuid.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_notion_id_from_url() {
        let parsed =
            notion_id_from_ref("https://www.notion.so/My-Page-be633bf1dfa0436db259571129a590e5")
                .expect("should parse");
        assert_eq!(parsed, "be633bf1-dfa0-436d-b259-571129a590e5");
    }

    #[test]
    fn extracts_page_title_from_properties() {
        let title = notion_object_title(&json!({
            "properties": {
                "Name": {
                    "type": "title",
                    "title": [
                        { "plain_text": "Roadmap" }
                    ]
                }
            }
        }));
        assert_eq!(title, "Roadmap");
    }

    #[test]
    fn splits_large_content_into_notional_paragraphs() {
        let content = "a".repeat(MAX_BLOCK_TEXT_CHARS + 5);
        let children = notion_children_from_text(&content);
        assert_eq!(children.len(), 2);
    }
}
