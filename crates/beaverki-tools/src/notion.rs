use std::collections::HashSet;

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
const MAX_APPEND_BLOCKS: usize = 100;

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
            description: "Create a Notion page under a parent page or data source using the configured Notion integration. If parent_kind and parent_ref are null or empty, BeaverKi uses the configured Notion default parent. For data sources, BeaverKi fills the title property automatically when it can infer it from the schema.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "parent_kind": {
                        "type": ["string", "null"],
                        "enum": ["page", "data_source", null]
                    },
                    "parent_ref": { "type": ["string", "null"] },
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
            .filter(|value| !value.is_empty());
        let parent_ref = input
            .get("parent_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
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

        let (parent_kind, parent_ref) =
            notion_create_parent(parent_kind, parent_ref, context).map_err(ToolError::Failed)?;
        let parent_id = notion_id_from_ref(&parent_ref).map_err(ToolError::Failed)?;
        let mut body = json!({
            "parent": {},
            "properties": {},
        });
        match parent_kind.as_str() {
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

pub struct NotionUpdatePageTool;

#[async_trait]
impl Tool for NotionUpdatePageTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_update_page".to_owned(),
            description: "Update properties on an existing Notion page using the configured Notion integration. Property values are normalized against the page's current schema.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ref": { "type": "string" },
                    "properties_json": { "type": "string" }
                },
                "required": ["ref", "properties_json"],
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
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_update_page requires ref")))?;
        let raw_updates = input
            .get("properties_json")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ToolError::Failed(anyhow!("notion_update_page requires properties_json"))
            })?;
        let updates = serde_json::from_str::<Value>(raw_updates)
            .map_err(|error| {
                ToolError::Failed(anyhow!(
                    "notion_update_page properties_json must be valid JSON: {error}"
                ))
            })?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                ToolError::Failed(anyhow!(
                    "notion_update_page properties_json must decode to a JSON object"
                ))
            })?;
        if updates.is_empty() {
            return Err(ToolError::Failed(anyhow!(
                "notion_update_page requires at least one property update"
            )));
        }

        let page_id = notion_id_from_ref(reference).map_err(ToolError::Failed)?;
        let existing = notion_request_json(Method::GET, &format!("pages/{page_id}"), None, &config)
            .await
            .map_err(ToolError::Failed)?;
        let properties = notion_update_properties_payload(existing.get("properties"), &updates)
            .map_err(ToolError::Failed)?;
        let updated = notion_request_json(
            Method::PATCH,
            &format!("pages/{page_id}"),
            Some(json!({ "properties": properties })),
            &config,
        )
        .await
        .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: json!({
                "object": "page",
                "id": updated.get("id").cloned().unwrap_or(Value::Null),
                "url": updated.get("url").cloned().unwrap_or(Value::Null),
                "title": notion_object_title(&updated),
                "updated_properties": updates.keys().cloned().collect::<Vec<_>>(),
                "last_edited_time": updated.get("last_edited_time").cloned().unwrap_or(Value::Null),
                "properties": summarize_properties(updated.get("properties")),
            }),
        })
    }
}

pub struct NotionAppendBlockChildrenTool;

#[async_trait]
impl Tool for NotionAppendBlockChildrenTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_append_block_children".to_owned(),
            description: "Append Markdown-like content blocks to an existing Notion page or block. Uses the same content conversion rules as notion_create_page.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "parent_ref": { "type": "string" },
                    "content": { "type": "string" },
                    "position": {
                        "type": ["string", "null"],
                        "enum": ["start", "end", "after_block", null]
                    },
                    "after_block_ref": { "type": ["string", "null"] }
                },
                "required": ["parent_ref", "content", "position", "after_block_ref"],
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
        let parent_ref = input
            .get("parent_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| {
                ToolError::Failed(anyhow!("notion_append_block_children requires parent_ref"))
            })?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ToolError::Failed(anyhow!("notion_append_block_children requires content"))
            })?;
        let position = input.get("position").and_then(Value::as_str);
        let after_block_ref = input.get("after_block_ref").and_then(Value::as_str);

        let parent_id = notion_id_from_ref(parent_ref).map_err(ToolError::Failed)?;
        let mut children = notion_children_from_text(content);
        if children.is_empty() {
            return Err(ToolError::Failed(anyhow!(
                "notion_append_block_children requires content that produces at least one block"
            )));
        }
        if children.len() > MAX_APPEND_BLOCKS {
            children.truncate(MAX_APPEND_BLOCKS);
        }

        let mut body = json!({ "children": children });
        if let Some(insert_position) =
            notion_append_position(position, after_block_ref).map_err(ToolError::Failed)?
        {
            body["position"] = insert_position;
        }

        let response = notion_request_json(
            Method::PATCH,
            &format!("blocks/{parent_id}/children"),
            Some(body),
            &config,
        )
        .await
        .map_err(ToolError::Failed)?;
        let appended = response
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        Ok(ToolOutput {
            payload: json!({
                "object": "block_list",
                "parent_id": parent_id,
                "appended_count": appended.len(),
                "appended_block_ids": appended
                    .iter()
                    .filter_map(|item| item.get("id").cloned())
                    .collect::<Vec<_>>(),
                "has_more": response.get("has_more").cloned().unwrap_or(Value::Bool(false)),
                "next_cursor": response.get("next_cursor").cloned().unwrap_or(Value::Null),
            }),
        })
    }
}

pub struct NotionCreateCommentTool;

pub struct NotionDeleteBlockTool;

#[async_trait]
impl Tool for NotionDeleteBlockTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_delete_block".to_owned(),
            description: "Delete one or more Notion blocks by ID or URL using the configured Notion integration. Always fetch the current page content first with notion_fetch and delete only block IDs returned by that latest read. This moves each block to Notion trash; duplicate, missing, or already-archived block IDs are skipped idempotently.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "block_refs": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "maxItems": 100
                    }
                },
                "required": ["block_refs"],
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
        let refs = input
            .get("block_refs")
            .and_then(Value::as_array)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_delete_block requires block_refs")))?;
        if refs.is_empty() {
            return Err(ToolError::Failed(anyhow!(
                "notion_delete_block requires at least one block ref"
            )));
        }

        let mut deleted = Vec::new();
        let mut skipped_duplicates = Vec::new();
        let mut already_archived_count = 0usize;
        let mut not_found_count = 0usize;
        let mut seen = HashSet::new();
        for reference in refs.iter().take(MAX_APPEND_BLOCKS) {
            let reference = reference.as_str().map(str::trim).ok_or_else(|| {
                ToolError::Failed(anyhow!("notion_delete_block block_refs must be strings"))
            })?;
            let block_id = notion_id_from_ref(reference).map_err(ToolError::Failed)?;
            if !seen.insert(block_id.clone()) {
                skipped_duplicates.push(json!({
                    "id": block_id,
                    "reason": "duplicate_block_ref",
                }));
                continue;
            }
            match notion_request_json(Method::DELETE, &format!("blocks/{block_id}"), None, &config)
                .await
            {
                Ok(response) => {
                    deleted.push(normalize_block_delete_result(&response, &block_id));
                }
                Err(error) if notion_error_is_already_archived(&error) => {
                    already_archived_count += 1;
                    deleted.push(normalize_already_archived_block_delete_result(&block_id));
                }
                Err(error) if notion_error_is_not_found(&error) => {
                    not_found_count += 1;
                    deleted.push(normalize_missing_block_delete_result(&block_id));
                }
                Err(error) => {
                    return Err(ToolError::Failed(error));
                }
            }
        }

        Ok(ToolOutput {
            payload: json!({
                "object": "block_delete_list",
                "requested_count": refs.len(),
                "deleted_count": deleted.len(),
                "deleted_blocks": deleted,
                "skipped_duplicate_count": skipped_duplicates.len(),
                "skipped_duplicates": skipped_duplicates,
                "already_archived_count": already_archived_count,
                "not_found_count": not_found_count,
                "truncated": refs.len() > MAX_APPEND_BLOCKS,
            }),
        })
    }
}

pub struct NotionApiRequestTool;

#[async_trait]
impl Tool for NotionApiRequestTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_api_request".to_owned(),
            description: "Call a Notion REST API endpoint through BeaverKi's configured Notion integration. Use this for current Notion endpoints that do not yet have a dedicated BeaverKi helper, such as views, file uploads, markdown page content, block updates, page move/trash, data source queries, or comment update/delete. Path must be relative to /v1.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "enum": ["GET", "POST", "PATCH", "DELETE"]
                    },
                    "path": { "type": "string" },
                    "body_json": {
                        "type": ["string", "null"],
                        "description": "Optional JSON request body encoded as a string. Use null for requests without a JSON body."
                    }
                },
                "required": ["method", "path", "body_json"],
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
        let method = input
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_api_request requires method")))
            .and_then(|method| notion_http_method(method).map_err(ToolError::Failed))?;
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_api_request requires path")))
            .and_then(|path| notion_api_path(path).map_err(ToolError::Failed))?;
        let body = input
            .get("body_json")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|body| {
                serde_json::from_str::<Value>(body).map_err(|error| {
                    ToolError::Failed(anyhow!(
                        "notion_api_request body_json must be valid JSON: {error}"
                    ))
                })
            })
            .transpose()?;

        let response = notion_request_json(method, &path, body, &config)
            .await
            .map_err(ToolError::Failed)?;
        let serialized = serde_json::to_string(&response).map_err(|error| {
            ToolError::Failed(anyhow!(
                "failed to serialize notion_api_request response: {error}"
            ))
        })?;

        Ok(ToolOutput {
            payload: json!({
                "method": input.get("method").cloned().unwrap_or(Value::Null),
                "path": path,
                "response_json": truncate_text(&serialized, context.max_output_chars),
            }),
        })
    }
}

#[async_trait]
impl Tool for NotionCreateCommentTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "notion_create_comment".to_owned(),
            description: "Create a comment on a Notion page or block using the configured Notion integration.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "parent_kind": {
                        "type": "string",
                        "enum": ["page", "block"]
                    },
                    "parent_ref": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["parent_kind", "parent_ref", "content"],
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
            .ok_or_else(|| {
                ToolError::Failed(anyhow!("notion_create_comment requires parent_kind"))
            })?;
        let parent_ref = input
            .get("parent_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| {
                ToolError::Failed(anyhow!("notion_create_comment requires parent_ref"))
            })?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ToolError::Failed(anyhow!("notion_create_comment requires content")))?;

        let parent_id = notion_id_from_ref(parent_ref).map_err(ToolError::Failed)?;
        let parent = notion_comment_parent(parent_kind, &parent_id).map_err(ToolError::Failed)?;
        let created = notion_request_json(
            Method::POST,
            "comments",
            Some(json!({
                "parent": parent,
                "rich_text": notion_rich_text_segments(content),
            })),
            &config,
        )
        .await
        .map_err(ToolError::Failed)?;

        Ok(ToolOutput {
            payload: normalize_comment_result(&created),
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

fn notion_update_properties_payload(
    existing_properties: Option<&Value>,
    updates: &Map<String, Value>,
) -> Result<Value> {
    let existing = existing_properties
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Notion page response is missing properties"))?;
    let mut normalized = Map::new();
    for (name, value) in updates {
        let property = existing.get(name).ok_or_else(|| {
            anyhow!(
                "Notion page has no property named '{}'; available properties: {}",
                name,
                notion_property_names(existing)
            )
        })?;
        normalized.insert(
            name.clone(),
            normalize_page_property_update(name, property, value)?,
        );
    }
    Ok(Value::Object(normalized))
}

fn notion_property_names(properties: &Map<String, Value>) -> String {
    let mut names = properties.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names.join(", ")
}

fn normalize_page_property_update(
    property_name: &str,
    existing_property: &Value,
    value: &Value,
) -> Result<Value> {
    let property_type = existing_property
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("property '{}' is missing a type", property_name))?;

    match property_type {
        "title" => Ok(json!({
            "title": normalize_rich_text_input(value, property_name, false)?
        })),
        "rich_text" => Ok(json!({
            "rich_text": normalize_rich_text_input(value, property_name, true)?
        })),
        "number" => {
            if value.is_number() || value.is_null() {
                Ok(json!({ "number": value.clone() }))
            } else {
                bail!("property '{}' expects a number or null", property_name);
            }
        }
        "select" => Ok(json!({
            "select": normalize_named_select_value(value, property_name)?
        })),
        "status" => Ok(json!({
            "status": normalize_named_select_value(value, property_name)?
        })),
        "multi_select" => Ok(json!({
            "multi_select": normalize_multi_select_value(value, property_name)?
        })),
        "checkbox" => {
            if let Some(flag) = value.as_bool() {
                Ok(json!({ "checkbox": flag }))
            } else {
                bail!("property '{}' expects a boolean", property_name);
            }
        }
        "date" => Ok(json!({
            "date": normalize_date_value(value, property_name)?
        })),
        "url" | "email" | "phone_number" => {
            if value.is_string() || value.is_null() {
                Ok(json!({ property_type: value.clone() }))
            } else {
                bail!("property '{}' expects a string or null", property_name);
            }
        }
        "people" => Ok(json!({
            "people": normalize_id_list(value, property_name, "people")?
        })),
        "relation" => Ok(json!({
            "relation": normalize_id_list(value, property_name, "relation")?
        })),
        other => bail!(
            "property '{}' has unsupported or non-editable type '{}'",
            property_name,
            other
        ),
    }
}

fn normalize_rich_text_input(
    value: &Value,
    property_name: &str,
    allow_null: bool,
) -> Result<Value> {
    if value.is_null() {
        if allow_null {
            return Ok(Value::Array(Vec::new()));
        }
        bail!("property '{}' cannot be null", property_name);
    }
    if let Some(text) = value.as_str() {
        return Ok(Value::Array(notion_rich_text_segments(text)));
    }
    if let Some(items) = value.as_array() {
        return Ok(Value::Array(items.clone()));
    }
    if allow_null {
        bail!(
            "property '{}' expects a string, null, or a rich_text array",
            property_name
        );
    }
    bail!(
        "property '{}' expects a string or a rich_text array",
        property_name
    );
}

fn normalize_named_select_value(value: &Value, property_name: &str) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if let Some(name) = value.as_str() {
        return Ok(json!({ "name": name }));
    }
    if let Some(object) = value.as_object()
        && (object.contains_key("name") || object.contains_key("id"))
    {
        return Ok(Value::Object(object.clone()));
    }
    bail!(
        "property '{}' expects a string, null, or an object with 'name'/'id'",
        property_name
    );
}

fn normalize_multi_select_value(value: &Value, property_name: &str) -> Result<Value> {
    let Some(items) = value.as_array() else {
        bail!("property '{}' expects an array", property_name);
    };
    let mut normalized = Vec::new();
    for item in items {
        if let Some(name) = item.as_str() {
            normalized.push(json!({ "name": name }));
            continue;
        }
        if let Some(object) = item.as_object()
            && (object.contains_key("name") || object.contains_key("id"))
        {
            normalized.push(Value::Object(object.clone()));
            continue;
        }
        bail!(
            "property '{}' expects multi_select entries to be strings or objects with 'name'/'id'",
            property_name
        );
    }
    Ok(Value::Array(normalized))
}

fn normalize_date_value(value: &Value, property_name: &str) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if let Some(start) = value.as_str() {
        return Ok(json!({ "start": start }));
    }
    if let Some(object) = value.as_object() {
        return Ok(Value::Object(object.clone()));
    }
    bail!(
        "property '{}' expects a string date, null, or a Notion date object",
        property_name
    );
}

fn normalize_id_list(value: &Value, property_name: &str, key: &str) -> Result<Value> {
    let Some(items) = value.as_array() else {
        bail!("property '{}' expects an array", property_name);
    };
    let mut normalized = Vec::new();
    for item in items {
        if let Some(id) = item.as_str() {
            normalized.push(json!({ "id": notion_id_from_ref(id)? }));
            continue;
        }
        if let Some(object) = item.as_object()
            && let Some(id) = object.get("id").and_then(Value::as_str)
        {
            normalized.push(json!({ "id": notion_id_from_ref(id)? }));
            continue;
        }
        bail!(
            "property '{}' expects {} entries to be strings or objects with 'id'",
            property_name,
            key
        );
    }
    Ok(Value::Array(normalized))
}

fn notion_append_position(
    position: Option<&str>,
    after_block_ref: Option<&str>,
) -> Result<Option<Value>> {
    match position.unwrap_or("end") {
        "end" => {
            if after_block_ref.is_some() {
                bail!("after_block_ref requires position 'after_block'");
            }
            Ok(None)
        }
        "start" => {
            if after_block_ref.is_some() {
                bail!("after_block_ref requires position 'after_block'");
            }
            Ok(Some(json!({ "type": "start" })))
        }
        "after_block" => {
            let after_block_ref = after_block_ref
                .ok_or_else(|| anyhow!("position 'after_block' requires after_block_ref"))?;
            Ok(Some(json!({
                "type": "after_block",
                "after_block": {
                    "id": notion_id_from_ref(after_block_ref)?
                }
            })))
        }
        other => bail!("unsupported append position '{other}'"),
    }
}

fn notion_create_parent(
    parent_kind: Option<&str>,
    parent_ref: Option<&str>,
    context: &ToolContext,
) -> Result<(String, String)> {
    match (parent_kind, parent_ref) {
        (Some(kind), Some(reference)) => {
            validate_notion_parent_kind(kind)?;
            Ok((kind.to_owned(), reference.to_owned()))
        }
        (None, None) => {
            let reference = context
                .notion_default_parent_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "notion_create_page requires parent_kind and parent_ref, or a configured Notion default parent"
                    )
                })?;
            let kind = context
                .notion_default_parent_kind
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("page");
            validate_notion_parent_kind(kind)?;
            Ok((kind.to_owned(), reference.to_owned()))
        }
        _ => bail!("notion_create_page requires both parent_kind and parent_ref together"),
    }
}

fn validate_notion_parent_kind(parent_kind: &str) -> Result<()> {
    match parent_kind {
        "page" | "data_source" => Ok(()),
        other => bail!("unsupported notion_create_page parent_kind '{other}'"),
    }
}

fn notion_comment_parent(parent_kind: &str, parent_id: &str) -> Result<Value> {
    match parent_kind {
        "page" => Ok(json!({ "page_id": parent_id })),
        "block" => Ok(json!({ "block_id": parent_id })),
        other => bail!("unsupported notion_create_comment parent_kind '{other}'"),
    }
}

fn notion_http_method(method: &str) -> Result<Method> {
    match method.trim().to_ascii_uppercase().as_str() {
        "GET" => Ok(Method::GET),
        "POST" => Ok(Method::POST),
        "PATCH" => Ok(Method::PATCH),
        "DELETE" => Ok(Method::DELETE),
        other => bail!("unsupported notion_api_request method '{other}'"),
    }
}

fn notion_api_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("notion_api_request path cannot be empty");
    }
    if trimmed.contains("://") || trimmed.starts_with("//") {
        bail!("notion_api_request path must be relative to /v1");
    }

    let without_prefix = trimmed
        .trim_start_matches('/')
        .strip_prefix("v1/")
        .unwrap_or_else(|| trimmed.trim_start_matches('/'));
    if without_prefix.is_empty()
        || without_prefix.starts_with('/')
        || without_prefix.contains("..")
        || without_prefix.chars().any(|ch| ch.is_control())
    {
        bail!("notion_api_request path must be a relative Notion API path");
    }
    Ok(without_prefix.to_owned())
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

fn normalize_comment_result(comment: &Value) -> Value {
    json!({
        "object": "comment",
        "id": comment.get("id").cloned().unwrap_or(Value::Null),
        "discussion_id": comment.get("discussion_id").cloned().unwrap_or(Value::Null),
        "parent": comment.get("parent").cloned().unwrap_or(Value::Null),
        "created_time": comment.get("created_time").cloned().unwrap_or(Value::Null),
        "last_edited_time": comment.get("last_edited_time").cloned().unwrap_or(Value::Null),
        "text": notion_rich_text_field(comment.get("rich_text")),
        "rich_text": comment.get("rich_text").cloned().unwrap_or(Value::Null),
    })
}

fn normalize_block_delete_result(block: &Value, fallback_id: &str) -> Value {
    json!({
        "object": block.get("object").cloned().unwrap_or_else(|| json!("block")),
        "id": block.get("id").cloned().unwrap_or_else(|| json!(fallback_id)),
        "type": block.get("type").cloned().unwrap_or(Value::Null),
        "in_trash": block.get("in_trash").cloned().unwrap_or(Value::Null),
        "archived": block.get("archived").cloned().unwrap_or(Value::Null),
        "has_children": block.get("has_children").cloned().unwrap_or(Value::Null),
        "text": notion_block_text(block),
    })
}

fn normalize_already_archived_block_delete_result(block_id: &str) -> Value {
    json!({
        "object": "block",
        "id": block_id,
        "type": Value::Null,
        "in_trash": true,
        "archived": true,
        "has_children": Value::Null,
        "text": "",
        "already_archived": true,
    })
}

fn normalize_missing_block_delete_result(block_id: &str) -> Value {
    json!({
        "object": "block",
        "id": block_id,
        "type": Value::Null,
        "in_trash": Value::Null,
        "archived": Value::Null,
        "has_children": Value::Null,
        "text": "",
        "not_found": true,
    })
}

fn notion_error_is_already_archived(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("Can't edit block that is archived")
        || message.contains("must unarchive the block before editing")
}

fn notion_error_is_not_found(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("status 404 Not Found")
        || message.contains("\"code\":\"object_not_found\"")
        || message.contains("Could not find block with ID")
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
    let mut children: Vec<Value> = Vec::new();
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;
    // Index of the most recent top-level nestable block. An indented line becomes its child.
    // Cleared by blank lines, headings, dividers, and code blocks.
    let mut last_top_idx: Option<usize> = None;

    while i < lines.len() && children.len() < MAX_CREATE_BLOCKS {
        let raw_line = lines[i];

        // Empty line — clear nesting context
        if raw_line.trim().is_empty() {
            last_top_idx = None;
            i += 1;
            continue;
        }

        let (is_indented, line) = strip_one_indent(raw_line);
        let trimmed = line.trim();

        // Fenced code block — always top-level; clears parent context
        if line.starts_with("```") || line.starts_with("~~~") {
            last_top_idx = None;
            let fence = if line.starts_with("```") {
                "```"
            } else {
                "~~~"
            };
            let lang = line.trim_start_matches('`').trim_start_matches('~').trim();
            let mut code_lines: Vec<&str> = Vec::new();
            i += 1;
            while i < lines.len() && !lines[i].trim_start().starts_with(fence) {
                code_lines.push(lines[i]);
                i += 1;
            }
            if i < lines.len() {
                i += 1;
            }
            let code_text = code_lines.join("\n");
            for chunk in split_text_chunks(&code_text, MAX_BLOCK_TEXT_CHARS) {
                if children.len() >= MAX_CREATE_BLOCKS {
                    return children;
                }
                children.push(json!({
                    "object": "block",
                    "type": "code",
                    "code": {
                        "language": if lang.is_empty() { "plain text" } else { lang },
                        "rich_text": [{"type": "text", "text": {"content": chunk}}]
                    }
                }));
            }
            continue;
        }

        // Divider — always top-level; clears parent context
        if trimmed == "---" || trimmed == "___" {
            last_top_idx = None;
            children.push(json!({"object": "block", "type": "divider", "divider": {}}));
            i += 1;
            continue;
        }

        // Headings — always top-level; clear parent context
        if let Some(text) = line.strip_prefix("### ") {
            last_top_idx = None;
            children.push(make_rich_block("heading_3", text));
            i += 1;
            continue;
        }
        if let Some(text) = line.strip_prefix("## ") {
            last_top_idx = None;
            children.push(make_rich_block("heading_2", text));
            i += 1;
            continue;
        }
        if let Some(text) = line.strip_prefix("# ") {
            last_top_idx = None;
            children.push(make_rich_block("heading_1", text));
            i += 1;
            continue;
        }

        // Single-line block types
        let block: Option<Value> = if let Some(text) = line
            .strip_prefix("- [ ] ")
            .or_else(|| line.strip_prefix("* [ ] "))
        {
            Some(
                json!({"object":"block","type":"to_do","to_do":{"checked":false,"rich_text":notion_rich_text_segments(text)}}),
            )
        } else if let Some(text) = line
            .strip_prefix("- [x] ")
            .or_else(|| line.strip_prefix("- [X] "))
            .or_else(|| line.strip_prefix("* [x] "))
            .or_else(|| line.strip_prefix("* [X] "))
        {
            Some(
                json!({"object":"block","type":"to_do","to_do":{"checked":true,"rich_text":notion_rich_text_segments(text)}}),
            )
        } else if let Some(text) = line.strip_prefix("- ").or_else(|| line.strip_prefix("* ")) {
            Some(make_rich_block("bulleted_list_item", text))
        } else if let Some(text) = strip_numbered_list_prefix(line) {
            Some(make_rich_block("numbered_list_item", text))
        } else {
            line.strip_prefix("> ")
                .map(|text| make_rich_block("quote", text))
        };

        if let Some(b) = block {
            push_or_nest(&mut children, &mut last_top_idx, b, is_indented);
            i += 1;
            continue;
        }

        // Paragraph: collect consecutive plain lines at the same indent level
        let mut para_lines: Vec<&str> = vec![line];
        i += 1;
        while i < lines.len() {
            let l = lines[i];
            if l.trim().is_empty() {
                break;
            }
            let (l_ind, l_ded) = strip_one_indent(l);
            if is_block_start(l_ded) || l_ind != is_indented {
                break;
            }
            para_lines.push(l_ded);
            i += 1;
        }
        let para_text = para_lines.join("\n");
        for chunk in split_text_chunks(&para_text, MAX_BLOCK_TEXT_CHARS) {
            if children.len() >= MAX_CREATE_BLOCKS {
                return children;
            }
            let b = json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": notion_rich_text_segments(&chunk)}
            });
            push_or_nest(&mut children, &mut last_top_idx, b, is_indented);
        }
    }

    children
}

/// Pushes `block` as a child of the current parent when indented, otherwise adds it to the
/// top level. Updates `last_top_idx` for block types that support children.
fn push_or_nest(
    children: &mut Vec<Value>,
    last_top_idx: &mut Option<usize>,
    block: Value,
    is_indented: bool,
) {
    if is_indented && let Some(pidx) = *last_top_idx {
        nest_child_block(&mut children[pidx], block);
        // Keep last_top_idx unchanged - the next sibling nests under the same parent.
        return;
    }
    if is_indented {
        // No parent available - fall through to top-level.
    }
    let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
    let can_be_parent = matches!(
        block_type,
        "bulleted_list_item" | "numbered_list_item" | "to_do" | "quote" | "paragraph"
    );
    let idx = children.len();
    children.push(block);
    if can_be_parent {
        *last_top_idx = Some(idx);
    }
}

fn nest_child_block(parent: &mut Value, child: Value) {
    let parent_type = parent
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    if let Some(inner) = parent.get_mut(&parent_type)
        && let Some(obj) = inner.as_object_mut()
    {
        let children = obj
            .entry("children".to_owned())
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(children) = children.as_array_mut() {
            children.push(child);
        }
    }
}

/// Strips one indent level (2 spaces, 4 spaces, or a tab) from the start of a line.
fn strip_one_indent(line: &str) -> (bool, &str) {
    if let Some(rest) = line.strip_prefix("    ") {
        (true, rest)
    } else if let Some(rest) = line.strip_prefix("  ") {
        (true, rest)
    } else if let Some(rest) = line.strip_prefix('\t') {
        (true, rest)
    } else {
        (false, line)
    }
}

fn make_rich_block(block_type: &str, text: &str) -> Value {
    let mut block = json!({"object": "block", "type": block_type});
    if let Some(object) = block.as_object_mut() {
        object.insert(
            block_type.to_owned(),
            json!({"rich_text": notion_rich_text_segments(text)}),
        );
    }
    block
}

fn is_block_start(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed == "---"
        || trimmed == "___"
        || line.starts_with("```")
        || line.starts_with("~~~")
        || line.starts_with("# ")
        || line.starts_with("## ")
        || line.starts_with("### ")
        || line.starts_with("- ")
        || line.starts_with("* ")
        || line.starts_with("> ")
        || strip_numbered_list_prefix(line).is_some()
}

fn strip_numbered_list_prefix(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i > 0 && i + 1 < bytes.len() && bytes[i] == b'.' && bytes[i + 1] == b' ' {
        Some(&line[i + 2..])
    } else {
        None
    }
}

fn notion_rich_text_segments(text: &str) -> Vec<Value> {
    let mut out = Vec::new();
    parse_inline_markdown(text, &mut out);
    if out.is_empty() {
        out.push(json!({"type": "text", "text": {"content": ""}}));
    }
    out
}

fn parse_inline_markdown(mut text: &str, out: &mut Vec<Value>) {
    let mut plain = String::new();

    while !text.is_empty() {
        // Inline code: `...`
        if text.starts_with('`') {
            let rest = &text[1..];
            if let Some(end) = rest.find('`') {
                flush_inline_plain(&mut plain, out);
                push_inline_annotated(out, &rest[..end], false, false, true, false);
                text = &rest[end + 1..];
                continue;
            }
        }
        // Bold + italic: ***...***
        if text.starts_with("***") {
            let rest = &text[3..];
            if let Some(end) = rest.find("***") {
                flush_inline_plain(&mut plain, out);
                push_inline_annotated(out, &rest[..end], true, true, false, false);
                text = &rest[end + 3..];
                continue;
            }
        }
        // Bold: **...**
        if text.starts_with("**") {
            let rest = &text[2..];
            if let Some(end) = rest.find("**") {
                flush_inline_plain(&mut plain, out);
                push_inline_annotated(out, &rest[..end], true, false, false, false);
                text = &rest[end + 2..];
                continue;
            }
        }
        // Italic: *...* (single star, not **)
        if text.starts_with('*') && !text.starts_with("**") {
            let rest = &text[1..];
            if let Some(end) = find_single_star(rest) {
                flush_inline_plain(&mut plain, out);
                push_inline_annotated(out, &rest[..end], false, true, false, false);
                text = &rest[end + 1..];
                continue;
            }
        }
        // Strikethrough: ~~...~~
        if text.starts_with("~~") {
            let rest = &text[2..];
            if let Some(end) = rest.find("~~") {
                flush_inline_plain(&mut plain, out);
                push_inline_annotated(out, &rest[..end], false, false, false, true);
                text = &rest[end + 2..];
                continue;
            }
        }
        // Link: [text](url)
        if text.starts_with('[')
            && let Some(bracket_end) = text.find("](")
        {
            let link_text = &text[1..bracket_end];
            let rest = &text[bracket_end + 2..];
            if let Some(paren_end) = rest.find(')') {
                let url = &rest[..paren_end];
                flush_inline_plain(&mut plain, out);
                for chunk in split_text_chunks(link_text, MAX_BLOCK_TEXT_CHARS) {
                    out.push(json!({
                        "type": "text",
                        "text": {"content": chunk, "link": {"url": url}}
                    }));
                }
                text = &rest[paren_end + 1..];
                continue;
            }
        }
        // Color: {color:text} or {color_background:text}
        if text.starts_with('{')
            && let Some(colon_pos) = text[1..].find(':')
        {
            let color = &text[1..colon_pos + 1];
            if is_notion_color(color) {
                let rest = &text[colon_pos + 2..];
                if let Some(close_pos) = rest.find('}') {
                    let colored = &rest[..close_pos];
                    flush_inline_plain(&mut plain, out);
                    for chunk in split_text_chunks(colored, MAX_BLOCK_TEXT_CHARS) {
                        out.push(json!({
                            "type": "text",
                            "text": {"content": chunk},
                            "annotations": {"color": color}
                        }));
                    }
                    text = &rest[close_pos + 1..];
                    continue;
                }
            }
        }
        let ch_len = text.chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        plain.push_str(&text[..ch_len]);
        text = &text[ch_len..];
    }
    flush_inline_plain(&mut plain, out);
}

fn find_single_star(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'*' {
            if i + 1 >= bytes.len() || bytes[i + 1] != b'*' {
                return Some(i);
            }
            i += 2;
            continue;
        }
        i += 1;
    }
    None
}

fn is_notion_color(s: &str) -> bool {
    matches!(
        s,
        "default"
            | "gray"
            | "brown"
            | "orange"
            | "yellow"
            | "green"
            | "blue"
            | "purple"
            | "pink"
            | "red"
            | "gray_background"
            | "brown_background"
            | "orange_background"
            | "yellow_background"
            | "green_background"
            | "blue_background"
            | "purple_background"
            | "pink_background"
            | "red_background"
    )
}

fn flush_inline_plain(plain: &mut String, out: &mut Vec<Value>) {
    if plain.is_empty() {
        return;
    }
    for chunk in split_text_chunks(plain, MAX_BLOCK_TEXT_CHARS) {
        out.push(json!({"type": "text", "text": {"content": chunk}}));
    }
    plain.clear();
}

fn push_inline_annotated(
    out: &mut Vec<Value>,
    text: &str,
    bold: bool,
    italic: bool,
    code: bool,
    strikethrough: bool,
) {
    for chunk in split_text_chunks(text, MAX_BLOCK_TEXT_CHARS) {
        let mut annotations = Map::new();
        if bold {
            annotations.insert("bold".to_owned(), Value::Bool(true));
        }
        if italic {
            annotations.insert("italic".to_owned(), Value::Bool(true));
        }
        if code {
            annotations.insert("code".to_owned(), Value::Bool(true));
        }
        if strikethrough {
            annotations.insert("strikethrough".to_owned(), Value::Bool(true));
        }
        let mut seg = json!({"type": "text", "text": {"content": chunk}});
        if !annotations.is_empty()
            && let Some(object) = seg.as_object_mut()
        {
            object.insert("annotations".to_owned(), Value::Object(annotations));
        }
        out.push(seg);
    }
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

    #[test]
    fn normalizes_page_property_updates_from_page_schema() {
        let existing = json!({
            "Title": { "id": "title", "type": "title", "title": [] },
            "Status": { "id": "status", "type": "status", "status": null },
            "Done": { "id": "done", "type": "checkbox", "checkbox": false },
            "Tags": { "id": "tags", "type": "multi_select", "multi_select": [] },
            "Due": { "id": "due", "type": "date", "date": null },
            "Related": { "id": "rel", "type": "relation", "relation": [] }
        });
        let updates = json!({
            "Title": "Pick up groceries",
            "Status": "In Progress",
            "Done": true,
            "Tags": ["shopping", "errands"],
            "Due": "2026-04-22",
            "Related": ["be633bf1dfa0436db259571129a590e5"]
        });

        let payload = notion_update_properties_payload(
            Some(&existing),
            updates.as_object().expect("update object"),
        )
        .expect("payload");

        assert_eq!(
            payload,
            json!({
                "Title": { "title": notion_rich_text_segments("Pick up groceries") },
                "Status": { "status": { "name": "In Progress" } },
                "Done": { "checkbox": true },
                "Tags": {
                    "multi_select": [
                        { "name": "shopping" },
                        { "name": "errands" }
                    ]
                },
                "Due": { "date": { "start": "2026-04-22" } },
                "Related": {
                    "relation": [
                        { "id": "be633bf1-dfa0-436d-b259-571129a590e5" }
                    ]
                }
            })
        );
    }

    #[test]
    fn rejects_unknown_page_property_updates() {
        let existing = json!({
            "Title": { "id": "title", "type": "title", "title": [] }
        });
        let updates = json!({
            "Missing": "value"
        });

        let error = notion_update_properties_payload(
            Some(&existing),
            updates.as_object().expect("update object"),
        )
        .expect_err("should reject unknown property");

        assert!(error.to_string().contains("Missing"));
        assert!(error.to_string().contains("Title"));
    }

    #[test]
    fn builds_append_position_after_block() {
        let position = notion_append_position(
            Some("after_block"),
            Some("https://www.notion.so/My-Page-be633bf1dfa0436db259571129a590e5"),
        )
        .expect("position");

        assert_eq!(
            position,
            Some(json!({
                "type": "after_block",
                "after_block": {
                    "id": "be633bf1-dfa0-436d-b259-571129a590e5"
                }
            }))
        );
    }

    #[test]
    fn resolves_configured_default_create_parent() {
        let mut context = ToolContext::new(std::env::temp_dir(), vec![]);
        context.notion_default_parent_kind = Some("page".to_owned());
        context.notion_default_parent_ref = Some("be633bf1-dfa0-436d-b259-571129a590e5".to_owned());

        let parent = notion_create_parent(None, None, &context).expect("default parent");

        assert_eq!(
            parent,
            (
                "page".to_owned(),
                "be633bf1-dfa0-436d-b259-571129a590e5".to_owned()
            )
        );
    }

    #[test]
    fn parses_properties_json_for_update_flow() {
        let decoded: Value =
            serde_json::from_str(r#"{"Title":"Household Inbox","Status":"Done","Done":true}"#)
                .expect("valid json");
        let payload = notion_update_properties_payload(
            Some(&json!({
                "Title": { "id": "title", "type": "title", "title": [] },
                "Status": { "id": "status", "type": "status", "status": null },
                "Done": { "id": "done", "type": "checkbox", "checkbox": false }
            })),
            decoded.as_object().expect("object"),
        )
        .expect("payload");

        assert_eq!(
            payload,
            json!({
                "Title": { "title": notion_rich_text_segments("Household Inbox") },
                "Status": { "status": { "name": "Done" } },
                "Done": { "checkbox": true }
            })
        );
    }

    #[test]
    fn builds_comment_parent_and_normalizes_comment_response() {
        let parent =
            notion_comment_parent("page", "be633bf1-dfa0-436d-b259-571129a590e5").expect("parent");
        assert_eq!(
            parent,
            json!({ "page_id": "be633bf1-dfa0-436d-b259-571129a590e5" })
        );

        let normalized = normalize_comment_result(&json!({
            "object": "comment",
            "id": "comment_123",
            "discussion_id": "discussion_456",
            "parent": parent,
            "created_time": "2026-04-21T10:05:00.000Z",
            "rich_text": [{ "plain_text": "Please buy oat milk" }]
        }));
        assert_eq!(normalized["id"], json!("comment_123"));
        assert_eq!(normalized["text"], json!("Please buy oat milk"));
    }

    #[test]
    fn normalizes_deleted_block_response() {
        let normalized = normalize_block_delete_result(
            &json!({
                "object": "block",
                "id": "be633bf1-dfa0-436d-b259-571129a590e5",
                "type": "paragraph",
                "in_trash": true,
                "paragraph": {
                    "rich_text": [{ "plain_text": "Delete me" }]
                }
            }),
            "fallback",
        );

        assert_eq!(
            normalized["id"],
            json!("be633bf1-dfa0-436d-b259-571129a590e5")
        );
        assert_eq!(normalized["in_trash"], json!(true));
        assert_eq!(normalized["text"], json!("Delete me"));
    }

    #[test]
    fn recognizes_already_archived_delete_error() {
        let error = anyhow!(
            "Notion API request to 'blocks/id' failed with status 400 Bad Request: {{\"message\":\"Can't edit block that is archived. You must unarchive the block before editing.\"}}"
        );

        assert!(notion_error_is_already_archived(&error));

        let normalized =
            normalize_already_archived_block_delete_result("be633bf1-dfa0-436d-b259-571129a590e5");
        assert_eq!(normalized["in_trash"], json!(true));
        assert_eq!(normalized["already_archived"], json!(true));
    }

    #[test]
    fn recognizes_missing_block_delete_error() {
        let error = anyhow!(
            "Notion API request to 'blocks/id' failed with status 404 Not Found: {{\"code\":\"object_not_found\",\"message\":\"Could not find block with ID: id\"}}"
        );

        assert!(notion_error_is_not_found(&error));

        let normalized =
            normalize_missing_block_delete_result("be633bf1-dfa0-436d-b259-571129a590e5");
        assert_eq!(normalized["not_found"], json!(true));
        assert_eq!(
            normalized["id"],
            json!("be633bf1-dfa0-436d-b259-571129a590e5")
        );
    }

    #[test]
    fn normalizes_generic_api_paths() {
        assert_eq!(
            notion_api_path("/v1/pages/page-id").expect("path"),
            "pages/page-id"
        );
        assert_eq!(
            notion_api_path("data_sources/source-id/query").expect("path"),
            "data_sources/source-id/query"
        );
        assert!(notion_api_path("https://api.notion.com/v1/users/me").is_err());
        assert!(notion_api_path("../users/me").is_err());
    }
}
