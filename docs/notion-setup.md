# Notion Setup Guide

BeaverKi currently integrates with Notion through the official Notion REST API, not through Notion's hosted MCP server.

That is intentional for now:

- BeaverKi is a long-lived local runtime that benefits from stable bearer-token auth for background work.
- Notion's hosted MCP currently uses interactive OAuth + PKCE and is a weaker fit for unattended automation.
- The REST API aligns with BeaverKi's existing encrypted-secret configuration model.

## Quickstart

If you just want to enable the Notion tools, the shortest path is:

1. Create a Notion internal integration and copy its token.
2. Run `cargo run -p beaverki-cli -- integration notion configure --enable`.
3. Restart the BeaverKi daemon if it was already running.
4. Run a task that asks BeaverKi to search, fetch, or create a Notion page.

The detailed steps below explain each part.

## 1. Create A Notion Integration

1. Open the Notion developer dashboard.
2. Create an internal integration for the workspace you want BeaverKi to access.
3. Grant the integration the capabilities you need.
   For the current BeaverKi tools, `Read content` and `Insert content` are the important ones.
4. Copy the integration token.

You also need to share the target pages or data sources with the integration inside Notion, otherwise search and fetch will not see them.

## 2. Configure BeaverKi

Export the token:

```bash
export NOTION_API_TOKEN="secret_xxx"
```

Enable the integration:

```bash
cargo run -p beaverki-cli -- integration notion configure --enable
```

Optionally configure a default parent page or data source where BeaverKi should create new Notion pages when the user has not named a parent. This avoids repeated parent-discovery searches:

```bash
cargo run -p beaverki-cli -- integration notion configure \
  --default-parent-kind page \
  --default-parent-ref <notion-page-url-or-id>
```

If BeaverKi is already running as a daemon, restart it after enabling Notion so the runtime loads the decrypted token into memory:

```bash
make daemon-stop
make daemon-start
```

Inspect the stored configuration and verify the API connection:

```bash
cargo run -p beaverki-cli -- integration notion show
```

The command decrypts the stored token and calls the Notion API (`/users/me`) to confirm the credentials are valid.

Expected output when everything is working:

```
Notion enabled: true
API base URL: https://api.notion.com/v1
API version: 2026-03-11
Default parent kind: page
Default parent ref: https://www.notion.so/...
API token secret ref: secret://local/notion_api_token
API connection: ok
Bot name: My Integration
Workspace: My Workspace Name
```

If `API connection: FAILED` appears instead, the token is likely wrong or expired — re-run `notion configure --enable` with a fresh token.

The token is written to BeaverKi's encrypted local secret store, following the same pattern as the OpenAI and Discord credentials.

## 3. Verify That The Tools Work

Once the integration is enabled and the daemon has been restarted, run a simple task such as:

```bash
cargo run -p beaverki-cli -- task run \
  --objective "Search my Notion workspace for pages related to roadmap planning and summarize the top matches."
```

If you want to test page creation, ask BeaverKi to create a page under a specific parent page or data source that has been shared with the Notion integration.

If BeaverKi says it cannot find anything in Notion, the most common cause is that the relevant page or data source has not been shared with the integration inside Notion.

## 4. Available Agent Tools

When the Notion integration is enabled, BeaverKi exposes these built-in tools to the agent:

- `notion_search`: search pages or data sources shared with the integration
- `notion_fetch`: fetch a page or data source by Notion URL or ID
- `notion_create_page`: create a page under a parent page or data source, or under the configured default parent when no parent is supplied
- `notion_update_page`: update properties on an existing page using the page's current schema
- `notion_append_block_children`: append Markdown-like content blocks to an existing page or block
- `notion_delete_block`: delete page content blocks by ID or URL after reading the current page content; duplicate, missing, or already-deleted block IDs are skipped idempotently
- `notion_create_comment`: leave a comment on a page or block
- `notion_api_request`: call current Notion REST API endpoints that do not yet have a dedicated helper

This repository also ships a starter packaged skill in `skills/notion` with higher-level wrappers:

- `notion_workspace_search`
- `notion_read_entry`
- `notion_capture_note`
- `notion_update_entry`
- `notion_append_content`
- `notion_delete_blocks`
- `notion_raw_api_request`
- `notion_comment`

Those skill tools are Lua wrappers over the Rust-side Notion capability boundary, which keeps network access and credential handling in the host runtime rather than in Lua.

Practical examples for the new write tools:

- update a household page status or due date without creating a new entry
- append fresh shopping items or meeting notes to an existing shared page
- fetch the current page content, delete obsolete blocks by IDs returned from that latest read, then append rewritten content
- use the generic API request wrapper for newer endpoints such as enhanced markdown page content, views, file uploads, page move/trash, block updates, data source queries, and comment update/delete
- leave review feedback or a follow-up note as a page or block comment

## 5. Troubleshooting

- If `integration notion show` still reports `Notion enabled: false`, rerun the configure command and check that you wrote to the correct `--config-dir`.
- If BeaverKi still cannot use the Notion tools after enabling them, restart the daemon so it reloads integration secrets.
- If search or fetch returns nothing useful, confirm that the target pages or data sources were shared with the Notion integration in the Notion UI.
- If page creation fails for a data source, the current implementation may not support that data source's property schema yet. Creating under a normal parent page is the simplest first test.

## 6. Current Limits

- This first cut uses the Notion REST API only.
- Hosted Notion MCP is not wired into BeaverKi yet.
- `notion_create_page` can create content under a parent page directly.
- `notion_update_page` normalizes a practical subset of editable property types based on the target page's existing schema.
- `notion_append_block_children` uses the same Markdown-like block conversion as `notion_create_page`.
- `notion_create_comment` currently targets page-level or block-level comments.
- For data source parents, BeaverKi only auto-populates the title property it can infer from the schema.
- Advanced property mapping for custom data sources is still follow-up work.
