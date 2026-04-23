# CLI and Operations Guide

This guide holds the command-oriented operational details for BeaverKi. The README stays focused on what the project is, what state it is in, and how to get a household installation running.

## Prerequisites And Defaults

- install a Rust toolchain with `cargo`
- install `make` if you want the short wrapper commands from the repository root
- set `OPENAI_API_KEY` before first-time setup
- optionally set `BEAVERKI_MASTER_PASSPHRASE` to avoid repeated passphrase prompts
- optionally set `DISCORD_BOT_TOKEN` before enabling the Discord connector

Default model selections at setup time are:

- planner: `gpt-5.4`
- executor: `gpt-5.4-mini`
- summarizer: `gpt-5.4-mini`
- safety review: `gpt-5.4-mini`

Default storage locations are platform-standard:

- Linux: `~/.config/beaverki` for config and `~/.local/share/beaverki` or `~/.local/state/beaverki` for runtime state
- macOS: `~/Library/Application Support/beaverki`
- Windows: `%APPDATA%\beaverki` and `%LOCALAPPDATA%\beaverki`

If you want a non-default config location, pass `--config-dir` to direct CLI commands or set `CONFIG_DIR=/path/to/config` when using `make`.

Packaged filesystem skills are discovered from an ordered search path. By default BeaverKi checks:

- explicit `skill_search_paths` entries in `runtime.yaml`
- `BEAVERKI_SKILLS_DIRS`
- `<data_dir>/skills`
- install-relative locations near the executable such as `../share/beaverki/skills` and `../skills`
- Unix system locations such as `/usr/local/share/beaverki/skills` and `/usr/share/beaverki/skills`
- the repository `skills/` directory only when BeaverKi is being run from a Cargo `target/` build during development

## Make Targets vs Direct CLI

The repository root Makefile wraps the most common commands:

```bash
make setup
make daemon-start
make daemon-status
make daemon-stop
make web-ui
make role-list
make user-list
make package-release RELEASE_TAG=2026-04-23.1 PLATFORM_ID=linux-x86_64
make run-task OBJECTIVE="Inspect the repository and summarize it."
make show-task TASK_ID=<task-id>
```

All of those commands eventually call the Rust CLI directly:

```bash
cargo run -p beaverki-cli -- <subcommand>
```

Use the direct CLI form when you need flags that do not have a dedicated Make target.

The local web UI uses its own binary:

```bash
cargo run -p beaverki-web -- --config-dir /path/to/config
```

Inspect the effective packaged skill search paths and discovered filesystem skill tools:

```bash
cargo run -p beaverki-cli -- skill list
```

For tagged release installs, a curlable installer is available:

```bash
curl -fsSL https://raw.githubusercontent.com/torlenor/beaverki/master/packaging/install.sh | bash -s -- --tag 2026-04-23.1
```

## Setup And Model Configuration

Verify that the OpenAI credential works:

```bash
cargo run -p beaverki-cli -- setup verify-openai
```

Initialize a new BeaverKi installation:

```bash
cargo run -p beaverki-cli -- setup init
```

Inspect or change the configured models:

```bash
cargo run -p beaverki-cli -- setup show-models
cargo run -p beaverki-cli -- setup set-models \
  --planner-model gpt-5.4 \
  --executor-model gpt-5.4-mini \
  --summarizer-model gpt-5.4-mini \
  --safety-review-model gpt-5.4-mini
```

`setup init` verifies the OpenAI API token unless `--skip-openai-check` is passed. The token is stored as an encrypted local secret reference under the BeaverKi state directory.

## Daemon Lifecycle

Start, inspect, and stop the long-lived runtime:

```bash
cargo run -p beaverki-cli -- daemon start
cargo run -p beaverki-cli -- daemon status
cargo run -p beaverki-cli -- daemon stop
```

For a foreground process instead of a spawned background daemon:

```bash
cargo run -p beaverki-cli -- daemon run
```

The runtime reads the master passphrase from `BEAVERKI_MASTER_PASSPHRASE` when available. Otherwise it prompts interactively.

## Local Web UI

Start the BeaverKi daemon first, then launch the local web UI in a separate terminal:

```bash
make daemon-start
make web-ui
```

Or run the web UI directly:

```bash
cargo run -p beaverki-web -- --config-dir /path/to/config
```

By default the UI listens on `127.0.0.1:7676`.

Design and access model for the first slice:

- the web UI talks to the BeaverKi daemon through the supported daemon client surface rather than reading SQLite directly
- the server binds only to a loopback address and rejects non-loopback bind targets
- anyone with access to the local machine and browser session can operate the local BeaverKi instance through this UI
- public internet hosting, reverse-proxy hardening, and multi-tenant auth are intentionally out of scope for this slice

Current operator flows in the UI include:

- submit tasks and inspect results
- inspect and resolve pending approvals
- inspect and reset or archive sessions
- inspect recent memory
- inspect household users and add new users with built-in roles
- create, edit, activate, disable, replay, delete, and schedule workflows

## Household Users And Roles

List the built-in roles:

```bash
cargo run -p beaverki-cli -- role list
```

List users:

```bash
cargo run -p beaverki-cli -- user list
```

Add household members:

```bash
cargo run -p beaverki-cli -- user add --display-name Casey --role adult
cargo run -p beaverki-cli -- user add --display-name Sam --role child
```

Each user gets:

- a stable BeaverKi user ID
- a persistent primary agent
- one or more built-in household roles

## Tasks And Approvals

Run a task as the owner or default user:

```bash
cargo run -p beaverki-cli -- task run --objective "Inspect the repository and summarize it."
```

Run a task as a specific household member:

```bash
cargo run -p beaverki-cli -- task run --user user_casey --objective "Inspect my recent activity."
```

Inspect a task:

```bash
cargo run -p beaverki-cli -- task show --task-id <task-id>
```

Inspect and resolve approvals:

```bash
cargo run -p beaverki-cli -- approval list
cargo run -p beaverki-cli -- approval approve --approval-id <approval-id>
cargo run -p beaverki-cli -- approval deny --approval-id <approval-id>
```

Approvals are especially relevant for generated shell actions that exceed the allowed risk threshold.

## Workflow Pipelines

For the durable workflow model, stage semantics, and the agent-side workflow authoring tool surface, see [Workflow Pipelines](workflow-pipelines.md).

Example workflow definition:

```text
docs/examples/morning-news-digest-workflow.json
```

Create a workflow definition from JSON:

```bash
cargo run -p beaverki-cli -- automation workflow create \
  --definition-file docs/examples/morning-news-digest-workflow.json \
  --summary "Fetch news, summarize it, hand it to an agent, and notify the owner."
```

Re-run the same command with the same `--workflow-id` to create a new workflow version and replace the current editable definition:

```bash
cargo run -p beaverki-cli -- automation workflow create \
  --workflow-id morning_digest \
  --definition-file /tmp/revised-workflow.json \
  --summary "Revise the workflow after a failed run."
```

List and inspect workflows:

```bash
cargo run -p beaverki-cli -- automation workflow list
cargo run -p beaverki-cli -- automation workflow show --workflow-id <workflow-id>
```

Review, activate, and replay a workflow:

```bash
cargo run -p beaverki-cli -- automation workflow review \
  --workflow-id <workflow-id> \
  --summary "Re-run workflow review after editing referenced artifacts."

cargo run -p beaverki-cli -- automation workflow activate --workflow-id <workflow-id>
cargo run -p beaverki-cli -- automation workflow replay --workflow-id <workflow-id>
```

Schedule a workflow instead of a single Lua script:

```bash
cargo run -p beaverki-cli -- automation schedule add \
  --workflow-id <workflow-id> \
  --schedule-id morning_digest \
  --cron "0 8 * * *"
```

The schedule command now accepts exactly one of:

- `--script-id`
- `--workflow-id`

Cron input accepts standard 5-field expressions such as `0 7 * * *`. It also accepts 6- or 7-field variants with a leading seconds field, and an optional leading timezone hint such as `TZ=Europe/Vienna 0 7 * * *` or `CRON_TZ=Europe/Vienna 0 7 * * *`.

## Memory Inspection

List memories:

```bash
cargo run -p beaverki-cli -- memory list
```

Inspect a specific memory:

```bash
cargo run -p beaverki-cli -- memory show --memory-id <memory-id>
```

Inspect history for a subject key:

```bash
cargo run -p beaverki-cli -- memory history --subject-key profile.preferred_name
```

Forget an incorrect memory:

```bash
cargo run -p beaverki-cli -- memory forget --memory-id <memory-id> --reason "Wrong fact"
```

## Discord Connector

For a full end-to-end Discord setup walkthrough, including Discord Developer Portal steps, user ID mapping, channel allowlisting, and testing, see [Discord Setup Guide](discord-setup.md).

Inspect the current connector configuration:

```bash
cargo run -p beaverki-cli -- connector discord show
```

List Discord allowlisted channels with their configured mode:

```bash
cargo run -p beaverki-cli -- connector discord list-channels
```

Enable Discord and store the bot token:

```bash
cargo run -p beaverki-cli -- connector discord configure --enable
```

Add or update a Discord guild channel in household mode:

```bash
cargo run -p beaverki-cli -- connector discord add-channel --channel-id 1234567890 --mode household
```

Add or update a Discord guild channel in guest mode:

```bash
cargo run -p beaverki-cli -- connector discord add-channel --channel-id 1234567890 --mode guest
```

Remove a Discord guild channel from the allowlist:

```bash
cargo run -p beaverki-cli -- connector discord remove-channel --channel-id 1234567890
```

Map a Discord user to a BeaverKi household user:

```bash
cargo run -p beaverki-cli -- connector discord map-user \
  --external-user-id 111111111111111111 \
  --mapped-user-id user_casey
```

List the configured identity mappings:

```bash
cargo run -p beaverki-cli -- connector discord list-mappings
```

Direct messages are accepted by default. Guild messages are accepted only from explicitly allowlisted channels and only when they either start with the configured command prefix or include a direct mention of the bot.

In practice, that means household members can DM the bot directly without a prefix, while shared server channels should either use the configured prefix such as `!bk` or include a direct mention such as `@BeaverKi`.

## Discord Developer Portal Checklist

If you are configuring the bot from scratch, make sure the Discord application has:

- a bot user with a valid token
- Message Content intent enabled
- access to the server and channels you intend to allowlist
- permissions that let it read messages, send replies, read channel history, and add reactions in the channels where BeaverKi will operate

## Notion Integration

BeaverKi currently integrates with Notion through the official Notion REST API and encrypted local token storage. This is a better fit for BeaverKi's background automation than Notion's hosted MCP server, which currently uses interactive OAuth.

Inspect the current Notion integration configuration:

```bash
cargo run -p beaverki-cli -- integration notion show
```

Enable Notion and store the API token from `NOTION_API_TOKEN` or from an interactive prompt:

```bash
cargo run -p beaverki-cli -- integration notion configure --enable
```

If the BeaverKi daemon is already running, restart it after changing the Notion integration so the runtime reloads the token from encrypted storage.

Override the API version or base URL if needed:

```bash
cargo run -p beaverki-cli -- integration notion configure \
  --api-version 2026-03-11 \
  --api-base-url https://api.notion.com/v1
```

Once enabled, the agent can use:

- built-in tools: `notion_search`, `notion_fetch`, `notion_create_page`, `notion_update_page`, `notion_append_block_children`, `notion_create_comment`
- packaged skill wrappers from `skills/notion`: `notion_workspace_search`, `notion_read_entry`, `notion_capture_note`, `notion_update_entry`, `notion_append_content`, `notion_comment`

## Lua Automation And Schedules

Create and inspect a Lua script:

```bash
cargo run -p beaverki-cli -- automation script create \
  --summary "Daily inbox cleanup" \
  --source-file ./script.lua

cargo run -p beaverki-cli -- automation script show --script-id <script-id>
```

Run review and activation flow:

```bash
cargo run -p beaverki-cli -- automation script review --script-id <script-id> --summary "Review proposed script"
cargo run -p beaverki-cli -- automation script activate --script-id <script-id>
cargo run -p beaverki-cli -- automation script disable --script-id <script-id>
```

Attach a schedule and manage it later:

```bash
cargo run -p beaverki-cli -- automation schedule add --script-id <script-id> --cron "0 9 * * *"
cargo run -p beaverki-cli -- automation schedule list
cargo run -p beaverki-cli -- automation schedule disable --schedule-id <schedule-id>
cargo run -p beaverki-cli -- automation schedule enable --schedule-id <schedule-id>
```

## Local Validation

Run the standard workspace checks before opening a PR:

```bash
make fmt
make lint
make check
make test
```
