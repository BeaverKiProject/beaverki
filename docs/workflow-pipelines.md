# Workflow Pipelines

This document explains BeaverKi's M6 scheduled workflow pipeline model.

## What M6 Adds

Before M6, BeaverKi schedules could directly materialize a single reviewed Lua script run.

M6 adds a durable workflow layer on top of that baseline:

- workflow definitions with ordered reviewed stages
- persistent workflow runs that survive restart
- schedule materialization for `target_type=workflow`
- explicit stage boundaries and stage artifacts
- workflow version history for iterative edits
- fail-closed autonomous execution for approved scheduled runs
- a first-class `user_notify` delivery stage

The current stage kinds are:

- `lua_script`
- `lua_tool`
- `agent_task`
- `user_notify`

## Execution Model

A workflow definition is owned by one BeaverKi user and contains ordered stages.

The stable identifier is `workflow_id`.

Edits create a new workflow version under the same `workflow_id`, and the latest version becomes the current definition used for activation, replay, and scheduled execution.

When a workflow is triggered, BeaverKi creates:

- a durable `workflow_run`
- a runnable task of kind `workflow_run`
- a private scheduled-run conversation session when the trigger came from a schedule

The runtime then executes stages in order.

Each stage receives explicit structured input built from:

- workflow identity
- current stage identity
- the last stage result
- the carried workflow artifacts object
- stage-specific config

Each stage writes a structured result back into the workflow artifacts store. The runtime persists the run's current stage index and artifacts after each successful stage.

That persisted run state can be inspected later to debug failures and refine the workflow with the user.

## Safety And Approval Model

Workflow definitions go through their own blocking safety review even if referenced Lua scripts or Lua tools were already reviewed individually.

Activation and scheduling remain approval-gated actions.

Scheduled workflow execution is autonomous only when:

- the workflow definition is safety-approved
- the workflow definition is active
- referenced Lua scripts are safety-approved and active
- referenced Lua tools are safety-approved and active

If a stage exceeds the approved envelope at runtime, the workflow does not enter an interactive approval pause. Instead, the run is persisted as blocked and the materialized task fails closed.

## Current CLI Surface

Create a workflow from a JSON definition:

```bash
cargo run -p beaverki-cli -- automation workflow create \
  --definition-file docs/examples/morning-news-digest-workflow.json \
  --summary "Fetch headlines, summarize them, hand them to an agent, and notify the owner."
```

Inspect workflows:

```bash
cargo run -p beaverki-cli -- automation workflow list
cargo run -p beaverki-cli -- automation workflow show --workflow-id <workflow-id>
```

Review and activate:

```bash
cargo run -p beaverki-cli -- automation workflow review \
  --workflow-id <workflow-id> \
  --summary "Re-run workflow safety review after edits."

cargo run -p beaverki-cli -- automation workflow activate --workflow-id <workflow-id>
```

Schedule a workflow:

```bash
cargo run -p beaverki-cli -- automation schedule add \
  --workflow-id <workflow-id> \
  --schedule-id morning_digest \
  --cron "0 8 * * *"
```

Recurring schedules accept standard 5-field cron, optional 6- or 7-field forms with leading seconds, and optional timezone prefixes such as `TZ=Europe/Vienna 0 7 * * *`.

Replay a workflow immediately:

```bash
cargo run -p beaverki-cli -- automation workflow replay --workflow-id <workflow-id>
```

## Agent Tooling

The primary agent now has workflow authoring and operations tools:

- `workflow_list`
- `workflow_get`
- `workflow_write`
- `workflow_activate`
- `workflow_schedule`
- `workflow_replay`
- `workflow_run_list`
- `workflow_run_get`

That lets an agent:

- inspect existing workflows
- create or update a reviewed workflow definition
- activate it after approval
- schedule it after approval
- replay it immediately for testing
- inspect prior workflow runs, artifacts, final results, and block reasons

Current behavior:

- reusing an existing `workflow_id` with `workflow_write` creates a new version
- `workflow_get` returns current stages, reviews, schedules, runs, and version history
- `workflow_run_get` returns persisted run artifacts and failure context for debugging

## Example Definition

See [docs/examples/morning-news-digest-workflow.json](./examples/morning-news-digest-workflow.json) for a minimal end-to-end workflow definition.
