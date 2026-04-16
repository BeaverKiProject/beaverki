# BeaverKI Delivery Plan

## 1. Purpose

This document is the living implementation plan for BeaverKI until the repository is scaffolded enough that work can be managed primarily through GitHub issues.

It exists to:

- define staged delivery milestones
- list concrete implementation stories
- keep near-term sequencing explicit
- avoid overloading the technical spec with project-management detail

Once the workspace, CI, and initial runtime scaffolding exist, these stories should be turned into GitHub issues and maintained there.

## 2. Planning Rules

- Keep milestones small enough to ship incrementally.
- Prefer end-to-end slices over subsystem stubs with no usable path.
- Do not start a later milestone if the earlier one is still architecturally unstable.
- Treat this document as the pre-issue backlog, not as the long-term source of truth once GitHub issue management is active.

## 3. Milestones

## 3.1 M0 Foundation

Goal:

Ship a minimal single-user CLI runtime that can execute the core agent loop with SQLite-backed state, OpenAI as provider, and shell/filesystem tools.

Status:

- implemented in the repository

Out of scope:

- multi-user support
- Discord
- browser automation
- Lua runtime
- safety agent for scripts

Stories:

- `M0-001` Create the Rust workspace and crate skeleton matching the technical spec.
- `M0-002` Add a `README`-backed local developer command surface such as `Makefile` or `justfile`.
- `M0-003` Add a minimal CI workflow for formatting, linting, and tests.
- `M0-004` Implement config loading for `runtime.yaml` and `providers.yaml`.
- `M0-005` Implement setup assistant support for choosing OpenAI and storing credentials securely.
- `M0-006` Add SQLite migrations and database initialization.
- `M0-007` Implement the core task, audit, and memory tables needed for M0.
- `M0-008` Implement scope-aware memory retrieval APIs even though M0 is single-user, so later multi-user work is not retrofitted.
- `M0-009` Implement the OpenAI provider adapter with API-token authentication.
- `M0-010` Implement the primary-agent execution loop for a single user.
- `M0-011` Implement shell tool execution with risk classification.
- `M0-012` Implement filesystem read, write, and search tools.
- `M0-013` Persist task execution, tool invocations, and audit events.
- `M0-014` Add a CLI flow to submit a task and inspect its result.

Exit criteria:

- a user can configure OpenAI
- a user can run the CLI locally
- the runtime can execute a simple task using shell/filesystem tools
- state is persisted in SQLite
- CI passes

## 3.2 M1 Core Multi-User

Goal:

Extend the core runtime to support multiple users, RBAC, private plus household memory, approvals, and bounded sub-agent execution on the existing CLI-driven execution model.

Status:

- implemented in the repository

Stories:

- `M1-001` Add users, roles, and connector identity abstractions to the data model.
- `M1-002` Implement built-in roles: `owner`, `adult`, `child`, `guest`, `service`.
- `M1-003` Implement per-user primary agents.
- `M1-004` Implement private and household memory scopes.
- `M1-005` Enforce retrieval-time scope filtering for all prompt assembly paths.
- `M1-006` Implement approval records and CLI approval flows.
- `M1-007` Implement sub-agent spawning with minimal task-slice handoff.
- `M1-008` Add audit coverage for scope resolution, approvals, and sub-agent creation.
- `M1-009` Add integration tests for cross-user isolation and denial cases.

Exit criteria:

- multiple users can exist in one runtime
- primary agents cannot retrieve or leak another user's private memory
- approvals work through CLI
- sub-agents operate on explicit bounded context only

## 3.3 M1.5 Runtime Daemon

Goal:

Introduce the always-running local runtime process that later connector and automation work will build on.

Status:

- implemented in the repository

Stories:

- `M1.5-001` Add a long-lived daemon mode for BeaverKI with clean startup and shutdown.
- `M1.5-002` Add local runtime management commands to the CLI.
- `M1.5-003` Add a local IPC surface such as a Unix socket or named pipe for task submission and status inspection.
- `M1.5-004` Move task execution behind the daemon while preserving the current CLI workflow.
- `M1.5-005` Add a background worker loop for queued tasks.
- `M1.5-006` Add persistent runtime status, health, and audit events for daemon lifecycle.
- `M1.5-007` Add a minimal heartbeat loop scaffold without autonomous planning enabled by default.
- `M1.5-008` Add tests for daemon startup, shutdown, IPC submission, and persistence across restart.

Exit criteria:

- BeaverKI can run as a long-lived local process
- CLI task submission can target the daemon instead of running inline
- the runtime survives restart without losing persisted tasks and state
- the foundation exists for connectors, scheduling, and background work

## 3.4 M2 Integrations

Goal:

Add the first external control surface and remote execution environment support on top of the daemonized runtime.

Stories:

- `M2-001` Implement browser tooling with interactive and headless modes.
- `M2-002` Implement Discord connector configuration and token handling.
- `M2-003` Implement Discord identity mapping to BeaverKI users.
- `M2-004` Support command intake from DMs and allowlisted channels.
- `M2-005` Support approval interactions through Discord.
- `M2-006` Add audit coverage for connector events and approval responses.
- `M2-007` Add end-to-end tests for Discord task submission and approval routing.

Exit criteria:

- a mapped Discord user can trigger tasks
- approvals can be handled through Discord where policy allows
- browser automation works in desktop and VPS headless modes

## 3.5 M3 Automation

Goal:

Add automation and programmable behavior on top of the stable runtime core.

Stories:

- `M3-001` Implement the safety-agent review path.
- `M3-002` Implement Lua runtime embedding via `mlua`.
- `M3-003` Implement script storage and metadata.
- `M3-004` Implement blocking review for Lua scripts.
- `M3-005` Implement safety review for medium/high/critical generated shell commands.
- `M3-006` Implement schedule persistence and recurring execution.
- `M3-007` Implement CLI flows for creating, reviewing, and enabling automations.
- `M3-008` Add audit and replay coverage for script creation and execution.

Exit criteria:

- agent-authored Lua scripts can be reviewed, stored, activated, and scheduled
- shell review path is functioning for risky commands
- automation behavior is auditable end to end

## 3.6 Post-V1

Candidate follow-up work:

- Lua-defined tools
- additional model providers
- WhatsApp and Telegram connectors
- local web UI
- richer policy expressions beyond RBAC
- signed skills and trust metadata

## 4. Immediate Next Stories

These are the first stories to execute before moving backlog management fully into GitHub:

1. `M2-001` Implement browser tooling with interactive and headless modes.
2. `M2-002` Implement Discord connector configuration and token handling.
3. `M2-003` Implement Discord identity mapping to BeaverKI users.
4. `M2-004` Support command intake from DMs and allowlisted channels.
5. `M2-005` Support approval interactions through Discord.
6. `M2-006` Add audit coverage for connector events and approval responses.
7. `M2-007` Add end-to-end tests for Discord task submission and approval routing.

## 5. Migration To GitHub Issues

When the workspace is scaffolded enough to support normal development flow:

1. create one GitHub issue per story
2. link each issue to the relevant milestone
3. copy acceptance criteria from this document into the issue
4. keep this file as a milestone overview, not the active task tracker
