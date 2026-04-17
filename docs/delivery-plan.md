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

Status:

- implemented in the repository

Stories:

- `M2-001` Implement browser tooling with interactive and headless modes.
- `M2-002` Implement Discord connector configuration and token handling.
- `M2-003` Implement Discord identity mapping to BeaverKI users.
- `M2-004` Support command intake from DMs and allowlisted channels.
- `M2-005` Support approval interactions through Discord.
- `M2-006` Add audit coverage for connector events and approval responses.
- `M2-007` Add end-to-end tests for Discord task submission and approval routing.
- `M2-008` Add durable connector follow-up delivery so long-running connector tasks can acknowledge immediately and publish later state transitions through a connector-agnostic dispatcher.

Exit criteria:

- a mapped Discord user can trigger tasks
- approvals can be handled through Discord where policy allows
- connector-originated tasks that outlive the initial wait window still produce a later remote follow-up
- browser automation works in desktop and VPS headless modes

## 3.5 M2.5 Approval UX And Remote Safety

Goal:

Harden approvals for non-technical users across chat-facing control surfaces so BeaverKI can safely request and resolve sensitive actions without depending on CLI-only workflows or brittle free-form chat commands.

Status:

- implemented in the repository

Why this stage exists:

- the repository already supports CLI approvals and a basic Discord approval flow
- the current remote pattern is functional but too technical for mainstream users
- approval UX is a security boundary, so it should be tightened before broader connector and automation expansion relies on it

Implementation notes:

- remote approvals now use single-use opaque approval action tokens rather than raw approval IDs over Discord
- Discord approvals are DM-first by default, while channel-originated requests that need approval are redirected to the DM inbox flow
- critical remote approvals require a second confirmation token before the task is resumed
- the CLI approval path remains available as the fallback administrative surface

Stories:

- `M2.5-001` Introduce a first-class approval presentation model with concise human-readable action summaries, risk labels, requester identity, and target details.
- `M2.5-002` Add connector-safe approval actions using single-use opaque approval tokens or signed interaction payloads instead of relying on free-form text parsing alone.
- `M2.5-003` Add an approval inbox surface over supported connectors so users can list, inspect, approve, or deny pending requests without switching to the CLI.
- `M2.5-004` Restrict remote approvals to trusted contexts by default, such as mapped identities, DMs, or explicitly allowlisted channels, with replay protection and expiry.
- `M2.5-005` Add step-up confirmation for higher-risk approvals, such as requiring a second confirmation gesture, short-lived confirmation code, or local-device confirmation policy.
- `M2.5-006` Distinguish approval authority from task-submission authority in connector UX so a user cannot silently self-elevate through a loosely trusted chat path.
- `M2.5-007` Add audit coverage for approval prompt rendering, token issuance, expiry, denial, resolution channel, and connector identity used for the decision.
- `M2.5-008` Add end-to-end tests for approval flows over connectors, including replay attempts, stale approvals, mismatched identities, and ambiguous user replies.

Exit criteria:

- non-technical users can safely approve or deny pending actions from a chat interface without memorizing internal IDs
- remote approvals are bound to authenticated mapped identities and trusted channels
- approval interactions are resistant to replay, ambiguity, and accidental approval from plain conversational text
- the CLI remains available as the administrative fallback path

## 3.6 M3 Automation

Goal:

Add automation and programmable behavior on top of the stable runtime core.

Status:

- implemented in the repository

Implementation notes:

- Lua automation is embedded through `mlua` with BeaverKI host APIs for memory access, tool calls, notifications, and deferred wake-ups.
- Lua scripts are stored with review metadata, blocking safety review, activation gating, and recurring schedule support.
- Safety review now preserves non-approved verdicts such as `needs_changes` while still preventing activation or execution.
- Risky shell commands generated by the agent go through safety review and approval flow; Lua tool calls fail closed outside the low-risk shell envelope and emit explicit denial audit/task events.
- Due schedules now either materialize runnable automation tasks or emit `schedule_skipped` audit records and advance to the next run when the target script is missing, inactive, or not safety-approved.
- CLI automation commands expose review findings and required changes so non-approved reviews are actionable from the normal operator workflow.
- Lua-defined agent tools remain out of scope for V1 and stay in the post-V1 backlog.

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

## 3.7 M4 Semantic Memory

Goal:

Add first-class durable semantic memory behavior on top of the multi-user runtime and automation foundation.

Status:

- implemented in the repository

Stories:

- `M4-001` Implement first-class semantic memory promotion so a primary agent can decide that stable facts should be remembered and persist them into the requesting user's durable private brain/memory by default.
- `M4-002` Add role-aware household semantic memory writes so higher-trust users can promote shared facts into household brain/memory, with explicit policy enforcement for who may write and who may later retrieve that scope.
- `M4-003` Distinguish semantic memory from episodic task summaries by adding memory classification, deduplication, correction, and source attribution for user facts such as names, preferences, identities, and durable household facts.
- `M4-004` Add audit coverage and end-to-end tests for agent-driven remembering, private-versus-household promotion, correction flows, and denial cases proving that guests cannot retrieve household brain/memory.

Exit criteria:

- the primary agent can automatically persist durable user facts into per-user private brain/memory when they should be remembered
- higher-trust household users can store shared durable facts into household brain/memory under policy control
- semantic memory is distinguished from episodic task summaries with classification, deduplication, correction, and source attribution
- guests cannot retrieve household brain/memory
- semantic memory behavior is auditable end to end

## 3.8 M4.5 Lua-Defined Tools

Goal:

Add reviewed, activatable Lua-defined agent tools on top of the existing Lua automation runtime without yet introducing unrestricted new file or network host APIs.

Status:

- proposed as the next milestone after M4

Implementation notes:

- user-authored or agent-authored Lua-defined tools should be stored in the runtime database with ownership, review metadata, capability profile, activation state, and audit history
- skill-packaged or repo-packaged Lua-defined tools should remain filesystem assets and load through the same runtime tool interface as database-backed local tools
- the first iteration should keep Lua-defined tools constrained to Rust-controlled host APIs and composition of existing built-in tools rather than granting raw `fs.*` or `net.*` primitives
- every active Lua-defined tool should surface as a normal tool definition to the primary agent, with schemas validated through the same tool-definition contract used for built-in tools

Out of scope:

- unrestricted Lua filesystem or network host APIs
- signed skill distribution or external trust metadata
- connector-specific packaging or marketplace installation flows
- broader policy expressions beyond current RBAC and capability-profile enforcement

Stories:

- `M4.5-001` Add persistent storage for database-backed Lua-defined tools, including source text, ownership, input schema, output schema, capability profile, lifecycle status, and originating task metadata.
- `M4.5-002` Load active filesystem-packaged and database-backed Lua-defined tools through a shared runtime registration path so the agent can discover and invoke them like built-in tools.
- `M4.5-003` Execute Lua-defined tools through the reviewed Lua runtime with Rust-controlled host APIs, initially limited to safe composition of existing built-in tools and already-approved Lua host functions.
- `M4.5-004` Add blocking safety review and activation gating for newly created or modified database-backed Lua-defined tools, preserving actionable review findings and denial states.
- `M4.5-005` Enforce schema validation, capability-profile checks, ownership rules, and fail-closed behavior when a Lua-defined tool requests disallowed operations.
- `M4.5-006` Add audit coverage and end-to-end tests for tool registration, invocation, denial paths, review and activation flow, and parity between filesystem-packaged versus database-backed tool loading.

Exit criteria:

- a reviewed active Lua-defined tool can be discovered and invoked by the agent through the normal tool interface
- user-local Lua-defined tools are database-backed and carry ownership, review, activation, and audit metadata
- packaged Lua-defined tools can load from disk without being treated as mutable runtime state
- Lua-defined tools remain constrained to Rust-controlled capabilities and fail closed on disallowed operations
- schema validation, safety review, and auditability work end to end for both storage modes

## 3.9 M5 Household Delivery And Reminders

Goal:

Add first-class targeted household reminders and notifications so one user's agent can understand a request, schedule the follow-up work, and deliver it to another mapped household user through the right channel.

Status:

- proposed after M4.5

Implementation notes:

- reminder creation should capture both the requesting user and the intended recipient user as distinct identities
- natural-language requests such as "remind my girlfriend to buy kiwis when she goes to the shop this afternoon" should be normalized into structured reminder state with message, timing window, and delivery policy
- one-shot deferred reminders should reuse the durable scheduling and wake-up machinery rather than introducing a separate best-effort timer path
- delivery should be connector-agnostic, using the recipient's mapped connector identity or configured local fallback channel
- cross-user delivery must remain policy-gated and auditable so BeaverKI can explain who asked for the reminder, who received it, and why the delivery was allowed
- reminders should deduplicate delivery across restart and retry paths so a missed wake-up replay does not spam the recipient

Stories:

- `M5-001` Add persistent reminder records that distinguish requester identity, recipient user, payload, delivery target, timing metadata, and lifecycle state.
- `M5-002` Implement agent-facing reminder creation flows that can turn natural-language household coordination requests into structured deferred delivery work.
- `M5-003` Extend the scheduler and wake-up pipeline to materialize one-shot reminder deliveries in addition to recurring Lua schedules.
- `M5-004` Add connector-agnostic targeted delivery routing that resolves the recipient's preferred mapped identity and fallback channel at send time.
- `M5-005` Enforce RBAC and policy checks for cross-user reminder creation and delivery, including denial cases for untrusted roles or unmapped recipients.
- `M5-006` Add audit coverage and end-to-end tests for reminder creation, restart-safe delivery, deduplication, and recipient-versus-requester attribution.

Exit criteria:

- an agent can create a deferred reminder addressed to another mapped household user
- the reminder survives restart and is delivered once through the selected connector or fallback path
- cross-user reminder delivery is policy-gated and fully auditable
- natural-language reminder requests are normalized into structured scheduled work rather than handled as ad hoc connector replies

## 3.10 Post-V1

Candidate follow-up work:

- Better context management and retrieval optimization (including status for context windows and token usage per user)
- additional model providers
- WhatsApp and Telegram connectors
- local web UI
- richer policy expressions beyond RBAC
- signed skills and trust metadata

## 4. Migration To GitHub Issues

When the workspace is scaffolded enough to support normal development flow:

1. create one GitHub issue per story
2. link each issue to the relevant milestone
3. copy acceptance criteria from this document into the issue
4. keep this file as a milestone overview, not the active task tracker
