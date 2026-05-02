# BeaverKi Delivery Plan

## 1. Purpose

This document is the living implementation plan for BeaverKi until the repository is scaffolded enough that work can be managed primarily through GitHub issues.

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
- When a milestone changes user-visible behavior or operator workflows, update or add user-facing documentation as part of that milestone when appropriate.
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

- `M1.5-001` Add a long-lived daemon mode for BeaverKi with clean startup and shutdown.
- `M1.5-002` Add local runtime management commands to the CLI.
- `M1.5-003` Add a local IPC surface such as a Unix socket or named pipe for task submission and status inspection.
- `M1.5-004` Move task execution behind the daemon while preserving the current CLI workflow.
- `M1.5-005` Add a background worker loop for queued tasks.
- `M1.5-006` Add persistent runtime status, health, and audit events for daemon lifecycle.
- `M1.5-007` Add a minimal heartbeat loop scaffold without autonomous planning enabled by default.
- `M1.5-008` Add tests for daemon startup, shutdown, IPC submission, and persistence across restart.

Exit criteria:

- BeaverKi can run as a long-lived local process
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
- `M2-003` Implement Discord identity mapping to BeaverKi users.
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

Harden approvals for non-technical users across chat-facing control surfaces so BeaverKi can safely request and resolve sensitive actions without depending on CLI-only workflows or brittle free-form chat commands.

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

- Lua automation is embedded through `mlua` with BeaverKi host APIs for memory access, tool calls, notifications, and deferred wake-ups.
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

## 3.8 M4.5 Session-Scoped Conversation Context

Goal:

Introduce first-class conversation sessions so BeaverKi separates transcript continuity from durable memory and task audit history.

Status:

- implemented in the repository

Implementation notes:

- add a first-class `conversation_sessions` model and attach every new interactive or scheduled task to one session
- session kinds should start with `direct_message`, `group_room`, `cli`, and `cron_run`
- direct-message sessions should be keyed to the mapped BeaverKi user and shared across DM entrypoints for that user rather than tied to a single raw DM channel identifier
- group or room sessions should be keyed to the shared connector target, such as connector plus workspace plus channel or thread, and should carry audience policy plus a maximum memory scope cap
- the effective memory scope for a session should be the intersection of the acting user's visible scopes and the session's maximum memory scope
- cron jobs should create a fresh session per run and should not inherit transcript continuity from prior runs
- user-facing session reset commands such as `/new` should be intercepted by the intake layer and handled directly by the runtime rather than passed through the agent as tool calls or normal objectives
- clearing conversation history should reset or archive the session transcript window without deleting task audit rows or durable semantic memory
- session rows should carry lifecycle-ready state such as `last_activity_at`, `last_reset_at`, `archived_at`, and a coarse lifecycle reason, but they should not yet embed configurable cleanup policy; `M4.6` adds policy definition and operator controls on top of that state
- the first slice should reconstruct transcript continuity from tasks linked to a session and bounded by the session's active transcript window, rather than introducing a second transcript-copy store up front
- session resolution and reset logic should live in shared runtime helpers used by CLI intake, connector intake, scheduled runs, deferred follow-up routing, and approval resume so `M4.6` can reuse the same reset or archive primitive for automatic cleanup
- to simplify rollout, the migration may drop current task-derived conversation continuity instead of backfilling it; legacy tasks can remain as audit records without participating in future transcript reconstruction
- the first implementation slice should resolve sessions for CLI, Discord DMs, Discord channels, and scheduled automation runs
- a practical delivery order is: add schema and indexes, add shared session resolution helpers, wire intake paths, switch prompt assembly and resume paths to `session_id`, then add runtime-handled reset flows and end-to-end tests

Out of scope:

- transcript summarization or compression beyond bounded recent-session retrieval
- richer room membership policy expressions beyond the initial audience and max-scope caps
- web or connector-native history management UIs beyond the existing administrative surfaces

Stories:

- `M4.5-001` Add persistent conversation-session storage and task linkage, including session kind, stable session key, audience policy, maximum memory scope, lifecycle-ready timestamps and markers, and originating connector metadata.
- `M4.5-002` Resolve or create sessions during task intake for CLI, direct-message, group-room, and cron-run paths, with direct-message continuity tied to BeaverKi user identity rather than a raw channel ID.
- `M4.5-003` Replace owner-wide recent-task prompt assembly with session-scoped transcript retrieval and follow-up handling for prompt construction, completion routing, and approval resume paths, carrying persisted `session_id` forward instead of re-resolving continuity from owner lookup.
- `M4.5-004` Add runtime-intercepted session reset flows, including a user-facing `/new` command, so operators can clear conversation history without forgetting durable memory or deleting audit and task rows, using the same reset or archive primitive that later lifecycle automation will call.
- `M4.5-005` Enforce session audience and maximum-scope caps so shared rooms can be configured as household-level or guest-level contexts independent of the initiating user's broader personal entitlements.
- `M4.5-006` Add audit coverage and end-to-end tests for session creation, transcript reset, DM continuity, group isolation, cron freshness, and the no-backfill legacy-history migration path.

Exit criteria:

- follow-up continuity comes from explicit session-scoped transcript state rather than owner-wide task lookup
- direct-message conversations keep continuity across DM entrypoints for the same mapped BeaverKi user
- group or room conversations have isolated shared context with explicit household-versus-guest policy
- cron jobs always start with fresh transcript state per run
- operators can clear conversation history through a runtime-handled `/new` style command without affecting semantic memory or deleting audit records
- existing legacy task history may remain in the database but is not required for transcript continuity after the migration

## 3.9 M4.6 Session Management

Goal:

Add policy-driven lifecycle management for conversation sessions after first-class session scoping is in place.

Status:

- implemented in the repository

Implementation notes:

- session lifecycle rules should remain runtime-managed rather than agent-managed
- automatic clearing or archival should be configurable by session kind, connector context, and inactivity thresholds
- scheduled cleanup should call the same reset or archive primitive introduced in `M4.5` rather than adding a second cleanup mechanism
- scheduled cleanup should reset transcript continuity without deleting audit rows or durable memory
- the first implementation should favor simple deterministic policies such as inactivity TTLs or fixed cleanup windows over semantic heuristics
- commands to view and manage sessions via CLI should be added so operators can inspect active sessions, their last activity time, and manually trigger resets or archival when needed
- manual reset through `/new` remains part of M4.5; this milestone adds automatic policies and operator controls around them

Out of scope:

- LLM-driven decisions about whether a session should be reset
- rich end-user UI for browsing historical sessions beyond existing CLI and connector control surfaces

Stories:

- `M4.6-001` Add configurable session lifecycle policies, including inactivity-based reset or archive thresholds by session kind.
- `M4.6-002` Implement scheduled runtime cleanup that automatically resets or archives eligible sessions without deleting tasks, audits, or durable memory.
- `M4.6-003` Add operator-facing inspection and policy controls for session state, including visibility into last activity, reset markers, and lifecycle reason.
- `M4.6-004` Add audit coverage and end-to-end tests for timed session clearing, policy exemptions, repeated cleanup idempotency, and separation between transcript reset versus memory forgetting.
- `M4.6-005` Add CLI commands to list active sessions per user, inspect their state and activity, and manually trigger resets or archival when needed.

Exit criteria:

- sessions can be automatically reset or archived at configured time points or inactivity thresholds
- automatic cleanup is deterministic, auditable, and does not delete durable memory or task history
- operators can view active sessions, their last activity time, and lifecycle state through CLI commands
- operators can manually trigger session resets or archival through CLI commands when needed
- operators can inspect why a session was reset or archived and which lifecycle policy applied

## 3.10 M4.7 Lua-Defined Tools

Goal:

Add reviewed, activatable Lua-defined agent tools on top of the existing Lua automation runtime without yet introducing unrestricted new file or network host APIs.

Status:

- implemented in the repository

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

- `M4.7-001` Add persistent storage for database-backed Lua-defined tools, including source text, ownership, input schema, output schema, capability profile, lifecycle status, and originating task metadata.
- `M4.7-002` Load active filesystem-packaged and database-backed Lua-defined tools through a shared runtime registration path so the agent can discover and invoke them like built-in tools.
- `M4.7-003` Execute Lua-defined tools through the reviewed Lua runtime with Rust-controlled host APIs, initially limited to safe composition of existing built-in tools and already-approved Lua host functions.
- `M4.7-004` Add blocking safety review and activation gating for newly created or modified database-backed Lua-defined tools, preserving actionable review findings and denial states.
- `M4.7-005` Enforce schema validation, capability-profile checks, ownership rules, and fail-closed behavior when a Lua-defined tool requests disallowed operations.
- `M4.7-006` Add audit coverage and end-to-end tests for tool registration, invocation, denial paths, review and activation flow, and parity between filesystem-packaged versus database-backed tool loading.

Exit criteria:

- a reviewed active Lua-defined tool can be discovered and invoked by the agent through the normal tool interface
- user-local Lua-defined tools are database-backed and carry ownership, review, activation, and audit metadata
- packaged Lua-defined tools can load from disk without being treated as mutable runtime state
- Lua-defined tools remain constrained to Rust-controlled capabilities and fail closed on disallowed operations
- schema validation, safety review, and auditability work end to end for both storage modes

## 3.11 M5 Household Direct Delivery

Goal:

Add first-class immediate household delivery so one user's agent can send a message directly to another mapped household user through the right channels.

Status:

- implemented in the repository

Implementation notes:

- the agent should have a clear direct-delivery capability for requests such as "tell Alice dinner is ready" so immediate household coordination is a normal conversational behavior rather than a scheduled-work workaround
- direct delivery should capture both the requesting user and the intended recipient user as distinct identities
- delivery should be connector-agnostic, using the recipient's mapped connector identity or configured local fallback channel
- recipient resolution should fail safely for ambiguous names, unmapped users, or disallowed cross-user routes, prompting correct agent responses
- cross-user direct delivery must remain policy-gated and auditable so BeaverKi can explain who asked for the message, who received it, and why the delivery was allowed
- direct-send retries should deduplicate across restart and retry paths so a recovered send does not spam the recipient
- this milestone should establish the base household-delivery data model and routing abstractions that deferred reminders and recurring delivery will later reuse
- the current implementation adds durable `household_deliveries` rows, an agent-facing `household_send_message` tool, runtime-backed immediate delivery, and Discord DM-first routing with fallback to the recipient's mapped channel when DM creation is unavailable
- the current policy slice allows direct household delivery for `owner`, `adult`, and `service` roles and denies untrusted roles such as `child`

Stories:

- `M5-001` Add persistent household-delivery records that distinguish requester identity, recipient user, payload, delivery mode, delivery target, and lifecycle state for immediate cross-user sends.
- `M5-002` Implement agent-facing direct household messaging flows that can turn natural-language requests into an immediate targeted delivery to another allowed household user.
- `M5-003` Add connector-agnostic targeted delivery routing that resolves the recipient's preferred mapped identity and fallback channel at send time.
- `M5-004` Enforce RBAC and policy checks for direct cross-user delivery, including denial cases for untrusted roles, ambiguous recipients, or unmapped recipients.
- `M5-005` Add audit coverage and end-to-end tests for direct send delivery, retry-safe deduplication, routing decisions, and recipient-versus-requester attribution.

Exit criteria:

- the workflow is a natural part of the agent's understanding so a user can ask their agent to notify another household member now without needing to know about connectors, channels, or technical details
- the agent can send an immediate direct message to another allowed mapped household user without routing it through a reminder or cron abstraction
- the direct delivery survives retry or restart and is delivered once through the selected connector or fallback path
- cross-user direct delivery is policy-gated and fully auditable
- the runtime has a reusable household-delivery foundation that later deferred reminder and schedule work can build on

## 3.12 M5.5 Household Reminders And Scheduled Delivery

Goal:

Build first-class deferred and recurring household delivery on top of the direct-delivery foundation so the agent can reliably create, manage, and explain reminders and cron-style scheduled messaging.

Status:

- implemented in the repository

Implementation notes:

- reminder and cron functionality should be promoted from a runtime capability into explicit agent-usable product behavior, with first-class tool flows for creating, listing, inspecting, updating, and cancelling scheduled deliveries
- reminder and cron functionality should build on the scheduled workflow pipeline foundation from `M4.9` rather than introducing a reminder-only orchestration path
- reminder creation should capture both the requesting user and the intended recipient user as distinct identities and extend the delivery model with timing, recurrence, and scheduled-job lifecycle state
- natural-language requests such as "remind my girlfriend to buy kiwis when she goes to the shop this afternoon" should be normalized into structured scheduled delivery intent with message, timing window or recurrence, and delivery policy
- one-shot deferred reminders and recurring household delivery schedules should reuse the same durable scheduling and wake-up machinery rather than splitting reminder logic from cron-like execution paths
- scheduled delivery should expose enough structured state back to the agent that it can confirm what was scheduled, explain when it will fire, and repair or replace stale schedules instead of silently failing to use the reminder or cron subsystem
- deferred delivery should remain connector-agnostic and should re-evaluate routing against the recipient's mapped identity or fallback channel at send time when appropriate
- reminder and scheduled-delivery execution should deduplicate across restart and retry paths so a missed wake-up replay does not spam the recipient
- policy checks should cover both schedule creation time and eventual delivery time so BeaverKi can fail closed if roles, mappings, or trust boundaries change before send

Stories:

- `M5.5-001` Extend household-delivery records with timing metadata, recurrence rules, delivery windows, scheduled-job state, and completion or failure tracking for deferred sends.
- `M5.5-002` Implement agent-facing reminder and scheduling flows that can turn natural-language household coordination requests into structured deferred delivery work, including enough tool feedback that the agent can reliably use reminder or cron-style scheduling in normal conversation.
- `M5.5-003` Add agent-facing schedule management flows to list, inspect, update, and cancel pending reminder or recurring household delivery jobs so scheduling becomes a usable control surface rather than a write-only backend mechanism.
- `M5.5-004` Extend the scheduler and wake-up pipeline to materialize one-shot reminder deliveries and recurring household delivery jobs alongside recurring Lua schedules, with shared persistence, retry, and recovery semantics.
- `M5.5-005` Enforce RBAC and policy checks for reminder creation, recurring delivery, and send-time revalidation, including denial cases when recipients become unmapped or disallowed before execution.
- `M5.5-006` Add audit coverage and end-to-end tests for reminder creation, recurring schedule management, restart-safe delivery, deduplication, agent-visible schedule confirmation, and recipient-versus-requester attribution.

Exit criteria:

- the workflow is a natural part of the agent's understanding so a user can ask their agent to schedule a later reminder or recurring household delivery without needing to know about connectors, channels, cron syntax, or technical details
- an agent can create, inspect, update, and cancel deferred reminders or recurring household delivery schedules addressed to another mapped household user
- reminder and cron-style delivery paths are reliable enough that the agent can confirm what was scheduled, when it will fire, and whether it later succeeded
- deferred delivery survives restart and is delivered once through the selected connector or fallback path
- cross-user deferred delivery is policy-gated and fully auditable
- natural-language reminder and scheduling requests are normalized into structured scheduled work rather than handled as ad hoc connector replies

## 3.13 M6 Scheduled Workflow Pipelines

Goal:

Extend the scheduler from single-target Lua execution into reviewed durable workflow pipelines so cron and reminder features can combine multiple Lua stages with later bounded agent interaction.

Example use case:
- A user asks their agent to "fetch news from orf.at and give me a news digest every morning at 8am"

Status:

- proposed after M5.5

Why this stage exists:

- the current runtime already has durable schedules, reviewed Lua scripts, Lua-defined tools, fresh cron-run sessions, and agent execution, but those pieces are not yet composable under one scheduled run
- today a due schedule materializes only a `lua_script` target and the follow-up path is limited to another deferred Lua run rather than an explicit workflow handoff
- many useful reminders, digests, maintenance tasks, and coordination flows need a scripted preparation phase before an agent interprets results or decides the next action

Implementation notes:

- keep the current single-script schedule path as the simple baseline, but make `target_type` genuinely extensible by supporting workflow targets in the runtime
- add a reviewed workflow definition model with ordered stages so the runtime, not an opaque Lua script chain, owns stage boundaries, state handoff, retries, and auditability
- initial stage kinds should be `lua_script`, `lua_tool`, `agent_task`, and `user_notify`
- workflow runs should persist owner identity, initiating identity, triggering schedule, current stage, stage outputs, carried artifacts, wake-up state, and fail-closed block reasons across restart
- agent stages should receive an explicit bounded task slice built from prior workflow outputs rather than inheriting broad hidden context, and should support a stored stage-specific prompt contract plus allowed tool or Lua-artifact references
- changing a workflow definition should go through blocking safety review and activation gating even when the referenced Lua artifacts were already reviewed individually
- scheduled workflow runs should not enter interactive approval waits; if the workflow and its referenced stages are approved and active, the run should execute autonomously, and if a stage exceeds that approved envelope at runtime the run should fail closed or block with audit visibility
- workflow-level user notification should be a first-class stage so a scheduled run can actually surface its result to a human; that delivery stage should align with and later reuse the M5 household-delivery routing and persistence foundation rather than creating a second notification subsystem
- later reminder and scheduled-delivery work should build on this workflow foundation instead of adding a second bespoke cron orchestration path

Out of scope:

- unrestricted arbitrary workflow graphs or general-purpose branching languages in the first slice
- opaque script-to-script dispatch where the runtime cannot see stage boundaries
- connector-specific scheduled UX beyond what is needed to prove the runtime workflow foundation; the first `user_notify` slice may stay minimal as long as it is aligned with the later M5 delivery model

Stories:

- `M6-001` Add persistent workflow-definition storage with ownership, ordered stages, review metadata, activation state, and originating task linkage.
- `M6-002` Extend schedule materialization so `target_type=workflow` creates durable workflow-run state instead of assuming every due schedule maps directly to one Lua script task.
- `M6-003` Implement initial workflow stages for reviewed Lua script execution, reviewed Lua-defined tool execution, bounded agent-task handoff with explicit task-slice construction and output capture, and a minimal connector-agnostic `user_notify` stage for surfacing workflow results.
- `M6-004` Persist workflow-run progression, defer or wake-up behavior, retry state, fail-closed block reasons, and per-stage artifacts so scheduled pipelines survive restart without losing where they were.
- `M6-005` Add CLI and agent-facing workflow creation, activation, scheduling, inspection, and replay flows so staged automation becomes operable rather than an internal-only runtime abstraction.
- `M6-006` Add safety-review coverage, RBAC enforcement, audit events, and end-to-end tests for workflow activation, autonomous scheduled execution inside the approved envelope, fail-closed behavior when a stage exceeds that envelope, result delivery through `user_notify`, and compatibility with existing single-script schedules.

Exit criteria:

- a scheduled trigger can run one or more reviewed Lua stages and then hand off to a bounded agent stage within one durable auditable pipeline
- simple existing single-script schedules still work without forced migration
- each stage has explicit input or output state, retry behavior, and audit visibility
- scheduled workflow runs execute autonomously once the workflow and its referenced parts are approved, and they fail closed rather than pausing for fresh approval
- a workflow can surface its result to a user through a first-class notification or delivery stage that can later converge with the M5 delivery foundation
- later reminder and recurring household-delivery work can reuse the same workflow-run machinery instead of introducing a separate scheduled orchestration model

## 3.14 Post-V1

Candidate follow-up work:

- Further context-window optimization beyond the current summary-overlay and token-budget inspection implementation
- Additional model providers:
  - Anthropic
  - Azure OpenAI
  - Local LLM via LM Studio
  - OpenAI ChatGPT/Codex
- WhatsApp and Telegram connectors
- Local web UI
- Richer policy expressions beyond RBAC
- Clear separation between personal life and professional life for users who want to use BeaverKi in both contexts without commingling data, tools, or connectors
- Signed skills and trust metadata
- Pre-packaged skills:
  - Personal productivity (calendars, email, to-do)
  - Household management (shared calendars, shopping lists, reminders)
- Skills marketplace and external distribution

## 4. Migration To GitHub Issues

When the workspace is scaffolded enough to support normal development flow:

1. create one GitHub issue per story
2. link each issue to the relevant milestone
3. copy acceptance criteria from this document into the issue
4. keep this file as a milestone overview, not the active task tracker
