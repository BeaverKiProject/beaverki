# BeaverKI Design Document

## 1. Overview

BeaverKI is an open-source autonomous AI agent framework designed to act as a personal, always-on digital assistant that runs on the user's own machine or server. Unlike passive chat interfaces, BeaverKI performs actions in the local environment: it can execute terminal commands, read and write files, control browsers, schedule background work, and interact with messaging platforms such as Telegram, Slack, Discord, and WhatsApp through modular integrations.

The framework is local-first, model-agnostic, and security-conscious. It is intended to run on macOS, Windows, Linux, and VPS environments, with configuration and history stored locally in transparent, user-editable Markdown and YAML files. The platform is implemented in Rust for safety, portability, performance, and strong systems-level control.

The core idea is to provide a durable agent runtime with:

- autonomous task execution
- structured memory and local auditability
- user-controlled permissions and role-based access
- pluggable skills and integrations
- optional Lua scripting for advanced workflows
- support for multiple LLM providers and local models

This document describes the initial product and technical design before implementation.

## 1.1 Current Product Decisions

The following product-shaping decisions are now assumed for v1:

- The runtime must support multiple human users, such as members of a household, sharing one BeaverKI installation.
- The product should target both desktop and headless VPS operation from the start.
- The default operating mode should be bounded autonomy, with policy-defined limits rather than manual approval for every step.
- Messaging integrations are first-class remote control surfaces, not only notification channels.
- Connector priority is Discord first, then Telegram, then WhatsApp, then Slack.
- Each human user should have a persistent primary agent, with dynamic short-lived sub-agents spawned for specific tasks.
- RBAC is sufficient for v1, including control over who may create or modify skills, scripts, and scheduled automations.
- Agents may propose and write Lua scripts for automation, and may schedule them as recurring jobs when policy allows.
- Agent-authored Lua scripts may become active immediately if they remain within the initiating user's RBAC limits, but every Lua script must be reviewed by a blocking safety agent before execution is considered trusted.
- Discord commands should be accepted in direct messages and in explicitly approved channels.
- VPS deployments should support headless browser automation as a first-class mode in v1.
- V1 may rely on RBAC and strict tool restrictions rather than requiring a full OS-level sandbox layer from day one.
- The first provider implementation should be OpenAI, exposed through a setup-assistant provider selection flow with API-token authentication and an optional Codex-style authenticated account flow when OpenAI is chosen.
- Per-user memory and household memory should be separate first-class scopes.
- Canonical operational memory should be stored in SQLite, while YAML and Markdown remain useful for configuration, manifests, journals, and exports.
- Sub-agents should receive only a minimal task slice prepared by the parent agent.

## 2. Goals

### Product Goals

- Provide a personal AI agent that can operate continuously, not only in response to a prompt.
- Support multiple trusted human users with distinct identities and permissions.
- Let users run the system entirely on their own hardware or VPS.
- Make the system useful for real operational tasks: automation, research, messaging, browser actions, and local workstation assistance.
- Support both direct user requests and proactive background work.
- Enable extension through installable skills without requiring core framework changes.

### Technical Goals

- Build a secure, cross-platform Rust runtime.
- Keep the core framework model-provider agnostic.
- Make all important state auditable and locally inspectable.
- Support safe action execution with explicit permission boundaries.
- Support dynamic agent and sub-agent lifecycles without losing auditability or policy control.
- Allow simple skills for declarative workflows and advanced skills via scripting.
- Separate reasoning, planning, execution, and memory concerns cleanly.

## 3. Non-Goals

- Fully replacing general-purpose operating system security controls.
- Hiding all complexity from advanced users. This system should be inspectable and configurable.
- Providing unrestricted self-modification by default.
- Optimizing first for large-scale enterprise multi-tenant SaaS hosting.
- Supporting arbitrary remote code execution from untrusted third-party skills without sandboxing and review.

## 4. Core Principles

### Local-First

User data, memory, audit logs, configuration, and installed skills should live on the local filesystem by default. Cloud services may be used for model inference or integrations, but the agent's durable state remains user-owned.

### Transparent State

State should be legible. Histories, task records, permissions, and skill metadata should be stored in plain formats where practical. Users should be able to inspect and edit state without proprietary tooling.

### Action With Accountability

The agent should be able to act, but every material action must have policy controls, execution traces, and post-hoc auditability.

### Modular Capability Model

Capabilities should be added through skills and integrations, not through uncontrolled growth of the core runtime.

### Safe Autonomy

Autonomy is useful only if it is bounded. The system should support background operation, but with clear policies, rate limits, escalation rules, and role constraints.

## 5. High-Level Use Cases

- Personal operations assistant: manage reminders, file organization, email and chat triage, and recurring system maintenance.
- Research assistant: watch feeds, gather information, summarize changes, and draft follow-up actions.
- Development assistant: run local tooling, inspect repositories, patch files, and coordinate recurring engineering tasks.
- Messaging automation: monitor approved channels, propose or send responses, and trigger workflows from chat commands.
- Browser operator: log into services, collect data, submit forms, and perform repetitive web tasks.
- Home-lab/VPS steward: monitor services, rotate logs, run health checks, and notify on incidents.
- Shared family assistant: maintain separate user profiles, permissions, reminders, and message-triggered workflows for different household members.

## 6. System Overview

At a high level, BeaverKI consists of the following major subsystems:

1. Runtime Core
2. Agent Orchestrator
3. Policy and Permissions Engine
4. Memory and Local State Store
5. Skill System
6. Tool Execution Layer
7. Scripting Runtime (Lua)
8. Provider Abstraction Layer
9. Background Scheduler / Heartbeat Engine
10. Interfaces and Integrations

## 7. Runtime Architecture

### 7.1 Runtime Core

The runtime core is the long-lived Rust process responsible for:

- loading configuration
- initializing providers, skills, and connectors
- managing agent lifecycles
- scheduling background work
- persisting state
- exposing local APIs or CLI interfaces

Suggested implementation direction:

- async Rust with `tokio`
- trait-based abstractions for providers, tools, skills, and policy evaluators
- event-driven internals with structured messages between subsystems

### 7.2 Agent Orchestrator

The orchestrator manages one or more agents. Each agent has:

- identity
- role or persona
- capabilities
- memory scope
- execution limits
- schedule or heartbeat
- task queue

In v1, each authenticated human user should have one persistent primary agent that owns that user's long-lived conversational context, preferences, and personal task continuity. Task-specific sub-agents should remain ephemeral by default.

The orchestrator coordinates:

- intake of new tasks from users, schedules, or integrations
- planning and decomposition
- tool invocation
- retries and recovery
- long-running task state
- interruption and cancellation

The orchestrator must support dynamic sub-agent creation. A parent agent may spawn a child agent for a bounded purpose such as:

- researching a subproblem
- executing a narrow workflow with a reduced permission set
- handling a separate user conversation context
- maintaining a recurring background task

Spawned agents should inherit only the minimum required context and capabilities. They should have:

- explicit parent linkage
- scoped memory access
- bounded lifetime or termination conditions
- a defined tool and skill budget
- full audit visibility

The parent agent should explicitly prepare the task slice handed to the sub-agent. That slice may include:

- the task objective
- a bounded context summary
- relevant artifacts or references
- the exact permission scope
- completion and reporting expectations

The orchestrator should distinguish between:

- reactive runs: user asks for something now
- scheduled runs: a cron-like or interval trigger fires
- event-driven runs: a message arrives on Discord, Telegram, Slack, or another connector
- autonomous runs: the heartbeat notices pending follow-up work

### 7.3 Execution Loop

An agent execution cycle may look like this:

1. Resolve the execution identity, visible scopes, and policy context for the run.
2. Load current task context and relevant memory with scope constraints enforced at retrieval time.
3. Build model context only from the scoped retrieval result set.
4. Ask an LLM to reason, propose a plan, or decide the next action.
5. Validate the proposed action against policies and allowed tools.
6. Execute the action or require approval if needed.
7. Persist observations, outputs, and state changes.
8. Re-evaluate whether the task is complete, blocked, or needs follow-up.
9. Schedule continuation if background work is appropriate.

This loop should support both single-shot and multi-step durable execution.

### 7.3.1 Prompt Assembly Invariant

In a shared multi-user runtime, scope metadata in storage is not enough by itself. The agent execution contract must require that scope filtering happens before prompt assembly.

That means:

- each run resolves the caller identity and visible scopes first
- memory queries apply those scope constraints directly in the retrieval path
- prompt assembly may only use the records returned from that scoped retrieval
- post-retrieval filtering is not an acceptable primary safety boundary
- sub-agents may only receive the task slice explicitly prepared by the parent agent

Without this invariant, the design would retain a latent cross-user data leakage path.

### 7.4 Multi-User Identity Model

Because one runtime serves multiple people, task execution must carry a clear initiating identity. Every request should resolve to:

- an authenticated human user or approved system identity
- the connector or local interface it arrived through
- the RBAC role set of the caller
- the memory and data scopes visible to that caller
- the approval rules that apply to actions taken on their behalf

This identity context must propagate through sub-agent spawning, scheduled jobs, and deferred tasks so the runtime can answer, at any point, who asked for something and under what authority it is continuing.

## 8. Memory and State Model

### 8.1 Storage Principles

State should be stored locally in a way that remains operationally robust under continuous agent activity. For v1, the best tradeoff is a SQLite-first memory model combined with plain-text configuration and export formats.

Proposed storage split:

- YAML for configuration, policies, provider setup, and skill manifests
- SQLite as the canonical store for memory, execution records, queues, approvals, schedules, and searchable metadata
- Markdown for human-readable journals, summaries, and optional exports
- binary/blob directories for attachments, screenshots, cached downloads, and artifacts

This approach keeps runtime-critical state reliable and queryable while preserving local ownership and human inspectability.

### 8.2 Proposed Directory Layout

```text
beaverki/
  config/
    agent.yaml
    users.yaml
    providers.yaml
    roles.yaml
    permissions.yaml
    schedules.yaml
  memory/
    journal/
      2026-04-16.md
    entities/
      people/
      projects/
      systems/
    tasks/
      task-000123.yaml
    conversations/
      2026/
      users/
    summaries/
  skills/
    builtin/
    installed/
    local/
  scripts/
    lua/
  data/
    state.db
    cache/
    artifacts/
    browser/
  logs/
    runtime.log
    audit.log
```

### 8.3 Memory Categories

- Episodic memory: prior interactions, task histories, execution traces
- Semantic memory: stable facts about the user, systems, projects, and preferences
- Working memory: current task context and scratch state
- Procedural memory: reusable workflows, skill usage patterns, and scripts

### 8.4 Memory Management

Memory should not be naive append-only accumulation. The framework needs:

- summarization of long histories
- compaction of old task traces
- tagging and linking between entities and tasks
- retention policies
- user-editable protected facts
- source attribution for important facts

### 8.5 Multi-User Separation

Because the runtime is shared, memory must support scoped visibility:

- private user memory
- shared household memory
- agent-private working state
- task-scoped scratch memory

The storage layer should support metadata on each record such as:

- owner or subject user
- visibility scope
- originating connector
- sensitivity tag
- retention class

This can be represented in frontmatter for file-based records and indexed in SQLite for fast filtering.

For v1, SQLite should be the canonical memory backend for both:

- per-user private memory
- shared household memory

Markdown should be treated as a projection or export layer for summaries, journals, and user-readable records rather than the source of truth for active memory retrieval.

### 8.6 Retrieval-Time Scope Enforcement

The storage and memory APIs should not expose a generic "load broad context and filter later" flow for agent prompt construction.

Instead, memory retrieval for agent execution should always be parameterized by:

- caller identity
- allowed visibility scopes
- allowed owner set where relevant
- explicit task-scoped memory references when used

This rule applies to:

- primary-agent prompt assembly
- sub-agent task-slice construction
- background summarization jobs
- connector-triggered event handling

## 9. Skill System

### 9.1 Skill Definition

A skill is an installable capability package that teaches the agent how to perform a class of tasks. Skills may contain:

- manifest metadata
- prompt or instruction templates
- declarative tool access requirements
- configuration schema
- Lua scripts
- optional static assets
- test fixtures
- documentation

### 9.2 Skill Types

- Built-in skills: shipped with the framework
- Local skills: user-authored skills in the local skills directory
- Community skills: third-party installable packages
- Organization skills: managed sets for teams or shared deployments

### 9.3 Skill Manifest

Example conceptual manifest:

```yaml
name: discord-assistant
version: 0.1.0
description: Monitor Discord chats and trigger approved workflows.
entrypoints:
  - type: message_handler
  - type: scheduled_job
permissions:
  tools:
    - discord.read
    - discord.send
    - filesystem.read
  requires_approval_for:
    - discord.send
config_schema: schemas/config.yaml
scripts:
  - scripts/triage.lua
```

### 9.4 Skill Loading Model

Skills should be loaded dynamically at runtime from manifests and registered as capability providers. The framework should validate:

- manifest schema
- version compatibility
- required permissions
- signature or trust metadata if skill signing is added later

### 9.5 Skill Isolation

Skills should not implicitly get full runtime access. They should operate through explicit capability grants:

- allowed tools
- allowed filesystem roots
- allowed network destinations if network policy exists
- allowed messaging channels or workspaces

### 9.6 Skill and Script Authoring Rights

V1 should treat skill and script authoring as privileged capabilities under RBAC. Separate permissions should exist for:

- creating a new local skill
- editing an installed skill
- creating a Lua script
- editing a Lua script
- enabling or disabling a schedule
- binding a script to a recurring trigger

The agent may propose or generate a skill or Lua script, but applying that change should still pass through the same permission checks as any other write operation.

## 10. Tool Execution Layer

The tool execution layer provides structured, auditable actions the agent can perform.

### 10.1 Initial Tool Categories

- terminal command execution
- filesystem read/write/search
- browser automation
- HTTP/API requests
- messaging connectors
- scheduler and timer control
- local notifications
- script execution

### 10.2 Tool Design Requirements

Each tool should expose:

- structured input schema
- structured output schema
- timeout policy
- idempotency hints
- required permission level
- audit record format

### 10.3 Terminal Execution

Terminal execution is powerful and dangerous. It should support:

- command allowlists or deny lists
- working-directory restrictions
- optional sandboxing
- timeouts
- stdout/stderr capture
- approval requirements for risky commands

On Unix-like platforms, execution may rely on subprocess control and optional sandbox wrappers. On Windows, equivalent process control and policy enforcement must be provided.

### 10.4 Filesystem Tools

Filesystem actions should separate read, write, move, and delete permissions. Sensitive paths should be protected by policy. Writes should be journaled in audit logs with before/after metadata where practical.

### 10.5 Browser Automation

Browser control should likely use a stable backend such as Playwright or a compatible automation layer. The framework should abstract browser actions behind a tool interface so the core runtime remains decoupled from a specific library.

V1 should support both interactive desktop browser control and headless browser sessions for VPS deployments.

### 10.6 Messaging Connectors

Messaging integrations should be implemented as connector modules with event ingestion and action APIs.

Candidate initial order:

- Discord
- Telegram
- WhatsApp
- Slack

Discord should define the first implementation slice because it is both operationally practical and suitable for full remote control. WhatsApp remains product-relevant but technically trickier due to API constraints, device pairing models, or unofficial automation paths, so it should follow later.

For v1, Discord command intake should support:

- direct messages
- explicitly approved channels
- per-channel and per-user allowlists

Connector-driven task delivery should not rely solely on a short inline wait for task completion. Some requests will complete quickly enough for an immediate reply, but longer-running or queued work should return a lightweight acceptance response and then continue through a durable follow-up path. The core runtime should persist connector context on the task itself, record that a follow-up is needed when the synchronous wait window expires, and later dispatch the final state transition through a connector-agnostic follow-up router. Connector modules should remain responsible only for rendering and transport details such as channel IDs, thread targets, interaction tokens, or platform-specific formatting.

## 11. Lua Scripting Runtime

Lua is included to support higher-order workflows without recompiling the Rust core.

### 11.1 Why Lua

- small embeddable runtime
- mature ecosystem
- good fit for automation and glue logic
- easier for users to author than Rust plugins
- safer to sandbox than unrestricted native extensions

### 11.2 Lua Use Cases

- post-processing model outputs
- custom decision policies
- skill-level workflow logic
- message formatting and routing
- event handlers
- task-specific helpers
- recurring automations created from user requests
- cron-like maintenance jobs authored by the agent under policy

### 11.3 Lua Integration Model

Recommended design:

- embed Lua via `mlua` or equivalent
- expose a narrow host API from Rust to Lua
- require explicit capability grants to each script
- block arbitrary host access unless granted
- support both skill-packaged scripts and user-local automation scripts

Host API examples:

- `memory.read(key)`
- `memory.write(key, value)`
- `tool.call(name, args)`
- `log.info(message)`
- `task.defer(duration)`
- `user.notify(message)`

### 11.4 Script Safety

Lua scripts should run with:

- execution timeouts
- memory limits if feasible
- capability-based API exposure
- disabled unsafe standard library features when possible

Every Lua script should also pass through a safety-agent review stage. That review should check for:

- dangerous capability use
- policy violations or privilege escalation attempts
- suspicious filesystem or shell behavior
- unexpected external communication paths
- mismatch between the originating task and the script's actual behavior

In v1, this safety review should be blocking for Lua scripts.

The same safety-review pattern should also be applied to generated shell commands in v1, even if the enforcement path differs from Lua activation.

### 11.5 Agent-Authored Automation

The system should support a workflow where an agent recognizes that a recurring automation would improve reliability or reduce cost. In that case it may:

1. propose a Lua automation or create it directly if policy allows
2. save it to the runtime database with metadata and ownership
3. submit it to a safety-agent review
4. if this is a new script, leave it in `draft` until it is explicitly activated
5. if this is a rewrite of an already `active` script and the new version passes safety review, keep it `active`
6. request approval to bind it to a schedule if required
7. register it as a recurring job under the requesting user's or shared household context

Generated scripts should carry metadata such as:

- creator identity
- originating task ID
- allowed capabilities
- schedule bindings
- last run status
- approval history
- safety review status
- lifecycle status such as `draft`, `active`, `disabled`, or `blocked`

## 12. Model Provider Abstraction

The system should not depend on a single LLM vendor.

### 12.1 Supported Provider Classes

- OpenAI-compatible APIs
- Anthropic
- local model runners
- future providers through a shared trait interface

The first concrete provider implementation should target OpenAI.

### 12.2 Provider Interface

The provider abstraction should support:

- chat completion / reasoning calls
- structured output modes
- tool calling or function calling
- token accounting
- streaming responses
- retry and rate-limit handling
- model capability metadata

For OpenAI specifically, the provider layer should be designed to support more than one authentication mode. The initial target modes are:

- API-token based access
- a Codex-style authenticated account flow, if it can be supported through a stable local auth abstraction without coupling the runtime to a fragile interface

This should be exposed through a setup assistant where the user selects a provider first. If OpenAI is selected, the assistant should offer the relevant OpenAI authentication modes.

### 12.3 Multi-Model Strategy

The framework should support different model roles:

- planner model: strongest reasoning
- executor model: cheaper tool-use and routine steps
- summarizer model: background compression and memory maintenance
- classifier model: routing, moderation, permission hints

Users should be able to assign providers and models per role in configuration.

V1 should also support assigning a separate safety-review model role used to inspect agent-authored Lua scripts and other risky generated artifacts before activation or trusted execution.

### 12.4 Local Model Support

Local models should be first-class where possible, but the system should not assume they match frontier-model tool-use quality. The runtime should degrade gracefully:

- use local models for summarization or classification
- use hosted models for high-stakes planning if configured
- allow pure-local operation for privacy-sensitive users

## 13. Autonomy and Heartbeat

One of the differentiators is proactive operation.

### 13.1 Heartbeat Engine

The heartbeat is a periodic scheduler that lets agents wake up and inspect pending work.

Heartbeat responsibilities:

- check incomplete tasks
- review inboxes or message queues
- perform follow-up actions
- update memory summaries
- run recurring maintenance skills
- raise alerts for issues requiring human attention

### 13.2 Autonomy Levels

The system should expose configurable autonomy tiers:

- Observe: gather information only
- Suggest: prepare plans or drafts but do not act
- Act with Approval: execute only after approval for sensitive actions
- Bounded Autonomy: act within policy-defined limits
- High Autonomy: perform broad approved workflows without step-by-step confirmation

Autonomy should be scoped per agent, per skill, and per user role.

V1 should default to Bounded Autonomy.

### 13.3 Durable Tasks

Long-running tasks should survive restarts. Each task should have:

- stable ID
- current state
- plan
- next scheduled wake-up
- retry count
- last observation
- permission context

## 14. Role and Rights Management

This is central to the framework.

### 14.1 Actors

The system should distinguish:

- local owner
- local trusted users
- remote users from messaging integrations
- agents
- skills
- scripts

### 14.1.1 Initial Built-In Roles

The initial family-style RBAC model should ship with these built-in roles:

- `owner`: full administrative control over runtime, policies, providers, skills, and approvals
- `adult`: broad day-to-day usage with bounded automation rights but limited administrative power
- `child`: restricted task execution, limited tool access, and no skill or script authoring by default
- `guest`: narrow interaction rights with minimal persistence and no privileged actions
- `service`: non-human integration identity for connectors, background jobs, and internal system tasks

### 14.2 Policy Model

Recommended approach: explicit RBAC plus contextual policy checks.

RBAC covers:

- who can invoke which agents
- who can access which skills
- who can read or write which memory scopes
- who can trigger which tools
- who can create or modify scripts
- who can create or modify skills
- who can approve schedules or recurring jobs
- who can spawn privileged sub-agents

Contextual policies cover:

- time-based restrictions
- path restrictions
- command restrictions
- rate limits
- financial or destructive action thresholds
- approval escalation rules

### 14.3 Approval Model

Sensitive actions should require approval based on policy, for example:

- deleting files
- running package installers
- sending messages externally
- browser actions against production systems
- executing unrestricted shell commands

Approvals may be:

- inline in the local UI/CLI
- via a trusted messaging channel
- pre-approved by policy for narrow cases

For remote-control use cases, approvals must be tied to authenticated user identities on each connector rather than trusting raw message content alone.

### 14.4 Trust Boundaries

Important trust boundaries include:

- user to agent
- agent to tool
- skill to tool
- remote chat identity to local authority
- planner output to actual execution

The implementation should never treat raw model output as automatically trustworthy.

## 15. Interfaces

### 15.1 Local Interfaces

Initial interface options:

- CLI for setup, task submission, approvals, logs, and debugging
- local web UI for monitoring, memory browsing, policy editing, and approvals
- local API for programmatic control

Recommended first implementation: CLI first, with the web UI deferred until the core runtime, policy model, and remote-control flows are stable.

### 15.2 Messaging Interfaces

Messaging connectors should support:

- receiving commands
- sending notifications
- routing approvals
- attaching task context
- restricting available commands by user role
- delivering deferred follow-up replies for long-running tasks

Message-originated tasks must carry an authenticated identity and permission scope.

Because messaging is a primary control surface, connectors should also support:

- session or thread continuity
- explicit user-to-runtime identity mapping
- rate limiting and anti-spam controls
- safe rendering of approval prompts and command results

When a message-originated task outlives the connector's synchronous response window, the runtime should preserve enough connector metadata to send later task updates without reconstructing the request from logs or in-memory state. This follow-up mechanism should deduplicate per connector and task state transition so a restart or retry does not produce repeated remote replies.

## 16. Observability and Auditability

The framework needs strong introspection.

### 16.1 Logs

At minimum:

- runtime logs
- agent decision logs
- tool execution logs
- policy decision logs
- connector event logs

### 16.2 Audit Records

For each material action, record:

- who or what initiated it
- which agent executed it
- tool name and arguments summary
- policy result
- timestamps
- outputs or artifact references

### 16.3 Replay and Debugging

It should be possible to reconstruct a task history and understand:

- what the model decided
- what tools were called
- what was blocked by policy
- what changed in local state

## 17. Packaging and Deployment

### 17.1 Runtime Targets

- macOS desktop or laptop
- Windows desktop or workstation
- Linux desktop, server, or VPS

### 17.2 Distribution Options

- standalone binaries
- package manager releases
- Docker for server/VPS deployments

Cross-platform browser automation and messaging adapters should be optional features so the minimal runtime remains lean.
The deployment model should treat desktop and VPS targets as equally supported, not as separate product tiers.

## 18. Proposed Internal Modules

An initial Rust crate/module breakdown could look like:

- `beaverki-core`: shared types, config, errors, IDs
- `beaverki-runtime`: process lifecycle, scheduling, orchestration
- `beaverki-agent`: planning and task execution loop
- `beaverki-memory`: filesystem and SQLite-backed storage
- `beaverki-policy`: RBAC, approvals, action gating
- `beaverki-tools`: tool traits and built-in tools
- `beaverki-skills`: manifest loading, validation, registry
- `beaverki-lua`: embedded scripting runtime
- `beaverki-models`: provider abstraction and adapters
- `beaverki-identity`: users, connector identities, sessions, role bindings
- `beaverki-connectors`: messaging and external integration modules
- `beaverki-cli`: command-line interface
- `beaverki-web`: optional local web UI

## 19. Suggested MVP Scope

The full vision is broad. A disciplined MVP should prove the architecture without overcommitting.

### MVP Capabilities

- multiple human users with authenticated identities
- persistent per-user primary agents plus dynamic sub-agents
- per-user private memory plus shared household memory
- CLI interface
- YAML configuration plus SQLite-first runtime state
- OpenAI provider with API-token authentication
- OpenAI provider path for Codex-style authenticated account access, subject to implementation feasibility
- provider abstraction plus one additional provider
- terminal tool with policy controls
- filesystem tools with scoped permissions
- basic browser automation integration
- heartbeat scheduler
- durable task queue
- RBAC and approval engine
- local skill loading from manifests
- Lua runtime with minimal host API
- agent-authored local Lua scripts with RBAC-gated scheduling and safety-agent review
- safety review for generated shell commands
- one messaging connector, starting with Discord

### Deferred From MVP

- WhatsApp support
- advanced UI
- community skill registry
- signed skills
- distributed execution
- advanced policy expressions beyond RBAC
- deep self-modification

## 20. Risks and Design Tensions

### 20.1 Autonomy vs Safety

The more autonomous the system, the more important bounded permissions, execution review, and robust defaults become.

### 20.2 Plain Files vs Operational Complexity

Plain Markdown and YAML are user-friendly, but pure file-based state will become difficult at scale. A hybrid file plus SQLite model is the practical compromise.

### 20.3 Cross-Platform Support

Cross-platform terminal control, sandboxing, path policy enforcement, and browser automation vary materially between operating systems.

### 20.4 Third-Party Integrations

Messaging services differ in API stability, auth models, and policy risk. The connector boundary must be clean and optional.

### 20.5 Model Unreliability

LLMs are non-deterministic and sometimes overconfident. The runtime must compensate with structured tools, validation, policy checks, and durable state.

## 21. Implementation Direction

Recommended order of work:

1. Define core types, config schema, identity model, and runtime skeleton in Rust.
2. Implement the SQLite-first storage layer, with per-user and household memory scopes plus file-based exports.
3. Implement RBAC, approval model, and user-to-connector identity mapping before powerful tools.
4. Add terminal and filesystem tools with strict permission gates and safety review for generated shell execution.
5. Add the OpenAI provider with API-token authentication, then layer in Codex-style account authentication if the abstraction remains stable.
6. Add the base agent execution loop, persistent per-user agents, and dynamic sub-agent spawning.
7. Add durable tasks and heartbeat scheduling.
8. Add skill manifests and Lua host runtime, including script metadata, safety review, and schedule registration.
9. Add browser integration with both interactive and headless execution modes.
10. Add the Discord connector as the first remote control surface.
11. Add local web UI after CLI workflows are stable.

## 22. Open Design Questions

The core product-shaping questions have been resolved enough to proceed to a technical spec. Remaining questions are implementation details rather than blockers:

1. Which concrete SQLite schema should be canonical for memory retrieval, task state, approvals, and audit events?
2. How should the setup assistant persist provider credentials securely across macOS, Windows, Linux, and VPS environments?
3. What is the exact contract between the main agent and the safety agent for script and shell review?
4. Which OpenAI models should be recommended by default for planner, executor, summarizer, and safety-review roles?
5. What is the exact Discord identity-mapping and approval UX for multi-user household deployments?

## 23. Immediate Next Step

Before coding, the best next step is to turn this design into a concrete v1 technical spec with:

- config schemas
- SQLite data model
- Rust crate boundaries
- agent and safety-agent interfaces
- provider and connector abstractions
