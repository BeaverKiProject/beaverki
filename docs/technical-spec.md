# BeaverKi V1 Technical Spec

## 1. Purpose

This document translates the product design into a concrete V1 implementation plan for BeaverKi.

It defines:

- the runtime shape
- Rust crate boundaries
- core config files
- the SQLite-first data model
- agent and safety-agent contracts
- provider abstractions
- connector abstractions
- the execution and approval flow

This spec assumes the decisions recorded in [design.md](./design.md).

## 2. V1 Scope

V1 includes:

- multi-user runtime with household deployment
- one persistent primary agent per user
- ephemeral sub-agents with minimal task-slice handoff
- SQLite-first operational state
- YAML-based config and manifests
- OpenAI as the first provider
- Discord as the first remote-control connector
- CLI-first operation
- RBAC and approval controls
- Lua scripting with blocking safety-agent review
- safety review for generated shell commands
- browser automation in desktop and headless VPS modes

V1 explicitly does not require:

- a web UI
- WhatsApp support
- advanced attribute-based policy expressions
- distributed runtimes
- signed skill marketplace support

Implementation staging is tracked in [delivery-plan.md](./delivery-plan.md). The technical spec defines the target architecture; the delivery plan defines the order and story breakdown for shipping it.

## 3. Runtime Topology

The runtime is a single long-lived Rust process with optional helper subprocesses for tools such as browser automation.

### 3.1 Process Model

- `beaverki` daemon/runtime process
- optional CLI client talking to the local runtime over a local API socket
- optional browser worker subprocesses
- optional connector workers inside the same process unless isolation is needed later

### 3.2 Core Subsystems

- config loader
- identity and RBAC engine
- orchestrator
- task scheduler
- memory store
- tool executor
- provider manager
- skill registry
- script runtime
- safety review engine
- connector manager
- audit logger

### 3.3 Execution Modes

- local desktop mode
- local server/VPS mode
- headless automation mode

The runtime should not materially change architecture between desktop and VPS modes. Differences should mostly be capability toggles and connector/browser settings.

## 4. Rust Crate Boundaries

The workspace should be split into focused crates.

### 4.1 Core Crates

- `beaverki-core`
  Shared IDs, enums, error types, time helpers, serialization types, and common traits.

- `beaverki-config`
  YAML config loading, validation, defaults, migration, and setup-assistant persistence.

- `beaverki-db`
  SQLite schema migrations, query layer, transactions, and repository interfaces.

- `beaverki-identity`
  Users, roles, connector identity mapping, household scoping, and permission resolution.

- `beaverki-policy`
  RBAC evaluation, approval checks, dangerous-action classification, and policy decisions.

- `beaverki-memory`
  Semantic and episodic memory APIs backed by SQLite, plus Markdown export/projection logic.

- `beaverki-models`
  Provider abstraction, model-role routing, request/response normalization, and auth adapters.

- `beaverki-tools`
  Tool trait definitions and built-in implementations for shell, filesystem, the current browser-visit baseline, and future web/browser, messaging, and script-facing tool surfaces.

- `beaverki-skills`
  Skill manifests, registry, activation state, capability declarations, and skill loading.

- `beaverki-lua`
  Embedded Lua runtime, host API, script metadata, execution limits, and schedule bindings.

- `beaverki-agent`
  Agent state, execution loop, task-slice preparation, sub-agent spawning, and result synthesis.

- `beaverki-safety`
  Safety-agent review contracts for Lua scripts and generated shell commands.

- `beaverki-connectors`
  Connector abstractions and implementations, starting with Discord.

- `beaverki-runtime`
  Process lifecycle, orchestrator wiring, scheduling, and subsystem startup/shutdown.

- `beaverki-cli`
  Setup assistant, runtime management, user/role administration, approvals, task submission, and logs.

### 4.2 Dependency Direction

High-level direction:

- leaf crates: `core`, `config`
- shared infra: `db`, `identity`, `policy`, `memory`, `models`, `tools`, `skills`, `lua`, `safety`
- orchestration: `agent`, `connectors`
- top-level: `runtime`, `cli`

`beaverki-runtime` should compose other crates rather than holding significant business logic itself.

## 5. Configuration Model

V1 should use YAML config files plus SQLite for active state.

### 5.1 Config Files

Expected config directory:

```text
config/
  runtime.yaml
  providers.yaml
  integrations.yaml
  users.yaml
  roles.yaml
  permissions.yaml
  schedules.yaml
```

Current repository baseline:

- the runtime currently loads `runtime.yaml`, `providers.yaml`, and `integrations.yaml`
- browser and Discord integration settings already live under `integrations.yaml`
- the additional user, role, policy, and schedule manifests remain valid architectural targets when those concerns are externalized further

### 5.2 `runtime.yaml`

Purpose:

- instance identity
- paths
- runtime mode
- default feature flags

Example:

```yaml
instance_id: "home-main"
mode: "desktop"
data_dir: "./data"
log_dir: "./logs"
default_timezone: "Europe/Vienna"
features:
  browser: true
  discord: true
  lua: true
  headless_browser: true
defaults:
  primary_agent_autostart: true
  markdown_exports: true
```

### 5.3 `users.yaml`

Purpose:

- household users
- role assignment
- agent binding
- memory scope defaults

Example:

```yaml
users:
  - user_id: "u_owner"
    display_name: "Alex"
    roles: ["owner"]
    primary_agent_id: "agent_alex"
    memory_scope: "private_plus_household"
    enabled: true
  - user_id: "u_child_1"
    display_name: "Sam"
    roles: ["child"]
    primary_agent_id: "agent_sam"
    memory_scope: "private_plus_household"
    enabled: true
```

### 5.4 `roles.yaml`

Purpose:

- built-in and custom roles
- tool categories allowed
- skill/script rights

Example:

```yaml
roles:
  - role_id: "owner"
    inherits: []
    permissions:
      invoke_agents: ["*"]
      tools: ["*"]
      skills:
        create: true
        edit: true
      scripts:
        create: true
        edit: true
        schedule: true
      approvals:
        grant: true
  - role_id: "child"
    inherits: []
    permissions:
      invoke_agents: ["personal"]
      tools: ["filesystem_read_text", "filesystem_search", "browser_visit"]
      skills:
        create: false
        edit: false
      scripts:
        create: false
        edit: false
        schedule: false
      approvals:
        grant: false
```

### 5.5 `providers.yaml`

Purpose:

- provider selection
- model-role mapping
- credential references

Example:

```yaml
providers:
  active: "openai_main"
  entries:
    - provider_id: "openai_main"
      kind: "openai"
      auth:
        mode: "api_token"
        secret_ref: "secret://providers/openai_main/api_token"
      models:
        planner: "gpt-5.4"
        executor: "gpt-5.4-mini"
        summarizer: "gpt-5.4-mini"
        safety_reviewer: "gpt-5.4-mini"
    - provider_id: "openai_codex"
      kind: "openai"
      auth:
        mode: "account_session"
        secret_ref: "secret://providers/openai_codex/session"
      models:
        planner: "gpt-5.4"
        executor: "gpt-5.4-mini"
        summarizer: "gpt-5.4-mini"
        safety_reviewer: "gpt-5.4-mini"
```

### 5.6 `integrations.yaml`

Purpose:

- browser launcher and headless browser settings
- Discord connector enablement
- allowed channels and approval UX settings

Example:

```yaml
integrations:
  browser:
    interactive_launcher: null
    headless_browser: "chromium"
    headless_args: []
  discord:
    enabled: true
    bot_token_secret_ref: "secret://connectors/discord/token"
    command_prefix: "!bk"
    allowed_channels:
      - channel_id: "111"
        mode: household
      - channel_id: "222"
        mode: guest
    task_wait_timeout_secs: 5
    approval_action_ttl_secs: 900
    approval_dm_only: true
    critical_confirmation_ttl_secs: 300
```

### 5.7 `permissions.yaml`

Purpose:

- explicit allow/deny overrides
- high-risk action policy
- approval thresholds

Example:

```yaml
policies:
  shell:
    require_safety_review: true
    require_approval_for:
      - "package_manager.*"
      - "delete.*"
      - "network_admin.*"
  scripts:
    require_safety_review: true
    block_on_failed_review: true
  browser:
    headless_enabled: true
```

### 5.8 `schedules.yaml`

Purpose:

- recurring jobs
- script bindings
- workflow bindings
- task wakeups

Example:

```yaml
schedules:
  - schedule_id: "sched_cleanup"
    owner_user_id: "u_owner"
    target_type: "lua_script"
    target_id: "script_cleanup_tmp"
    cron: "0 3 * * *"
    enabled: true
  - schedule_id: "sched_weekly_digest"
    owner_user_id: "u_owner"
    target_type: "workflow"
    target_id: "wf_weekly_digest"
    cron: "0 7 * * MON"
    enabled: true
```

Simple schedules may still target a single Lua script. More complex recurring work should target a reviewed workflow definition whose stages can include reviewed Lua execution, reviewed Lua-defined tools, and bounded agent handoff.

## 6. Setup Assistant

V1 should provide an interactive CLI setup assistant.

### 6.1 Responsibilities

- initialize the config directory
- create the first owner user
- choose the first provider
- collect provider credentials
- configure Discord if desired
- initialize the database
- generate primary agents for users

### 6.2 Provider Selection Flow

The setup assistant should ask:

1. Which provider do you want to configure first?
2. If `OpenAI` is selected:
   - use API token
   - use authenticated account flow, if supported
3. Which models should be assigned to planner, executor, summarizer, and safety-review roles?

### 6.3 Credential Storage

Config files should never store raw secrets.

The credential layer should abstract:

- macOS Keychain
- Windows Credential Manager
- Linux secret service when available
- encrypted local file fallback for VPS/headless environments

Open question:
The encrypted file fallback needs an opinionated bootstrapping method. My default is a master passphrase provided at setup and optionally stored in environment-specific secret tooling later.

## 7. SQLite Data Model

SQLite is the canonical store for runtime state and operational memory.

### 7.1 Principles

- normalized enough for correctness
- pragmatic denormalization for retrieval speed
- append-friendly audit/event tables
- explicit scope columns for private vs household memory
- stable IDs for replay and exports

### 7.1.1 Scope Enforcement Requirement

The memory schema is only effective if retrieval and prompt assembly enforce scope boundaries before any model call is constructed.

V1 requirement:

- agent-context retrieval must be scope-aware by contract
- storage queries must apply scope filters directly
- prompt builders may consume only the filtered result set
- broad retrieval followed by application-layer filtering is not an acceptable primary protection boundary

### 7.2 Primary Tables

Core entities:

- `users`
- `roles`
- `user_roles`
- `agents`
- `agent_links`
- `connector_identities`
- `tasks`
- `task_events`
- `approvals`
- `memories`
- `memory_edges`
- `artifacts`
- `skills`
- `scripts`
- `script_reviews`
- `schedules`
- `workflows`
- `workflow_steps`
- `workflow_runs`
- `workflow_run_steps`
- `tool_invocations`
- `policy_decisions`
- `provider_profiles`
- `audit_events`

### 7.3 Table Sketches

#### `users`

```sql
CREATE TABLE users (
  user_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL,
  primary_agent_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

#### `roles`

```sql
CREATE TABLE roles (
  role_id TEXT PRIMARY KEY,
  description TEXT NOT NULL
);
```

#### `user_roles`

```sql
CREATE TABLE user_roles (
  user_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  PRIMARY KEY (user_id, role_id)
);
```

#### `agents`

```sql
CREATE TABLE agents (
  agent_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,                -- primary | subagent | safety
  owner_user_id TEXT,
  parent_agent_id TEXT,
  status TEXT NOT NULL,
  persona TEXT,
  memory_scope TEXT NOT NULL,
  permission_profile TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

#### `tasks`

```sql
CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  initiating_identity_id TEXT NOT NULL,
  primary_agent_id TEXT NOT NULL,
  assigned_agent_id TEXT NOT NULL,
  parent_task_id TEXT,
  kind TEXT NOT NULL,                -- interactive | scheduled | event | autonomous
  state TEXT NOT NULL,               -- pending | running | waiting_approval | blocked | completed | failed
  objective TEXT NOT NULL,
  context_summary TEXT,
  scope TEXT NOT NULL,               -- private | household
  wake_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);
```

#### `task_events`

```sql
CREATE TABLE task_events (
  event_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  actor_type TEXT NOT NULL,          -- user | agent | tool | safety_agent | connector
  actor_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
```

#### `memories`

```sql
CREATE TABLE memories (
  memory_id TEXT PRIMARY KEY,
  owner_user_id TEXT,
  scope TEXT NOT NULL,               -- private | household | agent_private | task
  subject_type TEXT NOT NULL,        -- fact | conversation | summary | preference | procedure
  subject_key TEXT,
  content_text TEXT NOT NULL,
  content_json TEXT,
  sensitivity TEXT NOT NULL,
  source_type TEXT NOT NULL,         -- user | connector | tool | inference | summary
  source_ref TEXT,
  task_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_accessed_at TEXT
);
```

#### `scripts`

```sql
CREATE TABLE scripts (
  script_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  kind TEXT NOT NULL,                -- lua
  status TEXT NOT NULL,              -- draft | active | disabled | blocked
  source_text TEXT NOT NULL,
  capability_profile_json TEXT NOT NULL,
  created_from_task_id TEXT,
  safety_status TEXT NOT NULL,       -- pending | approved | rejected
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

For future Lua-defined tools, the recommended model is to keep user-authored local tools in database-backed tables parallel to `scripts`, rather than storing mutable local tool definitions as loose filesystem files. Filesystem-backed definitions remain appropriate for packaged skills and built-in assets.

#### `script_reviews`

```sql
CREATE TABLE script_reviews (
  review_id TEXT PRIMARY KEY,
  script_id TEXT NOT NULL,
  reviewer_agent_id TEXT NOT NULL,
  verdict TEXT NOT NULL,             -- approved | rejected | needs_changes
  findings_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
```

#### `workflows`

```sql
CREATE TABLE workflows (
  workflow_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  status TEXT NOT NULL,              -- draft | active | disabled | blocked
  entrypoint_kind TEXT NOT NULL,     -- schedule | manual | event
  definition_summary TEXT,
  created_from_task_id TEXT,
  safety_status TEXT NOT NULL,       -- pending | approved | needs_changes | rejected
  safety_summary TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

#### `workflow_steps`

```sql
CREATE TABLE workflow_steps (
  workflow_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  position INTEGER NOT NULL,
  step_kind TEXT NOT NULL,           -- lua_script | lua_tool | agent_task | user_notify
  target_ref TEXT NOT NULL,
  input_schema_json TEXT,
  output_schema_json TEXT,
  capability_profile_json TEXT,
  handoff_policy_json TEXT,
  PRIMARY KEY (workflow_id, step_id)
);
```

`workflow_steps.handoff_policy_json` should be flexible enough to carry step-type-specific contracts without forcing a separate table per initial stage kind. In particular:

- for `agent_task`, it should support a prompt template, required artifact refs, optional model-role override, allowed tool or Lua-artifact refs, and a declared output contract
- for `user_notify`, it should support recipient targeting, delivery mode, channel policy, message template, and deduplication or delivery semantics

#### `workflow_runs`

```sql
CREATE TABLE workflow_runs (
  run_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  schedule_id TEXT,
  owner_user_id TEXT NOT NULL,
  initiating_identity_id TEXT NOT NULL,
  state TEXT NOT NULL,               -- pending | running | blocked | completed | failed
  current_step_position INTEGER,
  artifact_bundle_json TEXT,
  wake_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);
```

#### `workflow_run_steps`

```sql
CREATE TABLE workflow_run_steps (
  run_step_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  task_id TEXT,
  state TEXT NOT NULL,               -- pending | running | blocked | completed | failed | skipped
  input_json TEXT,
  output_json TEXT,
  started_at TEXT,
  completed_at TEXT
);
```

#### `tool_invocations`

```sql
CREATE TABLE tool_invocations (
  invocation_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  request_json TEXT NOT NULL,
  response_json TEXT,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT
);
```

#### `policy_decisions`

```sql
CREATE TABLE policy_decisions (
  decision_id TEXT PRIMARY KEY,
  task_id TEXT,
  actor_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  target_ref TEXT,
  verdict TEXT NOT NULL,             -- allow | deny | approval_required
  rationale_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
```

#### `approvals`

```sql
CREATE TABLE approvals (
  approval_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  target_ref TEXT,
  requested_by_agent_id TEXT NOT NULL,
  requested_from_user_id TEXT NOT NULL,
  status TEXT NOT NULL,              -- pending | approved | denied | expired
  rationale_text TEXT,
  decided_at TEXT,
  created_at TEXT NOT NULL
);
```

#### `connector_identities`

```sql
CREATE TABLE connector_identities (
  identity_id TEXT PRIMARY KEY,
  connector_type TEXT NOT NULL,      -- discord
  external_user_id TEXT NOT NULL,
  external_channel_id TEXT,
  mapped_user_id TEXT NOT NULL,
  trust_level TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### 7.4 Retrieval Indexes

At minimum:

- `memories(scope, owner_user_id, subject_type)`
- `tasks(owner_user_id, state, wake_at)`
- `tool_invocations(task_id, status)`
- `scripts(owner_user_id, status, safety_status)`
- `connector_identities(connector_type, external_user_id)`

### 7.5 Markdown Export Layer

Markdown should not be a live operational store. It should be generated from SQLite for:

- daily journals
- user-visible summaries
- task reports
- memory exports

This keeps the runtime consistent while preserving inspectability.

### 7.6 Memory Retrieval Contract

The memory layer should expose explicit scope-aware retrieval functions rather than unbounded fetch APIs for agent execution.

Conceptually:

```rust
struct RetrievalScope {
    acting_user_id: UserId,
    acting_agent_id: AgentId,
    visible_scopes: Vec<MemoryScope>,
    allowed_owner_user_ids: Vec<UserId>,
    task_id: Option<TaskId>,
}
```

```rust
trait MemoryStore {
    async fn retrieve_for_agent_context(
        &self,
        scope: &RetrievalScope,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryRecord>>;
}
```

This contract exists to prevent cross-user leakage during:

- primary-agent context assembly
- sub-agent task preparation
- summarization and maintenance jobs
- connector-triggered interactive runs

## 8. Identity and RBAC

### 8.1 Built-In Roles

V1 built-ins:

- `owner`
- `adult`
- `child`
- `guest`
- `service`

### 8.2 Permission Categories

Permissions should be grouped as:

- agent invocation
- tool use
- skill management
- script management
- schedule management
- approval authority
- connector access
- memory access

### 8.3 Scope Model

At minimum, access checks need:

- acting user
- acting agent
- target scope: private or household
- connector origin
- tool category

### 8.4 Memory Visibility Rules

Default rules:

- a user can read their own private memory
- a user can read household memory if their role allows it
- a primary agent can read the owner's private memory plus household memory visible to that user
- a sub-agent receives only task-sliced memory prepared by the parent agent
- child and guest roles should have narrower household visibility by default

### 8.5 Context Assembly Rule

Before any LLM call is made, the runtime must assemble context through the scoped retrieval contract, not by issuing broad memory queries.

Required sequence:

1. resolve the acting user and agent
2. derive visible memory scopes from RBAC and task scope
3. retrieve candidate memory through scope-aware queries
4. build the prompt only from that filtered set

This must be treated as a hard execution invariant.

## 9. Agent Model

### 9.1 Primary Agents

Each user has exactly one persistent primary agent in V1.

Responsibilities:

- maintain long-lived context
- receive direct tasks
- manage user-specific memory retrieval
- prepare sub-agent task slices
- merge sub-agent results
- request approvals

### 9.2 Sub-Agents

Sub-agents are ephemeral.

Responsibilities:

- handle bounded tasks
- use reduced permissions
- return structured results
- terminate cleanly after completion or timeout

### 9.3 Safety Agent

The safety agent is a specialized reviewer agent.

Responsibilities:

- review Lua scripts before trusted execution
- review generated shell commands before execution
- emit findings, verdict, and rationale

V1 default:

- blocking for Lua
- blocking for generated shell commands in `medium`, `high`, and `critical` risk classes
- no blocking safety review required for `low` risk read-only inspection commands

## 10. Agent Contracts

### 10.1 Main Agent to Sub-Agent Contract

The parent agent must prepare a bounded task slice.

Suggested structure:

```json
{
  "task_id": "task_123",
  "objective": "Summarize household calendar conflicts for next week",
  "context_summary": "Need only family calendar records and last 7-day reminder thread",
  "allowed_tools": ["calendar.read", "memory.read"],
  "memory_refs": ["mem_1", "mem_2", "mem_3"],
  "artifacts": [],
  "expected_output_schema": {
    "type": "object",
    "properties": {
      "conflicts": {"type": "array"},
      "recommendation": {"type": "string"}
    }
  }
}
```

### 10.2 Main Agent to Safety Agent Contract

Suggested review request:

```json
{
  "review_type": "lua_script",
  "script_id": "script_cleanup_tmp",
  "originating_task_id": "task_123",
  "owner_user_id": "u_owner",
  "source_text": "-- lua source here",
  "capability_profile": {
    "tools": ["filesystem.read", "filesystem.write"],
    "paths": ["/tmp"]
  },
  "intended_behavior_summary": "Delete stale temp files older than 7 days."
}
```

Suggested review response:

```json
{
  "verdict": "approved",
  "risk_level": "medium",
  "findings": [],
  "required_changes": [],
  "summary": "Behavior matches stated intent and permissions."
}
```

## 11. Safety Review Semantics

### 11.1 Lua Review

Lifecycle:

1. main agent generates script
2. script saved as `draft`
3. safety agent reviews
4. if this is a new script and review is approved, it remains `draft` until explicitly activated
5. if this is a rewrite of an already `active` script and review is approved, it remains `active`
6. if rejected, script transitions to `blocked`

### 11.2 Shell Review

Lifecycle:

1. main agent proposes a shell command
2. policy engine classifies it
3. safety agent reviews if policy requires it
4. policy engine determines whether additional user approval is needed
5. tool executor runs or blocks the command

### 11.3 Risk Classes

Initial shell risk classes:

- low: read-only inspection commands
- medium: local file writes in allowed roots
- high: package installation, file deletion, permission changes, external system modification
- critical: privileged operations, credential access, destructive recursive actions

V1 default:

- blocking safety review for `medium`, `high`, and `critical`
- direct policy-gated execution for `low`

## 12. Provider Abstraction

### 12.1 Core Trait

Conceptually:

```rust
trait ModelProvider {
    fn provider_kind(&self) -> ProviderKind;
    async fn call_chat(&self, req: ChatRequest) -> Result<ChatResponse>;
    async fn call_structured(&self, req: StructuredRequest) -> Result<StructuredResponse>;
    async fn count_tokens(&self, req: TokenCountRequest) -> Result<TokenCountResponse>;
    fn capabilities(&self) -> ProviderCapabilities;
}
```

### 12.2 OpenAI Provider

Auth modes:

- `api_token`
- `account_session` if implemented safely and stably

The OpenAI adapter should normalize:

- plain chat calls
- structured output calls
- tool-usage oriented calls
- model metadata for routing by planner/executor/summarizer/safety-review roles

### 12.3 Model Roles

V1 roles:

- planner
- executor
- summarizer
- safety_reviewer

Each task execution context should resolve a model profile per role.

## 13. Tool Abstractions

### 13.1 Core Tool Trait

Conceptually:

```rust
trait Tool {
    fn name(&self) -> &'static str;
    fn input_schema(&self) -> JsonSchema;
    fn output_schema(&self) -> JsonSchema;
    fn risk_class(&self, input: &Value) -> ToolRiskClass;
    async fn execute(&self, ctx: ToolContext, input: Value) -> Result<Value>;
}
```

### 13.2 V1 Tools

Current repository baseline:

- `shell_exec`
- `filesystem_list`
- `filesystem_read_text`
- `filesystem_write_text`
- `filesystem_search`
- `browser_visit`

Planned expansion after the current baseline:

- `website_read` for lightweight page retrieval and normalization
- stateful browser session tools for navigation, extraction, and bounded interaction
- connector- or Lua-provided higher-level tools layered on the same registry contract

### 13.3 Tool Context

Every tool execution should receive:

- working directory
- allowed filesystem roots
- output-size budget
- approved shell-command overrides
- browser launcher and headless-browser settings

Runtime-owned metadata such as task ID, acting user, connector origin, and conversation session should still be available to policy and audit layers even if the minimal tool context struct does not yet carry every field directly.

## 14. Lua Runtime

### 14.1 Embedding

Use `mlua`.

`mlua` is a good fit for BeaverKi because it provides a practical Rust embedding layer for Lua while keeping the host/runtime boundary under Rust control. It is appropriate for both:

- workflow scripting
- user-authored or agent-authored tool logic

### 14.2 Host API Surface

Minimal V1 host functions:

- `memory.read`
- `memory.write`
- `tool.call`
- `task.defer`
- `log.info`
- `notify.user`

### 14.3 Lua-Defined Tools

BeaverKi should be designed so that Lua can define higher-level tools, not only scripts. The important constraint is that these are still host-managed tools, not unrestricted Lua programs.

Suggested model:

- a skill or script manifest may register a `lua_tool`
- the tool declares input and output schemas
- the tool declares a capability profile
- the runtime executes the Lua implementation through `beaverki-lua`
- every privileged operation still routes through Rust-controlled host APIs

Conceptual manifest shape:

```yaml
tools:
  - tool_id: "household.fetch_status_page"
    kind: "lua"
    input_schema: "schemas/fetch_status_input.json"
    output_schema: "schemas/fetch_status_output.json"
    capabilities:
      filesystem:
        read: ["/srv/status"]
      network:
        allow:
          - "https://status.example.com"
```

This allows Lua tools to feel powerful while keeping the trust boundary in the Rust runtime rather than inside Lua itself.

Storage recommendation:

- user-authored or agent-authored Lua-defined tools should be stored in the runtime database alongside their source text, schemas, capability profile, ownership, review state, and activation state
- skill-packaged or repo-packaged Lua-defined tools should be loaded from the filesystem as installed assets
- packaged assets and database-managed local artifacts may share the same runtime tool interface, but they should not share the same persistence model
- import/export between filesystem assets and database-managed tools is optional, but runtime-owned mutable artifacts should not depend on ad hoc files as their primary source of truth

### 14.4 Capability-Gated File and Web Access

Lua should not receive raw `os`, `io`, or unrestricted socket access by default. Instead, if a Lua script or Lua-defined tool needs file or web access, it should receive capability-scoped host functions such as:

- `fs.read_text(path)`
- `fs.write_text(path, content)`
- `web.read_page(url)`
- `browser.visit(url, mode)`

These host functions must enforce:

- allowlisted path roots
- read/write/delete separation
- allowlisted domains or URL prefixes
- timeout limits
- payload size limits
- audit logging
- policy checks before execution

This is the critical point: sandboxing should be enforced by the Rust host API surface, not by trusting Lua code to behave.

### 14.5 Restrictions

- explicit capability profile per script
- timeouts
- no arbitrary host process access except through tool calls
- no unrestricted filesystem access

### 14.6 Scheduled Workflow Composition

The current single-script schedule target should remain supported as the simplest automation path, but the scheduler should also be able to trigger a reviewed workflow definition.

Initial workflow-stage kinds should be:

- `lua_script` for reviewed scripted stages
- `lua_tool` for reviewed reusable Lua-defined tool stages
- `agent_task` for a bounded primary-agent or sub-agent handoff with an explicit task slice and stage-specific prompt contract
- `user_notify` for connector-agnostic result delivery to the workflow owner or another allowed target user

The important rule is that composition should remain runtime-orchestrated. A scheduled workflow run should expose explicit stage boundaries, structured handoff artifacts, fail-closed block reasons, retries, and wake-up state. This keeps cron-driven automations debuggable and auditable while allowing richer behavior than a single script can reasonably express.

For scheduled workflow runs, the runtime should treat prior review and activation as the approval boundary. A run whose workflow definition and referenced stages are active and safety-approved should execute autonomously without pausing for fresh approval. If runtime conditions reveal that a stage would need a new approval or exceeds its reviewed capability envelope, the run should fail closed or move to `blocked` with an audit record rather than entering an interactive approval wait.

For agent stages, the runtime should construct a bounded request from prior stage outputs and policy-approved context. The agent stage should not inherit unrestricted access to all prior internal state merely because it is part of the same workflow run. The workflow definition should be able to constrain that stage with a stored prompt template, explicit artifact bindings, and allowed tool or Lua references so recurring workflows can run a repeatable agent subtask rather than an underspecified general conversation.

For `user_notify` stages, the workflow engine should reuse the normal delivery abstraction rather than inventing a workflow-only notification path. In practice this means the first implementation may start with notifying the workflow owner or initiating user, but it should align with the household-delivery persistence and routing foundation introduced in M5 so later cross-user workflow delivery does not require a redesign.

### 14.6 V1 Recommendation

My recommendation is:

- use `mlua` in V1
- support Lua workflow scripts in V1
- design the runtime so Lua-defined tools are supported by the architecture from the start
- implement full Lua-defined network/file tools after core V1 is stable

V1 decision:

- Lua-defined tools are intentionally post-V1, but the runtime interfaces should be shaped so they can be added without redesigning the tool system.

That avoids overloading the earliest implementation while still keeping the right extension model.

## 15. Connector Abstraction

### 15.1 Core Connector Trait

Conceptually:

```rust
trait Connector {
    fn connector_type(&self) -> ConnectorType;
    async fn start(&self, runtime: ConnectorRuntime) -> Result<()>;
    async fn send_message(&self, target: ConnectorTarget, msg: ConnectorMessage) -> Result<()>;
}
```

### 15.1.1 Durable Follow-Up Contract

Connector reply delivery must support both synchronous replies and deferred follow-ups.

Required runtime behavior:

- when a connector-originated task is created, the runtime persists a `connector_message_context` task event that includes `connector_type` plus connector-specific target data such as channel, DM, thread, or interaction metadata
- if the task does not leave `pending` or `running` within the connector's synchronous wait window, the runtime records a `connector_follow_up_requested` task event rather than assuming the connector interaction is finished
- after any later task state transition to `waiting_approval`, `blocked`, `completed`, or `failed`, the daemon loads the persisted connector context and dispatches a follow-up through a connector-type router
- each connector records `connector_follow_up_sent` with the connector type and task state so follow-up delivery is idempotent across retries, restarts, or repeated state inspection

Boundary split:

- the runtime owns follow-up eligibility, event persistence, idempotency, and routing by `connector_type`
- each connector implementation owns target decoding, message rendering, and transport semantics

This split is required so future connectors can reuse the same durable follow-up model without copying Discord-specific timeout behavior into the core task engine.

### 15.2 Discord Connector V1

Must support:

- receiving DMs
- receiving messages from allowlisted channels through either the configured prefix or a direct bot mention anywhere in the message
- identity mapping from Discord user ID to BeaverKi user ID
- sending replies
- routing approval prompts
- sending deferred follow-up messages after initial acceptance when a task completes later

Message processing flow:

1. ingest Discord event
2. validate guild/channel/DM policy
3. map Discord identity to internal user
4. create or resume task on that user's primary agent
5. attempt synchronous wait for fast task completion
6. if needed, acknowledge the request and persist follow-up intent
7. record connector event in audit log

## 16. Orchestration Flow

### 16.1 Interactive Task

1. user message arrives from CLI or Discord
2. runtime resolves user and connector identity
3. runtime derives visible scopes and allowed owners for this run
4. primary agent retrieves private and household memory through the scoped retrieval contract
5. planner decides next step
6. policy engine checks the action
7. safety review runs if required
8. tool executes or approval is requested
9. memory and task state are persisted
10. if the result is ready within the synchronous channel window, it is returned immediately
11. otherwise the runtime returns an acceptance response and later sends a connector follow-up from persisted task context

### 16.2 Sub-Agent Flow

1. primary agent decides a sub-agent is useful
2. parent prepares minimal task slice
3. parent includes only explicit memory refs or bounded summaries in that slice
4. runtime creates sub-agent and child task
5. sub-agent executes within bounded permissions
6. result returns to parent
7. parent merges into final response or next action

### 16.3 Recurring Script Flow

1. user asks for an automation
2. main agent proposes Lua script
3. script saved to database
4. safety agent reviews
5. if this is a new script, it remains `draft` until explicitly activated
6. if this is a rewrite of an already `active` script and review is approved, it remains `active`
7. if approved and permitted, schedule is registered
8. scheduler wakes script at due time
9. execution is recorded like any other task/tool sequence

## 17. Approvals

### 17.1 Approval Triggers

At minimum:

- high-risk shell commands
- sending external messages when role or policy requires it
- schedule activation when policy requires it
- script activation if future policy overrides immediate activation

### 17.2 Approval Channels

- CLI
- Discord reply/interaction

### 17.3 Approval Records

Every approval request must capture:

- requester
- approver
- target action
- rationale
- final decision

## 18. Audit and Observability

### 18.1 Mandatory Audit Events

- user request received
- task created
- sub-agent spawned
- memory write
- policy decision
- safety review verdict
- tool invocation
- approval request
- approval resolution
- connector follow-up requested
- connector follow-up sent
- task completion/failure

### 18.2 Logs vs Audit

- logs are for operators and debugging
- audit events are durable business records

## 18.3 Repository Workflow For Autonomous Development

Because BeaverKi is intended to be developed with substantial AI-agent assistance, the GitHub workflow should be treated as part of the technical system rather than an afterthought.

Repository-level requirements:

- every material change starts from a GitHub issue
- issue templates enforce problem statement and acceptance criteria
- branches map cleanly to issue numbers
- PRs must include verification notes
- CI must expose stable top-level checks that agents can run locally
- protected branches should require passing checks before merge

Recommended default loop:

1. fetch or create an issue
2. branch from the issue
3. implement the change
4. run local validation
5. open a PR linked to the issue
6. review and address comments
7. merge after approval and green CI

This workflow is documented for contributors in `CONTRIBUTING.md` and `docs/developer-workflow.md`.

## 19. Recommended Defaults

My recommended V1 defaults:

- memory source of truth: SQLite
- Markdown as export layer only
- private memory by default
- explicit household sharing
- minimal sub-agent context only
- blocking Lua safety review
- blocking shell safety review for risky classes
- OpenAI token auth as the simplest initial path
- OpenAI account-session auth as optional if it remains stable
- Discord enabled only after explicit setup

## 20. Build Sequencing

Concrete milestone sequencing and pre-issue stories are tracked in [delivery-plan.md](./delivery-plan.md).

## 21. Questions

The core blockers are resolved. One implementation detail is now explicit:

- network-capable Lua tools should declare domain allowlists in their capability profiles
- global policy config should be able to further restrict or override those allowlists

This means effective network permission for a Lua tool is the intersection of:

- the tool or script capability profile
- the owning user's RBAC scope
- the global policy override
