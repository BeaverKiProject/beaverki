# BeaverKI Memory Design

## 1. Purpose

This document describes the current memory layout in BeaverKI after `M4 Semantic Memory`.

It is intentionally narrower than the full technical spec. The goal is to give us one practical place to inspect:

- what memory exists today
- how it is stored
- how it is retrieved
- which parts are stable
- which parts are expected to evolve

## 2. Current Model

BeaverKI currently treats memory as SQLite-backed records in the `memories` table.

The runtime distinguishes two broad classes:

- `semantic`
  Durable facts that should survive across conversations, such as identities, preferences, and long-lived household facts.
- `episodic`
  Task- or conversation-derived records, currently used for completion summaries and other non-durable context.

This is the current `M4` behavior, not the final long-term design.

## 3. Table Layout

The canonical operational store is SQLite.

Current columns in `memories`:

- `memory_id`
  Stable row identifier.
- `owner_user_id`
  The owning user for private memory. `NULL` means shared memory visible through the household scope.
- `scope`
  Current values: `private`, `household`, `agent_private`, `task`.
- `memory_kind`
  Current values: `semantic`, `episodic`.
- `subject_type`
  Current semantic usage is centered on `fact`, `identity`, and `preference`.
- `subject_key`
  Stable application-level key used to identify the fact being stored. This is the primary correction/dedup key.
- `content_text`
  Human-readable memory payload.
- `content_json`
  Structured metadata. Today this is mainly used for semantic-memory attribution such as `source_summary`.
- `sensitivity`
  Sensitivity marker. Current writes use `normal`.
- `source_type`
  Origin classification such as `user_statement`, `summary`, or `tool`.
- `source_ref`
  Optional lightweight pointer back to the originating turn, task, or script.
- `task_id`
  Optional task linkage.
- `created_at`, `updated_at`, `last_accessed_at`
  Operational timestamps.
- `superseded_by_memory_id`
  Correction chain. A superseded row remains in history but is no longer active.
- `forgotten_at`
  Tombstone timestamp for a memory that should no longer be used in future retrieval.
- `forgotten_reason`
  Human-readable reason for forgetting the memory.

## 4. Scopes

### 4.1 Private

- Stored with `scope='private'`
- Stored with `owner_user_id=<user>`
- Visible only to that user’s primary agent and allowed inspection paths for that same user

### 4.2 Household

- Stored with `scope='household'`
- New shared writes use `owner_user_id=NULL`
- Visible only to roles that can retrieve household memory
- Guests and children do not currently receive household memory by default

### 4.3 Other Scopes

`agent_private` and `task` remain in the schema because they are part of the broader runtime model, but `M4` memory promotion is currently focused on `private` and `household`.

## 5. Semantic Memory Behavior

Semantic memory is now first-class rather than being inferred from generic summary rows.

Current behavior:

- the primary agent can call `memory_remember`
- the primary agent can call `memory_forget`
- `private` is the default scope for durable facts
- `household` is only allowed for higher-trust roles
- the same `subject_key` is reused when a fact is corrected
- an identical new value deduplicates against the current active row
- a changed value creates a new row and marks the previous row as superseded
- a wrong or obsolete memory row can be tombstoned with `forgotten_at` so it stops participating in active retrieval

This gives us:

- durable memory promotion
- lightweight correction history
- explicit forgetting without destructive deletion
- source attribution
- auditability

Example:

- the agent stores `household.wifi_password = "wrong-password"`
- the user later says that value was wrong and was actually the Wi-Fi name
- the agent can call `memory_forget` on the incorrect password row
- the agent can then store the correct fact under `household.wifi_name`
- active retrieval will ignore the forgotten password row while still preserving it in history and audit logs

## 6. Subject Keys

`subject_key` is the most important semantic-memory coordination point.

Current expectations:

- it should be stable across time for the same fact
- it should be specific enough that unrelated facts do not collide
- it should be human-comprehensible enough to inspect from the CLI

Current examples:

- `profile.preferred_name`
- `profile.favorite_drink`
- `household.wifi_name`

This is still a convention, not yet a formal schema registry.

## 7. Retrieval Rules

The runtime should never load broad memory and filter later for agent execution.

Current retrieval principles:

- visible scopes are derived from the acting user’s roles
- private retrieval is bound to the acting user
- household retrieval is only available when the role set allows it
- active retrieval excludes rows where `superseded_by_memory_id IS NOT NULL`
- active retrieval excludes rows where `forgotten_at IS NOT NULL`
- active retrieval prefers semantic memory ahead of episodic memory

For operator inspection, the new CLI commands follow the same visibility rules.

## 8. Inspection Surface

Current CLI surface:

- `beaverki memory list`
- `beaverki memory show`
- `beaverki memory history`
- `beaverki memory forget`

Intended usage:

- `list`
  See currently visible memory rows, optionally filtered by scope, kind, and active/superseded state.
- `show`
  Inspect one concrete memory row in detail.
- `history`
  See all visible versions of a fact identified by `subject_key`.
- `forget`
  Tombstone one memory row so it no longer affects future retrieval while remaining auditable in history.

These commands work through the daemon when available and fall back to direct SQLite access when the daemon is not running.

## 9. Audit Model

Semantic-memory writes are auditable through `audit_events`.

Current event types:

- `semantic_memory_created`
- `semantic_memory_deduplicated`
- `semantic_memory_corrected`
- `semantic_memory_write_denied`
- `memory_forgotten`

This is enough to answer:

- what was remembered
- when it changed
- which writes were denied

## 10. Current Limitations

The current design is intentionally minimal. Important limitations:

- no embedding or vector retrieval yet
- no explicit confidence score
- no formal subject-key registry
- no merge policy beyond exact-value deduplication and full-row correction
- no first-class relationship graph between memories
- no separate maintenance job for compaction or summarization
- no Markdown export/projection layer yet
- household visibility still depends on role rules rather than richer policy expressions
- forgetting is row-oriented today; there is no higher-level contradiction detector that can automatically infer which older fact should be retired

## 11. Likely Iteration Points

These are the areas we should expect to revisit:

- stronger typing for semantic-memory payloads
- canonical subject-key conventions or registries
- richer source attribution and provenance chains
- confidence or trust metadata
- relationship edges between facts
- memory maintenance and cleanup jobs
- Markdown or web inspection views
- better handling for shared-but-user-attributed household facts
- retrieval ranking beyond simple semantic-first ordering

## 12. Recommended Constraints For Future Changes

When iterating on memory, preserve these invariants unless we deliberately redesign them:

- scope checks happen before model context assembly
- semantic and episodic memory stay distinguishable
- correction does not destroy history
- operator inspection uses the same visibility model as runtime retrieval
- household writes remain policy-gated
- raw storage remains auditable without hidden in-memory state
