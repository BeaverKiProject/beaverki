CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL,
  primary_agent_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
  agent_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  owner_user_id TEXT,
  parent_agent_id TEXT,
  status TEXT NOT NULL,
  persona TEXT,
  memory_scope TEXT NOT NULL,
  permission_profile TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  initiating_identity_id TEXT NOT NULL,
  primary_agent_id TEXT NOT NULL,
  assigned_agent_id TEXT NOT NULL,
  parent_task_id TEXT,
  kind TEXT NOT NULL,
  state TEXT NOT NULL,
  objective TEXT NOT NULL,
  context_summary TEXT,
  result_text TEXT,
  scope TEXT NOT NULL,
  wake_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS task_events (
  event_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memories (
  memory_id TEXT PRIMARY KEY,
  owner_user_id TEXT,
  scope TEXT NOT NULL,
  subject_type TEXT NOT NULL,
  subject_key TEXT,
  content_text TEXT NOT NULL,
  content_json TEXT,
  sensitivity TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_ref TEXT,
  task_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_accessed_at TEXT
);

CREATE TABLE IF NOT EXISTS tool_invocations (
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

CREATE TABLE IF NOT EXISTS audit_events (
  audit_id TEXT PRIMARY KEY,
  actor_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_owner_created_at ON tasks (owner_user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_memories_scope_owner_subject ON memories (scope, owner_user_id, subject_type);
CREATE INDEX IF NOT EXISTS idx_task_events_task_created_at ON task_events (task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_task_started_at ON tool_invocations (task_id, started_at);
