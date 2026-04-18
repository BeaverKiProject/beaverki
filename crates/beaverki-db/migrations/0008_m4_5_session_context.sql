CREATE TABLE IF NOT EXISTS conversation_sessions (
  session_id TEXT PRIMARY KEY,
  session_kind TEXT NOT NULL,
  session_key TEXT NOT NULL,
  audience_policy TEXT NOT NULL,
  max_memory_scope TEXT NOT NULL,
  originating_connector_type TEXT,
  originating_connector_target TEXT,
  last_activity_at TEXT NOT NULL,
  last_reset_at TEXT,
  archived_at TEXT,
  lifecycle_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_sessions_key
  ON conversation_sessions (session_key);

CREATE INDEX IF NOT EXISTS idx_conversation_sessions_kind_activity
  ON conversation_sessions (session_kind, last_activity_at DESC);

ALTER TABLE tasks ADD COLUMN session_id TEXT;

CREATE INDEX IF NOT EXISTS idx_tasks_session_created_at
  ON tasks (session_id, created_at DESC);
