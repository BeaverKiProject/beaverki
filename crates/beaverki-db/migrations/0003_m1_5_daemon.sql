CREATE TABLE IF NOT EXISTS runtime_sessions (
  session_id TEXT PRIMARY KEY,
  instance_id TEXT NOT NULL,
  state TEXT NOT NULL,
  pid INTEGER NOT NULL,
  socket_path TEXT NOT NULL,
  queue_depth INTEGER NOT NULL,
  active_task_id TEXT,
  automation_planning_enabled INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  last_heartbeat_at TEXT,
  stopped_at TEXT,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS runtime_heartbeats (
  heartbeat_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  state TEXT NOT NULL,
  queue_depth INTEGER NOT NULL,
  active_task_id TEXT,
  automation_planning_enabled INTEGER NOT NULL,
  observed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runtime_sessions_instance_started_at ON runtime_sessions (instance_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_heartbeats_session_observed_at ON runtime_heartbeats (session_id, observed_at DESC);