CREATE TABLE IF NOT EXISTS scripts (
  script_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  source_text TEXT NOT NULL,
  capability_profile_json TEXT NOT NULL,
  created_from_task_id TEXT,
  safety_status TEXT NOT NULL,
  safety_summary TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS script_reviews (
  review_id TEXT PRIMARY KEY,
  script_id TEXT NOT NULL,
  reviewer_agent_id TEXT NOT NULL,
  review_type TEXT NOT NULL,
  verdict TEXT NOT NULL,
  risk_level TEXT NOT NULL,
  findings_json TEXT NOT NULL,
  summary_text TEXT NOT NULL,
  reviewed_artifact_text TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
  schedule_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  cron_expr TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  next_run_at TEXT NOT NULL,
  last_run_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scripts_owner_created_at
  ON scripts (owner_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_script_reviews_script_created_at
  ON script_reviews (script_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at
  ON schedules (enabled, next_run_at);
