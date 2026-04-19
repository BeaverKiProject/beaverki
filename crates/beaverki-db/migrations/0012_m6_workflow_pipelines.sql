CREATE TABLE IF NOT EXISTS workflow_definitions (
  workflow_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  status TEXT NOT NULL,
  created_from_task_id TEXT,
  safety_status TEXT NOT NULL,
  safety_summary TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_stages (
  stage_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  stage_index INTEGER NOT NULL,
  stage_kind TEXT NOT NULL,
  stage_label TEXT,
  artifact_ref TEXT,
  stage_config_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (workflow_id, stage_index)
);

CREATE TABLE IF NOT EXISTS workflow_reviews (
  review_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  reviewer_agent_id TEXT NOT NULL,
  review_type TEXT NOT NULL,
  verdict TEXT NOT NULL,
  risk_level TEXT NOT NULL,
  findings_json TEXT NOT NULL,
  summary_text TEXT NOT NULL,
  reviewed_artifact_text TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_runs (
  workflow_run_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  initiating_identity_id TEXT NOT NULL,
  schedule_id TEXT,
  source_task_id TEXT,
  state TEXT NOT NULL,
  current_stage_index INTEGER NOT NULL,
  artifacts_json TEXT NOT NULL,
  wake_at TEXT,
  block_reason TEXT,
  last_error TEXT,
  retry_count INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  completed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_owner_created_at
  ON workflow_definitions (owner_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_stages_workflow_stage_index
  ON workflow_stages (workflow_id, stage_index ASC);

CREATE INDEX IF NOT EXISTS idx_workflow_reviews_workflow_created_at
  ON workflow_reviews (workflow_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_workflow_created_at
  ON workflow_runs (workflow_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_state_wake_at
  ON workflow_runs (state, wake_at);
