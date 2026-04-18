CREATE TABLE IF NOT EXISTS lua_tools (
  tool_id TEXT PRIMARY KEY,
  owner_user_id TEXT NOT NULL,
  description TEXT NOT NULL,
  source_text TEXT NOT NULL,
  input_schema_json TEXT NOT NULL,
  output_schema_json TEXT NOT NULL,
  capability_profile_json TEXT NOT NULL,
  status TEXT NOT NULL,
  created_from_task_id TEXT,
  safety_status TEXT NOT NULL,
  safety_summary TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lua_tool_reviews (
  review_id TEXT PRIMARY KEY,
  tool_id TEXT NOT NULL,
  reviewer_agent_id TEXT NOT NULL,
  review_type TEXT NOT NULL,
  verdict TEXT NOT NULL,
  risk_level TEXT NOT NULL,
  findings_json TEXT NOT NULL,
  summary_text TEXT NOT NULL,
  reviewed_artifact_text TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lua_tools_owner_created_at
  ON lua_tools (owner_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_lua_tools_owner_status
  ON lua_tools (owner_user_id, status, safety_status);
CREATE INDEX IF NOT EXISTS idx_lua_tool_reviews_tool_created_at
  ON lua_tool_reviews (tool_id, created_at DESC);
