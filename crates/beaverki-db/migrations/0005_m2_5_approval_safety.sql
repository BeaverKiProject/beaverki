ALTER TABLE approvals ADD COLUMN risk_level TEXT;
ALTER TABLE approvals ADD COLUMN action_summary TEXT;
ALTER TABLE approvals ADD COLUMN requester_display_name TEXT;
ALTER TABLE approvals ADD COLUMN target_details TEXT;

CREATE TABLE IF NOT EXISTS approval_actions (
  action_id TEXT PRIMARY KEY,
  approval_id TEXT NOT NULL,
  action_kind TEXT NOT NULL,
  action_token TEXT NOT NULL,
  status TEXT NOT NULL,
  issued_to_connector_type TEXT,
  issued_to_connector_identity_id TEXT,
  issued_to_channel TEXT,
  expires_at TEXT NOT NULL,
  consumed_at TEXT,
  created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_approval_actions_token ON approval_actions (action_token);
CREATE INDEX IF NOT EXISTS idx_approval_actions_approval_status ON approval_actions (approval_id, status, created_at);
