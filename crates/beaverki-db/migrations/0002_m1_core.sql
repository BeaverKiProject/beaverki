CREATE TABLE IF NOT EXISTS roles (
  role_id TEXT PRIMARY KEY,
  description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_roles (
  user_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS approvals (
  approval_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  target_ref TEXT,
  requested_by_agent_id TEXT NOT NULL,
  requested_from_user_id TEXT NOT NULL,
  status TEXT NOT NULL,
  rationale_text TEXT,
  decided_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_identities (
  identity_id TEXT PRIMARY KEY,
  connector_type TEXT NOT NULL,
  external_user_id TEXT NOT NULL,
  external_channel_id TEXT,
  mapped_user_id TEXT NOT NULL,
  trust_level TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON user_roles (user_id);
CREATE INDEX IF NOT EXISTS idx_approvals_requested_from_status ON approvals (requested_from_user_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_approvals_task_status ON approvals (task_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_connector_identities_lookup ON connector_identities (connector_type, external_user_id);

INSERT OR IGNORE INTO roles (role_id, description) VALUES
  ('owner', 'Full administrative control over runtime, tools, and approvals.'),
  ('adult', 'Trusted household user with household access and approval rights.'),
  ('child', 'Restricted household user with private-only access by default.'),
  ('guest', 'Temporary restricted user with private-only access.'),
  ('service', 'Scoped service identity for background automation.');
