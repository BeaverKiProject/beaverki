CREATE TABLE IF NOT EXISTS household_deliveries (
  delivery_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  requester_user_id TEXT NOT NULL,
  requester_identity_id TEXT NOT NULL,
  recipient_user_id TEXT NOT NULL,
  message_text TEXT NOT NULL,
  delivery_mode TEXT NOT NULL,
  connector_type TEXT NOT NULL,
  connector_identity_id TEXT NOT NULL,
  target_ref TEXT NOT NULL,
  fallback_target_ref TEXT,
  dedupe_key TEXT NOT NULL,
  status TEXT NOT NULL,
  failure_reason TEXT,
  external_message_id TEXT,
  delivered_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_household_deliveries_dedupe_key
  ON household_deliveries (dedupe_key);

CREATE INDEX IF NOT EXISTS idx_household_deliveries_task_created_at
  ON household_deliveries (task_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_household_deliveries_recipient_status_created_at
  ON household_deliveries (recipient_user_id, status, created_at DESC);