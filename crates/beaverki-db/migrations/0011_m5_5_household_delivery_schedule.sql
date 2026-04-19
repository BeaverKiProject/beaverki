ALTER TABLE household_deliveries
  ADD COLUMN parent_delivery_id TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN schedule_id TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN scheduled_for_at TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN window_starts_at TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN window_ends_at TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN recurrence_rule TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN scheduled_job_state TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN materialized_task_id TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN completed_at TEXT;

ALTER TABLE household_deliveries
  ADD COLUMN canceled_at TEXT;

CREATE INDEX IF NOT EXISTS idx_household_deliveries_schedule_id_created_at
  ON household_deliveries (schedule_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_household_deliveries_status_scheduled_for_at
  ON household_deliveries (status, scheduled_for_at);

CREATE INDEX IF NOT EXISTS idx_household_deliveries_requester_mode_created_at
  ON household_deliveries (requester_user_id, delivery_mode, created_at DESC);
