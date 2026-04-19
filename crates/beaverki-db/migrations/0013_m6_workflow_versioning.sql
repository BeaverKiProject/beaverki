ALTER TABLE workflow_definitions
  ADD COLUMN current_version_number INTEGER NOT NULL DEFAULT 1;

ALTER TABLE workflow_stages
  ADD COLUMN version_number INTEGER NOT NULL DEFAULT 1;

ALTER TABLE workflow_reviews
  ADD COLUMN version_number INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS workflow_versions (
  version_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  version_number INTEGER NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  created_from_task_id TEXT,
  created_at TEXT NOT NULL,
  UNIQUE (workflow_id, version_number)
);

INSERT INTO workflow_versions
  (version_id, workflow_id, version_number, name, description, created_from_task_id, created_at)
SELECT
  'workflow_version_' || workflow_id || '_v1',
  workflow_id,
  1,
  name,
  description,
  created_from_task_id,
  created_at
FROM workflow_definitions
WHERE NOT EXISTS (
  SELECT 1 FROM workflow_versions existing WHERE existing.workflow_id = workflow_definitions.workflow_id
);

CREATE INDEX IF NOT EXISTS idx_workflow_versions_workflow_version
  ON workflow_versions (workflow_id, version_number DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_stages_workflow_version_stage
  ON workflow_stages (workflow_id, version_number, stage_index ASC);

CREATE INDEX IF NOT EXISTS idx_workflow_reviews_workflow_version_created_at
  ON workflow_reviews (workflow_id, version_number, created_at DESC);
