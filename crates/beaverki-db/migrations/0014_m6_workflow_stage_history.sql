CREATE TABLE IF NOT EXISTS workflow_stage_versions (
  stage_version_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  version_number INTEGER NOT NULL,
  stage_index INTEGER NOT NULL,
  stage_kind TEXT NOT NULL,
  stage_label TEXT,
  artifact_ref TEXT,
  stage_config_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (workflow_id, version_number, stage_index)
);

INSERT INTO workflow_stage_versions
  (stage_version_id, workflow_id, version_number, stage_index, stage_kind, stage_label, artifact_ref, stage_config_json, created_at)
SELECT
  'workflow_stage_version_' || stage_id,
  workflow_id,
  version_number,
  stage_index,
  stage_kind,
  stage_label,
  artifact_ref,
  stage_config_json,
  created_at
FROM workflow_stages
WHERE NOT EXISTS (
  SELECT 1
  FROM workflow_stage_versions existing
  WHERE existing.workflow_id = workflow_stages.workflow_id
    AND existing.version_number = workflow_stages.version_number
    AND existing.stage_index = workflow_stages.stage_index
);

CREATE INDEX IF NOT EXISTS idx_workflow_stage_versions_workflow_version_stage
  ON workflow_stage_versions (workflow_id, version_number, stage_index ASC);
