ALTER TABLE memories ADD COLUMN forgotten_at TEXT;
ALTER TABLE memories ADD COLUMN forgotten_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_memories_active_lookup
  ON memories (scope, owner_user_id, memory_kind, subject_type, subject_key, updated_at)
  WHERE superseded_by_memory_id IS NULL AND forgotten_at IS NULL;
