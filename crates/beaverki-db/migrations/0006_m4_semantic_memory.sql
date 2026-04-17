ALTER TABLE memories ADD COLUMN memory_kind TEXT NOT NULL DEFAULT 'episodic';
ALTER TABLE memories ADD COLUMN superseded_by_memory_id TEXT;

CREATE INDEX IF NOT EXISTS idx_memories_active_semantic_subject
  ON memories (memory_kind, scope, owner_user_id, subject_type, subject_key, updated_at)
  WHERE superseded_by_memory_id IS NULL;
