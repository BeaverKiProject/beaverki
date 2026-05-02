CREATE TABLE IF NOT EXISTS session_transcript_summaries (
  summary_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  reset_boundary TEXT,
  source_through_task_created_at TEXT NOT NULL,
  source_task_count INTEGER NOT NULL,
  summary_text TEXT NOT NULL,
  estimated_tokens INTEGER NOT NULL,
  summarizer_task_id TEXT,
  superseded_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (session_id) REFERENCES conversation_sessions (session_id),
  FOREIGN KEY (summarizer_task_id) REFERENCES tasks (task_id)
);

CREATE INDEX IF NOT EXISTS idx_session_transcript_summaries_active
  ON session_transcript_summaries (session_id, reset_boundary, superseded_at, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_transcript_summaries_session_created
  ON session_transcript_summaries (session_id, created_at DESC);