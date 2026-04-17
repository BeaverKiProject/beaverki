use anyhow::Result;
use beaverki_core::{MemoryKind, MemoryScope};
use beaverki_db::{Database, MemoryRow, NewMemory};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct RetrievalScope {
    pub owner_user_id: Option<String>,
    pub visible_scopes: Vec<MemoryScope>,
    pub limit: i64,
}

#[derive(Debug, Clone)]
pub struct MemoryStore {
    db: Database,
}

#[derive(Debug, Clone)]
pub struct SemanticMemoryRecord<'a> {
    pub owner_user_id: Option<&'a str>,
    pub scope: MemoryScope,
    pub subject_type: &'a str,
    pub subject_key: &'a str,
    pub content_text: &'a str,
    pub sensitivity: &'a str,
    pub source_type: &'a str,
    pub source_ref: Option<&'a str>,
    pub source_summary: &'a str,
    pub task_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticMemoryWriteResult {
    Created { memory_id: String },
    Deduplicated { memory_id: String },
    Corrected {
        previous_memory_id: String,
        memory_id: String,
    },
}

impl MemoryStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub async fn retrieve_for_agent_context(
        &self,
        scope: &RetrievalScope,
    ) -> Result<Vec<MemoryRow>> {
        self.db
            .retrieve_memories(
                scope.owner_user_id.as_deref(),
                &scope.visible_scopes,
                scope.limit,
            )
            .await
    }

    pub async fn record_summary_memory(
        &self,
        owner_user_id: &str,
        scope: MemoryScope,
        task_id: &str,
        objective: &str,
        assistant_response: &str,
    ) -> Result<String> {
        self.db
            .insert_memory(NewMemory {
                owner_user_id: Some(owner_user_id),
                scope,
                memory_kind: MemoryKind::Episodic,
                subject_type: "summary",
                subject_key: None,
                content_text: &format_exchange_memory(objective, assistant_response),
                content_json: None,
                sensitivity: "normal",
                source_type: "summary",
                source_ref: Some("task_completion"),
                task_id: Some(task_id),
            })
            .await
    }

    pub async fn remember_semantic_memory(
        &self,
        record: SemanticMemoryRecord<'_>,
    ) -> Result<SemanticMemoryWriteResult> {
        let existing = self
            .db
            .find_active_memory_by_subject(
                record.owner_user_id,
                record.scope,
                MemoryKind::Semantic,
                record.subject_type,
                record.subject_key,
            )
            .await?;

        if let Some(ref existing) = existing
            && normalize_memory_text(&existing.content_text)
                == normalize_memory_text(record.content_text)
        {
            return Ok(SemanticMemoryWriteResult::Deduplicated {
                memory_id: existing.memory_id.clone(),
            });
        }

        let prior_memory_id = existing.as_ref().map(|memory| memory.memory_id.clone());
        let content_json = json!({
            "source_summary": record.source_summary,
            "corrected_memory_id": prior_memory_id,
        });
        let memory_id = self
            .db
            .insert_memory(NewMemory {
                owner_user_id: record.owner_user_id,
                scope: record.scope,
                memory_kind: MemoryKind::Semantic,
                subject_type: record.subject_type,
                subject_key: Some(record.subject_key),
                content_text: record.content_text,
                content_json: Some(&content_json),
                sensitivity: record.sensitivity,
                source_type: record.source_type,
                source_ref: record.source_ref,
                task_id: record.task_id,
            })
            .await?;

        if let Some(previous_memory_id) = prior_memory_id {
            self.db
                .mark_memory_superseded(&previous_memory_id, &memory_id)
                .await?;
            Ok(SemanticMemoryWriteResult::Corrected {
                previous_memory_id,
                memory_id,
            })
        } else {
            Ok(SemanticMemoryWriteResult::Created { memory_id })
        }
    }
}

fn format_exchange_memory(objective: &str, assistant_response: &str) -> String {
    format!(
        "User said: {}\nAssistant replied: {}",
        objective.trim(),
        assistant_response.trim()
    )
}

fn normalize_memory_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use beaverki_db::Database;

    use super::{
        MemoryStore, SemanticMemoryRecord, SemanticMemoryWriteResult, format_exchange_memory,
    };
    use beaverki_core::MemoryScope;

    #[test]
    fn exchange_memory_preserves_speaker_attribution() {
        let memory = format_exchange_memory("My name is Joe", "Understood. My name is Joe.");

        assert!(memory.contains("User said: My name is Joe"));
        assert!(memory.contains("Assistant replied: Understood. My name is Joe."));
    }

    #[tokio::test]
    async fn semantic_memory_correction_supersedes_prior_fact() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let db_path = tempdir.path().join("test.db");
        let db = Database::connect(&db_path).await.expect("connect");
        db.bootstrap_single_user("Alex").await.expect("bootstrap");
        let store = MemoryStore::new(db.clone());

        let first = store
            .remember_semantic_memory(SemanticMemoryRecord {
                owner_user_id: Some("user_alex"),
                scope: MemoryScope::Private,
                subject_type: "identity",
                subject_key: "profile.preferred_name",
                content_text: "Alex",
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-1"),
                source_summary: "User said their name is Alex.",
                task_id: Some("task_1"),
            })
            .await
            .expect("first semantic write");
        let second = store
            .remember_semantic_memory(SemanticMemoryRecord {
                owner_user_id: Some("user_alex"),
                scope: MemoryScope::Private,
                subject_type: "identity",
                subject_key: "profile.preferred_name",
                content_text: "Alexander",
                sensitivity: "normal",
                source_type: "user_statement",
                source_ref: Some("turn-2"),
                source_summary: "User corrected their preferred name to Alexander.",
                task_id: Some("task_2"),
            })
            .await
            .expect("corrected semantic write");

        let SemanticMemoryWriteResult::Created {
            memory_id: first_memory_id,
        } = first
        else {
            panic!("expected first write to create a memory");
        };
        let SemanticMemoryWriteResult::Corrected {
            previous_memory_id,
            memory_id,
        } = second
        else {
            panic!("expected second write to correct a memory");
        };

        assert_eq!(previous_memory_id, first_memory_id);
        let retrieved = db
            .retrieve_memories(Some("user_alex"), &[MemoryScope::Private], 10)
            .await
            .expect("retrieve memories");
        let corrected = retrieved
            .into_iter()
            .find(|row| row.memory_id == memory_id)
            .expect("corrected memory present");
        assert_eq!(corrected.content_text, "Alexander");

        let superseded = db
            .fetch_memory(&previous_memory_id)
            .await
            .expect("fetch superseded memory")
            .expect("superseded memory");
        assert_eq!(
            superseded.superseded_by_memory_id.as_deref(),
            Some(corrected.memory_id.as_str())
        );
    }
}
