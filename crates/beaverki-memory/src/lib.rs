use anyhow::Result;
use beaverki_core::MemoryScope;
use beaverki_db::{Database, MemoryRow, NewMemory};

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
        summary: &str,
    ) -> Result<String> {
        self.db
            .insert_memory(NewMemory {
                owner_user_id: Some(owner_user_id),
                scope,
                subject_type: "summary",
                subject_key: None,
                content_text: summary,
                sensitivity: "normal",
                source_type: "summary",
                source_ref: Some("task_completion"),
                task_id: Some(task_id),
            })
            .await
    }
}
