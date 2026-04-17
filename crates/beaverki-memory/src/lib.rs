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
        objective: &str,
        assistant_response: &str,
    ) -> Result<String> {
        self.db
            .insert_memory(NewMemory {
                owner_user_id: Some(owner_user_id),
                scope,
                subject_type: "summary",
                subject_key: None,
                content_text: &format_exchange_memory(objective, assistant_response),
                sensitivity: "normal",
                source_type: "summary",
                source_ref: Some("task_completion"),
                task_id: Some(task_id),
            })
            .await
    }
}

fn format_exchange_memory(objective: &str, assistant_response: &str) -> String {
    format!(
        "User said: {}\nAssistant replied: {}",
        objective.trim(),
        assistant_response.trim()
    )
}

#[cfg(test)]
mod tests {
    use super::format_exchange_memory;

    #[test]
    fn exchange_memory_preserves_speaker_attribution() {
        let memory = format_exchange_memory("My name is Joe", "Understood. My name is Joe.");

        assert!(memory.contains("User said: My name is Joe"));
        assert!(memory.contains("Assistant replied: Understood. My name is Joe."));
    }
}
