use beaverki_db::{
    ApprovalActionRow, ApprovalRow, ConversationSessionRow, LuaToolRow, MemoryRow, ScheduleRow,
    ScriptReviewRow, ScriptRow, TaskEventRow, TaskRow, ToolInvocationRow, UserRow,
    WorkflowDefinitionRow, WorkflowReviewRow, WorkflowRunRow, WorkflowStageRow, WorkflowVersionRow,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInspection {
    pub task: TaskRow,
    pub events: Vec<TaskEventRow>,
    pub tool_invocations: Vec<ToolInvocationRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptInspection {
    pub script: ScriptRow,
    pub reviews: Vec<ScriptReviewRow>,
    pub schedules: Vec<ScheduleRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInspection {
    pub workflow: WorkflowDefinitionRow,
    pub versions: Vec<WorkflowVersionRow>,
    pub stages: Vec<WorkflowStageRow>,
    pub reviews: Vec<WorkflowReviewRow>,
    pub schedules: Vec<ScheduleRow>,
    pub runs: Vec<WorkflowRunRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinitionInput {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub stages: Vec<WorkflowStageInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStageInput {
    pub kind: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub artifact_ref: Option<String>,
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInspection {
    pub memory: MemoryRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLifecycleExecution {
    pub session_id: String,
    pub policy_id: String,
    pub action: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSummary {
    pub user: UserRow,
    pub role_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session: ConversationSessionRow,
    pub owner_user_ids: Vec<String>,
    pub task_count: i64,
    pub matching_policy_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationCatalog {
    pub scripts: Vec<ScriptRow>,
    pub lua_tools: Vec<LuaToolRow>,
    pub workflows: Vec<WorkflowDefinitionRow>,
    pub schedules: Vec<ScheduleRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteApprovalActionOutcome {
    Inspection {
        action: ApprovalActionRow,
        approval: ApprovalRow,
    },
    StepUpRequired {
        action: ApprovalActionRow,
        approval: ApprovalRow,
        confirm_action: ApprovalActionRow,
    },
    Resolved {
        action: ApprovalActionRow,
        task: TaskRow,
    },
}
