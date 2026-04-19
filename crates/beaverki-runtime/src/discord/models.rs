use serde::Deserialize;
use serde_json::Value;

pub(crate) struct DiscordDirectDeliveryResult {
    pub(crate) target_kind: &'static str,
    pub(crate) target_ref: String,
    pub(crate) message_id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordGatewayInfo {
    pub(super) url: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordReadyPayload {
    pub(super) session_id: String,
    #[serde(default)]
    pub(super) resume_gateway_url: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct DiscordGatewaySessionState {
    pub(super) sequence_number: Option<i64>,
    pub(super) session_id: Option<String>,
    pub(super) resume_gateway_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DiscordGatewayConnectMode {
    Identify,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DiscordGatewaySessionOutcome {
    Shutdown,
    Reconnect,
}

impl DiscordGatewaySessionState {
    pub(super) fn connect_mode(&self) -> DiscordGatewayConnectMode {
        if self.sequence_number.is_some() && self.session_id.is_some() {
            DiscordGatewayConnectMode::Resume
        } else {
            DiscordGatewayConnectMode::Identify
        }
    }

    pub(super) fn websocket_url<'a>(&'a self, default_url: &'a str) -> &'a str {
        self.resume_gateway_url.as_deref().unwrap_or(default_url)
    }

    pub(super) fn update_ready(&mut self, ready: DiscordReadyPayload) {
        self.session_id = Some(ready.session_id);
        self.resume_gateway_url = ready.resume_gateway_url;
    }

    pub(super) fn clear_resume_state(&mut self) {
        self.sequence_number = None;
        self.session_id = None;
        self.resume_gateway_url = None;
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordRateLimitBody {
    pub(super) retry_after: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordMessageCreate {
    pub(super) id: String,
    pub(super) channel_id: String,
    pub(super) guild_id: Option<String>,
    pub(super) content: String,
    pub(super) author: DiscordAuthor,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordAuthor {
    pub(super) id: String,
    pub(super) bot: Option<bool>,
    pub(super) username: Option<String>,
    pub(super) global_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordCurrentUser {
    pub(super) id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordCreateDmResponse {
    pub(super) id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordCreatedMessage {
    pub(super) id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordRegisteredCommand {
    pub(super) name: String,
    pub(super) description: String,
    #[serde(rename = "type")]
    pub(super) command_type: i64,
    pub(super) contexts: Option<Vec<i64>>,
    #[serde(default)]
    pub(super) options: Vec<DiscordRegisteredCommandOption>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(super) struct DiscordRegisteredCommandOption {
    #[serde(rename = "type")]
    pub(super) option_type: i64,
    pub(super) name: String,
    pub(super) description: String,
    #[serde(default)]
    pub(super) required: bool,
    #[serde(default)]
    pub(super) options: Vec<DiscordRegisteredCommandOption>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordInteractionCreate {
    pub(super) id: String,
    pub(super) application_id: String,
    #[serde(rename = "type")]
    pub(super) interaction_type: i64,
    pub(super) token: String,
    pub(super) channel_id: Option<String>,
    pub(super) guild_id: Option<String>,
    pub(super) data: Option<DiscordInteractionData>,
    pub(super) member: Option<DiscordInteractionMember>,
    pub(super) user: Option<DiscordAuthor>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordInteractionMember {
    pub(super) user: Option<DiscordAuthor>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordInteractionData {
    pub(super) name: String,
    #[serde(default)]
    pub(super) options: Vec<DiscordInteractionOption>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DiscordInteractionOption {
    pub(super) name: String,
    pub(super) value: Option<Value>,
    #[serde(default)]
    pub(super) options: Vec<DiscordInteractionOption>,
}
