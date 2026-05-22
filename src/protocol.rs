// Wire types for the client protocol. These are what Wicket sends to Puzzle
// and Shotgun. The format is richer than what the renderer needs today —
// clients ignore what they don't use, and the protocol doesn't change when
// they start needing it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// -- Outbound: Wicket → Client --

#[derive(Debug, Clone, Serialize)]
pub struct NormalizedEntry {
    pub kind: &'static str,
    pub blocks: Vec<ContentBlock>,
    pub uuid: Option<String>,
    pub seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ContentBlock {
    Thinking { text: String },
    Text { text: String },
    ToolUse {
        name: String,
        input_summary: String,
        input: Value,
    },
    ToolResult {
        content: String,
        is_error: bool,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEvent {
    RoundStarted,
    RoundCompleted,
    RoundFailed { message: String },
}

// -- Inbound: Client → Wicket --

#[derive(Debug, Deserialize)]
pub struct InboundEnvelope {
    pub stream: String,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
pub struct ConnectPayload {
    pub slug: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClaudeMessage {
    pub message: String,
    #[serde(default)]
    pub remote: Option<String>,
    #[serde(default)]
    pub yolo: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApprovalDecision {
    pub behavior: String,
    #[serde(default)]
    pub message: Option<String>,
}
