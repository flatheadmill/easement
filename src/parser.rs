// Claude Code JSONL transcript format. Moved from Puzzle — Wicket now owns parsing. The types
// model the full schema faithfully so deserialization succeeds for all entry types, even those we
// filter out. The serde strategy: #[serde(tag = "type")] for the top-level enum,
// #[serde(untagged)] for UserContent (bare string or array), #[serde(other)] as catch-all.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)]
pub enum Entry {
    User(UserEntry),
    Assistant(AssistantEntry),
    System(SystemEntry),
    Progress(ProgressEntry),
    Summary(SummaryEntry),
    FileHistorySnapshot(FileHistorySnapshotEntry),
    QueueOperation(QueueOperationEntry),
    CustomTitle(CustomTitleEntry),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct UserEntry {
    pub message: UserMessage,
    pub uuid: Option<String>,
    pub timestamp: Option<String>,
    pub parent_uuid: Option<String>,
    #[serde(default)]
    pub is_sidechain: bool,
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct UserMessage {
    pub role: String,
    pub content: UserContent,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum UserContent {
    Text(String),
    Blocks(Vec<UserContentBlock>),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum UserContentBlock {
    ToolResult(ToolResultBlock),
    Text(TextBlock),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ToolResultBlock {
    pub tool_use_id: Option<String>,
    pub content: Option<ToolResultContent>,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum ToolResultContent {
    Text(String),
    Blocks(Vec<Value>),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AssistantEntry {
    pub message: AssistantMessage,
    pub uuid: Option<String>,
    pub timestamp: Option<String>,
    pub parent_uuid: Option<String>,
    #[serde(default)]
    pub is_sidechain: bool,
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct AssistantMessage {
    pub role: Option<String>,
    pub content: Vec<AssistantContentBlock>,
    pub model: Option<String>,
    pub stop_reason: Option<String>,
    pub usage: Option<Usage>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum AssistantContentBlock {
    Thinking(ThinkingBlock),
    Text(TextBlock),
    ToolUse(ToolUseBlock),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ThinkingBlock {
    pub thinking: String,
    pub signature: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TextBlock {
    pub text: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ToolUseBlock {
    pub id: Option<String>,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SystemEntry {
    pub subtype: Option<String>,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ProgressEntry {
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SummaryEntry {
    pub summary: Option<String>,
    pub leaf_uuid: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FileHistorySnapshotEntry {
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct QueueOperationEntry {
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct CustomTitleEntry {
    pub custom_title: Option<String>,
    pub session_id: Option<String>,
}

pub fn parse_line(line: &str) -> Option<Entry> {
    match serde_json::from_str::<Entry>(line) {
        Ok(entry) => Some(entry),
        Err(e) => {
            tracing::warn!("parse error: {}", e);
            None
        }
    }
}
