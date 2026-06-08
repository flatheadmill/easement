// Wicket: WebSocket and HTTP server on port 6502.
//
// Clients (Puzzle, Shotgun) connect over WebSocket. Claude's MCP approval requests arrive over
// HTTP at /mcp/<slug>. One process, one port.
//
// Each slug gets its own coordinator task that manages the ClaudePrint lifecycle, transcript
// persistence, and client broadcasting. Clients register with the coordinator for their slug and
// receive normalized entries and lifecycle events.

use std::collections::{BinaryHeap, HashMap};
use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

// NDJSON log. One channel, one file, queryable with jq. The broadcast channel sheds load
// automatically and reports how many messages were dropped via RecvError::Lagged(n).

#[derive(Clone, Serialize)]
struct LogMessage {
    when: String,
    level: u8,
    who: &'static str,
    what: &'static str,
    why: &'static str,
    #[serde(flatten)]
    payload: Value,
}

#[derive(Serialize)]
struct LogEntry {
    when: String,
    what: LogMessage,
}

static LOG: OnceLock<broadcast::Sender<LogMessage>> = OnceLock::new();

fn log(level: u8, msg: LogMessage) {
    if let Some(tx) = LOG.get() {
        let _ = tx.send(LogMessage { level, ..msg });
    }
}

macro_rules! trace {
    ($who:expr, $what:expr, $why:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(0, LogMessage {
            when: now(),
            level: 0,
            who: $who,
            what: $what,
            why: $why,
            payload: serde_json::json!({ $($key: $val),* }),
        })
    };
}

macro_rules! wire {
    ($who:expr, $what:expr, $why:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(1, LogMessage {
            when: now(),
            level: 1,
            who: $who,
            what: $what,
            why: $why,
            payload: serde_json::json!({ $($key: $val),* }),
        })
    };
}

macro_rules! dump {
    ($who:expr, $what:expr, $why:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(2, LogMessage {
            when: now(),
            level: 2,
            who: $who,
            what: $what,
            why: $why,
            payload: serde_json::json!({ $($key: $val),* }),
        })
    };
}

macro_rules! error {
    ($who:expr, $what:expr, $how:expr, $error:expr $(, $key:tt: $val:expr)* $(,)?) => {
        crate::log(0, LogMessage {
            when: now(),
            level: 0,
            who: $who,
            what: $what,
            why: "error",
            payload: serde_json::json!({ "how": $how, "error": $error.to_string() $(, $key: $val)* }),
        })
    };
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn init_log() {
    let home = env::var("HOME").expect("HOME not set");
    let log_dir = PathBuf::from(&home).join(".local/state/easement");
    let _ = std::fs::create_dir_all(&log_dir);

    let port = easement_port();
    let log_name = if port == 6502 {
        "easement.jsonl".to_string()
    } else {
        format!("easement-{}.jsonl", port)
    };
    let log_path = log_dir.join(log_name);

    let (tx, _) = broadcast::channel::<LogMessage>(4096);
    let mut rx = tx.subscribe();
    LOG.set(tx).expect("log already initialized");

    tokio::spawn(async move {
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("cannot open log file {}: {}", log_path.display(), e);
                return;
            }
        };

        use tokio::io::AsyncWriteExt;
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    let entry = LogEntry {
                        when: now(),
                        what: msg,
                    };
                    if let Ok(mut line) = serde_json::to_string(&entry) {
                        line.push('\n');
                        let _ = file.write_all(line.as_bytes()).await;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let shed = LogEntry {
                        when: now(),
                        what: LogMessage {
                            when: now(),
                            level: 0,
                            who: "log",
                            what: "lifecycle",
                            why: "shed",
                            payload: json!({ "count": n }),
                        },
                    };
                    if let Ok(mut line) = serde_json::to_string(&shed) {
                        line.push('\n');
                        let _ = file.write_all(line.as_bytes()).await;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

// Stdout events from the Claude CLI. Classified in the claudep select loop to drive the drain
// gate, capture session IDs, and broadcast deltas. The #[serde(other)] Unknown variant absorbs
// event types we do not act on so deserialization never fails.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
enum StdoutEvent {
    Result {
        subtype: Option<String>,
        #[serde(default)]
        is_error: bool,
        duration_ms: Option<u64>,
        num_turns: Option<u64>,
        result: Option<String>,
        session_id: Option<String>,
    },
    System {
        subtype: Option<String>,
        session_id: Option<String>,
    },
    Assistant {
        message: Value,
        session_id: String,
        uuid: String,
    },
    User {
        message: Value,
        session_id: String,
        #[serde(default)]
        #[serde(rename = "isReplay")]
        is_replay: bool,
    },
    StreamEvent {
        event: Value,
        session_id: String,
    },
    RateLimitEvent {
        rate_limit_info: Value,
    },
    #[serde(other)]
    Unknown,
}

// Transcript entry types from the Claude CLI's JSONL files and the Codex TUI's
// JSON-RPC messages.
//
// Fields are required unless there is a specific reason for Option. A field being
// absent in some CLI version is not a reason to make it optional -- it is a reason
// to find out why it is absent. Option<T> on a struct field propagates None checks
// into every function that touches the value, and each check is a silent decision
// to continue without data that should be there. When uuid is Option<String>, a
// user entry with no uuid passes deserialization, passes normalization, enters the
// transcript, and breaks chain validation later or never. When uuid is String, a
// user entry with no uuid fails deserialization at the boundary, the parse_entry
// call returns None, and we know immediately.
//
// We do not control these formats. The CLI and the TUI change across versions and
// we discover their behavior through observation, not documentation. But making a
// field required is not a claim that it will always be present. It is an assertion
// that our code depends on it being present. When the CLI changes and the assertion
// fires, we learn about it at the parse boundary instead of discovering corruption
// downstream. A panic from a missing field is a ten-second fix. Silent propagation
// of None through ten functions is a four-hour investigation.
//
// Legitimately optional fields do exist. parentUuid is absent on root entries.
// duration_ms is absent on incomplete results. Booleans default to false via
// #[serde(default)]. The #[serde(other)] Unknown variant absorbs entry types we
// have not observed yet. These are design decisions, not defensive programming.

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "kebab-case")]
#[allow(dead_code)]
enum Entry {
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

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct UserEntry {
    message: UserMessage,
    uuid: String,
    timestamp: String,
    parent_uuid: Option<String>,
    #[serde(default)]
    is_sidechain: bool,
    session_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct UserMessage {
    role: String,
    content: UserContent,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum UserContent {
    Text(String),
    Blocks(Vec<UserContentBlock>),
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UserContentBlock {
    ToolResult(ToolResultBlock),
    Text(TextBlock),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct ToolResultBlock {
    tool_use_id: String,
    content: Option<ToolResultContent>,
    #[serde(default)]
    is_error: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum ToolResultContent {
    Text(String),
    Blocks(Vec<Value>),
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct AssistantEntry {
    message: AssistantMessage,
    uuid: String,
    timestamp: String,
    parent_uuid: Option<String>,
    #[serde(default)]
    is_sidechain: bool,
    session_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct AssistantMessage {
    role: String,
    content: Vec<AssistantContentBlock>,
    model: Option<String>,
    stop_reason: Option<String>,
    usage: Option<TranscriptUsage>,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct TranscriptUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum AssistantContentBlock {
    Thinking(ThinkingBlock),
    Text(TextBlock),
    ToolUse(ToolUseBlock),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct ThinkingBlock {
    thinking: String,
    signature: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct TextBlock {
    text: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct ToolUseBlock {
    id: String,
    name: String,
    input: Value,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct SystemEntry {
    subtype: Option<String>,
    #[serde(flatten)]
    extra: Value,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct ProgressEntry {
    #[serde(flatten)]
    extra: Value,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct SummaryEntry {
    summary: String,
    leaf_uuid: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct FileHistorySnapshotEntry {
    #[serde(flatten)]
    extra: Value,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct QueueOperationEntry {
    #[serde(flatten)]
    extra: Value,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct CustomTitleEntry {
    custom_title: String,
    session_id: String,
}

fn parse_entry(line: &str) -> Option<Entry> {
    match serde_json::from_str::<Entry>(line) {
        Ok(entry) => Some(entry),
        Err(e) => {
            trace!("easement", "parser", "error", "error": e.to_string());
            None
        }
    }
}

// Converts parsed transcript entries into the broadcast format. Filters sidechains
// and noise entry types. Output uses who/what with content blocks.

#[derive(Clone, Serialize)]
struct NormalizedBlock {
    r#type: String,
    #[serde(flatten)]
    fields: Value,
}

#[derive(Clone, Serialize)]
struct NormalizedEntry {
    who: &'static str,
    what: &'static str,
    blocks: Vec<NormalizedBlock>,
    uuid: String,
    notification: bool,
}

fn split_user_message_meta(text: &str) -> (&str, bool) {
    match text.split_once('\x07') {
        Some((visible, _meta)) => (visible, true),
        None => (text, false),
    }
}

fn join_user_message_meta(message: String, meta: Option<String>) -> String {
    match meta {
        Some(meta) if !meta.is_empty() => format!("{}\x07 {}", message, meta),
        _ => message,
    }
}

fn normalize(entry: Entry) -> Option<NormalizedEntry> {
    match entry {
        Entry::User(user) if !user.is_sidechain => normalize_user(user),
        Entry::Assistant(assistant) if !assistant.is_sidechain => normalize_assistant(assistant),
        _ => None,
    }
}

fn normalize_user(entry: UserEntry) -> Option<NormalizedEntry> {
    let mut notification = false;
    let blocks = match entry.message.content {
        UserContent::Text(text) => {
            let (visible, is_notification) = split_user_message_meta(&text);
            notification |= is_notification;
            vec![NormalizedBlock {
                r#type: "text".to_string(),
                fields: json!({ "text": visible }),
            }]
        }
        UserContent::Blocks(content_blocks) => {
            let mut blocks = Vec::new();
            for block in content_blocks {
                match block {
                    UserContentBlock::ToolResult(ToolResultBlock {
                        content, is_error, ..
                    }) => {
                        let text = match content {
                            Some(ToolResultContent::Text(s)) => s,
                            Some(ToolResultContent::Blocks(parts)) => parts
                                .iter()
                                .filter_map(|b| {
                                    b.get("text")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string())
                                })
                                .collect::<Vec<_>>()
                                .join("\n"),
                            None => String::new(),
                        };
                        blocks.push(NormalizedBlock {
                            r#type: "tool_result".to_string(),
                            fields: json!({ "content": text, "is_error": is_error }),
                        });
                    }
                    UserContentBlock::Text(TextBlock { text }) => {
                        let (visible, is_notification) = split_user_message_meta(&text);
                        notification |= is_notification;
                        blocks.push(NormalizedBlock {
                            r#type: "text".to_string(),
                            fields: json!({ "text": visible }),
                        });
                    }
                    UserContentBlock::Unknown => {}
                }
            }
            blocks
        }
    };

    if blocks.is_empty() {
        return None;
    }

    Some(NormalizedEntry {
        who: "user",
        what: "message",
        blocks,
        uuid: entry.uuid,
        notification,
    })
}

fn normalize_assistant(entry: AssistantEntry) -> Option<NormalizedEntry> {
    let mut blocks = Vec::new();

    for block in entry.message.content {
        match block {
            AssistantContentBlock::Thinking(ThinkingBlock { thinking, .. }) => {
                blocks.push(NormalizedBlock {
                    r#type: "thinking".to_string(),
                    fields: json!({ "text": thinking }),
                });
            }
            AssistantContentBlock::Text(TextBlock { text }) => {
                blocks.push(NormalizedBlock {
                    r#type: "text".to_string(),
                    fields: json!({ "text": text }),
                });
            }
            AssistantContentBlock::ToolUse(ToolUseBlock { name, input, .. }) => {
                blocks.push(NormalizedBlock {
                    r#type: "tool_use".to_string(),
                    fields: json!({ "name": name, "input": input }),
                });
            }
            AssistantContentBlock::Unknown => {}
        }
    }

    if blocks.is_empty() {
        return None;
    }

    Some(NormalizedEntry {
        who: "assistant",
        what: "message",
        blocks,
        uuid: entry.uuid,
        notification: false,
    })
}

#[derive(Debug, Serialize)]
struct StdinUserMessage {
    r#type: &'static str,
    message: StdinUserMessageContent,
    uuid: String,
}

#[derive(Debug, Serialize)]
struct StdinUserMessageContent {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct StdinUserContentMessage {
    r#type: &'static str,
    message: StdinUserContentMessageContent,
    uuid: String,
}

#[derive(Debug, Serialize)]
struct StdinUserContentMessageContent {
    role: &'static str,
    content: Value,
}

fn format_user_message(content: &str) -> String {
    let msg = StdinUserMessage {
        r#type: "user",
        message: StdinUserMessageContent {
            role: "user",
            content: content.to_string(),
        },
        uuid: uuid::Uuid::new_v4().to_string(),
    };
    let mut s = serde_json::to_string(&msg).expect("UserMessage serialization cannot fail");
    s.push('\n');
    s
}

fn format_user_content_message(content: Value) -> String {
    let msg = StdinUserContentMessage {
        r#type: "user",
        message: StdinUserContentMessageContent {
            role: "user",
            content,
        },
        uuid: uuid::Uuid::new_v4().to_string(),
    };
    let mut s = serde_json::to_string(&msg).expect("UserMessage serialization cannot fail");
    s.push('\n');
    s
}

fn format_interrupt_message() -> String {
    let msg = json!({
        "type": "control_request",
        "request_id": uuid::Uuid::new_v4().to_string(),
        "request": { "subtype": "interrupt" },
    });
    let mut s = serde_json::to_string(&msg).expect("ControlRequest serialization cannot fail");
    s.push('\n');
    s
}

enum StdoutLine {
    Json(Value),
    Eof,
}

enum TranscriptLine {
    Entry(Value),
}

fn find_transcript_file(session_id: &str) -> Option<PathBuf> {
    let home = env::var("HOME").unwrap_or_default();
    let projects_dir = Path::new(&home).join(".claude").join("projects");
    let target = format!("{}.jsonl", session_id);
    for entry in std::fs::read_dir(&projects_dir).ok()? {
        let entry = entry.ok()?;
        if entry.file_type().ok()?.is_dir() {
            let candidate = entry.path().join(&target);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }
    None
}

fn cli_transcript_path(slug: &str, session_uuid: &str) -> PathBuf {
    let home = env::var("HOME").unwrap_or_default();
    let pane_dir = Path::new(&home).join("pane").join(slug);
    let dir_slug = pane_dir.to_string_lossy().replace('/', "-");
    Path::new(&home)
        .join(".claude")
        .join("projects")
        .join(&dir_slug)
        .join(format!("{}.jsonl", session_uuid))
}

fn emplace_transcript(path: &Path, entries: &[Value]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut content = String::new();
    for entry in entries {
        if let Ok(line) = serde_json::to_string(entry) {
            content.push_str(&line);
            content.push('\n');
        }
    }
    std::fs::write(path, &content).map_err(|e| format!("cannot write emplaced transcript: {}", e))
}

fn extract_session_uuid(entries: &[Value]) -> Option<String> {
    entries.iter().find_map(|e| {
        e.get("sessionId")
            .and_then(|v| v.as_str())
            .filter(|s| uuid::Uuid::parse_str(s).is_ok())
            .map(|s| s.to_string())
    })
}

async fn resolve_transcript(slug: &str, intent: &str) -> Option<String> {
    let home = env::var("HOME").unwrap_or_default();
    let dir = PathBuf::from(&home)
        .join(".local/state/easement")
        .join(slug);
    tokio::fs::create_dir_all(&dir)
        .await
        .unwrap_or_else(|e| panic!("cannot create transcript dir {}: {}", dir.display(), e));
    let ts_pattern = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.jsonl$").ok()?;
    let mut transcripts: Vec<String> = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str() {
            if ts_pattern.is_match(name) {
                transcripts.push(name.trim_end_matches(".jsonl").to_string());
            }
        }
    }
    transcripts.sort();
    match intent {
        "new" => {
            let ts = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
            let path = dir.join(format!("{}.jsonl", ts));
            tokio::fs::File::create(&path)
                .await
                .unwrap_or_else(|e| panic!("cannot create transcript {}: {}", path.display(), e));
            Some(ts)
        }
        "full" => {
            if transcripts.len() >= 2 {
                Some(transcripts[transcripts.len() - 2].clone())
            } else {
                None
            }
        }
        _ => {
            if transcripts.is_empty() {
                let ts = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
                let path = dir.join(format!("{}.jsonl", ts));
                tokio::fs::File::create(&path).await.unwrap_or_else(|e| {
                    panic!("cannot create transcript {}: {}", path.display(), e)
                });
                Some(ts)
            } else {
                transcripts.last().cloned()
            }
        }
    }
}

struct ToolResult {
    output: String,
    exit_code: i32,
}

#[derive(Serialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Broadcast {
    History {
        slug: String,
        transcript: String,
        #[serde(flatten)]
        event: HistoryBroadcast,
    },
    Delta {
        slug: String,
        transcript: String,
        event: Value,
    },
    Usage {
        slug: String,
        transcript: String,
        #[serde(flatten)]
        usage: Value,
    },
    Turn {
        slug: String,
        transcript: String,
        #[serde(flatten)]
        event: TurnBroadcast,
    },
    UserMessage {
        slug: String,
        transcript: String,
        text: String,
        notification: bool,
    },
    ToolResult {
        slug: String,
        transcript: String,
        tool_use_id: String,
        output: String,
        #[serde(default)]
        is_error: bool,
    },
}

#[derive(Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum HistoryBroadcast {
    Begin {
        replay_id: String,
        last_uuid: Option<String>,
    },
    Entry {
        replay_id: String,
        entry: Value,
    },
}

#[derive(Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum TurnBroadcast {
    Started { turn_id: String },
    Completed { turn_id: String, status: String },
}

#[derive(Serialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Dispatch {
    Tool {
        slug: String,
        transcript: String,
        #[serde(flatten)]
        event: ToolDispatch,
    },
    Shell {
        slug: String,
        transcript: String,
        #[serde(flatten)]
        event: ShellDispatch,
    },
}

#[derive(Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ToolDispatch {
    Run {
        call_id: String,
        #[serde(flatten)]
        args: Value,
    },
}

#[derive(Serialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ShellDispatch {
    Run {
        id: String,
        command: String,
        r#where: String,
    },
}

fn dispatch(tx: &mpsc::UnboundedSender<String>, msg: Dispatch) {
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = tx.send(json);
    }
}

fn broadcast(tx: &broadcast::Sender<String>, msg: Broadcast) {
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = tx.send(json);
    }
}

const STEER_SENTINEL: &str = "\n\x07---\n";

// Built-in CLI tools we replace with our own MCP tools. Claude never sees these.
const DISALLOWED_TOOLS: &[&str] = &[
    "Bash",
    "Write",
    "Edit",
    "Read",
    "Glob",
    "Grep",
    "Skill",
    "ToolSearch",
    "NotebookEdit",
    "WebFetch",
    "WebSearch",
    "CronCreate",
    "CronDelete",
    "CronList",
    "RemoteTrigger",
    "TaskOutput",
    "TaskStop",
    "EnterWorktree",
    "ExitWorktree",
    "ExitPlanMode",
    "Monitor",
    "PushNotification",
    "AskUserQuestion",
    "ScheduleWakeup",
    "ShareOnboardingGuide",
];

#[derive(Debug, serde::Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

fn jsonrpc_response(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: Some(result),
        error: None,
    }
}

fn jsonrpc_error(id: Value, code: i32, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(JsonRpcError { code, message }),
    }
}

fn make_json_response(resp: JsonRpcResponse) -> Response<Full<Bytes>> {
    let json = serde_json::to_string(&resp).unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(json)))
        .unwrap()
}

// One claudep task per window (slug + transcript). Loads the transcript, emplaces it, spawns
// Claude, and enters the select loop. History replay comes from the already-loaded entries.
// Claude is running because Easement is a live authorized bridge around a CLI session, not a
// transcript server. If all anyone wants is the history they can jq the transcript file. The
// reconciliation after each round adapts to whatever the CLI did to the chain, so the logic
// lives here next to the code that deals with the consequences, not in a separate module.
async fn claudep(
    slug: &str,
    transcript: &str,
    mut claude_rx: mpsc::UnboundedReceiver<ClaudeEvent>,
    broadcast_tx: broadcast::Sender<String>,
    main_tx: mpsc::UnboundedSender<MainEvent>,
    cancel: tokio_util::sync::CancellationToken,
) {
    trace!("easement", "claudep", "started", "slug": slug, "transcript": transcript);
    let home = env::var("HOME").unwrap_or_default();

    // Slurp our transcript. If the file does not exist, abend -- the caller should have created
    // it (even zero-length for a new session).
    let transcript_path = PathBuf::from(&home)
        .join(".local/state/easement")
        .join(slug)
        .join(format!("{}.jsonl", transcript));
    let content = tokio::fs::read_to_string(&transcript_path)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "transcript does not exist at {}: {}",
                transcript_path.display(),
                e
            )
        });

    let mut entries: Vec<Value> = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(data) = serde_json::from_str::<Value>(line) {
            entries.push(data);
        }
    }

    // Validate the chain. Entries under our control (entrypoint sdk-cli) must chain correctly.
    // Entries from the CLI (entrypoint cli) predate our management and may have branches we
    // cannot validate.
    let mut prev_uuid: Option<String> = None;
    for data in &entries {
        let entrypoint = data
            .get("entrypoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if entrypoint == "cli" {
            break;
        }
        let uuid = data.get("uuid").and_then(|v| v.as_str());
        let parent = data.get("parentUuid").and_then(|v| v.as_str());
        if let Some(u) = uuid {
            if let (Some(p), Some(prev)) = (parent, prev_uuid.as_deref()) {
                if p != prev {
                    trace!("easement", "transcript", "branch",
                        "uuid": u, "parent": p, "head": prev);
                }
            }
            prev_uuid = Some(u.to_string());
        }
    }

    let mut session_uuid = extract_session_uuid(&entries);
    let mut seen_uuids: std::collections::HashSet<String> = entries
        .iter()
        .filter_map(|e| {
            e.get("uuid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    trace!("easement", "claudep", "loaded", "slug": slug, "transcript": transcript, "entries": entries.len(), "session_uuid": session_uuid);

    // claudep organizes transcripts by working directory. The pane directory is the cwd for the
    // round and determines the project slug in ~/.claude/projects/.
    let pane_dir = PathBuf::from(&home).join("pane").join(slug);
    let _ = tokio::fs::create_dir_all(&pane_dir).await;
    std::env::set_current_dir(&pane_dir)
        .unwrap_or_else(|e| panic!("cannot cd to {}: {}", pane_dir.display(), e));

    // Trust injection -- write hasTrustDialogAccepted into ~/.claude.json so claudep does not hang
    // waiting for interactive approval.
    let config_path = PathBuf::from(&home).join(".claude.json");
    let directory = pane_dir.to_str().unwrap_or("");
    {
        let lock_path = config_path.with_extension("json.lock");
        let locked = tokio::fs::create_dir(&lock_path).await.is_ok();
        if locked {
            async {
                let mut config: Value = match tokio::fs::read_to_string(&config_path).await {
                    Ok(content) => serde_json::from_str(&content).unwrap_or_else(|e| {
                        panic!("cannot parse {}: {}", config_path.display(), e)
                    }),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Value::Object(serde_json::Map::new())
                    }
                    Err(e) => panic!("cannot read {}: {}", config_path.display(), e),
                };

                let already = config
                    .get("projects")
                    .and_then(|p| p.get(directory))
                    .and_then(|e| e.get("hasTrustDialogAccepted"))
                    .and_then(|v| v.as_bool())
                    == Some(true);

                if !already {
                    let obj = config.as_object_mut().expect("config not an object");
                    let projects = obj
                        .entry("projects")
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    let project = projects
                        .as_object_mut()
                        .expect("projects not an object")
                        .entry(directory)
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    project
                        .as_object_mut()
                        .expect("project entry not an object")
                        .insert("hasTrustDialogAccepted".to_string(), Value::Bool(true));

                    let content = serde_json::to_string_pretty(&config).expect("unreachable");
                    tokio::fs::write(&config_path, &content)
                        .await
                        .unwrap_or_else(|e| {
                            panic!("cannot write {}: {}", config_path.display(), e)
                        });

                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = tokio::fs::set_permissions(
                            &config_path,
                            std::fs::Permissions::from_mode(0o600),
                        )
                        .await;
                    }

                    trace!("easement", "claudep", "trust_injected");
                }
            }
            .await;

            let _ = tokio::fs::remove_dir(&lock_path).await;
        } else {
            panic!("trust lock contention");
        }
    }

    let resume_arg: Option<String> = if let Some(ref uuid) = session_uuid {
        assert!(
            !entries.is_empty(),
            "session uuid {} with no transcript entries",
            uuid
        );
        let cli_path = cli_transcript_path(slug, uuid);
        emplace_transcript(&cli_path, &entries)
            .unwrap_or_else(|e| panic!("emplacement failed: {}", e));
        trace!("easement", "claudep", "emplaced", "uuid": uuid, "path": cli_path.display().to_string(), "entries": entries.len());
        Some(uuid.clone())
    } else {
        None
    };

    trace!("easement", "claudep", "spawning", "resume_arg": format!("{:?}", resume_arg));

    // Wicket's seatbelt/bwrap policy is the permission system. Claude Code's
    // approval classifier treats Wicket-mediated reads and shell commands as
    // attempts to bypass denied built-in tools, so skip that layer entirely.
    let mut cmd = Command::new("claude");
    cmd.env("MCP_TOOL_TIMEOUT", "2147483647");
    cmd.arg("--print")
        .arg("--input-format")
        .arg("stream-json")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--replay-user-messages")
        .arg("--verbose")
        .arg("--model")
        .arg("claude-opus-4-6[1m]")
        .arg("--thinking-display")
        .arg("summarized")
        .arg("--max-thinking-tokens")
        .arg("31999")
        .arg("--add-dir")
        .arg(format!("{}/code", home))
        .arg("--dangerously-skip-permissions")
        .arg("--disallowed-tools")
        .arg(DISALLOWED_TOOLS.join(","));

    if let Some(ref ra) = resume_arg {
        cmd.arg("--resume").arg(ra);
    }

    // The MCP config could be shared across all claudep instances for a slug -- the URL contains
    // the slug and transcript but the transcript could be resolved server-side. One file per slug
    // instead of one per round.
    let mcp_config_dir = PathBuf::from(&home)
        .join(".local/state/easement")
        .join(slug);
    let mcp_config_path = mcp_config_dir.join("mcp.json");
    let mcp_config = json!({
        "mcpServers": {
            "o": {
                "type": "http",
                "url": format!("http://localhost:{}/mcp/{}/{}", easement_port(), slug, transcript)
            }
        }
    });
    tokio::fs::write(&mcp_config_path, mcp_config.to_string())
        .await
        .unwrap_or_else(|e| panic!("cannot write mcp config: {}", e));
    cmd.arg("--mcp-config").arg(&mcp_config_path);

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("cannot spawn claude: {}", e));

    let child_stdin = child.stdin.take().expect("stdin was piped");
    let child_stdout = child.stdout.take().expect("stdout was piped");
    let child_stderr = child.stderr.take().expect("stderr was piped");

    // Stderr logger.
    tokio::spawn(async move {
        let mut reader = BufReader::new(child_stderr);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        dump!("easement", "claudep", "stderr", "line": trimmed);
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Stdout reader task. Reads claudep stdout and pushes typed events onto
    // Stdout reader. Parses JSON, sends the Value. Classification happens in
    // the claudep loop where the logic lives.
    let (stdout_tx, mut stdout_rx) = mpsc::channel::<StdoutLine>(256);

    tokio::spawn(async move {
        let mut reader = BufReader::new(child_stdout);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    let _ = stdout_tx.send(StdoutLine::Eof).await;
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(data) => {
                            let _ = stdout_tx.send(StdoutLine::Json(data)).await;
                        }
                        Err(e) => {
                            error!("easement", "claudep", "stdout_invalid_json", e);
                        }
                    }
                }
                Err(e) => {
                    error!("easement", "claudep", "stdout_read_error", e);
                    let _ = stdout_tx.send(StdoutLine::Eof).await;
                    break;
                }
            }
        }
    });

    let mut child_stdin: Option<tokio::process::ChildStdin> = Some(child_stdin);

    let (transcript_tx, mut transcript_rx) = mpsc::channel::<TranscriptLine>(256);

    let mut session_id: Option<String> = None;
    let mut tailing = false;
    let mut last_usage: Option<Value> = None;
    let mut turn_queue: std::collections::VecDeque<(String, String)> =
        std::collections::VecDeque::new();
    let mut steer_queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut active_turn_id: Option<String> = None;
    let mut sent: u64 = 0;
    let mut replayed: u64 = 0;
    let mut saw_terminal_result = false;
    let mut replayed_user_since_result = false;
    let mut rewinding = !entries.is_empty();
    let mut transcript_file: Option<tokio::fs::File> = if entries.is_empty() {
        Some(
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(&transcript_path)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "transcript does not exist at {}: {}",
                        transcript_path.display(),
                        e
                    )
                }),
        )
    } else {
        None
    };

    loop {
        tokio::select! {
            Some(event) = claude_rx.recv() => {
                match event {
                    ClaudeEvent::Turn { turn_id, message } => {
                        let text = message;
                        let (visible, notification) = split_user_message_meta(&text);
                        if active_turn_id.is_none() {
                            active_turn_id = Some(turn_id.clone());
                            trace!("easement", "claudep", "turn_started", "turn_id": turn_id);
                            broadcast(&broadcast_tx, Broadcast::Turn {
                                slug: slug.to_string(), transcript: transcript.to_string(),
                                event: TurnBroadcast::Started { turn_id },
                            });
                        } else {
                            trace!("easement", "claudep", "turn_written_while_active", "turn_id": turn_id, "message": text);
                        }
                        if notification {
                            broadcast(&broadcast_tx, Broadcast::UserMessage {
                                slug: slug.to_string(),
                                transcript: transcript.to_string(),
                                text: visible.to_string(),
                                notification,
                            });
                        }
                        let msg = format_user_message(&text);
                        if let Some(ref mut stdin) = child_stdin {
                            let _ = stdin.write_all(msg.as_bytes()).await;
                            let _ = stdin.flush().await;
                            sent += 1;
                        }
                    }
                    ClaudeEvent::ContentTurn { turn_id, content, reply } => {
                        if active_turn_id.is_none() {
                            active_turn_id = Some(turn_id.clone());
                            trace!("easement", "claudep", "turn_started", "turn_id": turn_id);
                            broadcast(&broadcast_tx, Broadcast::Turn {
                                slug: slug.to_string(), transcript: transcript.to_string(),
                                event: TurnBroadcast::Started { turn_id },
                            });
                        } else {
                            trace!("easement", "claudep", "content_turn_written_while_active", "turn_id": turn_id);
                        }
                        let msg = format_user_content_message(content);
                        if let Some(ref mut stdin) = child_stdin {
                            let _ = stdin.write_all(msg.as_bytes()).await;
                            let _ = stdin.flush().await;
                            sent += 1;
                        }
                        let _ = reply.send(());
                    }
                    ClaudeEvent::Steer { message, expected_turn_id } => {
                        if active_turn_id.as_deref() != Some(&expected_turn_id) {
                            trace!("easement", "claudep", "steer_turn_mismatch", "expected": expected_turn_id, "active": active_turn_id);
                        } else {
                            trace!("easement", "claudep", "steer_written", "expected": expected_turn_id, "message": message);
                        }
                        let msg = format_user_message(&message);
                        if let Some(ref mut stdin) = child_stdin {
                            let _ = stdin.write_all(msg.as_bytes()).await;
                            let _ = stdin.flush().await;
                            sent += 1;
                        }
                    }
                    ClaudeEvent::Interrupt { turn_id } => {
                        if turn_id.as_deref().is_some() && turn_id.as_deref() != active_turn_id.as_deref() {
                            trace!("easement", "claudep", "interrupt_turn_mismatch", "expected": turn_id, "active": active_turn_id);
                        } else {
                            trace!("easement", "claudep", "interrupt_written", "turn_id": turn_id, "active": active_turn_id);
                        }
                        let msg = format_interrupt_message();
                        if let Some(ref mut stdin) = child_stdin {
                            let _ = stdin.write_all(msg.as_bytes()).await;
                            let _ = stdin.flush().await;
                        }
                    }
                    ClaudeEvent::FlushSteers { call_id } => {
                        trace!("easement", "claudep", "flush_steers", "call_id": call_id, "queued": steer_queue.len());
                        // TODO: flush steer queue to stdin before dispatching
                        let _ = main_tx.send(MainEvent::ToolSteered { call_id });
                    }
                    ClaudeEvent::HistoryReplay { replay_id } => {
                        let last_uuid = entries.iter().rev()
                            .find_map(|e| {
                                let line = serde_json::to_string(e).ok()?;
                                let parsed = parse_entry(&line)?;
                                let normalized = normalize(parsed)?;
                                Some(normalized.uuid)
                            });
                        let s = slug.to_string();
                        let t = transcript.to_string();
                        broadcast(&broadcast_tx, Broadcast::History {
                            slug: s.clone(), transcript: t.clone(),
                            event: HistoryBroadcast::Begin {
                                replay_id: replay_id.clone(),
                                last_uuid,
                            },
                        });
                        let mut broadcast_count = 0u64;
                        for entry in &entries {
                            let line = serde_json::to_string(entry).unwrap_or_default();
                            if let Some(parsed) = parse_entry(&line) {
                                if let Some(normalized) = normalize(parsed) {
                                    broadcast(&broadcast_tx, Broadcast::History {
                                        slug: s.clone(), transcript: t.clone(),
                                        event: HistoryBroadcast::Entry {
                                            replay_id: replay_id.clone(),
                                            entry: serde_json::to_value(&normalized).unwrap_or_default(),
                                        },
                                    });
                                    broadcast_count += 1;
                                }
                            }
                        }
                        if let Some(ref usage) = last_usage {
                            broadcast(&broadcast_tx, Broadcast::Usage {
                                slug: slug.to_string(), transcript: transcript.to_string(),
                                usage: usage.clone(),
                            });
                        }
                        trace!("easement", "claudep", "history_replayed", "replay_id": replay_id, "entries": entries.len(), "slug": slug, "transcript": transcript);
                    }
                }
            }
            Some(line) = stdout_rx.recv() => {
                match line {
                    StdoutLine::Json(data) => {
                        dump!("easement", "claudep", "stdout", "data": data);

                        if let Ok(event) = serde_json::from_value::<StdoutEvent>(data.clone()) {
                            match &event {
                                StdoutEvent::StreamEvent { event, .. } => {
                                    broadcast(&broadcast_tx, Broadcast::Delta {
                                        slug: slug.to_string(), transcript: transcript.to_string(),
                                        event: event.clone(),
                                    });
                                }
                                StdoutEvent::User { is_replay: true, message, session_id: sid, .. } => {
                                    match &session_id {
                                        None => {
                                            assert!(uuid::Uuid::parse_str(sid).is_ok(), "session_id is not a UUID: {}", sid);
                                            trace!("easement", "claudep", "session_id", "session_id": sid, "slug": slug, "transcript": transcript);
                                            session_id = Some(sid.clone());

                                            // Start the transcript tailer now that we know the session ID and the CLI's file path.
                                            if !tailing {
                                                tailing = true;
                                                trace!("easement", "transcript", "tailing", "session_id": sid, "slug": slug, "transcript": transcript);

                                                let cli_path = cli_transcript_path(slug, sid);
                                                let tx = transcript_tx.clone();
                                                let tailer_token = cancel.child_token();
                                                tokio::spawn(async move {
                                                    let mut mux = match linemux::MuxedLines::new() {
                                                        Ok(m) => m,
                                                        Err(e) => {
                                                            panic!("cannot create tailer: {}", e);
                                                        }
                                                    };
                                                    if let Err(e) = mux.add_file_from_start(&cli_path).await {
                                                        panic!("cannot tail transcript at {}: {}", cli_path.display(), e);
                                                    }
                                                    loop {
                                                        tokio::select! {
                                                            // Await the channel for file read backpressure.
                                                            result = mux.next_line() => {
                                                                match result {
                                                                    Ok(Some(line)) => {
                                                                        let text = line.line().trim();
                                                                        assert!(!text.is_empty(), "empty line in CLI transcript");
                                                                        let data: Value = serde_json::from_str(text)
                                                                            .unwrap_or_else(|e| panic!("invalid JSON in CLI transcript: {}", e));
                                                                        let _ = tx.send(TranscriptLine::Entry(data)).await;
                                                                    }
                                                                    Ok(None) => break,
                                                                    Err(e) => {
                                                                        panic!("tailer read error: {}", e);
                                                                    }
                                                                }
                                                            }
                                                            _ = tailer_token.cancelled() => break,
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                        Some(existing) => {
                                            assert_eq!(existing.as_str(), sid.as_str(), "session id changed from {} to {}", existing, sid);
                                        }
                                    }
                                    replayed += 1;
                                    replayed_user_since_result = true;
                                    let is_steer_ack = replayed > 1;
                                    let text = message
                                        .get("content")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    trace!("easement", "claudep", "drain_gate", "sent": sent, "replayed": replayed, "is_steer_ack": is_steer_ack, "message": text);
                                    if is_steer_ack && !text.is_empty() {
                                        let parts: Vec<&str> = text.split(STEER_SENTINEL).collect();
                                        for part in &parts {
                                            let trimmed = part.trim();
                                            if !trimmed.is_empty() {
                                                trace!("easement", "claudep", "steer_broadcast", "text": trimmed);
                                                let (visible, notification) = split_user_message_meta(trimmed);
                                                broadcast(&broadcast_tx, Broadcast::UserMessage {
                                                    slug: slug.to_string(), transcript: transcript.to_string(),
                                                    text: visible.to_string(),
                                                    notification,
                                                });
                                            }
                                        }
                                    }
                                }
                                StdoutEvent::User { is_replay: false, message, .. } => {
                                    if let Some(content) = message.get("content").and_then(|v| v.as_array()) {
                                        for block in content {
                                            if block.get("type").and_then(|v| v.as_str()) == Some("tool_result") {
                                                let tool_use_id = block.get("tool_use_id")
                                                    .and_then(|v| v.as_str())
                                                    .expect("tool_result missing tool_use_id")
                                                    .to_string();
                                                let is_error = block.get("is_error")
                                                    .and_then(|v| v.as_bool())
                                                    .unwrap_or(false);
                                                let output = match block.get("content") {
                                                    Some(Value::String(s)) => s.clone(),
                                                    Some(Value::Array(parts)) => parts.iter()
                                                        .filter_map(|p| p.get("text").and_then(|v| v.as_str()))
                                                        .collect::<Vec<_>>()
                                                        .join("\n"),
                                                    _ => String::new(),
                                                };
                                                broadcast(&broadcast_tx, Broadcast::ToolResult {
                                                    slug: slug.to_string(),
                                                    transcript: transcript.to_string(),
                                                    tool_use_id,
                                                    output,
                                                    is_error,
                                                });
                                            }
                                        }
                                    }
                                }
                                StdoutEvent::Result { subtype, .. } => {
                                    let usage = data.get("usage").cloned();
                                    let is_interrupted = subtype.as_deref() == Some("error_during_execution");

                                    if let Some(ref u) = usage {
                                        broadcast(&broadcast_tx, Broadcast::Usage {
                                            slug: slug.to_string(), transcript: transcript.to_string(),
                                            usage: u.clone(),
                                        });
                                        last_usage = usage;
                                    }

                                    if is_interrupted {
                                        if let Some(ref tid) = active_turn_id {
                                            broadcast(&broadcast_tx, Broadcast::Turn {
                                                slug: slug.to_string(), transcript: transcript.to_string(),
                                                event: TurnBroadcast::Completed { turn_id: tid.clone(), status: "interrupted".to_string() },
                                            });
                                        }
                                    }

                                    if saw_terminal_result
                                        && !replayed_user_since_result
                                        && active_turn_id.is_some()
                                    {
                                        if sent == replayed + 1 {
                                            replayed += 1;
                                            trace!("easement", "claudep", "replay_inferred_from_tape", "sent": sent, "replayed": replayed, "turn_id": active_turn_id);
                                        } else {
                                            trace!("easement", "claudep", "replay_gap_not_inferred", "sent": sent, "replayed": replayed, "turn_id": active_turn_id);
                                        }
                                    }

                                    saw_terminal_result = true;
                                    replayed_user_since_result = false;

                                    if sent == replayed {
                                        if !steer_queue.is_empty() {
                                            if let Some(ref mut stdin) = child_stdin {
                                                let joined = steer_queue
                                                    .drain(..)
                                                    .collect::<Vec<_>>()
                                                    .join(STEER_SENTINEL);
                                                let steer_turn_id = uuid::Uuid::new_v4().to_string();
                                                broadcast(&broadcast_tx, Broadcast::Turn {
                                                    slug: slug.to_string(), transcript: transcript.to_string(),
                                                    event: TurnBroadcast::Started { turn_id: steer_turn_id },
                                                });
                                                let msg = format_user_message(&joined);
                                                let _ = stdin.write_all(msg.as_bytes()).await;
                                                let _ = stdin.flush().await;
                                                sent += 1;
                                                trace!("easement", "claudep", "steers_flushed");
                                            }
                                            continue;
                                        }

                                        let completed_turn_id = active_turn_id.take();

                                        if !is_interrupted {
                                            if let Some(ref tid) = completed_turn_id {
                                                broadcast(&broadcast_tx, Broadcast::Turn {
                                                    slug: slug.to_string(), transcript: transcript.to_string(),
                                                    event: TurnBroadcast::Completed { turn_id: tid.clone(), status: "completed".to_string() },
                                                });
                                            }
                                        }

                                        trace!("easement", "claudep", "trun_completed", "turn_id": completed_turn_id);

                                        // Dispatch next queued turn if any.
                                        if let Some((next_turn_id, text)) = turn_queue.pop_front() {
                                            let (visible, notification) = split_user_message_meta(&text);
                                            active_turn_id = Some(next_turn_id.clone());
                                            trace!("easement", "claudep", "turn_started", "turn_id": next_turn_id, "from_queue": true);
                                            broadcast(&broadcast_tx, Broadcast::Turn {
                                                slug: slug.to_string(), transcript: transcript.to_string(),
                                                event: TurnBroadcast::Started { turn_id: next_turn_id },
                                            });
                                            if notification {
                                                broadcast(&broadcast_tx, Broadcast::UserMessage {
                                                    slug: slug.to_string(),
                                                    transcript: transcript.to_string(),
                                                    text: visible.to_string(),
                                                    notification,
                                                });
                                            }
                                            let msg = format_user_message(&text);
                                            if let Some(ref mut stdin) = child_stdin {
                                                let _ = stdin.write_all(msg.as_bytes()).await;
                                                let _ = stdin.flush().await;
                                                sent += 1;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    StdoutLine::Eof => {
                        trace!("easement", "claudep", "stdout_eof");

                        child_stdin.take();
                        let _ = child.wait().await;

                        trace!("easement", "claudep", "exited_unexpectedly");
                        if let Some(ref tid) = active_turn_id {
                            broadcast(&broadcast_tx, Broadcast::Turn {
                                slug: slug.to_string(), transcript: transcript.to_string(),
                                event: TurnBroadcast::Completed { turn_id: tid.clone(), status: "failed".to_string() },
                            });
                        }

                        break;
                    }
                }
            }
            Some(line) = transcript_rx.recv() => {
                match line {
                    TranscriptLine::Entry(data) => {
                        dump!("easement", "transcript", "line", "data": data);
                        let uuid = match data.get("uuid").and_then(|v| v.as_str()) {
                            Some(u) => u,
                            None => continue,
                        };
                        if seen_uuids.contains(uuid) {
                            continue;
                        }
                        if entries.is_empty() {
                            trace!("easement", "transcript", "root", "uuid": uuid);
                        } else {
                            let parent = data.get("parentUuid").and_then(|v| v.as_str())
                                .expect("missing parentUuid on non-root entry");
                            let chain_head = entries.last()
                                .and_then(|e| e.get("uuid").and_then(|v| v.as_str()))
                                .expect("unreachable");

                            if rewinding {
                                if chain_head != parent {
                                    if seen_uuids.contains(parent) {
                                        let cut = entries.iter().rposition(|e| {
                                            e.get("uuid").and_then(|v| v.as_str()) == Some(parent)
                                        }).expect("parent in seen_uuids but not in entries");
                                        let removed: Vec<Value> = entries.drain(cut + 1..).collect();
                                        trace!("easement", "transcript", "rewind",
                                            "parent": parent, "cut": removed.len(),
                                            "removed": removed,
                                            "slug": slug, "transcript": transcript);
                                        for r in &removed {
                                            if let Some(u) = r.get("uuid").and_then(|v| v.as_str()) {
                                                seen_uuids.remove(u);
                                            }
                                        }

                                        let rewind_dir = PathBuf::from(&home)
                                            .join(".local/state/easement")
                                            .join(slug)
                                            .join("rewind")
                                            .join(transcript);
                                        tokio::fs::create_dir_all(&rewind_dir).await
                                            .unwrap_or_else(|e| panic!("cannot create rewind dir: {}", e));
                                        let rewind_ts = chrono::Local::now()
                                            .format("%Y-%m-%d-%H-%M-%S").to_string();
                                        let rewind_path = rewind_dir.join(format!("{}.jsonl", rewind_ts));
                                        assert!(!rewind_path.exists(),
                                            "rewind file already exists: {}", rewind_path.display());
                                        tokio::fs::rename(&transcript_path, &rewind_path).await
                                            .unwrap_or_else(|e| panic!("cannot move transcript to rewind: {}", e));

                                        let mut content = String::new();
                                        for e in &entries {
                                            let line = serde_json::to_string(e)
                                                .expect("entry serialization cannot fail");
                                            content.push_str(&line);
                                            content.push('\n');
                                        }
                                        tokio::fs::write(&transcript_path, &content).await
                                            .unwrap_or_else(|e| panic!("cannot write rewound transcript: {}", e));

                                        transcript_file = Some(tokio::fs::OpenOptions::new()
                                            .append(true)
                                            .open(&transcript_path)
                                            .await
                                            .unwrap_or_else(|e| panic!("cannot open transcript for append: {}", e)));
                                    } else {
                                        panic!("transcript entry {} chains from unknown parent {}", uuid, parent);
                                    }
                                } else {
                                    transcript_file = Some(tokio::fs::OpenOptions::new()
                                        .append(true)
                                        .open(&transcript_path)
                                        .await
                                        .unwrap_or_else(|e| panic!("cannot open transcript for append: {}", e)));
                                }
                                rewinding = false;
                            } else if chain_head != parent {
                                trace!("easement", "transcript", "branch",
                                    "uuid": uuid, "parent": parent, "head": chain_head);
                            }
                        }

                        wire!("easement", "transcript", "entry", "data": data);
                        let entry_line = {
                            let mut s = serde_json::to_string(&data)
                                .expect("entry serialization cannot fail");
                            s.push('\n');
                            s
                        };
                        seen_uuids.insert(uuid.to_string());
                        entries.push(data);
                        transcript_file.as_mut().expect("unreachable")
                            .write_all(entry_line.as_bytes()).await
                            .expect("transcript write failed");
                    }
                }
            }
            _ = cancel.cancelled() => {
                trace!("easement", "claudep", "cancelled");
                child_stdin.take();
                break;
            }
        }
    }
}

fn easement_port() -> u16 {
    env::var("EASEMENT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6502)
}

struct GcpInternalHost {
    instance: String,
    zone: String,
    project: String,
}

fn parse_gcp_internal_host(host: &str) -> Option<GcpInternalHost> {
    let without_suffix = host.strip_suffix(".internal")?;
    let (before_c, project) = without_suffix.rsplit_once(".c.")?;
    let (instance, zone) = before_c.split_once('.')?;
    if instance.is_empty() || zone.is_empty() || project.is_empty() {
        return None;
    }

    Some(GcpInternalHost {
        instance: instance.to_string(),
        zone: zone.to_string(),
        project: project.to_string(),
    })
}

fn spawn_wicket(host: &str) -> Result<tokio::process::Child, String> {
    let port = easement_port();
    let is_orb = host.contains("orb");
    let wicket_url = if is_orb {
        format!("ws://host.internal:{}", port)
    } else {
        format!("ws://localhost:{}", port)
    };

    let mut cmd = if host == "localhost" {
        trace!("easement", "wicket", "spawn_branch", "host": host, "branch": "local");
        let mut c = tokio::process::Command::new("wicket");
        c.arg(&wicket_url).arg(host);
        c
    } else if let Some(gcp) = parse_gcp_internal_host(host) {
        trace!(
            "easement",
            "wicket",
            "spawn_branch",
            "host": host,
            "branch": "gcp",
            "instance": gcp.instance,
            "zone": gcp.zone,
            "project": gcp.project
        );
        let mut c = tokio::process::Command::new("gcloud");
        c.arg("compute")
            .arg("ssh")
            .arg(&gcp.instance)
            .arg(format!("--project={}", gcp.project))
            .arg(format!("--zone={}", gcp.zone))
            .arg(format!("--ssh-flag=-R {}:localhost:{}", port, port))
            .arg(format!(
                "--command=PATH=\"$HOME/.local/bin:$PATH\" exec wicket {} {}",
                wicket_url, host
            ));
        c
    } else {
        trace!("easement", "wicket", "spawn_branch", "host": host, "branch": "ssh");
        let mut c = tokio::process::Command::new("ssh");
        if !is_orb {
            c.arg("-R").arg(format!("{}:localhost:{}", port, port));
        }
        c.arg(host).arg("wicket").arg(&wicket_url).arg(host);
        c
    };

    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    cmd.spawn()
        .map_err(|e| format!("failed to spawn wicket on {}: {}", host, e))
}

#[derive(Debug, serde::Deserialize)]
struct ToolCallParams {
    name: String,
    arguments: Value,
}

async fn handle_mcp(
    req: Request<Incoming>,
    slug: &str,
    transcript: Option<&str>,
    main_tx: mpsc::UnboundedSender<MainEvent>,
) -> Response<Full<Bytes>> {
    trace!("easement", "mcp", "request", "slug": slug, "transcript": transcript, "method": req.method().to_string());
    if req.method() != hyper::Method::POST {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::new()))
            .unwrap();
    }

    let body = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("failed to read body")))
                .unwrap();
        }
    };
    let body_str = String::from_utf8_lossy(&body);

    let request: JsonRpcRequest = match serde_json::from_str(&body_str) {
        Ok(r) => r,
        Err(e) => {
            return make_json_response(jsonrpc_error(
                Value::Null,
                -32700,
                format!("Parse error: {}", e),
            ));
        }
    };

    let id = request.id.unwrap_or(Value::Null);

    let response = match request.method.as_str() {
        "initialize" => jsonrpc_response(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "easement", "version": "0.1.0" }
            }),
        ),
        "tools/list" => jsonrpc_response(
            id,
            json!({
                "tools": [{
                    "name": "approve",
                    "description": "Request human approval for a tool call",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "tool_name": { "type": "string", "description": "Name of the tool requesting approval" },
                            "input": { "type": "object", "description": "Input arguments for the tool" },
                            "tool_use_id": { "type": "string", "description": "Optional tool use ID" }
                        },
                        "required": ["tool_name", "input"]
                    }
                }, {
                    "name": "call",
                    "description": "Call a function on a connected client. Use tools() first to discover available functions. The function is identified by who (the client) and f (the function name). Arguments are passed as args.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "who": { "type": "string", "description": "The client to call (e.g. wicket, shotgun)." },
                            "f": { "type": "string", "description": "The function name (e.g. zsh, screenshot, tabs_create)." },
                            "args": { "type": "object", "description": "Arguments to pass to the function." }
                        },
                        "required": ["who", "f"]
                    }
                }, {
                    "name": "tools",
                    "description": "Discover available functions from all connected clients. Returns a list of { who, f, description } for every function that can be called right now.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }]
            }),
        ),
        "tools/call" => {
            let params: ToolCallParams = match serde_json::from_value(request.params) {
                Ok(p) => p,
                Err(e) => {
                    return make_json_response(jsonrpc_error(
                        id,
                        -32602,
                        format!("Invalid params: {}", e),
                    ));
                }
            };

            trace!("easement", "mcp", "tools_call", "tool": params.name, "arguments": params.arguments);
            let ts = transcript.unwrap_or("");

            if params.name == "approve" {
                let updated_input = params
                    .arguments
                    .get("input")
                    .cloned()
                    .unwrap_or(params.arguments.clone());
                let text = json!({
                    "behavior": "allow",
                    "updatedInput": updated_input
                });
                let text = serde_json::to_string(&text).unwrap();
                return make_json_response(jsonrpc_response(
                    id,
                    json!({ "content": [{ "type": "text", "text": text }] }),
                ));
            }

            if params.name == "tools" {
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = main_tx.send(MainEvent::ToolCheck {
                    event: Box::new(MainEvent::ToolsQuery { reply: reply_tx }),
                });
                return make_json_response(match reply_rx.await {
                    Ok(result) => jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": result }] }),
                    ),
                    Err(_) => jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": "tool discovery failed" }], "isError": true }),
                    ),
                });
            }

            let who = params
                .arguments
                .get("who")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let f = params
                .arguments
                .get("f")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let args = params.arguments.get("args").cloned().unwrap_or(json!({}));

            if who.is_empty() || f.is_empty() {
                return make_json_response(jsonrpc_error(
                    id,
                    -32602,
                    "call requires who and f".to_string(),
                ));
            }

            let call_id = uuid::Uuid::new_v4().to_string();
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = main_tx.send(MainEvent::ToolCheck {
                event: Box::new(MainEvent::ToolCall {
                    call_id,
                    slug: slug.to_string(),
                    transcript: ts.to_string(),
                    tool: f.clone(),
                    args: json!({ "who": who, "f": f, "args": args }),
                    reply: reply_tx,
                }),
            });

            match reply_rx.await {
                Ok(result) => {
                    if who == "wicket" && f == "read_pdf" && result.exit_code == 0 {
                        match serde_json::from_str::<Value>(&result.output) {
                            Ok(content) => {
                                let (inject_tx, inject_rx) = oneshot::channel();
                                let _ = main_tx.send(MainEvent::UserContentTurn {
                                    slug: slug.to_string(),
                                    transcript: ts.to_string(),
                                    turn_id: uuid::Uuid::new_v4().to_string(),
                                    content,
                                    reply: inject_tx,
                                });
                                let _ = inject_rx.await;
                                jsonrpc_response(
                                    id,
                                    json!({ "content": [{ "type": "text", "text": "PDF attached to the conversation." }] }),
                                )
                            }
                            Err(_) => jsonrpc_response(
                                id,
                                json!({ "content": [{ "type": "text", "text": result.output }] }),
                            ),
                        }
                    } else if (f == "view_image" || f == "screenshot") && result.exit_code == 0 {
                        match serde_json::from_str::<Value>(&result.output) {
                            Ok(content) => jsonrpc_response(id, json!({ "content": content })),
                            Err(_) => jsonrpc_response(
                                id,
                                json!({ "content": [{ "type": "text", "text": result.output }] }),
                            ),
                        }
                    } else {
                        let output = if result.exit_code != 0 {
                            format!("{}\n[exit code: {}]", result.output, result.exit_code)
                        } else {
                            result.output
                        };
                        jsonrpc_response(
                            id,
                            json!({ "content": [{ "type": "text", "text": output }] }),
                        )
                    }
                }
                Err(_) => jsonrpc_response(
                    id,
                    json!({ "content": [{ "type": "text", "text": "tool execution failed: reply dropped" }], "isError": true }),
                ),
            }
        }
        "notifications/initialized" => jsonrpc_response(id, json!({})),
        other => jsonrpc_error(id, -32601, format!("Method not found: {}", other)),
    };

    make_json_response(response)
}

async fn handle_request(
    mut req: Request<Incoming>,
    main_tx: mpsc::UnboundedSender<MainEvent>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let peer = req
        .extensions()
        .get::<SocketAddr>()
        .copied()
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));

    if hyper_tungstenite::is_upgrade_request(&req) {
        match hyper_tungstenite::upgrade(&mut req, None) {
            Ok((response, websocket)) => {
                let _ = main_tx.send(MainEvent::Listen {
                    ws: websocket,
                    peer,
                });
                Ok(response)
            }
            Err(e) => {
                error!("easement", "websocket", "upgrade_error", e);
                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from(format!("upgrade error: {}", e))))
                    .unwrap())
            }
        }
    } else if let Some(rest) = path.strip_prefix("/mcp/") {
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        if parts.is_empty() || parts[0].is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from(
                    "missing slug in /mcp/<slug>/<transcript>",
                )))
                .unwrap())
        } else {
            let slug = parts[0].to_string();
            let transcript = parts.get(1).map(|s| s.to_string());
            Ok(handle_mcp(req, &slug, transcript.as_deref(), main_tx).await)
        }
    } else if path == "/health" {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("ok")))
            .unwrap())
    } else {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("not found")))
            .unwrap())
    }
}

struct Delayed {
    ms: u64,
    event: MainEvent,
}

struct Deadline {
    when: tokio::time::Instant,
    event: MainEvent,
}

impl PartialEq for Deadline {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
    }
}
impl Eq for Deadline {}
impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.when.cmp(&self.when)
    }
}

fn run_timer(
    mut schedule_rx: mpsc::UnboundedReceiver<Delayed>,
    main_tx: mpsc::UnboundedSender<MainEvent>,
) {
    tokio::spawn(async move {
        let mut heap: BinaryHeap<Deadline> = BinaryHeap::new();
        let mut sleep = std::pin::pin!(tokio::time::sleep(std::time::Duration::from_secs(86400)));

        loop {
            tokio::select! {
                Some(delayed) = schedule_rx.recv() => {
                    let when = tokio::time::Instant::now() + std::time::Duration::from_millis(delayed.ms);
                    let should_reset = heap.peek().map_or(true, |top| when < top.when);
                    heap.push(Deadline { when, event: delayed.event });
                    if should_reset {
                        sleep.as_mut().reset(heap.peek().unwrap().when);
                    }
                }
                () = &mut sleep => {
                    let now = tokio::time::Instant::now();
                    while let Some(top) = heap.peek() {
                        if top.when > now {
                            break;
                        }
                        let deadline = heap.pop().unwrap();
                        let _ = main_tx.send(deadline.event);
                    }
                    let next = heap.peek()
                        .map(|top| top.when)
                        .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(86400));
                    sleep.as_mut().reset(next);
                }
            }
        }
    });
}

enum ClaudeEvent {
    Turn {
        turn_id: String,
        message: String,
    },
    ContentTurn {
        turn_id: String,
        content: Value,
        reply: oneshot::Sender<()>,
    },
    Steer {
        message: String,
        expected_turn_id: String,
    },
    Interrupt {
        turn_id: Option<String>,
    },
    FlushSteers {
        call_id: String,
    },
    HistoryReplay {
        replay_id: String,
    },
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Tool {
    f: String,
    description: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ToolSet {
    who: String,
    tools: Vec<Tool>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Packet {
    Tool(ToolPacket),
    Turn(TurnPacket),
    History(HistoryPacket),
    Socket(SocketPacket),
    Shell(ShellPacket),
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ShellPacket {
    Run {
        slug: String,
        transcript: String,
        command: String,
    },
    Claim {
        id: String,
    },
    Response {
        id: String,
        slug: String,
        transcript: String,
        output: String,
        #[serde(default)]
        exit_code: i32,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum SocketPacket {
    Connect {
        who: String,
        r#where: Option<String>,
        tools: Vec<Tool>,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum ToolPacket {
    Claim {
        call_id: String,
    },
    Response {
        call_id: String,
        output: String,
        #[serde(default)]
        exit_code: i32,
    },
    BackgroundOutput {
        job_id: String,
        output_path: String,
        line: String,
    },
    Notification {
        slug: String,
        transcript: String,
        message: String,
        #[serde(default)]
        meta: Option<String>,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum TurnPacket {
    Start {
        slug: String,
        transcript: String,
        turn_id: String,
        message: String,
        #[serde(default)]
        notification: bool,
    },
    Steer {
        slug: String,
        transcript: String,
        message: String,
        expected_turn_id: String,
    },
    Interrupt {
        slug: String,
        transcript: String,
        #[serde(default)]
        turn_id: Option<String>,
    },
}

#[derive(serde::Deserialize)]
#[serde(tag = "why", rename_all = "snake_case")]
enum HistoryPacket {
    Replay {
        slug: String,
        transcript: String,
        replay_id: String,
    },
}

enum MainEvent {
    Listen {
        ws: hyper_tungstenite::HyperWebsocket,
        peer: SocketAddr,
    },
    Packet {
        client_id: u64,
        data: Value,
    },
    Disconnected {
        client_id: u64,
    },
    ToolCheck {
        event: Box<MainEvent>,
    },
    ToolsQuery {
        reply: oneshot::Sender<String>,
    },
    ToolCall {
        call_id: String,
        slug: String,
        transcript: String,
        tool: String,
        args: Value,
        reply: oneshot::Sender<ToolResult>,
    },
    ToolCallEnsured {
        call_id: String,
        slug: String,
        transcript: String,
        tool: String,
        args: Value,
        reply: oneshot::Sender<ToolResult>,
    },
    ToolSteered {
        call_id: String,
    },
    WicketExited {
        host: String,
        exit_code: i32,
    },
    ToolClaimTimeout {
        call_id: String,
    },
    ToolCallTimeout {
        call_id: String,
    },
    UserContentTurn {
        slug: String,
        transcript: String,
        turn_id: String,
        content: Value,
        reply: oneshot::Sender<()>,
    },
    WicketSpawnTimeout {
        host: String,
    },
}

#[tokio::main]
async fn main() {
    init_log();

    let (broadcast_tx, _) = broadcast::channel::<String>(65536);
    let port = easement_port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            trace!("easement", "lifecycle", "started", "port": port, "addr": addr.to_string());
            l
        }
        Err(e) => {
            panic!("failed to bind {}: {}", addr, e);
        }
    };

    fn window_for<'a>(
        windows: &'a mut HashMap<(String, String), Window>,
        slug: &str,
        transcript: &str,
        token: &tokio_util::sync::CancellationToken,
        broadcast_tx: &broadcast::Sender<String>,
        main_tx: &mpsc::UnboundedSender<MainEvent>,
    ) -> &'a mut Window {
        let key = (slug.to_string(), transcript.to_string());

        if !windows.contains_key(&key) {
            let (claude_tx, claude_rx) = mpsc::unbounded_channel::<ClaudeEvent>();
            let child_token = token.child_token();
            let slug_owned = slug.to_string();
            let transcript_owned = transcript.to_string();
            let btx = broadcast_tx.clone();
            let mtx = main_tx.clone();
            tokio::spawn(async move {
                claudep(
                    &slug_owned,
                    &transcript_owned,
                    claude_rx,
                    btx,
                    mtx,
                    child_token,
                )
                .await;
            });
            windows.insert(
                key.clone(),
                Window {
                    claude_tx,
                    shebang_host: "localhost".to_string(),
                },
            );
        }

        windows.get_mut(&key).expect("just inserted")
    }

    let (main_tx, mut main_rx) = mpsc::unbounded_channel::<MainEvent>();
    let token = tokio_util::sync::CancellationToken::new();

    let (timer_tx, timer_rx) = mpsc::unbounded_channel::<Delayed>();
    run_timer(timer_rx, main_tx.clone());

    let mut next_client_id: u64 = 0;

    struct Socket {
        tx: mpsc::UnboundedSender<String>,
        who: Option<String>,
        r#where: String,
        tools: Option<ToolSet>,
    }
    let mut sockets: HashMap<u64, Socket> = HashMap::new();

    struct ToolClaim {
        reply: oneshot::Sender<ToolResult>,
        slug: String,
        transcript: String,
        args: Value,
    }
    let mut shutdown = false;
    let mut tool_claims: HashMap<String, ToolClaim> = HashMap::new();
    let mut tool_calls: HashMap<String, ToolClaim> = HashMap::new();
    let mut shell_claims: std::collections::HashSet<String> = std::collections::HashSet::new();
    struct Window {
        claude_tx: mpsc::UnboundedSender<ClaudeEvent>,
        shebang_host: String,
    }
    let mut windows: HashMap<(String, String), Window> = HashMap::new();

    enum WicketState {
        Starting,
        Connected { client_id: u64 },
        Disconnected,
    }

    struct Wicket {
        state: WicketState,
        stashed: Vec<MainEvent>,
    }
    let mut wickets: HashMap<String, Wicket> = HashMap::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(s) => s,
                    Err(e) => {
                        error!("easement", "lifecycle", "accept_error", e);
                        continue;
                    }
                };

                let io = TokioIo::new(stream);
                let main_tx = main_tx.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let main_tx = main_tx.clone();
                        async move { handle_request(req, main_tx).await }
                    });

                    if let Err(e) = http1::Builder::new()
                        .serve_connection(io, service)
                        .with_upgrades()
                        .await
                    {
                        trace!("easement", "websocket", "connection_error", "peer": peer.to_string(), "error": e.to_string());
                    }
                });
            }
            Some(event) = main_rx.recv() => {
                match event {
                    MainEvent::Listen { ws, peer } => {
                        let ws_stream = match ws.await {
                            Ok(s) => s,
                            Err(e) => {
                                error!("easement", "websocket", "upgrade_failed", e, "peer": peer.to_string());
                                continue;
                            }
                        };

                        let client_id = next_client_id;
                        next_client_id += 1;
                        trace!("easement", "websocket", "connected", "client_id": client_id, "peer": peer.to_string());

                        let (mut sink, mut stream) = ws_stream.split();

                        // Writer task: socket_rx -> WebSocket.
                        let (socket_tx, mut socket_rx) = mpsc::unbounded_channel::<String>();
                        sockets.insert(client_id, Socket { tx: socket_tx, who: None, r#where: "localhost".to_string(), tools: None });

                        let mut broadcast_rx = broadcast_tx.subscribe();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    Some(msg) = socket_rx.recv() => {
                                        if sink.send(Message::text(msg)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Ok(msg) = broadcast_rx.recv() => {
                                        if sink.send(Message::text(msg)).await.is_err() {
                                            break;
                                        }
                                    }
                                    else => break,
                                }
                            }
                        });

                        // Reader task: WebSocket -> main_tx.
                        let main_tx = main_tx.clone();
                        tokio::spawn(async move {
                            while let Some(result) = stream.next().await {
                                match result {
                                    Ok(Message::Text(text)) => {
                                        match serde_json::from_str::<Value>(&text) {
                                            Ok(data) => {
                                                let _ = main_tx.send(MainEvent::Packet { client_id, data });
                                            }
                                            Err(e) => {
                                                error!("easement", "websocket", "bad_json", e, "client_id": client_id);
                                            }
                                        }
                                    }
                                    Ok(Message::Close(_)) => break,
                                    Err(e) => {
                                        error!("easement", "websocket", "stream", e, "client_id": client_id);
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            trace!("easement", "websocket", "reader_exited", "client_id": client_id);
                            let _ = main_tx.send(MainEvent::Disconnected { client_id });
                        });
                    }
                    MainEvent::ToolCheck { event } => {
                        if shutdown {
                            match *event {
                                MainEvent::ToolCall { reply, .. } => {
                                    let _ = reply.send(ToolResult {
                                        output: "[meta] MCP tools and assistant harness shutting down, please end your turn".to_string(),
                                        exit_code: 1,
                                    });
                                }
                                MainEvent::ToolsQuery { reply } => {
                                    let _ = reply.send("[meta] MCP tools and assistant harness shutting down, please end your turn".to_string());
                                }
                                _ => panic!("unexpected event in ToolCheck"),
                            }
                        } else {
                            let _ = main_tx.send(*event);
                        }
                    }
                    MainEvent::ToolsQuery { reply } => {
                        let host = "localhost".to_string();
                        match wickets.get_mut(&host) {
                            Some(Wicket { state: WicketState::Connected { .. }, .. }) |
                            Some(Wicket { state: WicketState::Disconnected, .. }) => {
                                let tools: Vec<Value> = sockets.values()
                                    .filter_map(|s| s.tools.as_ref())
                                    .flat_map(|ts| ts.tools.iter().map(|t| {
                                        json!({ "who": ts.who, "f": t.f, "description": t.description })
                                    }))
                                    .collect();
                                let _ = reply.send(serde_json::to_string_pretty(&tools).unwrap_or_else(|_| "[]".to_string()));
                            }
                            Some(wicket @ Wicket { state: WicketState::Starting, .. }) => {
                                wicket.stashed.push(MainEvent::ToolsQuery { reply });
                            }
                            None => {
                                match spawn_wicket(&host) {
                                    Ok(mut child) => {
                                        trace!("easement", "wicket", "spawning", "where": host);
                                        let main_tx = main_tx.clone();
                                        let where_clone = host.clone();
                                        tokio::spawn(async move {
                                            let status = child.wait().await;
                                            let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                                            let _ = main_tx.send(MainEvent::WicketExited {
                                                host: where_clone, exit_code: code,
                                            });
                                        });
                                        wickets.insert(host.clone(), Wicket {
                                            state: WicketState::Starting,
                                            stashed: vec![MainEvent::ToolsQuery { reply }],
                                        });
                                        let _ = timer_tx.send(Delayed {
                                            ms: 10_000,
                                            event: MainEvent::WicketSpawnTimeout { host },
                                        });
                                    }
                                    Err(e) => {
                                        trace!("easement", "wicket", "spawn_failed", "where": host, "error": e.to_string());
                                        let tools: Vec<Value> = sockets.values()
                                            .filter_map(|s| s.tools.as_ref())
                                            .flat_map(|ts| ts.tools.iter().map(|t| {
                                                json!({ "who": ts.who, "f": t.f, "description": t.description })
                                            }))
                                            .collect();
                                        let _ = reply.send(serde_json::to_string_pretty(&tools).unwrap_or_else(|_| "[]".to_string()));
                                    }
                                }
                            }
                        }
                    }
                    MainEvent::ToolCall { call_id, slug, transcript, tool, args, reply } => {
                        let who = args.get("who").and_then(|v| v.as_str()).unwrap_or("");
                        if who != "wicket" {
                            let _ = main_tx.send(MainEvent::ToolCallEnsured {
                                call_id, slug, transcript, tool, args, reply,
                            });
                            continue;
                        }

                        let r#where = match args.get("args").and_then(|a| a.get("where")).and_then(|v| v.as_str()) {
                            Some(w) => w.to_string(),
                            None => {
                                let _ = reply.send(ToolResult {
                                    output: "wicket tool call missing required where argument".to_string(),
                                    exit_code: 1,
                                });
                                continue;
                            }
                        };
                        let ensured = MainEvent::ToolCallEnsured {
                            call_id: call_id.clone(), slug: slug.clone(),
                            transcript: transcript.clone(), tool, args, reply,
                        };

                        match wickets.get_mut(&r#where) {
                            Some(Wicket { state: WicketState::Connected { .. }, .. }) => {
                                let _ = main_tx.send(ensured);
                            }
                            Some(wicket @ Wicket { state: WicketState::Starting, .. }) => {
                                trace!("easement", "tool", "stashed", "call_id": call_id, "where": r#where);
                                wicket.stashed.push(ensured);
                            }
                            Some(Wicket { state: WicketState::Disconnected, .. }) => {
                                if let MainEvent::ToolCallEnsured { reply, .. } = ensured {
                                    let _ = reply.send(ToolResult {
                                        output: format!("wicket on {} disconnected", r#where),
                                        exit_code: 1,
                                    });
                                }
                            }
                            None => {
                                match spawn_wicket(&r#where) {
                                    Ok(mut child) => {
                                        trace!("easement", "wicket", "spawning", "where": r#where);
                                        let main_tx = main_tx.clone();
                                        let where_clone = r#where.clone();
                                        tokio::spawn(async move {
                                            let status = child.wait().await;
                                            let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                                            let _ = main_tx.send(MainEvent::WicketExited {
                                                host: where_clone, exit_code: code,
                                            });
                                        });
                                        wickets.insert(r#where.clone(), Wicket {
                                            state: WicketState::Starting,
                                            stashed: vec![ensured],
                                        });
                                        let _ = timer_tx.send(Delayed {
                                            ms: 10_000,
                                            event: MainEvent::WicketSpawnTimeout { host: r#where },
                                        });
                                    }
                                    Err(e) => {
                                        trace!("easement", "wicket", "spawn_failed", "where": r#where, "error": e.to_string());
                                        if let MainEvent::ToolCallEnsured { reply, .. } = ensured {
                                            let _ = reply.send(ToolResult {
                                                output: format!("failed to spawn wicket on {}: {}", r#where, e),
                                                exit_code: 1,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    MainEvent::ToolCallEnsured { call_id, slug, transcript, tool, args, reply } => {
                        trace!("easement", "tool", "ensured", "call_id": call_id, "slug": slug, "tool": tool);

                        let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                        let _ = win.claude_tx.send(ClaudeEvent::FlushSteers { call_id: call_id.clone() });

                        tool_claims.insert(call_id, ToolClaim { reply, slug, transcript, args });
                    }
                    MainEvent::ToolSteered { call_id } => {
                        trace!("easement", "tool", "steered", "call_id": call_id);
                        let claim = match tool_claims.remove(&call_id) {
                            Some(c) => c,
                            None => {
                                trace!("easement", "tool", "steer_unknown", "call_id": call_id);
                                continue;
                            }
                        };
                        let who = claim.args.get("who").and_then(|v| v.as_str()).unwrap_or("");
                        let f = claim.args.get("f").and_then(|v| v.as_str()).unwrap_or("");
                        let r#where = claim.args.get("args")
                            .and_then(|a| a.get("where"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("localhost");
                        let inner_args = claim.args.get("args").cloned().unwrap_or(json!({}));

                        let socket_tx = sockets.values()
                            .find(|s| s.who.as_deref() == Some(who) && s.r#where == r#where)
                            .map(|s| &s.tx);

                        match socket_tx {
                            Some(tx) => {
                                trace!("easement", "tool", "dispatched", "call_id": call_id, "who": who, "f": f, "where": r#where);
                                let mut flat_args = inner_args.as_object().cloned().unwrap_or_default();
                                flat_args.insert("f".to_string(), json!(f));
                                dispatch(tx, Dispatch::Tool {
                                    slug: claim.slug.clone(),
                                    transcript: claim.transcript.clone(),
                                    event: ToolDispatch::Run {
                                        call_id: call_id.clone(),
                                        args: Value::Object(flat_args),
                                    },
                                });
                                tool_calls.insert(call_id.clone(), claim);
                                let _ = timer_tx.send(Delayed {
                                    ms: 86_400_000,
                                    event: MainEvent::ToolCallTimeout { call_id },
                                });
                            }
                            None => {
                                trace!("easement", "tool", "no_socket", "call_id": call_id, "who": who, "where": r#where);
                                let _ = claim.reply.send(ToolResult {
                                    output: format!("no connected client for who={} where={}", who, r#where),
                                    exit_code: 1,
                                });
                            }
                        }
                    }
                    // Tool call lifecycle: broadcast goes out, one client claims, that client
                    // sends the response. We assume one claim per call_id. If two clients claim
                    // the same call, something is wrong with the network topology -- two Wickets
                    // on different hosts both handling the same slug, or a stale Wicket that
                    // should have been killed. A duplicate claim in production could mean a
                    // destructive command reaches the wrong host. We log and panic because
                    // silent corruption is worse than a crash.
                    MainEvent::Packet { client_id, data } => {
                        match serde_json::from_value::<Packet>(data) {
                            Ok(Packet::Tool(ToolPacket::Claim { call_id })) => {
                                if let Some(claim) = tool_claims.remove(&call_id) {
                                    trace!("easement", "tool", "claimed", "client_id": client_id, "call_id": call_id);
                                    tool_calls.insert(call_id.clone(), claim);
                                    let _ = timer_tx.send(Delayed {
                                        ms: 86_400_000,
                                        event: MainEvent::ToolCallTimeout { call_id },
                                    });
                                } else {
                                    trace!("easement", "tool", "stale_claim", "client_id": client_id, "call_id": call_id);
                                }
                            }
                            Ok(Packet::Tool(ToolPacket::Response { call_id, output, exit_code })) => {
                                if let Some(claim) = tool_calls.remove(&call_id) {
                                    trace!("easement", "tool", "response", "client_id": client_id, "call_id": call_id, "exit_code": exit_code);
                                    let r#where = claim.args.get("args")
                                        .and_then(|a| a.get("where"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("localhost");
                                    let key = (claim.slug.clone(), claim.transcript.clone());
                                    if let Some(win) = windows.get_mut(&key) {
                                        win.shebang_host = r#where.to_string();
                                    }
                                    let _ = claim.reply.send(ToolResult { output, exit_code });
                                } else {
                                    trace!("easement", "tool", "unknown_response", "client_id": client_id, "call_id": call_id);
                                }
                            }
                            Ok(Packet::Tool(ToolPacket::BackgroundOutput { job_id, output_path, line })) => {
                                trace!("easement", "tool", "background_output", "client_id": client_id, "job_id": job_id, "output_path": output_path, "line": line);
                            }
                            Ok(Packet::Tool(ToolPacket::Notification {
                                slug,
                                transcript,
                                message,
                                meta,
                            })) => {
                                trace!("easement", "tool", "notification", "client_id": client_id, "message": message, "has_meta": meta.is_some());
                                let text = join_user_message_meta(message, meta);
                                let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                                let _ = win.claude_tx.send(ClaudeEvent::Turn {
                                    turn_id: uuid::Uuid::new_v4().to_string(),
                                    message: text,
                                });
                            }
                            Ok(Packet::Turn(TurnPacket::Start { slug, transcript, turn_id, message, notification })) => {
                                let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                                let message = if notification && !message.contains('\x07') {
                                    format!("{}\x07", message)
                                } else {
                                    message
                                };
                                let _ = win.claude_tx.send(ClaudeEvent::Turn { turn_id, message });
                            }
                            Ok(Packet::Turn(TurnPacket::Steer { slug, transcript, message, expected_turn_id })) => {
                                let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                                let _ = win.claude_tx.send(ClaudeEvent::Steer { message, expected_turn_id });
                            }
                            Ok(Packet::Turn(TurnPacket::Interrupt { slug, transcript, turn_id })) => {
                                let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                                let _ = win.claude_tx.send(ClaudeEvent::Interrupt { turn_id });
                            }
                            Ok(Packet::History(HistoryPacket::Replay { slug, transcript, replay_id })) => {
                                match resolve_transcript(&slug, &transcript).await {
                                    Some(resolved) => {
                                        trace!("easement", "history", "replay", "slug": slug, "transcript": resolved, "replay_id": replay_id);
                                        let win = window_for(&mut windows, &slug, &resolved, &token, &broadcast_tx, &main_tx);
                                        let _ = win.claude_tx.send(ClaudeEvent::HistoryReplay { replay_id });
                                    }
                                    None => {
                                        trace!("easement", "history", "not_found", "slug": slug, "transcript": transcript);
                                    }
                                }
                            }
                            Ok(Packet::Shell(ShellPacket::Run { slug, transcript, command })) => {
                                let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                                let r#where = win.shebang_host.clone();
                                let shell_id = uuid::Uuid::new_v4().to_string();
                                let socket_tx = sockets.values()
                                    .find(|s| s.who.as_deref() == Some("wicket") && s.r#where == r#where)
                                    .map(|s| &s.tx);
                                match socket_tx {
                                    Some(tx) => {
                                        dispatch(tx, Dispatch::Shell {
                                            slug, transcript,
                                            event: ShellDispatch::Run {
                                                id: shell_id,
                                                command,
                                                r#where,
                                            },
                                        });
                                    }
                                    None => {
                                        trace!("easement", "shell", "no_wicket", "where": r#where);
                                    }
                                }
                            }
                            Ok(Packet::Shell(ShellPacket::Claim { id })) => {
                                trace!("easement", "shell", "claimed", "client_id": client_id, "id": id);
                            }
                            Ok(Packet::Shell(ShellPacket::Response { id, slug: _, transcript: _, output: _, exit_code })) => {
                                trace!("easement", "shell", "response", "client_id": client_id, "id": id, "exit_code": exit_code);
                                // TODO: route shell result back to originating client
                            }
                            Ok(Packet::Socket(SocketPacket::Connect { who, r#where, tools })) => {
                                let toolset = ToolSet { who: who.clone(), tools };
                                let resolved_where = r#where.clone().unwrap_or_else(|| "localhost".to_string());
                                trace!("easement", "socket", "connect", "client_id": client_id, "who": who, "where": resolved_where, "tools": toolset.tools.len());
                                let Some(socket) = sockets.get_mut(&client_id) else {
                                    panic!("socket connect from unknown client {}", client_id);
                                };
                                socket.who = Some(who.clone());
                                socket.r#where = resolved_where.clone();
                                socket.tools = Some(toolset);
                                if who == "wicket" {
                                    let r#where = r#where.unwrap_or_else(|| {
                                        panic!("wicket connect without where from client {}", client_id);
                                    });
                                    if let Some(wicket) = wickets.get_mut(&r#where) {
                                        wicket.state = WicketState::Connected { client_id };
                                        let drained: Vec<MainEvent> = wicket.stashed.drain(..).collect();
                                        trace!("easement", "wicket", "connected_draining", "where": r#where, "stashed": drained.len());
                                        for event in drained {
                                            let _ = main_tx.send(MainEvent::ToolCheck { event: Box::new(event) });
                                        }
                                    } else {
                                        wickets.insert(r#where.clone(), Wicket {
                                            state: WicketState::Connected { client_id },
                                            stashed: Vec::new(),
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                error!("easement", "websocket", "unrecognized", e, "client_id": client_id);
                            }
                        }
                    }
                    MainEvent::UserContentTurn { slug, transcript, turn_id, content, reply } => {
                        let win = window_for(&mut windows, &slug, &transcript, &token, &broadcast_tx, &main_tx);
                        let event = ClaudeEvent::ContentTurn {
                            turn_id,
                            content,
                            reply,
                        };
                        if let Err(err) = win.claude_tx.send(event) {
                            let ClaudeEvent::ContentTurn { reply, .. } = err.0 else {
                                panic!("unexpected claude event returned from content turn send");
                            };
                            let _ = reply.send(());
                        }
                    }
                    MainEvent::Disconnected { client_id } => {
                        sockets.remove(&client_id);
                        for wicket in wickets.values_mut() {
                            if matches!(wicket.state, WicketState::Connected { client_id: cid } if cid == client_id) {
                                trace!("easement", "wicket", "disconnected", "client_id": client_id);
                                wicket.state = WicketState::Disconnected;
                            }
                        }
                        trace!("easement", "websocket", "disconnected", "client_id": client_id);
                    }
                    MainEvent::WicketExited { host, exit_code } => {
                        trace!("easement", "wicket", "exited", "host": host, "exit_code": exit_code);
                        if let Some(wicket) = wickets.remove(&host) {
                            for event in wicket.stashed {
                                let _ = main_tx.send(MainEvent::ToolCheck { event: Box::new(event) });
                            }
                        }
                    }
                    MainEvent::ToolClaimTimeout { call_id } => {
                        if let Some(claim) = tool_claims.remove(&call_id) {
                            trace!("easement", "tool", "claim_timeout", "call_id": call_id);
                            let _ = claim.reply.send(ToolResult {
                                output: "no client claimed this tool call".to_string(),
                                exit_code: 1,
                            });
                        }
                    }
                    MainEvent::ToolCallTimeout { call_id } => {
                        if let Some(claim) = tool_calls.remove(&call_id) {
                            trace!("easement", "tool", "call_timeout", "call_id": call_id);
                            let _ = claim.reply.send(ToolResult {
                                output: "tool call timed out".to_string(),
                                exit_code: 1,
                            });
                        }
                    }
                    MainEvent::WicketSpawnTimeout { host } => {
                        if let Some(wicket) = wickets.get_mut(&host) &&
                             matches!(wicket.state, WicketState::Starting) {
                            trace!("easement", "wicket", "spawn_timeout", "host": host);
                            wicket.state = WicketState::Disconnected;
                            let stashed: Vec<MainEvent> = wicket.stashed.drain(..).collect();
                            for event in stashed {
                                let _ = main_tx.send(MainEvent::ToolCheck { event: Box::new(event) });
                            }
                        }
                    }
                }
            }
        }
    }
}
