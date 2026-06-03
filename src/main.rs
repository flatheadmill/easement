// Wicket: WebSocket and HTTP server on port 6502.
//
// Clients (Puzzle, Shotgun) connect over WebSocket. Claude's MCP approval
// requests arrive over HTTP at /mcp/<slug>. One process, one port.
//
// Each slug gets its own coordinator task that manages the ClaudePrint
// lifecycle, transcript persistence, and client broadcasting. Clients
// register with the coordinator for their slug and receive normalized
// entries and lifecycle events.

mod normalize;
mod parser;
mod protocol;
mod transcript;

use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use crate::protocol::{
    InboundEnvelope, LifecycleEvent,
    NormalizedEntry,
};
use crate::transcript::Transcript;

// -- Exchange log --

struct ExchangeLog {
    path: std::path::PathBuf,
}

impl ExchangeLog {
    fn new(slug: &str) -> Self {
        let home = env::var("HOME").expect("HOME not set");
        let dir = std::path::Path::new(&home)
            .join(".local/state/easement")
            .join(slug);
        let _ = std::fs::create_dir_all(&dir);
        Self {
            path: dir.join("exchange.jsonl"),
        }
    }

    fn log(&self, dir: &str, data: &Value) {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let entry = json!({
            "ts": now,
            "dir": dir,
            "data": data,
        });
        if let Ok(mut line) = serde_json::to_string(&entry) {
            line.push('\n');
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
            {
                let _ = std::io::Write::write_all(&mut file, line.as_bytes());
            }
        }
    }
}

// -- Stdout event types from Claude CLI --

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
        session_id: Option<String>,
        uuid: Option<String>,
    },
    User {
        message: Value,
        session_id: Option<String>,
        #[serde(default)]
        #[serde(rename = "isReplay")]
        is_replay: bool,
    },
    RateLimitEvent {
        rate_limit_info: Value,
    },
    #[serde(other)]
    Unknown,
}

// -- Output envelope --

#[derive(Debug, Serialize)]
struct OutEnvelope<'a> {
    stream: &'a str,
    data: Value,
}

fn envelope_json(stream: &str, data: Value) -> Option<String> {
    serde_json::to_string(&OutEnvelope { stream, data }).ok()
}

// -- Logging --

fn init_tracing() -> WorkerGuard {
    let home = env::var("HOME").expect("HOME not set");
    let log_dir = std::path::Path::new(&home)
        .join(".local")
        .join("state")
        .join("easement");
    let _ = std::fs::create_dir_all(&log_dir);

    let port = easement_port();
    let log_name = if port == 6502 {
        "easement.log".to_string()
    } else {
        format!("easement-{}.log", port)
    };

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join(&log_name))
        .expect("failed to open log file");

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("easement=debug"));

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .init();

    guard
}

// -- ClaudePrint: stdin message formatting --

#[derive(Debug, Serialize)]
struct UserMessage {
    r#type: &'static str,
    message: UserMessageContent,
    uuid: String,
}

#[derive(Debug, Serialize)]
struct UserMessageContent {
    role: &'static str,
    content: String,
}

fn format_user_message(content: &str) -> String {
    let msg = UserMessage {
        r#type: "user",
        message: UserMessageContent {
            role: "user",
            content: content.to_string(),
        },
        uuid: uuid::Uuid::new_v4().to_string(),
    };
    let mut s = serde_json::to_string(&msg).expect("UserMessage serialization cannot fail");
    s.push('\n');
    s
}

// -- ClaudePrint: events from the stdout reader task --

enum ClaudeEvent {
    Delta(Value),
    Replay { message: String },
    Result { usage: Option<Value>, is_interrupted: bool },
    SessionId(String),
    Eof,
}

// -- ClaudePrint: trust injection --

fn ensure_trust(config_path: &Path, directory: &str) -> Result<(), String> {
    let lock_path = config_path.with_extension("json.lock");
    if std::fs::create_dir(&lock_path).is_err() {
        return Err("lock contention".to_string());
    }

    let result = (|| -> Result<(), String> {
        let mut config: Value = match std::fs::read_to_string(config_path) {
            Ok(content) => serde_json::from_str(&content).map_err(|e| e.to_string())?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Value::Object(serde_json::Map::new())
            }
            Err(e) => return Err(e.to_string()),
        };

        let already = config
            .get("projects")
            .and_then(|p| p.get(directory))
            .and_then(|e| e.get("hasTrustDialogAccepted"))
            .and_then(|v| v.as_bool())
            == Some(true);

        if already {
            return Ok(());
        }

        let obj = config.as_object_mut().ok_or("config not an object")?;
        let projects = obj
            .entry("projects")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        let project = projects
            .as_object_mut()
            .ok_or("projects not an object")?
            .entry(directory)
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        project
            .as_object_mut()
            .ok_or("project entry not an object")?
            .insert(
                "hasTrustDialogAccepted".to_string(),
                Value::Bool(true),
            );

        let content = serde_json::to_string_pretty(&config).map_err(|e| e.to_string())?;
        std::fs::write(config_path, &content).map_err(|e| e.to_string())?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                config_path,
                std::fs::Permissions::from_mode(0o600),
            );
        }

        tracing::info!(directory, "trust injected");
        Ok(())
    })();

    let _ = std::fs::remove_dir(&lock_path);
    result
}

// -- ClaudePrint: round log --

struct RoundLog {
    dir: PathBuf,
}

impl RoundLog {
    fn begin(slug: &str) -> Self {
        let home = env::var("HOME").unwrap_or_default();
        let now = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let pid = std::process::id();
        let dir = Path::new(&home)
            .join(".local/state/easement")
            .join(slug)
            .join("rounds")
            .join(format!("{}-{}", now, pid));
        let _ = std::fs::create_dir_all(&dir);
        tracing::info!(round_dir = %dir.display(), "round log started");
        Self { dir }
    }

    fn log_sent(&self, entries: &[Value]) {
        let path = self.dir.join("sent.jsonl");
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
        {
            for entry in entries {
                if let Ok(mut line) = serde_json::to_string(entry) {
                    line.push('\n');
                    let _ = std::io::Write::write_all(&mut file, line.as_bytes());
                }
            }
        }
    }

    fn log_stdout(&self, raw_line: &str) {
        let path = self.dir.join("stdout.jsonl");
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let entry = json!({ "ts": now, "line": raw_line });
        if let Ok(mut line) = serde_json::to_string(&entry) {
            line.push('\n');
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
            {
                let _ = std::io::Write::write_all(&mut file, line.as_bytes());
            }
        }
    }

    fn copy_transcript(&self, cli_transcript: &Path) {
        let dest = self.dir.join("transcript.jsonl");
        if let Err(e) = std::fs::copy(cli_transcript, &dest) {
            tracing::warn!(error = %e, "failed to copy CLI transcript to round log");
        }
    }
}

// -- ClaudePrint: transcript file discovery --

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

// -- Emplacement: place our transcript in the CLI's project directory --

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
    std::fs::write(path, &content)
        .map_err(|e| format!("cannot write emplaced transcript: {}", e))
}

fn extract_session_uuid(entries: &[Value]) -> Option<String> {
    entries.iter().find_map(|e| {
        e.get("sessionId")
            .and_then(|v| v.as_str())
            .filter(|s| uuid::Uuid::parse_str(s).is_ok())
            .map(|s| s.to_string())
    })
}

// -- ClaudePrint state --

struct ClaudePrint {
    child: tokio::process::Child,
    stdin: Option<tokio::process::ChildStdin>,
    event_rx: mpsc::Receiver<ClaudeEvent>,
    session_id: Option<String>,
    drain_sent: u64,
    drain_replayed: u64,
    turn_id: Option<String>,
    round_log: Arc<RoundLog>,
}

impl ClaudePrint {
    fn is_drained(&self) -> bool {
        self.drain_sent == self.drain_replayed
    }
}

// -- Server state --

type Clients = HashMap<u64, mpsc::UnboundedSender<String>>;

struct ServerState {
    coordinators: HashMap<(String, String), CoordinatorHandle>,
    next_client_id: u64,
    bus_tx: broadcast::Sender<String>,
    wicket_mgr_tx: mpsc::UnboundedSender<WicketManagerMsg>,
}

impl ServerState {
    fn new(wicket_mgr_tx: mpsc::UnboundedSender<WicketManagerMsg>) -> Self {
        let (bus_tx, _) = broadcast::channel(65536);
        Self {
            coordinators: HashMap::new(),
            next_client_id: 0,
            bus_tx,
            wicket_mgr_tx,
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_client_id;
        self.next_client_id += 1;
        id
    }

    fn resolve_timestamp(slug: &str, intent: &str) -> Option<String> {
        let home = env::var("HOME").unwrap_or_default();
        let dir = Path::new(&home)
            .join(".local/state/easement")
            .join(slug);
        let ts_pattern = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.jsonl$").ok()?;
        let mut timestamps: Vec<String> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if ts_pattern.is_match(name) {
                        timestamps.push(name.trim_end_matches(".jsonl").to_string());
                    }
                }
            }
        }
        timestamps.sort();
        match intent {
            "full" => {
                if timestamps.len() >= 2 {
                    Some(timestamps[timestamps.len() - 2].clone())
                } else {
                    None
                }
            }
            _ => {
                if timestamps.is_empty() {
                    let ts = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
                    Some(ts)
                } else {
                    timestamps.last().cloned()
                }
            }
        }
    }

    fn find_or_create_coordinator(&mut self, slug: &str, timestamp: &str) -> mpsc::UnboundedSender<CoordMessage> {
        let key = (slug.to_string(), timestamp.to_string());
        let bus = self.bus_tx.clone();
        let wmgr = self.wicket_mgr_tx.clone();
        let handle = self.coordinators.entry(key).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            let slug_clone = slug.to_string();
            let ts_clone = timestamp.to_string();
            let self_tx = tx.clone();
            tokio::spawn(async move {
                run_coordinator(slug_clone, ts_clone, self_tx, rx, bus, wmgr).await;
            });
            CoordinatorHandle { tx }
        });
        handle.tx.clone()
    }
}

struct CoordinatorHandle {
    tx: mpsc::UnboundedSender<CoordMessage>,
}

// -- Messages to coordinator --

enum CoordMessage {
    ClientConnected {
        id: u64,
        protocol: String,
        timestamp: Option<String>,
        host: Option<String>,
        tx: mpsc::UnboundedSender<String>,
    },
    ClientDisconnected {
        id: u64,
    },
    Envelope {
        id: u64,
        envelope: InboundEnvelope,
    },
    ToolCall {
        call_id: String,
        tool: String,
        args: Value,
        reply: oneshot::Sender<ToolResult>,
    },
    SetHost {
        hostname: String,
        reply: oneshot::Sender<String>,
    },
    Message {
        message: String,
        full: bool,
        notification: bool,
        reply: oneshot::Sender<String>,
    },
    NewSession {
        reply: oneshot::Sender<String>,
    },
}

struct ToolResult {
    output: String,
    exit_code: i32,
}

// -- Service request/response (retained for Shotgun) --

struct ServiceResponse {
    content_type: String,
    body: Vec<u8>,
}

struct PendingService {
    id: String,
    reply: oneshot::Sender<ServiceResponse>,
}

// -- Wicket manager --

enum WicketManagerMsg {
    Ensure {
        host: String,
        slug: String,
        reply: oneshot::Sender<Result<String, String>>,
    },
    Connected {
        host: String,
        client_id: u64,
    },
    Disconnected {
        client_id: u64,
    },
}

struct PendingSpawn {
    host: String,
    child: tokio::process::Child,
    reply: oneshot::Sender<Result<String, String>>,
    deadline: tokio::time::Instant,
}

// -- Broadcast helpers --

fn bus_publish(bus_tx: &broadcast::Sender<String>, stream: &str, slug: &str, timestamp: &str, data: Value) {
    let msg = json!({
        "stream": stream,
        "slug": slug,
        "timestamp": timestamp,
        "data": data,
    });
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = bus_tx.send(json);
    }
}

fn bus_publish_replay(bus_tx: &broadcast::Sender<String>, stream: &str, slug: &str, timestamp: &str, replay_id: &str, data: Value) {
    let msg = json!({
        "stream": stream,
        "slug": slug,
        "timestamp": timestamp,
        "replay_id": replay_id,
        "data": data,
    });
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = bus_tx.send(json);
    }
}

fn send_to(clients: &Clients, client_id: u64, stream: &str, data: Value) {
    if let Some(json) = envelope_json(stream, data) {
        if let Some(tx) = clients.get(&client_id) {
            let _ = tx.send(json);
        }
    }
}

const STEER_SENTINEL: &str = "\n\x07---\n";

// -- MCP JSON-RPC types --

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

// -- Spawn ClaudePrint --

async fn spawn_claude_print(
    slug: &str,
    message: &str,
    transcript_entries: &[Value],
    session_uuid: Option<&str>,
    timestamp: &str,
) -> Result<ClaudePrint, String> {
    let home = env::var("HOME").unwrap_or_default();

    let pane_dir = PathBuf::from(&home).join("pane").join(slug);
    let _ = std::fs::create_dir_all(&pane_dir);
    if std::env::set_current_dir(&pane_dir).is_err() {
        return Err(format!("cannot cd to {}", pane_dir.display()));
    }

    let config_path = PathBuf::from(&home).join(".claude.json");
    if let Err(e) = ensure_trust(&config_path, pane_dir.to_str().unwrap_or("")) {
        tracing::warn!("failed to ensure trust: {}", e);
    }

    let round_log = Arc::new(RoundLog::begin(slug));

    let mut resume_arg: Option<String> = None;
    if let Some(uuid) = session_uuid {
        if !transcript_entries.is_empty() {
            round_log.log_sent(transcript_entries);
            let cli_path = cli_transcript_path(slug, uuid);
            emplace_transcript(&cli_path, transcript_entries)?;
            tracing::info!(uuid = %uuid, path = %cli_path.display(), entries = transcript_entries.len(), "emplaced transcript");
        }
        resume_arg = Some(uuid.to_string());
    } else if !transcript_entries.is_empty() {
        round_log.log_sent(transcript_entries);
        let tmp_path = std::env::temp_dir().join(format!("easement-{}.jsonl", slug));
        let mut content = String::new();
        for entry in transcript_entries {
            if let Ok(line) = serde_json::to_string(entry) {
                content.push_str(&line);
                content.push('\n');
            }
        }
        std::fs::write(&tmp_path, &content)
            .map_err(|e| format!("cannot write transcript temp file: {}", e))?;
        resume_arg = Some(tmp_path.to_string_lossy().to_string());
    }

    tracing::info!(resume_arg = ?resume_arg, "spawning claude");

    let mut cmd = Command::new("claude");
    cmd.env("MCP_TOOL_TIMEOUT", "2147483647");
    cmd.arg("--print")
        .arg("--input-format").arg("stream-json")
        .arg("--output-format").arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--replay-user-messages")
        .arg("--verbose")
        .arg("--model").arg("claude-opus-4-6[1m]")
        .arg("--thinking-display").arg("summarized")
        .arg("--max-thinking-tokens").arg("31999")
        .arg("--add-dir").arg(format!("{}/code", home));

    let mcp_config_path = std::env::temp_dir()
        .join(format!("easement-mcp-{}.json", slug));
    let mcp_config = json!({
        "mcpServers": {
            "o": {
                "type": "http",
                "url": format!("http://localhost:{}/mcp/{}/{}", easement_port(), slug, timestamp)
            }
        }
    });
    if let Err(e) = std::fs::write(&mcp_config_path, mcp_config.to_string()) {
        return Err(format!("cannot write mcp config: {}", e));
    }
    cmd.arg("--permission-prompt-tool").arg("mcp__o__approve")
        .arg("--mcp-config").arg(&mcp_config_path)
        .arg("--disallowed-tools").arg("Bash,Write,Edit,Read,Glob,Grep,Skill,ToolSearch,NotebookEdit,WebFetch,WebSearch,CronCreate,CronDelete,CronList,RemoteTrigger,TaskOutput,TaskStop,EnterWorktree,ExitWorktree,ExitPlanMode,Monitor,PushNotification,AskUserQuestion,ScheduleWakeup,ShareOnboardingGuide");

    if let Some(ref ra) = resume_arg {
        cmd.arg("--resume").arg(ra);
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().map_err(|e| format!("cannot spawn claude: {}", e))?;

    let mut child_stdin = child.stdin.take().expect("stdin was piped");
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
                        tracing::warn!(stderr = %trimmed, "claude stderr");
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Send kickoff message.
    let kickoff = format_user_message(message);
    child_stdin.write_all(kickoff.as_bytes()).await
        .map_err(|_| "failed to send kickoff message".to_string())?;
    let _ = child_stdin.flush().await;

    // Stdout reader task.
    let (event_tx, event_rx) = mpsc::channel::<ClaudeEvent>(256);
    let round_log_clone = round_log.clone();

    tokio::spawn(async move {
        let mut reader = BufReader::new(child_stdout);
        let mut line = String::new();
        let mut session_id_sent = false;

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    let _ = event_tx.send(ClaudeEvent::Eof).await;
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    round_log_clone.log_stdout(trimmed);

                    let data: Value = match serde_json::from_str(trimmed) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                    if !session_id_sent {
                        if let Some(sid) = data.get("session_id").and_then(|v| v.as_str()) {
                            let _ = event_tx.send(ClaudeEvent::SessionId(sid.to_string())).await;
                            session_id_sent = true;
                        }
                    }

                    let event_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");

                    if event_type == "stream_event" {
                        if let Some(event) = data.get("event") {
                            let _ = event_tx.send(ClaudeEvent::Delta(event.clone())).await;
                        }
                    } else if let Ok(event) = serde_json::from_value::<StdoutEvent>(data.clone()) {
                        match &event {
                            StdoutEvent::User { is_replay: true, message, .. } => {
                                let text = message.get("content")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let _ = event_tx.send(ClaudeEvent::Replay { message: text }).await;
                            }
                            StdoutEvent::Result { subtype, .. } => {
                                let usage = data.get("usage").cloned();
                                let is_interrupted = subtype.as_deref() == Some("error_during_execution");
                                let _ = event_tx.send(ClaudeEvent::Result { usage, is_interrupted }).await;
                            }
                            _ => {}
                        }
                    }
                }
                Err(_) => {
                    let _ = event_tx.send(ClaudeEvent::Eof).await;
                    break;
                }
            }
        }
    });

    Ok(ClaudePrint {
        child,
        stdin: Some(child_stdin),
        event_rx,
        session_id: None,
        drain_sent: 1,
        drain_replayed: 0,
        turn_id: None,
        round_log,
    })
}

fn easement_port() -> u16 {
    env::var("EASEMENT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6502)
}

// -- Wicket spawn --

fn spawn_wicket(host: &str, slug: &str) -> Result<tokio::process::Child, String> {
    let port = easement_port();
    let is_orb = host.contains("orb");
    let wicket_url = if is_orb {
        format!("ws://host.internal:{}", port)
    } else {
        format!("ws://localhost:{}", port)
    };

    let mut cmd = if host == "localhost" {
        let mut c = tokio::process::Command::new("wicket");
        c.arg(&wicket_url).arg(slug).arg("localhost");
        c
    } else {
        let mut c = tokio::process::Command::new("ssh");
        if !is_orb {
            c.arg("-R").arg(format!("{}:localhost:{}", port, port));
        }
        c.arg(host).arg("wicket").arg(&wicket_url).arg(slug).arg(host);
        c
    };

    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    cmd.spawn().map_err(|e| format!("failed to spawn wicket on {}: {}", host, e))
}

async fn run_wicket_manager(mut rx: mpsc::UnboundedReceiver<WicketManagerMsg>) {
    let mut hosts: HashMap<String, u64> = HashMap::new();
    let mut pending: Option<PendingSpawn> = None;

    loop {
        let deadline = pending.as_ref().map(|p| p.deadline);

        tokio::select! {
            Some(msg) = rx.recv() => {
                match msg {
                    WicketManagerMsg::Connected { host, client_id } => {
                        tracing::info!(host = %host, client_id, "wicket connected");
                        hosts.insert(host.clone(), client_id);
                        if let Some(ref p) = pending {
                            if p.host == host {
                                let p = pending.take().unwrap();
                                let _ = p.reply.send(Ok(host));
                            }
                        }
                    }
                    WicketManagerMsg::Disconnected { client_id } => {
                        let removed: Vec<String> = hosts.iter()
                            .filter(|(_, v)| **v == client_id)
                            .map(|(k, _)| k.clone())
                            .collect();
                        for host in &removed {
                            tracing::info!(host = %host, client_id, "wicket disconnected");
                        }
                        hosts.retain(|_, v| *v != client_id);
                    }
                    WicketManagerMsg::Ensure { host, slug, reply } => {
                        if hosts.contains_key(&host) {
                            tracing::info!(host = %host, "wicket already connected");
                            let _ = reply.send(Ok(host));
                        } else {
                            tracing::info!(host = %host, "spawning wicket");
                            match spawn_wicket(&host, &slug) {
                                Ok(child) => {
                                    pending = Some(PendingSpawn {
                                        host,
                                        child,
                                        reply,
                                        deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(10),
                                    });
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                }
                            }
                        }
                    }
                }
            }
            status = async {
                match pending.as_mut() {
                    Some(p) => p.child.wait().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(p) = pending.take() {
                    let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                    tracing::warn!(host = %p.host, exit_code = code, "wicket exited before connecting");
                    let _ = p.reply.send(Err(format!("wicket exited with code {}", code)));
                }
            }
            _ = async {
                match deadline {
                    Some(dl) => tokio::time::sleep_until(dl).await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(p) = pending.take() {
                    tracing::warn!(host = %p.host, "wicket spawn timed out");
                    let _ = p.reply.send(Err("timeout waiting for wicket to connect".to_string()));
                }
            }
        }
    }
}

// -- Coordinator (per window: slug + timestamp) --

async fn run_coordinator(slug: String, timestamp: String, coord_tx: mpsc::UnboundedSender<CoordMessage>, mut coord_rx: mpsc::UnboundedReceiver<CoordMessage>, bus_tx: broadcast::Sender<String>, wicket_mgr_tx: mpsc::UnboundedSender<WicketManagerMsg>) {
    let exchange = ExchangeLog::new(&slug);

    let mut transcript = Transcript::new(&slug, Some(&timestamp));
    let mut entries: Vec<NormalizedEntry> = transcript.load_history();

    let mut session_uuid: Option<String> = extract_session_uuid(transcript.entries());

    if let Some(ref uuid) = session_uuid {
        tracing::info!(session_uuid = %uuid, "restored session uuid from transcript");
    }

    let mut clients: Clients = HashMap::new();
    let mut claude: Option<ClaudePrint> = None;
    let mut last_usage: Option<Value> = None;
    let mut pending_service: Option<PendingService> = None;
    let mut pending_tool: Option<(String, oneshot::Sender<ToolResult>)> = None;
    let mut pending_escalation: Option<(String, String, Value)> = None;
    let mut turn_queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut steer_queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut pending_message_reply: Option<oneshot::Sender<String>> = None;
    let mut response_accumulator: String = String::new();
    let mut shell_host: String = "localhost".to_string();
    let mut pending_ensure: Option<oneshot::Receiver<Result<String, String>>> = None;

    tracing::info!(
        slug = %slug,
        timestamp = %timestamp,
        history = entries.len(),
        "coordinator started"
    );

    loop {
        tokio::select! {
            Some(msg) = coord_rx.recv() => {
                match msg {
                    CoordMessage::ClientConnected { id, protocol, timestamp: _connect_ts, host, tx } => {
                        if protocol == "wicket" {
                            clients.insert(id, tx);
                            tracing::info!(client_id = id, "wicket connected");
                            continue;
                        }
                        if protocol != "easement" {
                            tracing::warn!(client_id = id, protocol = %protocol, "unknown protocol, dropping");
                            continue;
                        }
                        clients.insert(id, tx);
                        tracing::info!(client_id = id, clients = clients.len(), "client connected");
                    }
                    CoordMessage::ClientDisconnected { id } => {
                        clients.remove(&id);
                        tracing::info!(client_id = id, clients = clients.len(), "client disconnected");
                    }
                    CoordMessage::ToolCall { call_id, tool, args, reply } => {
                        // Flush steers to ClaudePrint stdin before dispatching.
                        if !steer_queue.is_empty() {
                            if let Some(ref mut cp) = claude {
                                if let Some(ref mut stdin) = cp.stdin {
                                    let count = steer_queue.len();
                                    while let Some(steer) = steer_queue.pop_front() {
                                        let msg = format_user_message(&steer);
                                        let _ = stdin.write_all(msg.as_bytes()).await;
                                        cp.drain_sent += 1;
                                    }
                                    let _ = stdin.flush().await;
                                    tracing::info!(count, "flushed steers to ClaudePrint stdin");
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                }
                            }
                        }

                        // Scatter/gather for tool discovery.
                        if tool == "tools" {
                            tracing::info!(call_id = %call_id, "tools discovery query");
                            let mut bus_rx_gather = bus_tx.subscribe();
                            bus_publish(&bus_tx, "tools_query", &slug, &timestamp, json!({ "id": call_id }));
                            let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
                            let mut tools: Vec<Value> = Vec::new();
                            loop {
                                tokio::select! {
                                    result = bus_rx_gather.recv() => {
                                        if let Ok(msg) = result {
                                            if let Ok(parsed) = serde_json::from_str::<Value>(&msg) {
                                                if parsed.get("stream").and_then(|v| v.as_str()) == Some("tools_response") {
                                                    if let Some(id) = parsed.get("data").and_then(|d| d.get("id")).and_then(|v| v.as_str()) {
                                                        if id == call_id {
                                                            if let Some(manifest) = parsed.get("data").and_then(|d| d.get("tools")).and_then(|v| v.as_array()) {
                                                                tools.extend(manifest.iter().cloned());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ = tokio::time::sleep_until(deadline) => {
                                        break;
                                    }
                                }
                            }
                            tracing::info!(count = tools.len(), "tools discovery complete");
                            let _ = reply.send(ToolResult {
                                output: serde_json::to_string_pretty(&tools).unwrap_or_else(|_| "[]".to_string()),
                                exit_code: 0,
                            });
                            continue;
                        }

                        // Check escalation from args.
                        let inner_args = args.get("args").cloned().unwrap_or(json!({}));

                        // Ensure a Wicket is available on the target host.
                        let host = inner_args.get("host").and_then(|v| v.as_str()).unwrap_or("localhost");
                        if host != "localhost" {
                            let (ensure_tx, ensure_rx) = oneshot::channel();
                            let _ = wicket_mgr_tx.send(WicketManagerMsg::Ensure {
                                host: host.to_string(),
                                slug: slug.clone(),
                                reply: ensure_tx,
                            });
                            match ensure_rx.await {
                                Ok(Ok(h)) => {
                                    tracing::info!(host = %h, "wicket ensured for tool call");
                                }
                                Ok(Err(e)) => {
                                    tracing::warn!(host = %host, error = %e, "cannot ensure wicket");
                                    let _ = reply.send(ToolResult {
                                        output: format!("no wicket on {}: {}", host, e),
                                        exit_code: 1,
                                    });
                                    continue;
                                }
                                Err(_) => {
                                    let _ = reply.send(ToolResult {
                                        output: "wicket manager unavailable".to_string(),
                                        exit_code: 1,
                                    });
                                    continue;
                                }
                            }
                        }

                        let escalate = inner_args.get("escalate").and_then(|v| v.as_bool()).unwrap_or(false);

                        if escalate {
                            let who = args.get("who").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let f = args.get("f").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            tracing::info!(call_id = %call_id, who = %who, f = %f, "escalation requested");
                            pending_tool = Some((call_id.clone(), reply));
                            pending_escalation = Some((call_id.clone(), tool.clone(), args.clone()));
                            bus_publish(&bus_tx, "approval", &slug, &timestamp, json!({
                                "tool_name": f,
                                "input": inner_args,
                            }));
                        } else {
                            // Generic broadcast/claim dispatch.
                            tracing::info!(call_id = %call_id, tool = %tool, "broadcasting call");
                            pending_tool = Some((call_id.clone(), reply));
                            bus_publish(&bus_tx, "call", &slug, &timestamp, json!({
                                "id": call_id,
                                "who": args.get("who").and_then(|v| v.as_str()).unwrap_or(""),
                                "f": args.get("f").and_then(|v| v.as_str()).unwrap_or(""),
                                "args": inner_args,
                            }));
                            bus_publish(&bus_tx, "tool_start", &slug, &timestamp, json!({
                                "tool": tool,
                            }));
                        }
                    }
                    CoordMessage::SetHost { hostname, reply } => {
                        tracing::info!(to = %hostname, "host switch (no-op, host is in args)");
                        let _ = reply.send(hostname);
                    }
                    CoordMessage::Message { message, full, notification, reply } => {
                        let message = if notification {
                            format!("\x07**notification**: {}", message)
                        } else {
                            message
                        };
                        tracing::info!(message = %message, full, notification, "synchronous message");
                        if claude.is_some() {
                            turn_queue.push_back(message);
                        } else {
                            let turn_id = uuid::Uuid::new_v4().to_string();
                            bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                "event": "started",
                                "turn_id": turn_id,
                            }));
                            bus_publish(&bus_tx, "user_message", &slug, &timestamp, json!({
                                "text": message,
                            }));
                            if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundStarted) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }
                            let entries = transcript.entries().to_vec();
                            match spawn_claude_print(&slug, &message, &entries, session_uuid.as_deref(), &timestamp).await {
                                Ok(mut cp) => {
                                    cp.turn_id = Some(turn_id);
                                    claude = Some(cp);
                                }
                                Err(e) => {
                                    let _ = reply.send(format!("error: {}", e));
                                    continue;
                                }
                            }
                        }
                        pending_message_reply = Some(reply);
                    }
                    CoordMessage::NewSession { reply } => {
                        let ts = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
                        tracing::info!(timestamp = %ts, "new session requested");
                        let _ = reply.send(ts);
                    }
                    CoordMessage::Envelope { id, envelope } => {
                        exchange.log("client>wicket", &json!({
                            "client_id": id,
                            "stream": &envelope.stream,
                            "data": &envelope.data,
                        }));

                        match envelope.stream.as_str() {
                            "turn" | "claude" => {
                                let message = envelope.data.get("message")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();

                                if claude.is_some() {
                                    tracing::info!(message = %message, "turn queued, ClaudePrint active");
                                    turn_queue.push_back(message);
                                    continue;
                                }

                                let turn_id = uuid::Uuid::new_v4().to_string();

                                bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                    "event": "started",
                                    "turn_id": turn_id,
                                }));
                                bus_publish(&bus_tx, "user_message", &slug, &timestamp, json!({
                                    "text": message,
                                }));
                                if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundStarted) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }

                                let entries = transcript.entries().to_vec();
                                tracing::info!(turn_id = %turn_id, transcript_entries = entries.len(), "spawning ClaudePrint");

                                match spawn_claude_print(&slug, &message, &entries, session_uuid.as_deref(), &timestamp).await {
                                    Ok(mut cp) => {
                                        cp.turn_id = Some(turn_id);
                                        claude = Some(cp);
                                    }
                                    Err(e) => {
                                        tracing::error!("failed to spawn ClaudePrint: {}", e);
                                        bus_publish(&bus_tx, "error", &slug, &timestamp, json!({ "message": &e }));
                                        if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundFailed { message: e }) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }
                                    }
                                }
                            }
                            "steer" => {
                                let message = envelope.data.get("message")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                if !message.is_empty() {
                                    tracing::info!(message = %message, "steer queued");
                                    steer_queue.push_back(message);
                                }
                            }
                            "interrupt" => {
                                if let Some(ref mut cp) = claude {
                                    if let Some(ref mut stdin) = cp.stdin {
                                        let msg = json!({
                                            "type": "control_request",
                                            "request_id": uuid::Uuid::new_v4().to_string(),
                                            "request": { "subtype": "interrupt" }
                                        });
                                        let mut line = serde_json::to_string(&msg).unwrap();
                                        line.push('\n');
                                        let _ = stdin.write_all(line.as_bytes()).await;
                                        let _ = stdin.flush().await;
                                        tracing::info!("interrupt sent to claude");
                                    }
                                }
                            }
                            "shell" => {
                                let call_id = uuid::Uuid::new_v4().to_string();
                                if shell_host != "localhost" {
                                    let (ensure_tx, ensure_rx) = oneshot::channel();
                                    let _ = wicket_mgr_tx.send(WicketManagerMsg::Ensure {
                                        host: shell_host.clone(),
                                        slug: slug.clone(),
                                        reply: ensure_tx,
                                    });
                                    match ensure_rx.await {
                                        Ok(Ok(h)) => {
                                            tracing::info!(host = %h, "wicket ensured for shell command");
                                        }
                                        Ok(Err(e)) => {
                                            tracing::warn!(host = %shell_host, error = %e, "cannot ensure wicket for shell");
                                            bus_publish(&bus_tx, "shell_result", &slug, &timestamp, json!({
                                                "call_id": call_id,
                                                "output": format!("no wicket on {}: {}", shell_host, e),
                                                "exit_code": 1,
                                            }));
                                            continue;
                                        }
                                        Err(_) => {
                                            bus_publish(&bus_tx, "shell_result", &slug, &timestamp, json!({
                                                "call_id": call_id,
                                                "output": "wicket manager unavailable",
                                                "exit_code": 1,
                                            }));
                                            continue;
                                        }
                                    }
                                }
                                bus_publish(&bus_tx, "call", &slug, &timestamp, json!({
                                    "id": call_id,
                                    "who": "wicket",
                                    "f": "shell",
                                    "args": {
                                        "command": envelope.data.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                                        "host": &shell_host,
                                    },
                                }));
                                tracing::info!("shell command broadcast on bus");
                            }
                            "host" => {
                                let hostname = envelope.data.get("hostname")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("localhost")
                                    .to_string();
                                tracing::info!(to = %hostname, "ensuring wicket for host switch");
                                let (reply_tx, reply_rx) = oneshot::channel();
                                let _ = wicket_mgr_tx.send(WicketManagerMsg::Ensure {
                                    host: hostname,
                                    slug: slug.clone(),
                                    reply: reply_tx,
                                });
                                pending_ensure = Some(reply_rx);
                            }
                            "claim" => {
                                let claim_id = envelope.data.get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if let Some(ref pending) = pending_service {
                                    if pending.id == claim_id {
                                        tracing::info!(id = %claim_id, "service request claimed");
                                    }
                                }
                            }
                            "response" => {
                                let resp_id = envelope.data.get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                if let Some((pending_call_id, reply)) = pending_tool.take() {
                                    if pending_call_id == resp_id {
                                        let content_type = envelope.data.get("content_type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("text/plain")
                                            .to_string();
                                        let body_b64 = envelope.data.get("body")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("");
                                        let output = if content_type == "image/jpeg" || content_type == "image/png" {
                                            use base64::Engine;
                                            let content = json!([
                                                { "type": "text", "text": format!("screenshot ({})", content_type) },
                                                { "type": "image", "data": body_b64, "mimeType": content_type }
                                            ]);
                                            serde_json::to_string(&content).unwrap_or_default()
                                        } else {
                                            use base64::Engine;
                                            let decoded = base64::engine::general_purpose::STANDARD.decode(body_b64).unwrap_or_default();
                                            String::from_utf8_lossy(&decoded).to_string()
                                        };
                                        tracing::info!(id = %resp_id, content_type = %content_type, "shotgun response received");
                                        bus_publish(&bus_tx, "tool_done", &slug, &timestamp, json!({
                                            "tool": "shotgun",
                                            "output": &output,
                                            "exit_code": 0,
                                        }));
                                        let _ = reply.send(ToolResult { output, exit_code: 0 });
                                    } else {
                                        pending_tool = Some((pending_call_id, reply));
                                        // Fall through to pending_service check.
                                    }
                                }
                                if let Some(pending) = pending_service.take() {
                                    if pending.id == resp_id {
                                        let content_type = envelope.data.get("content_type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("application/octet-stream")
                                            .to_string();
                                        let body = envelope.data.get("body")
                                            .and_then(|v| v.as_str())
                                            .map(|b64| {
                                                use base64::Engine;
                                                base64::engine::general_purpose::STANDARD.decode(b64).unwrap_or_default()
                                            })
                                            .unwrap_or_default();
                                        tracing::info!(id = %resp_id, content_type = %content_type, bytes = body.len(), "service response received");
                                        let _ = pending.reply.send(ServiceResponse { content_type, body });
                                    } else {
                                        tracing::warn!(expected = %pending.id, got = %resp_id, "service response id mismatch");
                                        pending_service = Some(pending);
                                    }
                                }
                            }
                            "service_timeout" => {
                                let timeout_id = envelope.data.get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if let Some(pending) = pending_service.take() {
                                    if pending.id == timeout_id {
                                        tracing::warn!(id = %timeout_id, "service request timed out (no claim)");
                                    } else {
                                        pending_service = Some(pending);
                                    }
                                }
                            }
                            "tool_result" => {
                                tracing::info!(client_id = id, data = %envelope.data, "tool_result envelope received");
                                let call_id = envelope.data.get("call_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let output = envelope.data.get("output")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let exit_code = envelope.data.get("exit_code")
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(-1) as i32;
                                tracing::info!(call_id = %call_id, exit_code, output_len = output.len(), "tool result from wicket");
                                bus_publish(&bus_tx, "tool_done", &slug, &timestamp, json!({
                                    "tool": "zsh",
                                    "output": &output,
                                    "exit_code": exit_code,
                                }));
                                if let Some((pending_call_id, reply)) = pending_tool.take() {
                                    if pending_call_id == call_id {
                                        let _ = reply.send(ToolResult { output, exit_code });
                                    } else {
                                        tracing::warn!(expected = %pending_call_id, got = %call_id, "tool result call_id mismatch");
                                    }
                                }
                            }
                            "shell_result" => {
                                bus_publish(&bus_tx, "shell_result", &slug, &timestamp, envelope.data);
                            }
                            "background_output" => {
                                let path = envelope.data.get("output_path")
                                    .and_then(|v| v.as_str()).unwrap_or("");
                                let line = envelope.data.get("line")
                                    .and_then(|v| v.as_str()).unwrap_or("");
                                if !path.is_empty() {
                                    if let Some(parent) = std::path::Path::new(path).parent() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                    if let Ok(mut f) = std::fs::OpenOptions::new()
                                        .create(true).append(true).open(path)
                                    {
                                        let _ = std::io::Write::write_all(&mut f, line.as_bytes());
                                        let _ = std::io::Write::write_all(&mut f, b"\n");
                                    }
                                }
                            }
                            "background_done" => {
                                let task_uuid = envelope.data.get("task_uuid")
                                    .and_then(|v| v.as_str()).unwrap_or("");
                                let exit_code = envelope.data.get("exit_code")
                                    .and_then(|v| v.as_i64()).unwrap_or(-1);
                                let output_path = envelope.data.get("output_path")
                                    .and_then(|v| v.as_str()).unwrap_or("");
                                tracing::info!(task_uuid = %task_uuid, exit_code, output_path = %output_path, "background task done");
                                let notif_text = format!(
                                    "background task {} exited with code {}, output at {}",
                                    task_uuid, exit_code, output_path
                                );
                                let (reply_tx, _) = oneshot::channel();
                                let _ = coord_tx.send(CoordMessage::Message {
                                    message: notif_text,
                                    full: false,
                                    notification: true,
                                    reply: reply_tx,
                                });
                            }
                            "approval" => {
                                let behavior = envelope.data.get("behavior")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("deny");
                                tracing::info!(behavior = %behavior, "approval response from client");

                                if let Some((esc_call_id, _esc_tool, esc_args)) = pending_escalation.take() {
                                    if behavior == "allow" {
                                        let inner_args = esc_args.get("args").cloned().unwrap_or(json!({}));
                                        tracing::info!(call_id = %esc_call_id, "escalation approved, broadcasting unsandboxed");
                                        bus_publish(&bus_tx, "call", &slug, &timestamp, json!({
                                            "id": esc_call_id,
                                            "who": esc_args.get("who").and_then(|v| v.as_str()).unwrap_or(""),
                                            "f": esc_args.get("f").and_then(|v| v.as_str()).unwrap_or(""),
                                            "args": inner_args,
                                            "escalated": true,
                                        }));
                                        bus_publish(&bus_tx, "tool_start", &slug, &timestamp, json!({
                                            "tool": esc_args.get("f").and_then(|v| v.as_str()).unwrap_or(""),
                                        }));
                                    } else {
                                        tracing::info!(call_id = %esc_call_id, "escalation denied");
                                        if let Some((_call_id, reply)) = pending_tool.take() {
                                            let _ = reply.send(ToolResult {
                                                output: "escalation denied by operator".to_string(),
                                                exit_code: 1,
                                            });
                                        }
                                    }
                                }
                            }
                            "history_request" => {
                                let replay_id = envelope.data.get("replay_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                tracing::info!(replay_id = %replay_id, entries = entries.len(), "history replay starting");
                                for entry in &entries {
                                    if let Ok(data) = serde_json::to_value(entry) {
                                        bus_publish_replay(&bus_tx, "entry", &slug, &timestamp, &replay_id, data);
                                    }
                                }
                                bus_publish_replay(&bus_tx, "history_terminate", &slug, &timestamp, &replay_id, json!({}));
                                if let Some(ref usage) = last_usage {
                                    bus_publish(&bus_tx, "usage", &slug, &timestamp, usage.clone());
                                }
                                tracing::info!(replay_id = %replay_id, "history replay complete");
                            }
                            "heartbeat" => {}
                            "log" => {
                                let level = envelope.data.get("level")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("info");
                                let message = envelope.data.get("message")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let fields = envelope.data.get("fields");
                                match level {
                                    "error" => tracing::error!(client_id = id, slug = %slug, fields = ?fields, "[client] {}", message),
                                    "warn" => tracing::warn!(client_id = id, slug = %slug, fields = ?fields, "[client] {}", message),
                                    _ => tracing::info!(client_id = id, slug = %slug, fields = ?fields, "[client] {}", message),
                                }
                            }
                            "exit" => {
                                clients.remove(&id);
                                tracing::info!(client_id = id, "client sent exit");
                            }
                            other => {
                                tracing::debug!(stream = %other, "unknown inbound stream");
                            }
                        }
                    }
                }
            }


            // Wicket manager ensure reply.
            result = async {
                match pending_ensure.as_mut() {
                    Some(rx) => rx.await,
                    None => std::future::pending().await,
                }
            } => {
                pending_ensure = None;
                match result {
                    Ok(Ok(host)) => {
                        tracing::info!(host = %host, "host switch confirmed");
                        shell_host = host;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "host switch failed");
                        bus_publish(&bus_tx, "error", &slug, &timestamp, json!({ "message": format!("host switch failed: {}", e) }));
                    }
                    Err(_) => {
                        tracing::warn!("host switch failed: manager dropped reply");
                    }
                }
            }
            // ClaudePrint stdout events.
            Some(event) = async {
                match claude.as_mut() {
                    Some(cp) => cp.event_rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                let cp = claude.as_mut().unwrap();
                match event {
                    ClaudeEvent::Delta(ref delta) => {
                        if pending_message_reply.is_some() {
                            if let Some(text) = delta.get("delta")
                                .and_then(|d| d.get("text"))
                                .and_then(|v| v.as_str())
                            {
                                response_accumulator.push_str(text);
                            }
                        }
                        bus_publish(&bus_tx, "delta", &slug, &timestamp, delta.clone());
                    }
                    ClaudeEvent::SessionId(sid) => {
                        tracing::info!(session_id = %sid, "captured session id from claude");
                        cp.session_id = Some(sid.clone());
                        if session_uuid.is_none() {
                            if uuid::Uuid::parse_str(&sid).is_ok() {
                                session_uuid = Some(sid.clone());
                                tracing::info!(session_uuid = %sid, "captured session uuid for emplacement");
                            } else {
                                tracing::warn!(session_id = %sid, "first session id is not a uuid, emplacement deferred");
                            }
                        }
                    }
                    // The CLI echoes every user message we wrote to stdin as a
                    // replay event. The first replay is the kickoff message
                    // (the turn start). Replays after that are steers we
                    // injected before tool calls. We count replays for the
                    // drain gate and broadcast steer acks to clear the TUI
                    // preview. A stronger assertion would count queued_command
                    // attachment entries in the CLI's transcript file after the
                    // round and panic if the count doesn't match drain_sent - 1.
                    // See easement/observations/transcripts.md, concern on
                    // attachment entries.
                    ClaudeEvent::Replay { message } => {
                        cp.drain_replayed += 1;
                        let is_steer_ack = cp.drain_replayed > 1;
                        tracing::debug!(
                            sent = cp.drain_sent,
                            replayed = cp.drain_replayed,
                            is_steer_ack,
                            message = %message,
                            "drain gate: {}/{}",
                            cp.drain_replayed,
                            cp.drain_sent
                        );
                        if is_steer_ack && !message.is_empty() {
                            let parts: Vec<&str> = message.split(STEER_SENTINEL).collect();
                            for part in &parts {
                                let trimmed = part.trim();
                                if !trimmed.is_empty() {
                                    tracing::info!(steer = %trimmed, "broadcasting user_message");
                                    bus_publish(&bus_tx, "user_message", &slug, &timestamp, json!({
                                        "text": trimmed,
                                    }));
                                }
                            }
                        }
                    }
                    ClaudeEvent::Result { usage, is_interrupted } => {
                        if let Some(ref u) = usage {
                            bus_publish(&bus_tx, "usage", &slug, &timestamp, u.clone());
                            last_usage = usage;
                        }

                        if is_interrupted {
                            if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundInterrupted) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }
                            if let Some(ref tid) = cp.turn_id {
                                bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                    "event": "completed",
                                    "turn_id": tid,
                                    "status": "interrupted",
                                }));
                            }
                        }

                        if cp.is_drained() {
                            // If there are missed steers, join them with the
                            // bell-byte sentinel and write as one message. The
                            // model sees newlines and dashes. When the replay
                            // comes back we split on the sentinel and broadcast
                            // each piece as an individual committed_user_message.
                            if !steer_queue.is_empty() {
                                if let Some(ref mut stdin) = cp.stdin {
                                    let joined = steer_queue
                                        .drain(..)
                                        .collect::<Vec<_>>()
                                        .join(STEER_SENTINEL);
                                    let steer_turn_id = uuid::Uuid::new_v4().to_string();
                                    cp.turn_id = Some(steer_turn_id.clone());
                                    bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                        "event": "started",
                                        "turn_id": steer_turn_id,
                                    }));
                                    let msg = format_user_message(&joined);
                                    let _ = stdin.write_all(msg.as_bytes()).await;
                                    let _ = stdin.flush().await;
                                    cp.drain_sent += 1;
                                    tracing::info!("wrote missed steers as combined message, started turn");
                                }
                                continue;
                            }

                            let turn_id = cp.turn_id.clone();
                            let session_id = cp.session_id.clone();
                            let round_log = cp.round_log.clone();

                            // Close stdin so Claude exits cleanly.
                            cp.stdin.take();

                            // Wait for child to exit.
                            let status = cp.child.wait().await;
                            match &status {
                                Ok(s) => tracing::info!(exit_code = ?s.code(), "claude exited"),
                                Err(e) => tracing::error!(error = %e, "error waiting for claude"),
                            }

                            // Read the CLI transcript file and feed to our transcript.
                            let cli_path = if let Some(ref uuid) = session_uuid {
                                Some(cli_transcript_path(&slug, uuid))
                            } else if let Some(ref sid) = session_id {
                                find_transcript_file(sid)
                            } else {
                                None
                            };
                            if let Some(path) = cli_path {
                                round_log.copy_transcript(&path);
                                let new_entries = transcript.reconcile_cli_file(&path, &round_log.dir);
                                for entry in &new_entries {
                                    if let Ok(data) = serde_json::to_value(entry) {
                                        bus_publish(&bus_tx, "entry", &slug, &timestamp, data);
                                    }
                                    entries.push(entry.clone());
                                }
                            } else {
                                tracing::warn!("CLI transcript file not found");
                            }

                            if !is_interrupted {
                                if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundCompleted) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }
                                if let Some(ref tid) = turn_id {
                                    bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                        "event": "completed",
                                        "turn_id": tid,
                                        "status": "completed",
                                    }));
                                }
                            }

                            claude = None;
                            steer_queue.clear();
                            if let Some(reply) = pending_message_reply.take() {
                                let _ = reply.send(std::mem::take(&mut response_accumulator));
                            }
                            response_accumulator.clear();
                            tracing::info!("round completed");

                            if let Some(next_message) = turn_queue.pop_front() {
                                tracing::info!(message = %next_message, "dispatching queued turn");
                                let turn_id = uuid::Uuid::new_v4().to_string();
                                bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                    "event": "started",
                                    "turn_id": turn_id,
                                }));
                                if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundStarted) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }
                                let entries = transcript.entries().to_vec();
                                match spawn_claude_print(&slug, &next_message, &entries, session_uuid.as_deref(), &timestamp).await {
                                    Ok(mut cp) => {
                                        cp.turn_id = Some(turn_id);
                                        claude = Some(cp);
                                    }
                                    Err(e) => {
                                        tracing::error!("failed to spawn ClaudePrint for queued turn: {}", e);
                                        bus_publish(&bus_tx, "error", &slug, &timestamp, json!({ "message": &e }));
                                    }
                                }
                            }
                        }
                    }
                    ClaudeEvent::Eof => {
                        tracing::info!("claude stdout EOF");
                        let turn_id = cp.turn_id.clone();
                        let session_id = cp.session_id.clone();
                        let round_log = cp.round_log.clone();

                        cp.stdin.take();
                        let _ = cp.child.wait().await;

                        let cli_path = if let Some(ref uuid) = session_uuid {
                            Some(cli_transcript_path(&slug, uuid))
                        } else if let Some(ref sid) = session_id {
                            find_transcript_file(sid)
                        } else {
                            None
                        };
                        if let Some(path) = cli_path {
                            round_log.copy_transcript(&path);
                            let new_entries = transcript.reconcile_cli_file(&path, &round_log.dir);
                            for entry in &new_entries {
                                if let Ok(data) = serde_json::to_value(entry) {
                                    bus_publish(&bus_tx, "entry", &slug, &timestamp, data);
                                }
                                entries.push(entry.clone());
                            }
                        }

                        if let Ok(__lc_data) = serde_json::to_value(&LifecycleEvent::RoundFailed { message: "claude exited unexpectedly".to_string() }) { bus_publish(&bus_tx, "lifecycle", &slug, &timestamp, __lc_data); }
                        if let Some(ref tid) = turn_id {
                            bus_publish(&bus_tx, "turn", &slug, &timestamp, json!({
                                "event": "completed",
                                "turn_id": tid,
                                "status": "failed",
                            }));
                        }

                        claude = None;
                        steer_queue.clear();
                        turn_queue.clear();

                        if let Some(reply) = pending_message_reply.take() {
                            let _ = reply.send("error: claude exited unexpectedly".to_string());
                        }
                        response_accumulator.clear();
                    }
                }
            }
        }
    }
}

// -- WebSocket handler --

async fn handle_websocket(
    ws: hyper_tungstenite::HyperWebsocket,
    server: Arc<RwLock<ServerState>>,
) {
    let ws_stream = match ws.await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("websocket upgrade failed: {}", e);
            return;
        }
    };

    let (mut sink, mut stream) = ws_stream.split();

    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();
    let (client_id, mut bus_rx) = {
        let mut state = server.write().await;
        let id = state.next_id();
        let rx = state.bus_tx.subscribe();
        (id, rx)
    };

    let write_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(msg) = client_rx.recv() => {
                    if sink.send(Message::text(msg)).await.is_err() {
                        break;
                    }
                }
                Ok(msg) = bus_rx.recv() => {
                    if sink.send(Message::text(msg)).await.is_err() {
                        break;
                    }
                }
                else => break,
            }
        }
    });

    tracing::info!(client_id, "websocket connected");

    let mut coord_tx: Option<mpsc::UnboundedSender<CoordMessage>> = None;
    let mut slug: Option<String> = None;
    let mut wicket_host: Option<String> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok(Message::Text(text)) => {
                let data: Value = match serde_json::from_str(&text) {
                    Ok(d) => d,
                    Err(_) => continue,
                };

                if let Some(stream_name) = data.get("stream").and_then(|v| v.as_str()) {
                    if stream_name == "heartbeat" {
                        continue;
                    }

                    // History request: resolve timestamp, create coordinator, associate.
                    if stream_name == "history_request" {
                        let req_data = data.get("data").cloned().unwrap_or_default();
                        let req_slug = req_data.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let intent = req_data.get("intent").and_then(|v| v.as_str()).unwrap_or("latest");
                        let replay_id = req_data.get("replay_id").and_then(|v| v.as_str()).unwrap_or("").to_string();

                        if req_slug.is_empty() {
                            continue;
                        }

                        if let Some(ts) = ServerState::resolve_timestamp(&req_slug, intent) {
                            tracing::info!(client_id, slug = %req_slug, timestamp = %ts, intent, "history request resolved");

                            let tx = {
                                let mut state = server.write().await;
                                state.find_or_create_coordinator(&req_slug, &ts)
                            };

                            let _ = tx.send(CoordMessage::ClientConnected {
                                id: client_id,
                                protocol: "easement".to_string(),
                                timestamp: Some(ts.clone()),
                                host: None,
                                tx: client_tx.clone(),
                            });

                            let _ = tx.send(CoordMessage::Envelope {
                                id: client_id,
                                envelope: InboundEnvelope {
                                    stream: "history_request".to_string(),
                                    data: json!({ "replay_id": replay_id }),
                                },
                            });

                            slug = Some(req_slug);
                            coord_tx = Some(tx);
                        } else {
                            tracing::warn!(client_id, slug = %req_slug, intent, "no transcript found");
                        }
                        continue;
                    }

                    // Response with slug: route to the coordinator by (slug, timestamp).
                    if stream_name == "tools_response" {
                        let bus_tx = {
                            let state = server.read().await;
                            state.bus_tx.clone()
                        };
                        if let Ok(json) = serde_json::to_string(&data) {
                            let _ = bus_tx.send(json);
                        }
                        continue;
                    }

                    if stream_name == "response" || stream_name == "tool_result" || stream_name == "shell_result" || stream_name == "background_done" || stream_name == "background_output" {
                        let resp_slug = data.get("slug")
                            .and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let resp_ts = data.get("timestamp")
                            .and_then(|v| v.as_str()).unwrap_or("").to_string();

                        if !resp_slug.is_empty() && !resp_ts.is_empty() {
                            let tx = {
                                let state = server.read().await;
                                state.coordinators.get(&(resp_slug.clone(), resp_ts.clone()))
                                    .map(|h| h.tx.clone())
                            };
                            if let Some(tx) = tx {
                                if let Ok(env) = serde_json::from_value::<InboundEnvelope>(data) {
                                    let _ = tx.send(CoordMessage::Envelope {
                                        id: client_id,
                                        envelope: env,
                                    });
                                }
                            }
                            continue;
                        }
                    }

                    // Regular envelope: forward to associated coordinator.
                    if let Some(ref tx) = coord_tx {
                        if let Ok(env) = serde_json::from_value::<InboundEnvelope>(data) {
                            let _ = tx.send(CoordMessage::Envelope {
                                id: client_id,
                                envelope: env,
                            });
                        }
                    }

                // Slug association (Wicket connect): register with all coordinators for the slug.
                } else if let Some(connect_slug) = data.get("slug").and_then(|v| v.as_str()) {
                    if connect_slug.is_empty() {
                        continue;
                    }
                    let protocol = data.get("protocol").and_then(|v| v.as_str()).unwrap_or("easement").to_string();
                    let host = data.get("host").and_then(|v| v.as_str()).map(|s| s.to_string());

                    if protocol == "wicket" {
                        tracing::info!(client_id, slug = %connect_slug, host = ?host, "wicket associating");
                        if let Some(ref h) = host {
                            wicket_host = Some(h.clone());
                            let state_r = server.read().await;
                            let _ = state_r.wicket_mgr_tx.send(WicketManagerMsg::Connected {
                                host: h.clone(),
                                client_id,
                            });
                            drop(state_r);
                        }
                        let state = server.read().await;
                        let mut first_tx = None;
                        for ((s, _), handle) in state.coordinators.iter() {
                            if s == connect_slug {
                                let _ = handle.tx.send(CoordMessage::ClientConnected {
                                    id: client_id,
                                    protocol: protocol.clone(),
                                    timestamp: None,
                                    host: host.clone(),
                                    tx: client_tx.clone(),
                                });
                                if first_tx.is_none() {
                                    first_tx = Some(handle.tx.clone());
                                }
                            }
                        }
                        if let Some(tx) = first_tx {
                            slug = Some(connect_slug.to_string());
                            coord_tx = Some(tx);
                        }
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                tracing::warn!(client_id, "websocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    if let Some(ref tx) = coord_tx {
        let _ = tx.send(CoordMessage::ClientDisconnected { id: client_id });
    }
    if wicket_host.is_some() {
        let state = server.read().await;
        let _ = state.wicket_mgr_tx.send(WicketManagerMsg::Disconnected { client_id });
    }
    write_task.abort();
    tracing::info!(client_id, slug = ?slug, "websocket disconnected");
}

// -- MCP HTTP handler --

#[derive(Debug, serde::Deserialize)]
struct ToolCallParams {
    name: String,
    arguments: Value,
}

async fn handle_mcp(
    req: Request<Incoming>,
    slug: &str,
    timestamp: Option<&str>,
    server: Arc<RwLock<ServerState>>,
) -> Response<Full<Bytes>> {
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

            let coord_tx = {
                let state = server.read().await;
                let ts = timestamp.as_deref().unwrap_or("");
                state.coordinators.get(&(slug.to_string(), ts.to_string())).map(|h| h.tx.clone())
            };

            let coord_tx = match coord_tx {
                Some(tx) => tx,
                None => {
                    return make_json_response(jsonrpc_error(
                        id, -32000, "no active session for this slug".to_string(),
                    ));
                }
            };

            tracing::info!(tool_name = %params.name, arguments = %params.arguments, "MCP tools/call received");

            if params.name == "approve" {
                let updated_input = params.arguments.get("input")
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
                let _ = coord_tx.send(CoordMessage::ToolCall {
                    call_id: uuid::Uuid::new_v4().to_string(),
                    tool: "tools".to_string(),
                    args: json!({}),
                    reply: reply_tx,
                });
                return make_json_response(match reply_rx.await {
                    Ok(result) => jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": result.output }] }),
                    ),
                    Err(_) => jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": "tool discovery failed" }], "isError": true }),
                    ),
                });
            }

            // mcp__o__call — generic dispatch
            let who = params.arguments.get("who").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let f = params.arguments.get("f").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let args = params.arguments.get("args").cloned().unwrap_or(json!({}));

            if who.is_empty() || f.is_empty() {
                return make_json_response(jsonrpc_error(
                    id, -32602, "call requires who and f".to_string(),
                ));
            }

            let call_id = uuid::Uuid::new_v4().to_string();
            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = coord_tx.send(CoordMessage::ToolCall {
                call_id,
                tool: f.clone(),
                args: json!({ "who": who, "f": f, "args": args }),
                reply: reply_tx,
            });

            match reply_rx.await {
                Ok(result) => {
                    if (f == "view_image" || f == "screenshot") && result.exit_code == 0 {
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

// -- Capture HTTP handler --

async fn handle_capture(
    slug: &str,
    server: Arc<RwLock<ServerState>>,
) -> Response<Full<Bytes>> {
    let coord_tx = {
        let state = server.read().await;
        state.coordinators.iter()
            .find(|((s, _), _)| s == slug)
            .map(|(_, h)| h.tx.clone())
    };

    let coord_tx = match coord_tx {
        Some(tx) => tx,
        None => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("no coordinator for slug")))
                .unwrap();
        }
    };

    // Capture not wired for Ping/Pong. Shotgun service requests
    // will be restored when tools are added back.
    drop(coord_tx);
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("capture not available")))
        .unwrap()
}

// -- HTTP/WebSocket connection handler --

async fn handle_request(
    mut req: Request<Incoming>,
    server: Arc<RwLock<ServerState>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();

    if hyper_tungstenite::is_upgrade_request(&req) {
        match hyper_tungstenite::upgrade(&mut req, None) {
            Ok((response, websocket)) => {
                let server = server.clone();
                tokio::spawn(async move {
                    handle_websocket(websocket, server).await;
                });
                Ok(response)
            }
            Err(e) => {
                tracing::error!("websocket upgrade error: {}", e);
                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from(format!("upgrade error: {}", e))))
                    .unwrap())
            }
        }
    } else if let Some(slug) = path.strip_prefix("/capture/") {
        if slug.is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug in /capture/<slug>")))
                .unwrap())
        } else {
            let slug = slug.to_string();
            Ok(handle_capture(&slug, server).await)
        }
    } else if let Some(rest) = path.strip_prefix("/mcp/") {
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        if parts.is_empty() || parts[0].is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug in /mcp/<slug>/<timestamp>")))
                .unwrap())
        } else {
            let slug = parts[0].to_string();
            let timestamp = parts.get(1).map(|s| s.to_string());
            Ok(handle_mcp(req, &slug, timestamp.as_deref(), server).await)
        }
    } else if let Some(slug) = path.strip_prefix("/turn/") {
        if slug.is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug in /turn/<slug>")))
                .unwrap())
        } else if req.method() != hyper::Method::POST {
            Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::new()))
                .unwrap())
        } else {
            let slug = slug.to_string();
            let body = req.collect().await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let payload: Value = serde_json::from_slice(&body).unwrap_or_default();
            let message = payload.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if message.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("missing message")))
                    .unwrap());
            }

            let coord_tx = {
                let ts = ServerState::resolve_timestamp(&slug, "latest").unwrap_or_default();
                let mut state = server.write().await;
                state.find_or_create_coordinator(&slug, &ts)
            };

            let _ = coord_tx.send(CoordMessage::Envelope {
                id: 0,
                envelope: InboundEnvelope {
                    stream: "claude".to_string(),
                    data: json!({ "message": message }),
                },
            });

            tracing::info!(slug = %slug, "system turn initiated via HTTP");

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(json!({"status": "ok"}).to_string())))
                .unwrap())
        }
    } else if let Some(raw_slug) = path.strip_prefix("/message/") {
        if raw_slug.is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug")))
                .unwrap())
        } else if req.method() != hyper::Method::POST {
            Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::new()))
                .unwrap())
        } else {
            let (slug, full) = if let Some(s) = raw_slug.strip_suffix("@full") {
                (s.to_string(), true)
            } else {
                (raw_slug.to_string(), false)
            };

            let body = req.collect().await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let payload: Value = serde_json::from_slice(&body).unwrap_or_default();
            let message = payload.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if message.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("missing message")))
                    .unwrap());
            }

            let coord_tx = {
                let ts = ServerState::resolve_timestamp(&slug, "latest").unwrap_or_default();
                let mut state = server.write().await;
                state.find_or_create_coordinator(&slug, &ts)
            };

            let notification = payload.get("notification")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = coord_tx.send(CoordMessage::Message {
                message,
                full,
                notification,
                reply: reply_tx,
            });

            match tokio::time::timeout(std::time::Duration::from_secs(300), reply_rx).await {
                Ok(Ok(response_text)) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/plain; charset=utf-8")
                        .body(Full::new(Bytes::from(response_text)))
                        .unwrap())
                }
                Ok(Err(_)) => {
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Full::new(Bytes::from("coordinator dropped reply")))
                        .unwrap())
                }
                Err(_) => {
                    Ok(Response::builder()
                        .status(StatusCode::GATEWAY_TIMEOUT)
                        .body(Full::new(Bytes::from("response timeout")))
                        .unwrap())
                }
            }
        }
    } else if let Some(slug) = path.strip_prefix("/session/") {
        if slug.is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug")))
                .unwrap())
        } else if req.method() != hyper::Method::POST {
            Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::new()))
                .unwrap())
        } else {
            let slug = slug.to_string();
            let coord_tx = {
                let ts = ServerState::resolve_timestamp(&slug, "latest").unwrap_or_default();
                let mut state = server.write().await;
                state.find_or_create_coordinator(&slug, &ts)
            };

            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = coord_tx.send(CoordMessage::NewSession { reply: reply_tx });

            match reply_rx.await {
                Ok(timestamp) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Full::new(Bytes::from(json!({"timestamp": timestamp}).to_string())))
                        .unwrap())
                }
                Err(_) => {
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Full::new(Bytes::from("failed")))
                        .unwrap())
                }
            }
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

// -- Entry point --

#[tokio::main]
async fn main() {
    let _guard = init_tracing();

    let (wicket_mgr_tx, wicket_mgr_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_wicket_manager(wicket_mgr_rx));

    let server = Arc::new(RwLock::new(ServerState::new(wicket_mgr_tx.clone())));
    let addr = SocketAddr::from(([127, 0, 0, 1], easement_port()));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            tracing::info!("easement listening on {}", addr);
            l
        }
        Err(e) => {
            tracing::error!("failed to bind {}: {}", addr, e);
            eprintln!("failed to bind {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    // Spawn localhost Wicket at startup.
    tokio::spawn(async move {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = wicket_mgr_tx.send(WicketManagerMsg::Ensure {
            host: "localhost".to_string(),
            slug: "localhost".to_string(),
            reply: reply_tx,
        });
        match reply_rx.await {
            Ok(Ok(host)) => tracing::info!(host = %host, "localhost wicket ready"),
            Ok(Err(e)) => {
                tracing::error!(error = %e, "localhost wicket failed, aborting");
                std::process::exit(1);
            }
            Err(_) => {
                tracing::error!("localhost wicket ensure: manager dropped reply, aborting");
                std::process::exit(1);
            }
        }
    });

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("accept error: {}", e);
                continue;
            }
        };

        let io = TokioIo::new(stream);
        let server = server.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let server = server.clone();
                async move { handle_request(req, server).await }
            });

            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades()
                .await
            {
                tracing::warn!(peer = %peer, "connection error: {}", e);
            }
        });
    }
}
