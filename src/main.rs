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
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use crate::protocol::{
    ClaudeMessage, ConnectPayload, InboundEnvelope, LifecycleEvent,
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
            .join(".local/state/wicket")
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
        .join("wicket");
    let _ = std::fs::create_dir_all(&log_dir);

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("wicket.log"))
        .expect("failed to open wicket.log");

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("wicket=debug"));

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
    Replay,
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
    coordinators: HashMap<String, CoordinatorHandle>,
    next_client_id: u64,
}

impl ServerState {
    fn new() -> Self {
        Self {
            coordinators: HashMap::new(),
            next_client_id: 0,
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_client_id;
        self.next_client_id += 1;
        id
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

// -- Broadcast helpers --

fn send_to(clients: &Clients, client_id: u64, stream: &str, data: Value) {
    if let Some(json) = envelope_json(stream, data) {
        if let Some(tx) = clients.get(&client_id) {
            let _ = tx.send(json);
        }
    }
}

fn broadcast(clients: &Clients, stream: &'static str, data: Value) {
    if let Some(json) = envelope_json(stream, data) {
        for tx in clients.values() {
            let _ = tx.send(json.clone());
        }
    }
}

fn broadcast_entry(clients: &Clients, entry: &NormalizedEntry) {
    if let Ok(data) = serde_json::to_value(entry) {
        broadcast(clients, "entry", data);
    }
}

fn broadcast_lifecycle(clients: &Clients, event: LifecycleEvent) {
    if let Ok(data) = serde_json::to_value(&event) {
        broadcast(clients, "lifecycle", data);
    }
}

fn broadcast_error(clients: &Clients, message: &str) {
    broadcast(clients, "error", json!({ "message": message }));
}

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
    if !transcript_entries.is_empty() {
        round_log.log_sent(transcript_entries);
        let tmp_path = std::env::temp_dir().join(format!("wicket-{}.jsonl", slug));
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
        .arg("--model").arg("claude-opus-4-6")
        .arg("--thinking-display").arg("summarized")
        .arg("--max-thinking-tokens").arg("31999")
        .arg("--add-dir").arg(format!("{}/code", home));

    let mcp_config_path = std::env::temp_dir()
        .join(format!("wicket-mcp-{}.json", slug));
    let mcp_config = json!({
        "mcpServers": {
            "wicket": {
                "type": "http",
                "url": format!("http://localhost:6502/mcp/{}", slug)
            }
        }
    });
    if let Err(e) = std::fs::write(&mcp_config_path, mcp_config.to_string()) {
        return Err(format!("cannot write mcp config: {}", e));
    }
    cmd.arg("--permission-prompt-tool").arg("mcp__wicket__wicket_approve")
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
                            StdoutEvent::User { is_replay: true, .. } => {
                                let _ = event_tx.send(ClaudeEvent::Replay).await;
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

// -- Coordinator (per-slug) --

async fn run_coordinator(slug: String, mut coord_rx: mpsc::UnboundedReceiver<CoordMessage>) {
    let exchange = ExchangeLog::new(&slug);
    let mut transcript = Transcript::new(&slug, None);
    let history = transcript.load_history();

    let mut all_entries: Vec<NormalizedEntry> = history;
    let mut clients: Clients = HashMap::new();
    let mut claude: Option<ClaudePrint> = None;
    let mut last_usage: Option<Value> = None;
    let mut current_timestamp: Option<String> = None;
    let mut pending_service: Option<PendingService> = None;
    let mut smedly_id: Option<u64> = None;
    let mut smedly_child: Option<tokio::process::Child> = None;
    let mut pending_tool: Option<(String, oneshot::Sender<ToolResult>)> = None;
    let mut pending_tool_envelope: Option<(String, String, Value)> = None;

    tracing::info!(
        slug = %slug,
        history = all_entries.len(),
        "coordinator started"
    );

    loop {
        tokio::select! {
            Some(msg) = coord_rx.recv() => {
                match msg {
                    CoordMessage::ClientConnected { id, protocol, timestamp, host, tx } => {
                        if protocol == "smedly" {
                            smedly_id = Some(id);
                            clients.insert(id, tx);
                            tracing::info!(client_id = id, host = ?host, "smedly connected");
                            if let Some((call_id, tool, args)) = pending_tool_envelope.take() {
                                tracing::info!(call_id = %call_id, tool = %tool, "draining pending tool call to smedly");
                                send_to(&clients, id, &tool, json!({
                                    "call_id": call_id,
                                    "slug": slug,
                                    "command": args.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                                    "sandboxed": args.get("sandboxed").and_then(|v| v.as_bool()).unwrap_or(true),
                                    "patch": args.get("patch").and_then(|v| v.as_str()).unwrap_or(""),
                                    "path": args.get("path").and_then(|v| v.as_str()).unwrap_or(""),
                                }));
                                broadcast(&clients, "tool_start", json!({
                                    "tool": tool,
                                    "command": args.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                                }));
                            }
                            continue;
                        }
                        if let Some(ref ts) = timestamp {
                            current_timestamp = Some(ts.clone());
                            transcript = Transcript::new(&slug, Some(ts));
                            let history = transcript.load_history();
                            all_entries = history;
                            tracing::info!(timestamp = %ts, entries = all_entries.len(), "switched to timestamped transcript");
                        }
                        for entry in &all_entries {
                            if let Ok(data) = serde_json::to_value(entry) {
                                if let Some(json) = envelope_json("entry", data) {
                                    let _ = tx.send(json);
                                }
                            }
                        }
                        if let Some(ref usage) = last_usage {
                            if let Some(json) = envelope_json("usage", usage.clone()) {
                                let _ = tx.send(json);
                            }
                        }
                        clients.insert(id, tx);
                        tracing::info!(client_id = id, clients = clients.len(), "client connected");
                    }
                    CoordMessage::ClientDisconnected { id } => {
                        clients.remove(&id);
                        if smedly_id == Some(id) {
                            smedly_id = None;
                            tracing::info!(client_id = id, "smedly disconnected");
                            if let Some((call_id, reply)) = pending_tool.take() {
                                let _ = reply.send(ToolResult {
                                    output: "smedly disconnected".to_string(),
                                    exit_code: 1,
                                });
                            }
                        }
                        tracing::info!(client_id = id, clients = clients.len(), "client disconnected");
                    }
                    CoordMessage::ToolCall { call_id, tool, args, reply } => {
                        let ensure_smedly = || -> Option<u64> {
                            // placeholder — smedly_id is captured below
                            None
                        };
                        if let Some(sid) = smedly_id {
                            tracing::info!(call_id = %call_id, tool = %tool, args = %args, "forwarding tool call to smedly");
                            pending_tool = Some((call_id.clone(), reply));
                            send_to(&clients, sid, &tool, json!({
                                "call_id": call_id,
                                "slug": slug,
                                "command": args.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                                "sandboxed": args.get("sandboxed").and_then(|v| v.as_bool()).unwrap_or(true),
                                "patch": args.get("patch").and_then(|v| v.as_str()).unwrap_or(""),
                                "path": args.get("path").and_then(|v| v.as_str()).unwrap_or(""),
                            }));
                            broadcast(&clients, "tool_start", json!({
                                "tool": tool,
                                "command": args.get("command").and_then(|v| v.as_str()).unwrap_or(""),
                            }));
                        } else {
                            // No smedly connected — spawn one and stash the tool call.
                            // The select loop will process the connect-back and drain the pending call.
                            tracing::info!("no smedly connected, spawning localhost");
                            let mut cmd = tokio::process::Command::new("smedly");
                            cmd.arg("ws://localhost:6502").arg(&slug);
                            cmd.stdin(Stdio::null())
                                .stdout(Stdio::null())
                                .stderr(Stdio::null());
                            match cmd.spawn() {
                                Ok(child) => {
                                    smedly_child = Some(child);
                                    tracing::info!(call_id = %call_id, tool = %tool, args = %args, "stashing tool call, waiting for smedly");
                                    pending_tool = Some((call_id.clone(), reply));
                                    pending_tool_envelope = Some((call_id, tool, args));
                                }
                                Err(e) => {
                                    let _ = reply.send(ToolResult {
                                        output: format!("failed to spawn smedly: {}", e),
                                        exit_code: 1,
                                    });
                                }
                            }
                        }
                    }
                    CoordMessage::Envelope { id, envelope } => {
                        exchange.log("client>wicket", &json!({
                            "client_id": id,
                            "stream": &envelope.stream,
                            "data": &envelope.data,
                        }));

                        match envelope.stream.as_str() {
                            "claude" => {
                                if claude.is_some() {
                                    tracing::warn!("claude envelope while ClaudePrint is running, ignoring");
                                    continue;
                                }

                                let msg: ClaudeMessage = match serde_json::from_value(envelope.data) {
                                    Ok(m) => m,
                                    Err(e) => {
                                        tracing::warn!("bad claude message: {}", e);
                                        broadcast_error(&clients, &format!("bad claude message: {}", e));
                                        continue;
                                    }
                                };

                                let turn_id = uuid::Uuid::new_v4().to_string();

                                broadcast(&clients, "turn", json!({
                                    "event": "started",
                                    "turn_id": turn_id,
                                    "message": msg.message,
                                }));
                                broadcast_lifecycle(&clients, LifecycleEvent::RoundStarted);

                                let entries = transcript.entries().to_vec();
                                tracing::info!(turn_id = %turn_id, transcript_entries = entries.len(), "spawning ClaudePrint");

                                match spawn_claude_print(&slug, &msg.message, &entries).await {
                                    Ok(mut cp) => {
                                        cp.turn_id = Some(turn_id);
                                        claude = Some(cp);
                                    }
                                    Err(e) => {
                                        tracing::error!("failed to spawn ClaudePrint: {}", e);
                                        broadcast_error(&clients, &e);
                                        broadcast_lifecycle(&clients, LifecycleEvent::RoundFailed { message: e });
                                    }
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
                                tracing::info!(call_id = %call_id, exit_code, output_len = output.len(), "tool result from smedly");
                                broadcast(&clients, "tool_done", json!({
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

            // ClaudePrint stdout events.
            Some(event) = async {
                match claude.as_mut() {
                    Some(cp) => cp.event_rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                let cp = claude.as_mut().unwrap();
                match event {
                    ClaudeEvent::Delta(delta) => {
                        broadcast(&clients, "delta", delta);
                    }
                    ClaudeEvent::SessionId(sid) => {
                        tracing::info!(session_id = %sid, "captured session id from claude");
                        cp.session_id = Some(sid);
                    }
                    ClaudeEvent::Replay => {
                        cp.drain_replayed += 1;
                        tracing::debug!(
                            sent = cp.drain_sent,
                            replayed = cp.drain_replayed,
                            "drain gate: {}/{}",
                            cp.drain_replayed,
                            cp.drain_sent
                        );
                    }
                    ClaudeEvent::Result { usage, is_interrupted } => {
                        if let Some(ref u) = usage {
                            broadcast(&clients, "usage", u.clone());
                            last_usage = usage;
                        }

                        if is_interrupted {
                            broadcast_lifecycle(&clients, LifecycleEvent::RoundInterrupted);
                            if let Some(ref tid) = cp.turn_id {
                                broadcast(&clients, "turn", json!({
                                    "event": "completed",
                                    "turn_id": tid,
                                    "status": "interrupted",
                                }));
                            }
                        }

                        if cp.is_drained() {
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
                            if let Some(ref sid) = session_id {
                                if let Some(path) = find_transcript_file(sid) {
                                    round_log.copy_transcript(&path);
                                    if let Ok(content) = std::fs::read_to_string(&path) {
                                        for line in content.lines() {
                                            let line = line.trim();
                                            if line.is_empty() { continue; }
                                            if let Ok(data) = serde_json::from_str::<Value>(line) {
                                                let new_entries = transcript.handle_entry(data);
                                                for entry in &new_entries {
                                                    broadcast_entry(&clients, entry);
                                                    all_entries.push(entry.clone());
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    tracing::warn!(session_id = %sid, "CLI transcript file not found");
                                }
                            }

                            if !is_interrupted {
                                broadcast_lifecycle(&clients, LifecycleEvent::RoundCompleted);
                                if let Some(ref tid) = turn_id {
                                    broadcast(&clients, "turn", json!({
                                        "event": "completed",
                                        "turn_id": tid,
                                        "status": "completed",
                                    }));
                                }
                            }

                            claude = None;
                            tracing::info!("round completed");
                        }
                    }
                    ClaudeEvent::Eof => {
                        tracing::info!("claude stdout EOF");
                        let turn_id = cp.turn_id.clone();
                        let session_id = cp.session_id.clone();
                        let round_log = cp.round_log.clone();

                        cp.stdin.take();
                        let _ = cp.child.wait().await;

                        if let Some(ref sid) = session_id {
                            if let Some(path) = find_transcript_file(sid) {
                                round_log.copy_transcript(&path);
                                if let Ok(content) = std::fs::read_to_string(&path) {
                                    for line in content.lines() {
                                        let line = line.trim();
                                        if line.is_empty() { continue; }
                                        if let Ok(data) = serde_json::from_str::<Value>(line) {
                                            let new_entries = transcript.handle_entry(data);
                                            for entry in &new_entries {
                                                broadcast_entry(&clients, entry);
                                                all_entries.push(entry.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        broadcast_lifecycle(&clients, LifecycleEvent::RoundFailed {
                            message: "claude exited unexpectedly".to_string(),
                        });
                        if let Some(ref tid) = turn_id {
                            broadcast(&clients, "turn", json!({
                                "event": "completed",
                                "turn_id": tid,
                                "status": "failed",
                            }));
                        }

                        claude = None;
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

    let connect_text = match stream.next().await {
        Some(Ok(Message::Text(text))) => text,
        _ => {
            tracing::warn!("expected text connect payload as first message");
            return;
        }
    };

    let connect: ConnectPayload = match serde_json::from_str(&connect_text) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("invalid connect payload: {}", e);
            return;
        }
    };

    let slug = connect.slug.clone();
    tracing::info!(slug = %slug, "websocket client connecting");

    let (coord_tx, client_id) = {
        let mut state = server.write().await;
        let client_id = state.next_id();

        let handle = state.coordinators.entry(slug.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            let slug_clone = slug.clone();
            tokio::spawn(async move {
                run_coordinator(slug_clone, rx).await;
            });
            CoordinatorHandle { tx }
        });

        (handle.tx.clone(), client_id)
    };

    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();

    let protocol = connect.protocol.unwrap_or_else(|| "wicket".to_string());

    let _ = coord_tx.send(CoordMessage::ClientConnected {
        id: client_id,
        protocol,
        timestamp: connect.timestamp,
        host: connect.host,
        tx: client_tx,
    });

    let write_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            if sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    while let Some(result) = stream.next().await {
        match result {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<InboundEnvelope>(&text) {
                    Ok(env) => {
                        let _ = coord_tx.send(CoordMessage::Envelope {
                            id: client_id,
                            envelope: env,
                        });
                    }
                    Err(e) => {
                        tracing::warn!("bad inbound envelope: {}", e);
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                tracing::warn!("websocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    let _ = coord_tx.send(CoordMessage::ClientDisconnected { id: client_id });
    write_task.abort();
    tracing::info!(client_id, slug = %slug, "websocket client disconnected");
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
                "serverInfo": { "name": "wicket", "version": "0.1.0" }
            }),
        ),
        "tools/list" => jsonrpc_response(
            id,
            json!({
                "tools": [{
                    "name": "wicket_approve",
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
                    "name": "zsh",
                    "description": "Execute a command in a sandboxed Zsh shell. The command runs in a sandbox that restricts filesystem writes to the project directory. Use this for all shell commands. If a command fails with a permission error, you may retry with escalate: true to request approval to run outside the sandbox.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "command": { "type": "string", "description": "The Zsh command to execute" },
                            "escalate": { "type": "boolean", "description": "Request approval to run outside the sandbox. Only use after a sandboxed attempt failed with a permission error." },
                            "reason": { "type": "string", "description": "Why the command needs to run outside the sandbox." }
                        },
                        "required": ["command"]
                    }
                }, {
                    "name": "apply_patch",
                    "description": "Apply a patch to create, update, or delete files. The patch uses a structured diff format with context lines for updates. Files must be inside the sandbox writable roots.\n\nFormat:\n*** Begin Patch\n*** Add File: <path>\n+<line>\n*** Update File: <path>\n@@ <optional context header>\n <context line>\n-<removed line>\n+<added line>\n <context line>\n*** Delete File: <path>\n*** End Patch\n\nPaths are relative to the working directory. Context lines (prefixed with space) locate where changes apply. Include 3 lines of context before and after each change.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "patch": { "type": "string", "description": "The patch to apply in the structured diff format" }
                        },
                        "required": ["patch"]
                    }
                }, {
                    "name": "view_image",
                    "description": "View an image file. Returns the image inline so you can see it. Use this to view screenshots, diagrams, photos, or any image file.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string", "description": "Path to the image file" }
                        },
                        "required": ["path"]
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
                state.coordinators.get(slug).map(|h| h.tx.clone())
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

            if params.name == "wicket_approve" {
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

            let tool = params.name.clone();
            let call_id = uuid::Uuid::new_v4().to_string();

            let (reply_tx, reply_rx) = oneshot::channel();
            let _ = coord_tx.send(CoordMessage::ToolCall {
                call_id,
                tool,
                args: params.arguments,
                reply: reply_tx,
            });

            match reply_rx.await {
                Ok(result) => {
                    if params.name == "view_image" && result.exit_code == 0 {
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
        state.coordinators.get(slug).map(|h| h.tx.clone())
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
    } else if let Some(slug) = path.strip_prefix("/mcp/") {
        if slug.is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug in /mcp/<slug>")))
                .unwrap())
        } else {
            let slug = slug.to_string();
            Ok(handle_mcp(req, &slug, server).await)
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
                let mut state = server.write().await;
                let handle = state.coordinators.entry(slug.clone()).or_insert_with(|| {
                    let (tx, rx) = mpsc::unbounded_channel();
                    let slug_clone = slug.clone();
                    tokio::spawn(async move {
                        run_coordinator(slug_clone, rx).await;
                    });
                    CoordinatorHandle { tx }
                });
                handle.tx.clone()
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

    let server = Arc::new(RwLock::new(ServerState::new()));
    let addr = SocketAddr::from(([127, 0, 0, 1], 6502));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            tracing::info!("wicket listening on {}", addr);
            l
        }
        Err(e) => {
            tracing::error!("failed to bind {}: {}", addr, e);
            eprintln!("failed to bind {}: {}", addr, e);
            std::process::exit(1);
        }
    };

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
