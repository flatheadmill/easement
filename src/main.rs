// Wicket: WebSocket and HTTP server on port 6502.
//
// Clients (Puzzle, Shotgun) connect over WebSocket. Claude's MCP approval
// requests arrive over HTTP at /mcp/<slug>. One process, one port.
//
// Each slug gets its own coordinator task that manages Easement lifecycle,
// transcript persistence, drain gate, and session tracking. Clients
// register with the coordinator for their slug and receive normalized
// entries and lifecycle events.
//
// The Easement-facing edge is Unix process management: spawn per round,
// piped stdin/stdout, NDJSON. The client-facing edge is WebSocket JSON
// envelopes. The MCP approval edge is HTTP JSON-RPC. These three worlds
// meet in the coordinator.

mod normalize;
mod parser;
mod protocol;
mod transcript;

use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
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
    ApprovalDecision, ClaudeMessage, ConnectPayload, InboundEnvelope, LifecycleEvent,
    NormalizedEntry,
};
use crate::transcript::{Sessions, Transcript};

// -- Exchange log --
//
// Per-slug JSONL log of every envelope in both directions. Each line:
// {"ts":"...","dir":"easement>wicket","data":{...}}

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

// -- Stdout event types from Easement --

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

// -- Easement envelope from stdout --

#[derive(Debug, serde::Deserialize)]
struct EasementEnvelope {
    stream: String,
    data: Value,
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

// -- Easement payload --

#[derive(Debug, Serialize)]
struct EasementPayload {
    slug: String,
    #[serde(default)]
    yolo: bool,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transcript: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wicket_socket: Option<String>,
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

// -- Drain gate --

struct DrainGate {
    sent: u64,
    replayed: u64,
    drained: bool,
    session_id: Option<String>,
}

impl DrainGate {
    fn new() -> Self {
        Self {
            sent: 1,
            replayed: 0,
            drained: false,
            session_id: None,
        }
    }

    fn handle(&mut self, event: &StdoutEvent) -> bool {
        if self.session_id.is_none() {
            let event_sid = match event {
                StdoutEvent::System { session_id, .. } => session_id.as_ref(),
                StdoutEvent::Result { session_id, .. } => session_id.as_ref(),
                StdoutEvent::Assistant { session_id, .. } => session_id.as_ref(),
                StdoutEvent::User { session_id, .. } => session_id.as_ref(),
                _ => None,
            };
            if let Some(id) = event_sid {
                tracing::info!(session_id = %id, "captured session id");
                self.session_id = Some(id.clone());
            }
        }

        match event {
            StdoutEvent::User {
                is_replay: true, ..
            } => {
                self.replayed += 1;
                tracing::debug!(
                    sent = self.sent,
                    replayed = self.replayed,
                    "user replay, drain gate: {}/{}",
                    self.replayed,
                    self.sent
                );
            }
            StdoutEvent::Result { .. } => {
                tracing::info!(
                    sent = self.sent,
                    replayed = self.replayed,
                    drained = (self.sent == self.replayed),
                    "result event, drain gate: {}/{}",
                    self.replayed,
                    self.sent
                );
                if self.sent == self.replayed {
                    self.drained = true;
                    return true;
                }
            }
            _ => {}
        }
        false
    }

    fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
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
        session_id: Option<String>,
        protocol: String,
        timestamp: Option<String>,
        tx: mpsc::UnboundedSender<String>,
    },
    ClientDisconnected {
        id: u64,
    },
    Envelope {
        id: u64,
        envelope: InboundEnvelope,
    },
    McpApprovalRequest {
        tool_name: String,
        input: Value,
        tool_use_id: Option<String>,
        reply: oneshot::Sender<Value>,
    },
    ServiceRequest {
        request_type: String,
        reply: oneshot::Sender<ServiceResponse>,
    },
    ZshExec {
        command: String,
        sandboxed: bool,
        reply: oneshot::Sender<ZshResult>,
    },
    FileOp {
        op: String,
        args: Value,
        reply: oneshot::Sender<ZshResult>,
    },
    SetRemoteHost {
        host: Option<String>,
        reply: oneshot::Sender<Option<String>>,
    },
    GetRemoteHost {
        reply: oneshot::Sender<Option<String>>,
    },
}

struct ZshResult {
    output: String,
    exit_code: i32,
}

// -- Pending approval state --

struct PendingApproval {
    reply: oneshot::Sender<Value>,
    original_input: Value,
}

// -- Service request/response --

struct ServiceResponse {
    content_type: String,
    body: Vec<u8>,
}

struct PendingService {
    id: String,
    reply: oneshot::Sender<ServiceResponse>,
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

#[derive(Debug, serde::Deserialize)]
struct ToolCallParams {
    name: String,
    arguments: Value,
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

// -- Broadcast helpers --

fn send_to(clients: &Clients, client_id: u64, stream: &str, data: Value) {
    if let Some(json) = envelope_json(stream, data) {
        if let Some(tx) = clients.get(&client_id) {
            let _ = tx.send(json);
        }
    }
}

fn broadcast(clients: &Clients, stream: &'static str, data: Value) {
    broadcast_except(clients, None, stream, data);
}

fn broadcast_except(clients: &Clients, exclude: Option<u64>, stream: &'static str, data: Value) {
    if let Some(json) = envelope_json(stream, data) {
        for (&id, tx) in clients.iter() {
            if exclude == Some(id) { continue; }
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

fn broadcast_approval(clients: &Clients, data: Value) {
    broadcast(clients, "approval", data);
}

fn broadcast_meta(clients: &Clients, data: Value) {
    broadcast(clients, "meta", data);
}

fn broadcast_error(clients: &Clients, message: &str) {
    broadcast(clients, "error", json!({ "message": message }));
}

// -- Coordinator (per-slug) --

async fn run_coordinator(slug: String, coord_tx: mpsc::UnboundedSender<CoordMessage>, mut coord_rx: mpsc::UnboundedReceiver<CoordMessage>) {
    let exchange = ExchangeLog::new(&slug);
    let mut transcript = Transcript::new(&slug, None);
    let mut sessions = Sessions::new(&slug);
    let history = transcript.load_history();

    // Cache normalized entries for late-connecting clients.
    let mut all_entries: Vec<NormalizedEntry> = history;

    let mut clients: Clients = HashMap::new();

    let mut easement_client_id: Option<u64> = None;
    let mut easement_child: Option<tokio::process::Child> = None;
    let mut pending_approval: Option<PendingApproval> = None;
    let mut pending_service: Option<PendingService> = None;
    let mut pending_zsh: Option<oneshot::Sender<ZshResult>> = None;
    let mut last_usage: Option<Value> = None;
    let mut remote_host: Option<String> = None;
    let mut current_timestamp: Option<String> = None;

    tracing::info!(
        slug = %slug,
        history = all_entries.len(),
        "coordinator started"
    );

    loop {
        tokio::select! {
            // Coordinator messages (from WebSocket clients).
            Some(msg) = coord_rx.recv() => {
                match msg {
                    CoordMessage::ClientConnected { id, session_id, protocol, timestamp, tx } => {
                        if protocol == "easement" {
                            easement_client_id = Some(id);
                            tracing::info!(client_id = id, "easement client connected");
                        } else {
                            if let Some(ref ts) = timestamp {
                                current_timestamp = Some(ts.clone());
                                transcript = Transcript::new(&slug, Some(ts));
                                let history = transcript.load_history();
                                all_entries = history;
                                tracing::info!(timestamp = %ts, entries = all_entries.len(), "switched to timestamped transcript");
                            }
                            if let Some(sid) = session_id {
                                sessions.set_local(sid);
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
                        }
                        clients.insert(id, tx);
                        tracing::info!(client_id = id, protocol = %protocol, clients = clients.len(), "client connected");
                    }
                    CoordMessage::ClientDisconnected { id } => {
                        clients.remove(&id);
                        if easement_client_id == Some(id) {
                            easement_client_id = None;
                            tracing::info!(client_id = id, "easement client disconnected");
                        }
                        tracing::info!(client_id = id, clients = clients.len(), "client disconnected");
                    }
                    CoordMessage::McpApprovalRequest { tool_name, input, tool_use_id, reply } => {
                        // Auto-approve our own MCP tools. The sandbox is the gate.
                        // The CLI requires updatedInput as a record in the allow response.
                        if tool_name.starts_with("mcp__wicket__") {
                            tracing::info!(tool = %tool_name, "auto-approving wicket MCP tool");
                            let _ = reply.send(json!({
                                "behavior": "allow",
                                "updatedInput": input
                            }));
                        } else {
                            tracing::info!(tool = %tool_name, "MCP approval request");
                            let request_data = json!({
                                "tool_name": tool_name,
                                "input": input,
                                "tool_use_id": tool_use_id,
                            });
                            broadcast_approval(&clients, request_data);
                            pending_approval = Some(PendingApproval {
                                reply,
                                original_input: input,
                            });
                        }
                    }
                    CoordMessage::ServiceRequest { request_type, reply } => {
                        let request_id = uuid::Uuid::new_v4().to_string();
                        tracing::info!(request_type = %request_type, id = %request_id, "service request");
                        broadcast(&clients, "request", json!({
                            "type": request_type,
                            "id": request_id,
                        }));
                        let id_for_timeout = request_id.clone();
                        pending_service = Some(PendingService {
                            id: request_id,
                            reply,
                        });
                        // Claim timeout — if no client claims within 2 seconds,
                        // fire the oneshot with a 404-equivalent empty response.
                        let coord_tx_timeout = coord_tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            // Send a synthetic timeout envelope. The coordinator
                            // checks if the pending service still matches this id.
                            let _ = coord_tx_timeout.send(CoordMessage::Envelope {
                                id: 0,
                                envelope: InboundEnvelope {
                                    stream: "service_timeout".to_string(),
                                    data: json!({ "id": id_for_timeout }),
                                },
                            });
                        });
                    }
                    CoordMessage::ZshExec { command, sandboxed, reply } => {
                        tracing::info!(command = %command, sandboxed, "zsh exec request");
                        broadcast(&clients, "tool_start", json!({
                            "tool": "zsh",
                            "command": command,
                            "sandboxed": sandboxed
                        }));
                        if let Some(eid) = easement_client_id {
                            send_to(&clients, eid, "zsh", json!({
                                "command": command,
                                "sandboxed": sandboxed
                            }));
                            pending_zsh = Some(reply);
                        } else {
                            let _ = reply.send(ZshResult {
                                output: "no easement connected".to_string(),
                                exit_code: 1,
                            });
                        }
                    }
                    CoordMessage::FileOp { op, args, reply } => {
                        tracing::info!(op = %op, "file op request");
                        if let Some(eid) = easement_client_id {
                            send_to(&clients, eid, &op, args);
                            pending_zsh = Some(reply);
                        } else {
                            let _ = reply.send(ZshResult {
                                output: "no easement connected".to_string(),
                                exit_code: 1,
                            });
                        }
                    }
                    CoordMessage::SetRemoteHost { host, reply } => {
                        let effective = if host.as_deref() == Some("local") { None } else { host };
                        tracing::info!(remote_host = ?effective, "remote host set");
                        remote_host = effective.clone();
                        let _ = reply.send(effective);
                    }
                    CoordMessage::GetRemoteHost { reply } => {
                        let _ = reply.send(remote_host.clone());
                    }
                    CoordMessage::Envelope { id, envelope } => {
                        exchange.log("client>wicket", &json!({
                            "client_id": id,
                            "stream": &envelope.stream,
                            "data": &envelope.data,
                        }));

                        if easement_client_id == Some(id) {
                            let eid = id;
                            match envelope.stream.as_str() {
                                "delta" => {
                                    broadcast_except(&clients, Some(eid), "delta", envelope.data);
                                }
                                "boundary" => {
                                    tracing::debug!("boundary envelope ignored");
                                }
                                "transcript" => {
                                    let new_entries = transcript.handle_entry(envelope.data);
                                    for entry in &new_entries {
                                        broadcast_entry(&clients, entry);
                                        all_entries.push(entry.clone());
                                    }
                                }
                                "usage" => {
                                    broadcast_except(&clients, Some(eid), "usage", envelope.data.clone());
                                    last_usage = Some(envelope.data);
                                }
                                "lifecycle" => {
                                    let name = envelope.data.as_str().unwrap_or("");
                                    match name {
                                        "round_started" => {
                                            broadcast_lifecycle(&clients, LifecycleEvent::RoundStarted);
                                        }
                                        "round_completed" => {
                                            broadcast_lifecycle(&clients, LifecycleEvent::RoundCompleted);
                                        }
                                        "round_interrupted" => {
                                            tracing::info!("round_interrupted received from easement");
                                            broadcast_lifecycle(&clients, LifecycleEvent::RoundInterrupted);
                                        }
                                        _ => {
                                            tracing::debug!(lifecycle = %name, "unknown easement lifecycle");
                                        }
                                    }
                                }
                                "zsh_result" => {
                                    if let Some(reply) = pending_zsh.take() {
                                        let output = envelope.data.get("output")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("")
                                            .to_string();
                                        let exit_code = envelope.data.get("exit_code")
                                            .and_then(|v| v.as_i64())
                                            .unwrap_or(-1) as i32;
                                        tracing::info!(exit_code, output_len = output.len(), "zsh result received");
                                        broadcast_except(&clients, Some(eid), "tool_done", json!({
                                            "tool": "zsh",
                                            "output": &output,
                                            "exit_code": exit_code
                                        }));
                                        let _ = reply.send(ZshResult { output, exit_code });
                                    }
                                }
                                "shell_result" => {
                                    broadcast_except(&clients, Some(eid), "shell_result", envelope.data);
                                }
                                "meta" => {
                                    if let Some(sid) = envelope.data.get("session_id").and_then(|v| v.as_str()) {
                                        tracing::info!(session_id = %sid, "easement meta: session id");
                                    }
                                    broadcast_meta(&clients, envelope.data);
                                }
                                "error" => {
                                    let msg = envelope.data.get("message")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown error");
                                    tracing::error!("easement error: {}", msg);
                                    broadcast_error(&clients, msg);
                                }
                                "log" => {
                                    let level = envelope.data.get("level")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("info");
                                    let message = envelope.data.get("message")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("");
                                    let fields = envelope.data.get("fields");
                                    match level {
                                        "error" => tracing::error!(slug = %slug, fields = ?fields, "[easement] {}", message),
                                        "warn" => tracing::warn!(slug = %slug, fields = ?fields, "[easement] {}", message),
                                        _ => tracing::info!(slug = %slug, fields = ?fields, "[easement] {}", message),
                                    }
                                }
                                _ => {
                                    tracing::debug!(stream = %envelope.stream, "unknown easement envelope");
                                }
                            }
                            continue;
                        }

                        match envelope.stream.as_str() {
                            "claude" => {
                                // Spawn Easement if not connected.
                                if easement_client_id.is_none() && easement_child.is_none() {
                                    let effective_remote = remote_host.clone();
                                    let mut cmd = match &effective_remote {
                                        None => Command::new("easement"),
                                        Some(host) => {
                                            let mut c = Command::new("ssh");
                                            c.arg("-R").arg("6502:localhost:6502");
                                            c.arg(host).arg("easement");
                                            c
                                        }
                                    };
                                    cmd.stdin(Stdio::piped())
                                        .stdout(Stdio::null())
                                        .stderr(Stdio::null());

                                    match cmd.spawn() {
                                        Ok(mut child) => {
                                            if let Some(mut stdin) = child.stdin.take() {
                                                let bootstrap = json!({
                                                    "slug": slug,
                                                    "timestamp": current_timestamp
                                                });
                                                let mut bootstrap_json = serde_json::to_string(&bootstrap).unwrap();
                                                bootstrap_json.push('\n');
                                                let _ = stdin.write_all(bootstrap_json.as_bytes()).await;
                                                let _ = stdin.flush().await;
                                                drop(stdin);
                                            }
                                            tracing::info!("easement spawned, waiting for WebSocket connect");
                                            easement_child = Some(child);
                                        }
                                        Err(e) => {
                                            tracing::error!("failed to spawn easement: {}", e);
                                            broadcast_error(&clients, &format!("failed to spawn easement: {}", e));
                                            continue;
                                        }
                                    }

                                    // Wait for Easement to connect back.
                                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                                    while easement_client_id.is_none() {
                                        if tokio::time::Instant::now() > deadline {
                                            tracing::error!("timeout waiting for easement to connect");
                                            broadcast_error(&clients, "timeout waiting for easement to connect");
                                            break;
                                        }
                                        // Process coordinator messages while waiting.
                                        match tokio::time::timeout(
                                            std::time::Duration::from_millis(100),
                                            coord_rx.recv(),
                                        ).await {
                                            Ok(Some(CoordMessage::ClientConnected { id: cid, session_id: sid, protocol: proto, timestamp: _, tx })) => {
                                                if proto == "easement" {
                                                    easement_client_id = Some(cid);
                                                    tracing::info!(client_id = cid, "easement client connected");
                                                } else {
                                                    if let Some(sid) = sid {
                                                        sessions.set_local(sid);
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
                                                }
                                                clients.insert(cid, tx);
                                            }
                                            _ => {}
                                        }
                                    }

                                    if easement_client_id.is_none() {
                                        continue;
                                    }
                                }

                                if let Some(eid) = easement_client_id {
                                    let msg: ClaudeMessage = match serde_json::from_value(envelope.data) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            tracing::warn!("bad claude message: {}", e);
                                            broadcast_error(&clients, &format!("bad claude message: {}", e));
                                            continue;
                                        }
                                    };

                                    let claude_data = json!({
                                        "message": msg.message,
                                        "yolo": msg.yolo,
                                        "transcript": transcript.entries()
                                    });
                                    tracing::info!(transcript_entries = transcript.entries().len(), "forwarding claude envelope to easement");
                                    send_to(&clients, eid, "claude", claude_data);
                                }
                            }
                            "shell" => {
                                // Spawn Easement if not connected (same as claude handler).
                                if easement_client_id.is_none() && easement_child.is_none() {
                                    let effective_remote = remote_host.clone();
                                    let mut cmd = match &effective_remote {
                                        None => Command::new("easement"),
                                        Some(host) => {
                                            let mut c = Command::new("ssh");
                                            c.arg("-R").arg("6502:localhost:6502");
                                            c.arg(host).arg("easement");
                                            c
                                        }
                                    };
                                    cmd.stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::null());
                                    match cmd.spawn() {
                                        Ok(mut child) => {
                                            if let Some(mut stdin) = child.stdin.take() {
                                                let bootstrap = json!({ "slug": slug, "timestamp": current_timestamp });
                                                let mut bj = serde_json::to_string(&bootstrap).unwrap();
                                                bj.push('\n');
                                                let _ = stdin.write_all(bj.as_bytes()).await;
                                                let _ = stdin.flush().await;
                                                drop(stdin);
                                            }
                                            easement_child = Some(child);
                                            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                                            while easement_client_id.is_none() {
                                                if tokio::time::Instant::now() > deadline { break; }
                                                match tokio::time::timeout(std::time::Duration::from_millis(100), coord_rx.recv()).await {
                                                    Ok(Some(CoordMessage::ClientConnected { id: cid, protocol: proto, timestamp: _, session_id: _, tx })) => {
                                                        if proto == "easement" { easement_client_id = Some(cid); }
                                                        clients.insert(cid, tx);
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            broadcast_error(&clients, &format!("failed to spawn easement: {}", e));
                                        }
                                    }
                                }
                                if let Some(eid) = easement_client_id {
                                    send_to(&clients, eid, "shell", envelope.data);
                                    tracing::info!("shell command forwarded to easement");
                                } else {
                                    broadcast_error(&clients, "no easement connected for shell command");
                                }
                            }
                            "approval" => {
                                if let Some(pending) = pending_approval.take() {
                                    let decision: ApprovalDecision = match serde_json::from_value(envelope.data) {
                                        Ok(d) => d,
                                        Err(e) => {
                                            tracing::warn!("bad approval decision: {}", e);
                                            continue;
                                        }
                                    };

                                    let response = if decision.behavior == "allow" {
                                        json!({
                                            "behavior": "allow",
                                            "updatedInput": pending.original_input
                                        })
                                    } else {
                                        json!({
                                            "behavior": "deny",
                                            "message": decision.message.unwrap_or_else(|| "User denied permission".to_string())
                                        })
                                    };

                                    let _ = pending.reply.send(response);
                                    tracing::info!("approval decision sent via MCP");
                                } else {
                                    tracing::warn!("approval decision with no pending request");
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
                                        // Drop the reply sender — the HTTP handler
                                        // will see the channel close.
                                    } else {
                                        // Was claimed or fulfilled already, put it back.
                                        pending_service = Some(pending);
                                    }
                                }
                            }
                            "interrupt" => {
                                if let Some(eid) = easement_client_id {
                                    send_to(&clients, eid, "interrupt", serde_json::json!({}));
                                    tracing::info!("interrupt forwarded to easement");
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
                                    "error" => tracing::error!(
                                        client_id = id, slug = %slug,
                                        fields = ?fields, "[client] {}", message
                                    ),
                                    "warn" => tracing::warn!(
                                        client_id = id, slug = %slug,
                                        fields = ?fields, "[client] {}", message
                                    ),
                                    _ => tracing::info!(
                                        client_id = id, slug = %slug,
                                        fields = ?fields, "[client] {}", message
                                    ),
                                }
                            }
                            "exit" => {
                                clients.remove(&id);
                                tracing::info!(client_id = id, "client sent exit");
                            }
                            other => {
                                tracing::warn!("unknown inbound stream: {}", other);
                            }
                        }
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

    // Read connect payload (first message).
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

    // Get or create coordinator for this slug.
    let (coord_tx, client_id) = {
        let mut state = server.write().await;
        let client_id = state.next_id();

        let handle = state.coordinators.entry(slug.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            let slug_clone = slug.clone();
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                run_coordinator(slug_clone, tx_clone, rx).await;
            });
            CoordinatorHandle { tx }
        });

        (handle.tx.clone(), client_id)
    };

    // Channel for outbound messages from coordinator to this client.
    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();

    let protocol = connect.protocol.unwrap_or_else(|| "wicket".to_string());

    // Register with coordinator.
    let _ = coord_tx.send(CoordMessage::ClientConnected {
        id: client_id,
        session_id: connect.session_id,
        protocol,
        timestamp: connect.timestamp,
        tx: client_tx,
    });

    // Writer task: coordinator → WebSocket.
    let write_task = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            if sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Reader loop: WebSocket → coordinator.
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

    // Client disconnected.
    let _ = coord_tx.send(CoordMessage::ClientDisconnected { id: client_id });
    write_task.abort();
    tracing::info!(client_id, slug = %slug, "websocket client disconnected");
}

// -- MCP HTTP handler --

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
            // Find the coordinator for this slug.
            let coord_tx = {
                let state = server.read().await;
                state.coordinators.get(slug).map(|h| h.tx.clone())
            };

            let coord_tx = match coord_tx {
                Some(tx) => tx,
                None => {
                    tracing::warn!(slug, "MCP request for unknown slug");
                    let deny = json!({
                        "behavior": "deny",
                        "message": "No active session for this slug"
                    });
                    let text = serde_json::to_string(&deny).unwrap();
                    return make_json_response(jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": text }] }),
                    ));
                }
            };

            if params.name == "zsh" {
                let command = params.arguments["command"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                let escalate = params.arguments.get("escalate")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let reason = params.arguments.get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                if escalate {
                    // Escalation: ask Puzzle for approval before running unsandboxed.
                    tracing::info!(command = %command, reason = %reason, "zsh escalation request");

                    let (approval_tx, approval_rx) = oneshot::channel();
                    let _ = coord_tx.send(CoordMessage::McpApprovalRequest {
                        tool_name: "zsh (unsandboxed)".to_string(),
                        input: json!({ "command": command, "reason": reason }),
                        tool_use_id: None,
                        reply: approval_tx,
                    });

                    match approval_rx.await {
                        Ok(decision) => {
                            let behavior = decision.get("behavior")
                                .and_then(|v| v.as_str())
                                .unwrap_or("deny");

                            if behavior == "allow" {
                                // Run unsandboxed.
                                tracing::info!(command = %command, "escalation approved, running unsandboxed");
                                let (reply_tx, reply_rx) = oneshot::channel();
                                let _ = coord_tx.send(CoordMessage::ZshExec {
                                    command,
                                    sandboxed: false,
                                    reply: reply_tx,
                                });
                                // For now it runs sandboxed regardless. The sandboxed
                                // flag on the envelope is the next piece.
                                match reply_rx.await {
                                    Ok(result) => {
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
                                    Err(_) => jsonrpc_response(
                                        id,
                                        json!({ "content": [{ "type": "text", "text": "execution failed: reply dropped" }], "isError": true }),
                                    ),
                                }
                            } else {
                                jsonrpc_response(
                                    id,
                                    json!({ "content": [{ "type": "text", "text": "escalation denied by operator" }], "isError": true }),
                                )
                            }
                        }
                        Err(_) => jsonrpc_response(
                            id,
                            json!({ "content": [{ "type": "text", "text": "escalation request dropped" }], "isError": true }),
                        ),
                    }
                } else {
                    // Normal path: sandboxed execution.
                    tracing::info!(command = %command, "zsh tool call (sandboxed)");

                    let (reply_tx, reply_rx) = oneshot::channel();
                    let _ = coord_tx.send(CoordMessage::ZshExec {
                        command,
                        sandboxed: true,
                        reply: reply_tx,
                    });

                    match reply_rx.await {
                        Ok(result) => {
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
                        Err(_) => {
                            tracing::warn!("zsh exec reply channel dropped");
                            jsonrpc_response(
                                id,
                                json!({ "content": [{ "type": "text", "text": "command execution failed: reply dropped" }], "isError": true }),
                            )
                        }
                    }
                }
            } else if params.name == "apply_patch" {
                let patch = params.arguments["patch"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                tracing::info!("apply_patch tool call");

                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = coord_tx.send(CoordMessage::FileOp {
                    op: "apply_patch".to_string(),
                    args: json!({ "patch": patch }),
                    reply: reply_tx,
                });

                match reply_rx.await {
                    Ok(result) => {
                        let output = if result.exit_code != 0 {
                            format!("{}\n[error]", result.output)
                        } else {
                            result.output
                        };
                        jsonrpc_response(
                            id,
                            json!({ "content": [{ "type": "text", "text": output }] }),
                        )
                    }
                    Err(_) => jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": "apply_patch failed: reply dropped" }], "isError": true }),
                    ),
                }
            } else if params.name == "view_image" {
                let path = params.arguments["path"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                tracing::info!(path = %path, "view_image tool call");

                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = coord_tx.send(CoordMessage::FileOp {
                    op: "view_image".to_string(),
                    args: json!({ "path": path }),
                    reply: reply_tx,
                });

                match reply_rx.await {
                    Ok(result) => {
                        if result.exit_code != 0 {
                            jsonrpc_response(
                                id,
                                json!({ "content": [{ "type": "text", "text": result.output }], "isError": true }),
                            )
                        } else {
                            match serde_json::from_str::<Value>(&result.output) {
                                Ok(content) => jsonrpc_response(id, json!({ "content": content })),
                                Err(_) => jsonrpc_response(
                                    id,
                                    json!({ "content": [{ "type": "text", "text": result.output }] }),
                                ),
                            }
                        }
                    }
                    Err(_) => jsonrpc_response(
                        id,
                        json!({ "content": [{ "type": "text", "text": "view_image failed: reply dropped" }], "isError": true }),
                    ),
                }
            } else if params.name == "wicket_approve" {
                let tool_name = params.arguments["tool_name"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                let input = params.arguments["input"].clone();
                let tool_use_id = params.arguments["tool_use_id"]
                    .as_str()
                    .map(|s| s.to_string());

                // Send to coordinator and wait for the approval decision.
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = coord_tx.send(CoordMessage::McpApprovalRequest {
                    tool_name,
                    input,
                    tool_use_id,
                    reply: reply_tx,
                });

                match reply_rx.await {
                    Ok(decision) => {
                        let text = serde_json::to_string(&decision).unwrap();
                        jsonrpc_response(
                            id,
                            json!({ "content": [{ "type": "text", "text": text }] }),
                        )
                    }
                    Err(_) => {
                        tracing::warn!("approval reply channel dropped");
                        let deny = json!({
                            "behavior": "deny",
                            "message": "Approval request dropped"
                        });
                        let text = serde_json::to_string(&deny).unwrap();
                        jsonrpc_response(
                            id,
                            json!({ "content": [{ "type": "text", "text": text }] }),
                        )
                    }
                }
            } else {
                return make_json_response(jsonrpc_error(
                    id,
                    -32601,
                    format!("Unknown tool: {}", params.name),
                ));
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

    let (reply_tx, reply_rx) = oneshot::channel();
    let _ = coord_tx.send(CoordMessage::ServiceRequest {
        request_type: "capture".to_string(),
        reply: reply_tx,
    });

    match tokio::time::timeout(std::time::Duration::from_secs(30), reply_rx).await {
        Ok(Ok(resp)) => {
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", resp.content_type)
                .body(Full::new(Bytes::from(resp.body)))
                .unwrap()
        }
        Ok(Err(_)) => {
            // Channel dropped — no client claimed or timeout fired.
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("no client handled the request")))
                .unwrap()
        }
        Err(_) => {
            Response::builder()
                .status(StatusCode::GATEWAY_TIMEOUT)
                .body(Full::new(Bytes::from("capture timed out")))
                .unwrap()
        }
    }
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
    } else if let Some(slug) = path.strip_prefix("/easement/") {
        if slug.is_empty() {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("missing slug in /easement/<slug>")))
                .unwrap())
        } else {
            let slug = slug.to_string();
            let coord_tx = {
                let mut state = server.write().await;
                let handle = state.coordinators.entry(slug.clone()).or_insert_with(|| {
                    let (tx, rx) = mpsc::unbounded_channel();
                    let slug_clone = slug.clone();
                    let tx_clone = tx.clone();
                    tokio::spawn(async move {
                        run_coordinator(slug_clone, tx_clone, rx).await;
                    });
                    CoordinatorHandle { tx }
                });
                handle.tx.clone()
            };

            if req.method() == hyper::Method::POST {
                let body = req.collect().await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();
                let host = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|v| v.get("host").and_then(|h| h.as_str()).map(|s| s.to_string()));

                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = coord_tx.send(CoordMessage::SetRemoteHost {
                    host,
                    reply: reply_tx,
                });

                let current = reply_rx.await.unwrap_or(None);
                let body = json!({ "host": current }).to_string();
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap())
            } else {
                let (reply_tx, reply_rx) = oneshot::channel();
                let _ = coord_tx.send(CoordMessage::GetRemoteHost { reply: reply_tx });

                let current = reply_rx.await.unwrap_or(None);
                let body = json!({ "host": current }).to_string();
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap())
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
