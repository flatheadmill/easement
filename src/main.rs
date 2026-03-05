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
struct OutEnvelope {
    stream: &'static str,
    data: Value,
}

fn envelope_json(stream: &'static str, data: Value) -> Option<String> {
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
        .join("puzzle");
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
}

// -- Pending approval state --

struct PendingApproval {
    reply: oneshot::Sender<Value>,
    original_input: Value,
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

async fn run_coordinator(slug: String, mut coord_rx: mpsc::UnboundedReceiver<CoordMessage>) {
    let mut transcript = Transcript::new(&slug);
    let mut sessions = Sessions::new(&slug);
    let history = transcript.load_history();

    // Cache normalized entries for late-connecting clients.
    let mut all_entries: Vec<NormalizedEntry> = history;

    let mut clients: Clients = HashMap::new();

    // Easement state.
    let mut easement_stdin: Option<tokio::process::ChildStdin> = None;
    let mut easement_child: Option<tokio::process::Child> = None;
    let mut drain_gate: Option<DrainGate> = None;
    let mut pending_approval: Option<PendingApproval> = None;
    let mut stdout_rx: Option<mpsc::Receiver<String>> = None;

    tracing::info!(
        slug = %slug,
        history = all_entries.len(),
        "coordinator started"
    );

    loop {
        tokio::select! {
            // Easement stdout — process events.
            Some(line) = async {
                match &mut stdout_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Ok(envelope) = serde_json::from_str::<EasementEnvelope>(&line) {
                    match envelope.stream.as_str() {
                        "stdout" => {
                            if let Ok(event) = serde_json::from_value::<StdoutEvent>(envelope.data) {
                                if let StdoutEvent::Assistant { ref uuid, .. } = event {
                                    if let Some(uuid) = uuid {
                                        let new_entries = transcript.set_boundary(uuid.clone());
                                        for entry in &new_entries {
                                            broadcast_entry(&clients, entry);
                                            all_entries.push(entry.clone());
                                        }
                                    }
                                }

                                if let Some(ref mut gate) = drain_gate {
                                    let round_done = gate.handle(&event);

                                    if round_done {
                                        if let Some(sid) = gate.session_id() {
                                            let sid = sid.to_string();
                                            tracing::info!(session_id = %sid, "round completed, recording session");
                                            sessions.set_local(sid);
                                        }

                                        tracing::info!("round completed");
                                        broadcast_lifecycle(&clients, LifecycleEvent::RoundCompleted);

                                        easement_stdin.take();
                                        drain_gate = None;
                                    }
                                }
                            }
                        }
                        "transcript" => {
                            let new_entries = transcript.handle_entry(envelope.data);
                            for entry in &new_entries {
                                broadcast_entry(&clients, entry);
                                all_entries.push(entry.clone());
                            }
                        }
                        "approval" => {
                            tracing::info!("approval request received");
                            broadcast_approval(&clients, envelope.data);
                        }
                        "meta" => {
                            if let Some(sid) = envelope.data.get("session_id").and_then(|v| v.as_str()) {
                                tracing::info!(session_id = %sid, "meta: session id (not persisted until round completes)");
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
                        _ => {
                            tracing::warn!("unknown easement stream: {}", envelope.stream);
                        }
                    }
                }
            }

            // Coordinator messages (from WebSocket clients).
            Some(msg) = coord_rx.recv() => {
                match msg {
                    CoordMessage::ClientConnected { id, session_id, tx } => {
                        if let Some(sid) = session_id {
                            sessions.set_local(sid);
                        }
                        // Stream history to this client.
                        for entry in &all_entries {
                            if let Ok(data) = serde_json::to_value(entry) {
                                if let Some(json) = envelope_json("entry", data) {
                                    let _ = tx.send(json);
                                }
                            }
                        }
                        clients.insert(id, tx);
                        tracing::info!(client_id = id, clients = clients.len(), "client connected");
                    }
                    CoordMessage::ClientDisconnected { id } => {
                        clients.remove(&id);
                        tracing::info!(client_id = id, clients = clients.len(), "client disconnected");
                    }
                    CoordMessage::McpApprovalRequest { tool_name, input, tool_use_id, reply } => {
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
                    CoordMessage::Envelope { id, envelope } => {
                        match envelope.stream.as_str() {
                            "claude" => {
                                if let Some(ref mut stdin) = easement_stdin {
                                    // Round in flight — interjection.
                                    if let Some(msg) = envelope.data.get("message").and_then(|v| v.as_str()) {
                                        let user_msg = json!({
                                            "type": "user",
                                            "message": {
                                                "role": "user",
                                                "content": msg
                                            },
                                            "uuid": uuid::Uuid::new_v4().to_string()
                                        });
                                        let claude_env = json!({
                                            "stream": "claude",
                                            "data": user_msg
                                        });
                                        let mut env_line = serde_json::to_string(&claude_env).unwrap();
                                        env_line.push('\n');
                                        let _ = stdin.write_all(env_line.as_bytes()).await;
                                        let _ = stdin.flush().await;

                                        if let Some(ref mut gate) = drain_gate {
                                            gate.sent += 1;
                                        }
                                        tracing::info!("interjection forwarded");
                                    }
                                } else {
                                    // No round in flight — start a new round.
                                    let msg: ClaudeMessage = match serde_json::from_value(envelope.data) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            tracing::warn!("bad claude message: {}", e);
                                            broadcast_error(&clients, &format!("bad claude message: {}", e));
                                            continue;
                                        }
                                    };

                                    let is_remote = msg.remote.is_some();
                                    let is_yolo = msg.yolo;

                                    let active_session = if is_remote {
                                        sessions.remote().map(|s| s.to_string())
                                    } else {
                                        sessions.local().map(|s| s.to_string())
                                    };

                                    let payload = EasementPayload {
                                        slug: slug.clone(),
                                        yolo: is_yolo,
                                        message: msg.message,
                                        session_id: active_session.clone(),
                                        transcript: if active_session.is_none() {
                                            Some(transcript.entries().to_vec())
                                        } else {
                                            None
                                        },
                                        wicket_socket: None,
                                    };

                                    transcript.begin_round();

                                    tracing::info!(
                                        is_remote, is_yolo,
                                        has_session = active_session.is_some(),
                                        "building easement command"
                                    );
                                    let mut cmd = match &msg.remote {
                                        None => Command::new("easement"),
                                        Some(host) => {
                                            let mut c = Command::new("ssh");
                                            if !is_yolo {
                                                c.arg("-R").arg("6502:localhost:6502");
                                            }
                                            c.arg(host).arg("easement");
                                            c
                                        }
                                    };

                                    cmd.stdin(Stdio::piped())
                                        .stdout(Stdio::piped())
                                        .stderr(Stdio::null());

                                    tracing::info!("spawning easement");
                                    let mut child = match cmd.spawn() {
                                        Ok(c) => {
                                            tracing::info!("easement spawned");
                                            c
                                        }
                                        Err(e) => {
                                            tracing::error!("failed to spawn easement: {}", e);
                                            broadcast_error(&clients, &format!("failed to spawn easement: {}", e));
                                            broadcast_lifecycle(&clients, LifecycleEvent::RoundFailed {
                                                message: format!("spawn error: {}", e),
                                            });
                                            continue;
                                        }
                                    };

                                    let mut child_stdin = child.stdin.take()
                                        .expect("stdin was set to piped");
                                    let child_stdout = child.stdout.take()
                                        .expect("stdout was set to piped");

                                    let mut payload_json = serde_json::to_string(&payload)
                                        .expect("payload serialization cannot fail");
                                    tracing::info!(
                                        payload_len = payload_json.len(),
                                        "sending payload to easement"
                                    );
                                    payload_json.push('\n');
                                    if child_stdin
                                        .write_all(payload_json.as_bytes())
                                        .await
                                        .is_err()
                                    {
                                        tracing::error!("failed to write payload to easement");
                                        broadcast_error(&clients, "failed to write payload to easement");
                                        broadcast_lifecycle(&clients, LifecycleEvent::RoundFailed {
                                            message: "payload write failed".into(),
                                        });
                                        continue;
                                    }
                                    let _ = child_stdin.flush().await;
                                    tracing::info!("payload sent to easement");

                                    let (tx, rx) = mpsc::channel::<String>(256);
                                    tokio::spawn(async move {
                                        let mut stdout_reader = BufReader::new(child_stdout);
                                        let mut line = String::new();
                                        loop {
                                            line.clear();
                                            match stdout_reader.read_line(&mut line).await {
                                                Ok(0) => break,
                                                Ok(_) => {
                                                    let trimmed = line.trim().to_string();
                                                    if !trimmed.is_empty() {
                                                        if tx.send(trimmed).await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                }
                                                Err(_) => break,
                                            }
                                        }
                                    });

                                    easement_stdin = Some(child_stdin);
                                    easement_child = Some(child);
                                    stdout_rx = Some(rx);
                                    drain_gate = Some(DrainGate::new());

                                    tracing::info!("round started");
                                    broadcast_lifecycle(&clients, LifecycleEvent::RoundStarted);
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

            // Easement exited.
            status = async {
                match &mut easement_child {
                    Some(child) => child.wait().await,
                    None => std::future::pending().await,
                }
            } => {
                match status {
                    Ok(s) => {
                        let code = s.code().unwrap_or(-1);
                        tracing::info!(exit_code = code, "easement exited");

                        if drain_gate.is_some() {
                            tracing::warn!("easement exited before drain gate fired");
                            sessions.clear_local();
                            broadcast_lifecycle(&clients, LifecycleEvent::RoundFailed {
                                message: format!("easement exited with code {}", code),
                            });
                            drain_gate = None;
                        }
                    }
                    Err(e) => {
                        tracing::error!("error waiting for easement: {}", e);
                        broadcast_lifecycle(&clients, LifecycleEvent::RoundFailed {
                            message: format!("wait error: {}", e),
                        });
                        drain_gate = None;
                    }
                }
                easement_child = None;
                easement_stdin = None;
                stdout_rx = None;
                pending_approval = None;
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
            tokio::spawn(async move {
                run_coordinator(slug_clone, rx).await;
            });
            CoordinatorHandle { tx }
        });

        (handle.tx.clone(), client_id)
    };

    // Channel for outbound messages from coordinator to this client.
    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<String>();

    // Register with coordinator.
    let _ = coord_tx.send(CoordMessage::ClientConnected {
        id: client_id,
        session_id: connect.session_id,
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
            if params.name != "wicket_approve" {
                return make_json_response(jsonrpc_error(
                    id,
                    -32601,
                    format!("Unknown tool: {}", params.name),
                ));
            }

            let tool_name = params.arguments["tool_name"]
                .as_str()
                .unwrap_or("unknown")
                .to_string();
            let input = params.arguments["input"].clone();
            let tool_use_id = params.arguments["tool_use_id"]
                .as_str()
                .map(|s| s.to_string());

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
        }
        "notifications/initialized" => jsonrpc_response(id, json!({})),
        other => jsonrpc_error(id, -32601, format!("Method not found: {}", other)),
    };

    make_json_response(response)
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
