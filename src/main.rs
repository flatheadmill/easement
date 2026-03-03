// Wicket: long-lived coordinator between clients (Puzzle, Shotgun) and
// Easement. The client spawns Wicket once and holds it for the life of the
// window. Wicket owns the transcript, session tracking, JSONL parsing,
// deduplication, normalization, and the drain gate. Clients receive clean
// normalized entries and lifecycle events.
//
// Easement stays ephemeral — spawned per round, dies when the round
// completes. The persistence is in Wicket, not in the pipe to Claude.
//
// The client-facing edge is NDJSON by default or LPJSON with --input lpjson.
// The Easement-facing edge is always NDJSON.

mod framing;
mod normalize;
mod parser;
mod protocol;
mod transcript;

use std::env;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;

use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use crate::framing::write_client;
use crate::protocol::{
    ApprovalDecision, ClaudeMessage, ConnectPayload, InboundEnvelope, LifecycleEvent,
    NormalizedEntry,
};
use crate::transcript::{Sessions, Transcript};

// -- Stdout event types from Easement --
//
// These model what Claude emits on stdout, wrapped in Easement's
// {"stream":"stdout","data":{...}} envelopes. The drain gate depends on
// result events and replayed user messages.

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

// -- Output helpers --

#[derive(Debug, Serialize)]
struct OutEnvelope {
    stream: &'static str,
    data: Value,
}

fn emit(stream: &'static str, data: Value) {
    if let Ok(json) = serde_json::to_string(&OutEnvelope { stream, data }) {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        write_client(&mut lock, json.as_bytes());
    }
}

fn emit_entry(entry: &NormalizedEntry) {
    if let Ok(data) = serde_json::to_value(entry) {
        emit("entry", data);
    }
}

fn emit_lifecycle(event: LifecycleEvent) {
    if let Ok(data) = serde_json::to_value(&event) {
        emit("lifecycle", data);
    }
}

fn emit_approval(data: Value) {
    emit("approval", data);
}

fn emit_meta(data: Value) {
    emit("meta", data);
}

fn emit_error(message: &str) {
    emit("error", json!({ "message": message }));
}

// -- Easement payload (what Wicket sends to Easement's stdin) --

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
//
// Tracks sent messages vs replayed messages to detect when the round's
// new content begins. The gate opens at a result boundary when
// sent == replayed, meaning all history has been replayed and the
// response to the current prompt has completed.

struct DrainGate {
    sent: u64,
    replayed: u64,
    drained: bool,
    session_id: Option<String>,
}

impl DrainGate {
    fn new() -> Self {
        Self {
            sent: 1, // kickoff message counts as sent
            replayed: 0,
            drained: false,
            session_id: None,
        }
    }

    /// Process a stdout event. Returns true if the round just completed.
    fn handle(&mut self, event: &StdoutEvent) -> bool {
        // Capture session ID from the first event that carries one.
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

// -- Coordinator --

async fn run_coordinator() {
    let home = match env::var("HOME") {
        Ok(h) => h,
        Err(_) => {
            emit_error("HOME not set");
            std::process::exit(1);
        }
    };

    // Read the connect payload from stdin. The BufReader wraps stdin once
    // and is moved into the client reader task after the connect handshake.
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    let connect_str = match framing::read_client_message(&mut reader).await {
        Some(s) => s,
        None => {
            emit_error("failed to read connect payload");
            std::process::exit(1);
        }
    };

    let connect: ConnectPayload = match serde_json::from_str(&connect_str) {
        Ok(p) => p,
        Err(e) => {
            emit_error(&format!("invalid connect payload: {}", e));
            std::process::exit(1);
        }
    };

    tracing::info!(slug = %connect.slug, "connected");

    // Initialize transcript and sessions.
    let mut transcript = Transcript::new(&connect.slug);
    let mut sessions = Sessions::new(&connect.slug);

    // Override session if the connect payload provides one.
    if let Some(sid) = connect.session_id {
        sessions.set_local(sid);
    }

    // Stream conversation history to the client.
    let history = transcript.load_history();
    for entry in &history {
        emit_entry(entry);
    }
    tracing::info!(entries = history.len(), "history streamed");

    // Held state for in-flight rounds.
    let mut easement_stdin: Option<tokio::process::ChildStdin> = None;
    let mut easement_child: Option<tokio::process::Child> = None;
    let mut drain_gate: Option<DrainGate> = None;
    let mut approval_writer: Option<tokio::net::unix::OwnedWriteHalf> = None;
    let mut approval_listener: Option<UnixListener> = None;
    let mut stdout_rx: Option<mpsc::Receiver<String>> = None;

    // Channels from spawned tasks.
    let (client_tx, mut client_rx) = mpsc::channel::<InboundEnvelope>(256);

    // Spawn client stdin reader.
    let client_framing = framing::framing();
    tokio::spawn(async move {
        loop {
            let msg = match client_framing {
                framing::Framing::Ndjson => {
                    let mut line = String::new();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim().to_string();
                            if trimmed.is_empty() {
                                continue;
                            }
                            trimmed
                        }
                        Err(_) => break,
                    }
                }
                framing::Framing::Lpjson => match framing::read_lpjson(&mut reader).await {
                    Some(s) => s,
                    None => break,
                },
            };
            match serde_json::from_str::<InboundEnvelope>(&msg) {
                Ok(env) => {
                    if client_tx.send(env).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("bad inbound envelope: {}", e);
                }
            }
        }
        // Client stdin closed — synthetic exit.
        let _ = client_tx
            .send(InboundEnvelope {
                stream: "exit".to_string(),
                data: Value::Object(Default::default()),
            })
            .await;
    });

    // Socket path for approval bridge.
    let socket_path = PathBuf::from(&home)
        .join("pane")
        .join(&connect.slug)
        .join("wicket.sock");

    // Main event loop.
    loop {
        tokio::select! {
            // Easement stdout — process events.
            Some(line) = async {
                match &mut stdout_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                // Try to parse as an Easement envelope.
                if let Ok(envelope) = serde_json::from_str::<EasementEnvelope>(&line) {
                    match envelope.stream.as_str() {
                        "stdout" => {
                            if let Ok(event) = serde_json::from_value::<StdoutEvent>(envelope.data) {
                                // Capture boundary UUID from first assistant message.
                                if let StdoutEvent::Assistant { ref uuid, .. } = event {
                                    if let Some(uuid) = uuid {
                                        let new_entries = transcript.set_boundary(uuid.clone());
                                        for entry in &new_entries {
                                            emit_entry(entry);
                                        }
                                    }
                                }

                                if let Some(ref mut gate) = drain_gate {
                                    let round_done = gate.handle(&event);

                                    if round_done {
                                        // Only persist the session ID on a
                                        // successful round — a failed resume
                                        // produces a throwaway session ID that
                                        // must not overwrite the real one.
                                        if let Some(sid) = gate.session_id() {
                                            let sid = sid.to_string();
                                            tracing::info!(session_id = %sid, "round completed, recording session");
                                            sessions.set_local(sid);
                                        }

                                        tracing::info!("round completed");
                                        emit_lifecycle(LifecycleEvent::RoundCompleted);

                                        // Close Easement stdin to let it exit.
                                        easement_stdin.take();
                                        drain_gate = None;
                                    }
                                }
                            }
                        }
                        "transcript" => {
                            let new_entries = transcript.handle_entry(envelope.data);
                            for entry in &new_entries {
                                emit_entry(entry);
                            }
                        }
                        "approval" => {
                            tracing::info!("approval request received");
                            emit_approval(envelope.data);
                        }
                        "meta" => {
                            if let Some(sid) = envelope.data.get("session_id").and_then(|v| v.as_str()) {
                                tracing::info!(session_id = %sid, "meta: session id (not persisted until round completes)");
                            }
                            emit_meta(envelope.data);
                        }
                        "error" => {
                            let msg = envelope.data.get("message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown error");
                            tracing::error!("easement error: {}", msg);
                            emit_error(msg);
                        }
                        _ => {
                            tracing::warn!("unknown easement stream: {}", envelope.stream);
                        }
                    }
                }
            }

            // Client inbound envelopes.
            Some(envelope) = client_rx.recv() => {
                match envelope.stream.as_str() {
                    "claude" => {
                        // If a round is in flight, this is an interjection.
                        if let Some(ref mut stdin) = easement_stdin {
                            // Pass through as a user message to Easement.
                            if let Some(msg) = envelope.data.get("message").and_then(|v| v.as_str()) {
                                let user_msg = serde_json::json!({
                                    "type": "user",
                                    "message": {
                                        "role": "user",
                                        "content": msg
                                    },
                                    "uuid": uuid::Uuid::new_v4().to_string()
                                });
                                let mut line = serde_json::to_string(&user_msg).unwrap();
                                line.push('\n');
                                let claude_env = serde_json::json!({
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
                                    emit_error(&format!("bad claude message: {}", e));
                                    continue;
                                }
                            };

                            let is_remote = msg.remote.is_some();
                            let is_yolo = msg.yolo;

                            // Pick the session ID for this target.
                            let active_session = if is_remote {
                                sessions.remote().map(|s| s.to_string())
                            } else {
                                sessions.local().map(|s| s.to_string())
                            };

                            // Build the Easement payload.
                            let payload = EasementPayload {
                                slug: connect.slug.clone(),
                                yolo: is_yolo,
                                message: msg.message,
                                session_id: active_session.clone(),
                                transcript: if active_session.is_none() {
                                    Some(transcript.entries().to_vec())
                                } else {
                                    None
                                },
                                wicket_socket: None, // set below for SSH
                            };

                            transcript.begin_round();

                            // Bind the approval socket unless yolo.
                            if !is_yolo {
                                let _ = std::fs::remove_file(&socket_path);
                                match UnixListener::bind(&socket_path) {
                                    Ok(l) => {
                                        tracing::info!(path = %socket_path.display(), "approval socket bound");
                                        approval_listener = Some(l);
                                    }
                                    Err(e) => {
                                        emit_error(&format!("failed to bind approval socket: {}", e));
                                        continue;
                                    }
                                }
                            }

                            // Build the Easement command.
                            tracing::info!(
                                is_remote, is_yolo,
                                has_session = active_session.is_some(),
                                "building easement command"
                            );
                            let mut payload = payload;
                            let mut cmd = match &msg.remote {
                                None => Command::new("easement"),
                                Some(host) => {
                                    let mut c = Command::new("ssh");
                                    if !is_yolo {
                                        let short_id = &uuid::Uuid::new_v4().to_string()[..8];
                                        let remote_socket = format!(
                                            "/tmp/puzzle-{}-{}.sock",
                                            connect.slug, short_id
                                        );
                                        payload.wicket_socket = Some(remote_socket.clone());
                                        c.arg("-R").arg(format!(
                                            "{}:{}",
                                            remote_socket,
                                            socket_path.display()
                                        ));
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
                                    emit_error(&format!("failed to spawn easement: {}", e));
                                    emit_lifecycle(LifecycleEvent::RoundFailed {
                                        message: format!("spawn error: {}", e),
                                    });
                                    continue;
                                }
                            };

                            let mut child_stdin = child.stdin.take()
                                .expect("stdin was set to piped");
                            let child_stdout = child.stdout.take()
                                .expect("stdout was set to piped");

                            // Send payload to Easement.
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
                                emit_error("failed to write payload to easement");
                                emit_lifecycle(LifecycleEvent::RoundFailed {
                                    message: "payload write failed".into(),
                                });
                                continue;
                            }
                            let _ = child_stdin.flush().await;
                            tracing::info!("payload sent to easement");

                            // Spawn stdout reader task.
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
                            emit_lifecycle(LifecycleEvent::RoundStarted);
                        }
                    }
                    "approval" => {
                        if let Some(mut writer) = approval_writer.take() {
                            let decision: ApprovalDecision = match serde_json::from_value(envelope.data) {
                                Ok(d) => d,
                                Err(e) => {
                                    tracing::warn!("bad approval decision: {}", e);
                                    continue;
                                }
                            };

                            let mut response = serde_json::to_string(&decision)
                                .expect("approval serialization cannot fail");
                            response.push('\n');
                            let _ = writer.write_all(response.as_bytes()).await;
                            let _ = writer.flush().await;
                            let _ = writer.shutdown().await;
                            tracing::info!("approval response sent");
                        } else {
                            tracing::warn!("approval decision with no pending connection");
                        }
                    }
                    "exit" => {
                        tracing::info!("client disconnected");
                        // Close Easement stdin if a round is in flight.
                        easement_stdin.take();
                        break;
                    }
                    other => {
                        tracing::warn!("unknown inbound stream: {}", other);
                    }
                }
            }

            // Approval socket — accept connection from Easement's MCP server.
            result = async {
                match &approval_listener {
                    Some(l) => l.accept().await.map(|(s, _)| s),
                    None => std::future::pending().await,
                }
            }, if approval_writer.is_none() => {
                match result {
                    Ok(stream) => {
                        let (read_half, write_half) = stream.into_split();
                        let mut buf_reader = BufReader::new(read_half);
                        let mut line = String::new();
                        match buf_reader.read_line(&mut line).await {
                            Ok(0) => {
                                tracing::warn!("socket closed before sending request");
                            }
                            Ok(_) => {
                                if let Ok(request) = serde_json::from_str::<Value>(line.trim()) {
                                    tracing::info!("approval request from easement");
                                    emit_approval(request);
                                    approval_writer = Some(write_half);
                                } else {
                                    tracing::warn!("bad approval request: {}", line.trim());
                                }
                            }
                            Err(e) => {
                                tracing::warn!("failed to read approval request: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("approval accept error: {}", e);
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

                        // If the drain gate didn't fire, the round failed.
                        // Clear the session ID so the next round falls back
                        // to transcript mode rather than retrying a dead session.
                        if drain_gate.is_some() {
                            tracing::warn!("easement exited before drain gate fired");
                            sessions.clear_local();
                            emit_lifecycle(LifecycleEvent::RoundFailed {
                                message: format!("easement exited with code {}", code),
                            });
                            drain_gate = None;
                        }
                    }
                    Err(e) => {
                        tracing::error!("error waiting for easement: {}", e);
                        emit_lifecycle(LifecycleEvent::RoundFailed {
                            message: format!("wait error: {}", e),
                        });
                        drain_gate = None;
                    }
                }
                // Clean up round state.
                easement_child = None;
                easement_stdin = None;
                stdout_rx = None;
                approval_writer = None;
                if !connect.slug.is_empty() {
                    let _ = std::fs::remove_file(&socket_path);
                }
                approval_listener = None;
            }
        }
    }

    // Clean up.
    let _ = std::fs::remove_file(&socket_path);
    if let Some(mut child) = easement_child {
        let _ = child.wait().await;
    }
}

// -- Entry point --

#[tokio::main]
async fn main() {
    let _guard = init_tracing();

    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                if i < args.len() {
                    match args[i].as_str() {
                        "lpjson" => {
                            framing::set_framing(framing::Framing::Lpjson);
                        }
                        other => {
                            emit_error(&format!("unknown input format: {}", other));
                            std::process::exit(1);
                        }
                    }
                } else {
                    emit_error("--input requires a format argument");
                    std::process::exit(1);
                }
            }
            other => {
                emit_error(&format!("unknown argument: {}", other));
                std::process::exit(1);
            }
        }
        i += 1;
    }

    framing::init_default();
    tracing::info!(framing = ?framing::framing(), "wicket starting");
    run_coordinator().await;
}
