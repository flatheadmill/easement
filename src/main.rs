// Wicket: the coordinator between clients (Puzzle, Shotgun) and Easement.
//
// The client spawns Wicket with optional --remote <host>, --yolo, and
// --input lpjson flags. Wicket reads a payload from stdin, spawns Easement
// (locally or over SSH), binds the approval socket when needed, and
// multiplexes everything into a single envelope stream. The client-facing
// edge is NDJSON by default or LPJSON (4-byte LE length-prefixed JSON)
// when --input lpjson is set. The Easement-facing edge is always NDJSON.
// The client sends envelopes back: claude envelopes route to Easement,
// approval envelopes route to the held socket connection.
//
// Approval requests reach Wicket through the domain socket. On the remote
// side, Easement starts an HTTP MCP server on port 6502 that bridges to
// the forwarded socket. No relay binary is needed on the remote machine —
// only easement and claude.

use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

// ---- Framing ----

#[derive(Debug, Clone, Copy, PartialEq)]
enum Framing {
    Ndjson,
    Lpjson,
}

static FRAMING: OnceLock<Framing> = OnceLock::new();

fn framing() -> Framing {
    FRAMING.get().copied().unwrap_or(Framing::Ndjson)
}

// ---- Output ----

#[derive(Debug, Serialize)]
struct Envelope {
    stream: &'static str,
    data: Value,
}

fn write_client(lock: &mut io::StdoutLock, bytes: &[u8]) {
    match framing() {
        Framing::Ndjson => {
            let _ = lock.write_all(bytes);
            let _ = lock.write_all(b"\n");
        }
        Framing::Lpjson => {
            let _ = lock.write_all(&(bytes.len() as u32).to_le_bytes());
            let _ = lock.write_all(bytes);
        }
    }
    let _ = lock.flush();
}

fn emit(stream: &'static str, data: Value) {
    if let Ok(json) = serde_json::to_string(&Envelope { stream, data }) {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        write_client(&mut lock, json.as_bytes());
    }
}

fn emit_error(message: &str) {
    emit("error", json!({ "message": message }));
}

// ---- LPJSON reader ----

async fn read_lpjson<R: AsyncReadExt + Unpin>(reader: &mut R) -> Option<String> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(_) => return None,
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    match reader.read_exact(&mut buf).await {
        Ok(_) => {}
        Err(_) => return None,
    }
    String::from_utf8(buf).ok()
}

// ---- Logging ----

fn init_tracing() -> WorkerGuard {
    let home = env::var("HOME").expect("HOME not set");
    let log_dir = std::path::Path::new(&home)
        .join(".local").join("state").join("puzzle");
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

// ---- Payload ----

#[derive(Debug, Deserialize, Serialize)]
struct Payload {
    slug: String,
    #[serde(default)]
    yolo: bool,
    message: String,
    session_id: Option<String>,
    transcript: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wicket_socket: Option<String>,
}

// ---- Inbound envelope from client ----

#[derive(Debug, Deserialize)]
struct InboundEnvelope {
    stream: String,
    data: Value,
}

// ---- Coordinator ----

async fn run_coordinator(remote: Option<String>, yolo: bool) {
    let home = match env::var("HOME") {
        Ok(h) => h,
        Err(_) => {
            emit_error("HOME not set");
            std::process::exit(1);
        }
    };

    // Read the payload from stdin.
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    let payload_str = match framing() {
        Framing::Ndjson => {
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_err() {
                emit_error("failed to read payload from stdin");
                std::process::exit(1);
            }
            line
        }
        Framing::Lpjson => match read_lpjson(&mut reader).await {
            Some(s) => s,
            None => {
                emit_error("failed to read payload from stdin");
                std::process::exit(1);
            }
        },
    };

    let mut payload: Payload = match serde_json::from_str(payload_str.trim()) {
        Ok(p) => p,
        Err(e) => {
            emit_error(&format!("invalid payload: {}", e));
            std::process::exit(1);
        }
    };

    if yolo {
        payload.yolo = true;
    }

    tracing::info!(
        slug = %payload.slug,
        yolo = payload.yolo,
        remote = ?remote,
        has_session_id = payload.session_id.is_some(),
        has_transcript = payload.transcript.is_some(),
        "coordinator starting"
    );

    // Bind the approval socket unless yolo.
    let socket_path = PathBuf::from(&home)
        .join("pane").join(&payload.slug).join("wicket.sock");
    let approval_listener: Option<UnixListener> = if !yolo {
        let _ = std::fs::remove_file(&socket_path);
        match UnixListener::bind(&socket_path) {
            Ok(l) => {
                tracing::info!(path = %socket_path.display(), "approval socket bound");
                Some(l)
            }
            Err(e) => {
                emit_error(&format!("failed to bind approval socket: {}", e));
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // Build the Easement command.
    let mut cmd = match &remote {
        None => Command::new("easement"),
        Some(host) => {
            let mut c = Command::new("ssh");
            if !yolo {
                let short_id = &uuid::Uuid::new_v4().to_string()[..8];
                let remote_socket = format!(
                    "/tmp/puzzle-{}-{}.sock", payload.slug, short_id
                );
                payload.wicket_socket = Some(remote_socket.clone());
                c.arg("-R").arg(format!(
                    "{}:{}", remote_socket, socket_path.display()
                ));
            }
            c.arg(host).arg("easement");
            c
        }
    };

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            emit_error(&format!("failed to spawn easement: {}", e));
            std::process::exit(1);
        }
    };

    let mut easement_stdin: Option<tokio::process::ChildStdin> = Some(
        child.stdin.take().expect("stdin was set to piped"),
    );
    let easement_stdout = child.stdout.take()
        .expect("stdout was set to piped");

    // Send the payload to Easement.
    let mut payload_json = serde_json::to_string(&payload)
        .expect("payload serialization cannot fail");
    payload_json.push('\n');
    if let Some(ref mut stdin) = easement_stdin {
        if stdin.write_all(payload_json.as_bytes()).await.is_err() {
            emit_error("failed to write payload to easement");
            std::process::exit(1);
        }
        let _ = stdin.flush().await;
    }

    tracing::info!("easement spawned, payload forwarded");

    // Held approval socket writer — one approval at a time.
    let mut approval_writer: Option<tokio::net::unix::OwnedWriteHalf> = None;

    // Easement stdout → channel.
    let (stdout_tx, mut stdout_rx) = mpsc::channel::<String>(256);

    tokio::spawn(async move {
        let mut stdout_reader = BufReader::new(easement_stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match stdout_reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim().to_string();
                    if !trimmed.is_empty() {
                        if stdout_tx.send(trimmed).await.is_err() {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    // Client stdin → channel.
    let (client_tx, mut client_rx) = mpsc::channel::<InboundEnvelope>(256);
    let client_framing = framing();

    tokio::spawn(async move {
        loop {
            let msg = match client_framing {
                Framing::Ndjson => {
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
                Framing::Lpjson => match read_lpjson(&mut reader).await {
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
        // Client stdin closed — send a synthetic exit so the select loop
        // closes Easement's stdin, same as an explicit exit envelope.
        let _ = client_tx.send(InboundEnvelope {
            stream: "exit".to_string(),
            data: Value::Object(Default::default()),
        }).await;
    });

    loop {
        tokio::select! {
            // Easement stdout — pass through to client, re-framed if needed.
            Some(line) = stdout_rx.recv() => {
                let stdout = io::stdout();
                let mut lock = stdout.lock();
                write_client(&mut lock, line.as_bytes());
            }

            // Client inbound envelopes — demux by stream.
            Some(envelope) = client_rx.recv() => {
                match envelope.stream.as_str() {
                    "claude" => {
                        if let Some(ref mut stdin) = easement_stdin {
                            let mut data_line = serde_json::to_string(&envelope.data)
                                .expect("data serialization cannot fail");
                            data_line.push('\n');
                            if stdin.write_all(data_line.as_bytes()).await.is_err() {
                                tracing::warn!("failed to write to easement stdin");
                            }
                            let _ = stdin.flush().await;
                        }
                    }
                    "exit" => {
                        tracing::info!("exit envelope received, closing easement stdin");
                        easement_stdin.take();
                    }
                    "approval" => {
                        if let Some(mut writer) = approval_writer.take() {
                            let mut response = serde_json::to_string(&envelope.data)
                                .expect("approval data serialization cannot fail");
                            response.push('\n');
                            let _ = writer.write_all(response.as_bytes()).await;
                            let _ = writer.flush().await;
                            let _ = writer.shutdown().await;
                            tracing::info!("approval response sent");
                        } else {
                            tracing::warn!("approval envelope with no pending connection");
                        }
                    }
                    other => {
                        tracing::warn!("unknown inbound stream: {}", other);
                    }
                }
            }

            // Approval socket — accept a connection from Easement's HTTP MCP server.
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
                                    tracing::info!("approval request received");
                                    emit("approval", request);
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
            status = child.wait() => {
                match status {
                    Ok(s) => {
                        let code = s.code().unwrap_or(-1);
                        tracing::info!(exit_code = code, "easement exited");
                        emit("meta", json!({ "exit_code": code }));
                    }
                    Err(e) => {
                        tracing::error!("error waiting for easement: {}", e);
                        emit_error(&format!("error waiting for easement: {}", e));
                    }
                }
                break;
            }

            else => {
                tracing::info!("all channels closed, shutting down");
                break;
            }
        }
    }

    if !yolo {
        let _ = std::fs::remove_file(&socket_path);
    }
}

// ---- Entry point ----

#[tokio::main]
async fn main() {
    let _guard = init_tracing();

    let args: Vec<String> = env::args().collect();
    let mut remote: Option<String> = None;
    let mut yolo = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--remote" => {
                i += 1;
                if i < args.len() {
                    remote = Some(args[i].clone());
                } else {
                    emit_error("--remote requires a host argument");
                    std::process::exit(1);
                }
            }
            "--yolo" => {
                yolo = true;
            }
            "--input" => {
                i += 1;
                if i < args.len() {
                    match args[i].as_str() {
                        "lpjson" => {
                            let _ = FRAMING.set(Framing::Lpjson);
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

    let _ = FRAMING.get_or_init(|| Framing::Ndjson);
    tracing::info!(remote = ?remote, yolo, framing = ?framing(), "wicket starting");
    run_coordinator(remote, yolo).await;
}
