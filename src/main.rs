// Easement: a long-lived process on the execution target that connects back
// to Wicket over WebSocket. Receives work envelopes (claude turns, zsh
// commands, apply_patch), dispatches them, sends results back. The Claude
// CLI is ephemeral within Easement, spawned per turn.
//
// Wicket spawns Easement (locally or over SSH). Easement reads a bootstrap
// slug from stdin, connects to ws://localhost:6502 with protocol "easement",
// and enters the envelope loop.
//
// On remote machines, ssh -R 6502:localhost:6502 tunnels the port.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use futures_util::{SinkExt, StreamExt};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

fn init_tracing() -> WorkerGuard {
    let home = std::env::var("HOME").expect("HOME not set");
    let log_dir = std::path::Path::new(&home)
        .join(".local").join("state").join("puzzle");
    let _ = std::fs::create_dir_all(&log_dir);

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("easement.log"))
        .expect("failed to open easement.log");

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

// -- WebSocket send --

type WsSender = mpsc::UnboundedSender<String>;

fn ws_emit(tx: &WsSender, stream: &str, data: serde_json::Value) {
    let envelope = serde_json::json!({ "stream": stream, "data": data });
    if let Ok(json) = serde_json::to_string(&envelope) {
        let _ = tx.send(json);
    }
}

fn ws_emit_error(tx: &WsSender, message: &str) {
    ws_emit(tx, "error", serde_json::json!({ "message": message }));
}

fn ws_emit_log(tx: &WsSender, level: &str, message: &str, fields: serde_json::Value) {
    ws_emit(tx, "log", serde_json::json!({
        "level": level,
        "message": message,
        "fields": fields,
    }));
}

// -- Claude stdin message --

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

// -- Drain gate --

struct DrainGate {
    sent: u64,
    replayed: u64,
}

impl DrainGate {
    fn new() -> Self {
        Self { sent: 1, replayed: 0 }
    }

    fn is_drained(&self) -> bool {
        self.sent == self.replayed
    }
}

// -- Stdout event types from Claude CLI --

#[derive(Debug, Deserialize)]
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
        message: serde_json::Value,
        session_id: Option<String>,
        uuid: Option<String>,
    },
    User {
        message: serde_json::Value,
        session_id: Option<String>,
        #[serde(default)]
        #[serde(rename = "isReplay")]
        is_replay: bool,
    },
    #[serde(other)]
    Unknown,
}

// -- Transcript discovery --

fn find_transcript(projects_dir: &PathBuf, target_name: &str) -> Option<PathBuf> {
    let walker = walkdir::WalkDir::new(projects_dir)
        .max_depth(2)
        .into_iter()
        .filter_map(|e| e.ok());

    for entry in walker {
        if let Some(name) = entry.file_name().to_str() {
            if name == target_name {
                return Some(entry.into_path());
            }
        }
    }
    None
}

async fn watch_for_transcript(
    projects_dir: PathBuf,
    target_name: String,
    tx: mpsc::Sender<PathBuf>,
) {
    let (notify_tx, mut notify_rx) = mpsc::channel::<PathBuf>(16);

    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            if matches!(event.kind, EventKind::Create(_)) {
                for path in event.paths {
                    let _ = notify_tx.blocking_send(path);
                }
            }
        }
    })
    .expect("failed to create filesystem watcher");

    if watcher
        .watch(&projects_dir, RecursiveMode::Recursive)
        .is_err()
    {
        return;
    }

    if let Some(existing) = find_transcript(&projects_dir, &target_name) {
        tracing::info!(path = %existing.display(), "found transcript on scan after watch");
        let _ = tx.send(existing).await;
        return;
    }

    tracing::info!(target = %target_name, "transcript not found on scan, waiting for creation");

    let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while let Some(path) = notify_rx.recv().await {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name == target_name {
                    return Some(path);
                }
            }
        }
        None
    })
    .await;

    match result {
        Ok(Some(path)) => {
            tracing::info!(path = %path.display(), "watcher found transcript");
            let _ = tx.send(path).await;
        }
        Ok(None) => {
            tracing::error!("transcript watcher ended without finding transcript");
        }
        Err(_) => {
            tracing::error!(target_name, "timeout waiting for transcript after 15 seconds");
        }
    }
}

// -- Transcript tailer --

async fn tail_transcript(path: PathBuf, ws_tx: WsSender, stop: mpsc::Receiver<()>) -> usize {
    use tokio::fs::File;
    let mut stop = stop;
    let mut lines_emitted: usize = 0;

    loop {
        if path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let file = match File::open(&path).await {
        Ok(f) => f,
        Err(e) => {
            ws_emit_error(&ws_tx, &format!("cannot open transcript: {}", e));
            return 0;
        }
    };

    let mut reader = BufReader::new(file);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                        ws_emit(&ws_tx, "transcript", data);
                        lines_emitted += 1;
                    }
                }
            }
            Err(_) => break,
        }
    }

    let (notify_tx, mut notify_rx) = mpsc::channel::<()>(16);

    let watch_path = path.clone();
    let mut watcher = match notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            if matches!(event.kind, EventKind::Modify(_)) {
                for p in &event.paths {
                    if p == &watch_path {
                        let _ = notify_tx.blocking_send(());
                    }
                }
            }
        }
    }) {
        Ok(w) => w,
        Err(_) => return lines_emitted,
    };

    if let Some(parent) = path.parent() {
        if watcher.watch(parent, RecursiveMode::NonRecursive).is_err() {
            return lines_emitted;
        }
    }

    loop {
        tokio::select! {
            Some(()) = notify_rx.recv() => {
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim();
                            if !trimmed.is_empty() {
                                if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                    ws_emit(&ws_tx, "transcript", data);
                                    lines_emitted += 1;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            _ = stop.recv() => {
                return lines_emitted;
            }
        }
    }
}

// -- Trust injection --

fn ensure_trust(config_path: &Path, directory: &str) -> Result<(), String> {
    let lock_path = config_path.with_extension("json.lock");
    if std::fs::create_dir(&lock_path).is_err() {
        return Err("lock contention".to_string());
    }

    let result = (|| -> Result<(), String> {
        let mut config: serde_json::Value = match std::fs::read_to_string(config_path) {
            Ok(content) => serde_json::from_str(&content).map_err(|e| e.to_string())?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                serde_json::Value::Object(serde_json::Map::new())
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
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        let project = projects
            .as_object_mut()
            .ok_or("projects not an object")?
            .entry(directory)
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        project
            .as_object_mut()
            .ok_or("project entry not an object")?
            .insert(
                "hasTrustDialogAccepted".to_string(),
                serde_json::Value::Bool(true),
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

// -- Sandbox --

struct SandboxConfig {
    writable: Vec<String>,
}

fn read_sandbox_config(slug: &str) -> SandboxConfig {
    let home = std::env::var("HOME").unwrap_or_default();
    let path = std::path::Path::new(&home)
        .join(".local/state/puzzle")
        .join(slug)
        .join("sandbox.conf");

    let mut writable = Vec::new();

    if let Ok(content) = std::fs::read_to_string(&path) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(path) = line.strip_prefix("writable ") {
                let expanded = path.trim().replace("~/", &format!("{}/", home));
                writable.push(expanded);
            }
        }
    }

    SandboxConfig { writable }
}

#[cfg(target_os = "macos")]
fn build_sandbox_command(command: &str, config: &SandboxConfig) -> tokio::process::Command {
    let mut policy = String::new();
    policy.push_str("(version 1)\n");
    policy.push_str("(deny default)\n");
    policy.push_str("(allow process-exec)\n");
    policy.push_str("(allow process-fork)\n");
    policy.push_str("(allow signal (target same-sandbox))\n");
    policy.push_str("(allow process-info* (target same-sandbox))\n");
    policy.push_str("(allow file-read*)\n");
    for path in &config.writable {
        policy.push_str(&format!("(allow file-write* (subpath \"{}\"))\n", path));
    }
    policy.push_str("(allow file-write* (subpath \"/tmp\"))\n");
    policy.push_str("(allow file-write* (subpath \"/private/tmp\"))\n");
    policy.push_str(&format!(
        "(allow file-write* (subpath \"{}\"))\n",
        std::env::temp_dir().display()
    ));
    policy.push_str("(allow file-write-data (require-all (path \"/dev/null\") (vnode-type CHARACTER-DEVICE)))\n");
    policy.push_str("(allow pseudo-tty)\n");
    policy.push_str("(allow file-read* file-write* file-ioctl (literal \"/dev/ptmx\"))\n");
    policy.push_str("(allow file-read* file-write* (regex #\"^/dev/ttys[0-9]+\"))\n");
    policy.push_str("(allow file-ioctl (regex #\"^/dev/ttys[0-9]+\"))\n");
    policy.push_str("(allow sysctl-read)\n");
    policy.push_str("(allow mach-lookup)\n");
    policy.push_str("(allow network-outbound)\n");
    policy.push_str("(allow network-inbound)\n");
    policy.push_str("(allow system-socket)\n");
    policy.push_str("(allow ipc-posix-sem)\n");
    policy.push_str("(allow ipc-posix-shm-read*)\n");
    policy.push_str("(allow user-preference-read)\n");

    let mut cmd = tokio::process::Command::new("/usr/bin/sandbox-exec");
    cmd.arg("-p").arg(&policy).arg("--").arg("zsh").arg("-c").arg(command);
    cmd
}

#[cfg(target_os = "linux")]
fn build_sandbox_command(command: &str, config: &SandboxConfig) -> tokio::process::Command {
    let mut args = vec![
        "--new-session".to_string(),
        "--die-with-parent".to_string(),
        "--ro-bind".to_string(), "/".to_string(), "/".to_string(),
        "--dev".to_string(), "/dev".to_string(),
        "--proc".to_string(), "/proc".to_string(),
        "--tmpfs".to_string(), "/tmp".to_string(),
        "--unshare-pid".to_string(),
    ];
    for path in &config.writable {
        args.push("--bind".to_string());
        args.push(path.clone());
        args.push(path.clone());
    }
    args.push("--".to_string());
    args.push("zsh".to_string());
    args.push("-c".to_string());
    args.push(command.to_string());

    let mut cmd = tokio::process::Command::new("bwrap");
    cmd.args(&args);
    cmd
}

// -- Zsh execution --

async fn handle_zsh(ws_tx: &WsSender, slug: &str, data: serde_json::Value) {
    let command = data.get("command").and_then(|c| c.as_str()).unwrap_or("");
    let sandboxed = data.get("sandboxed").and_then(|v| v.as_bool()).unwrap_or(true);
    tracing::info!(command = %command, sandboxed, "zsh exec");
    ws_emit_log(ws_tx, "info", "zsh exec", serde_json::json!({
        "command": command, "sandboxed": sandboxed,
    }));

    let output = if sandboxed {
        let config = read_sandbox_config(slug);
        let mut cmd = build_sandbox_command(command, &config);
        cmd.output().await
    } else {
        tracing::warn!(command = %command, "running unsandboxed");
        let mut cmd = tokio::process::Command::new("zsh");
        cmd.arg("-c").arg(command);
        cmd.output().await
    };

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            let combined = if stderr.is_empty() {
                stdout.to_string()
            } else {
                format!("{}{}", stdout, stderr)
            };
            let code = out.status.code().unwrap_or(-1);
            ws_emit(ws_tx, "zsh_result", serde_json::json!({
                "output": combined,
                "exit_code": code,
            }));
        }
        Err(e) => {
            ws_emit(ws_tx, "zsh_result", serde_json::json!({
                "output": format!("failed to execute: {}", e),
                "exit_code": 1,
            }));
        }
    }
}

// -- Apply patch --

async fn handle_apply_patch(ws_tx: &WsSender, slug: &str, data: serde_json::Value) {
    let patch = data.get("patch").and_then(|v| v.as_str()).unwrap_or("");
    tracing::info!("apply_patch request");

    let sandbox_config = read_sandbox_config(slug);
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    match codex_apply_patch::parse_patch(patch) {
        Ok(parsed) => {
            let mut denied_path = None;
            for hunk in &parsed.hunks {
                let path = match hunk {
                    codex_apply_patch::Hunk::AddFile { path, .. } => path,
                    codex_apply_patch::Hunk::DeleteFile { path } => path,
                    codex_apply_patch::Hunk::UpdateFile { path, .. } => path,
                };
                let abs = cwd.join(path).to_string_lossy().to_string();
                let is_writable = sandbox_config.writable.iter()
                    .any(|root| abs.starts_with(root));
                if !is_writable {
                    denied_path = Some(abs);
                    break;
                }
            }

            if let Some(denied) = denied_path {
                ws_emit(ws_tx, "zsh_result", serde_json::json!({
                    "output": format!("patch denied: {} is not inside a writable root", denied),
                    "exit_code": 1,
                }));
            } else {
                let mut stdout_buf = Vec::new();
                let mut stderr_buf = Vec::new();
                match codex_apply_patch::apply_patch(patch, &mut stdout_buf, &mut stderr_buf) {
                    Ok(()) => {
                        let output = String::from_utf8_lossy(&stdout_buf);
                        ws_emit(ws_tx, "zsh_result", serde_json::json!({
                            "output": output.trim_end(),
                            "exit_code": 0,
                        }));
                    }
                    Err(e) => {
                        let stderr_str = String::from_utf8_lossy(&stderr_buf);
                        let output = if stderr_str.is_empty() {
                            format!("patch failed: {}", e)
                        } else {
                            format!("{}\npatch failed: {}", stderr_str.trim_end(), e)
                        };
                        ws_emit(ws_tx, "zsh_result", serde_json::json!({
                            "output": output,
                            "exit_code": 1,
                        }));
                    }
                }
            }
        }
        Err(e) => {
            ws_emit(ws_tx, "zsh_result", serde_json::json!({
                "output": format!("patch parse error: {}", e),
                "exit_code": 1,
            }));
        }
    }
}

// -- Claude turn --

async fn handle_claude_turn(ws_tx: &WsSender, slug: &str, data: serde_json::Value) {
    let home = std::env::var("HOME").unwrap_or_default();
    let message = data.get("message").and_then(|v| v.as_str()).unwrap_or("");
    let yolo = data.get("yolo").and_then(|v| v.as_bool()).unwrap_or(false);
    let transcript = data.get("transcript").and_then(|v| v.as_array());

    let pane_dir = PathBuf::from(&home).join("pane").join(slug);
    let _ = std::fs::create_dir_all(&pane_dir);
    if std::env::set_current_dir(&pane_dir).is_err() {
        ws_emit_error(ws_tx, &format!("cannot cd to {}", pane_dir.display()));
        return;
    }

    let config_path = PathBuf::from(&home).join(".claude.json");
    let pane_dir_str = pane_dir.to_str().unwrap_or("");
    if let Err(e) = ensure_trust(&config_path, pane_dir_str) {
        tracing::warn!("failed to ensure trust: {}", e);
    }

    let resume_arg: String;
    let mut temp_transcript: Option<PathBuf> = None;

    if let Some(entries) = transcript {
        let tmp_path = std::env::temp_dir().join(format!("easement-{}.jsonl", slug));
        let mut content = String::new();
        for entry in entries {
            if let Ok(line) = serde_json::to_string(entry) {
                content.push_str(&line);
                content.push('\n');
            }
        }
        if std::fs::write(&tmp_path, &content).is_err() {
            ws_emit_error(ws_tx, "cannot write transcript temp file");
            return;
        }
        resume_arg = tmp_path.to_string_lossy().to_string();
        temp_transcript = Some(tmp_path);
    } else {
        ws_emit_error(ws_tx, "claude envelope must have transcript");
        return;
    }

    tracing::info!(resume_arg = %resume_arg, "spawning claude");
    ws_emit_log(ws_tx, "info", "spawning claude", serde_json::json!({
        "resume_arg": &resume_arg,
    }));

    let mut cmd = Command::new("claude");
    cmd.arg("--print")
        .arg("--input-format").arg("stream-json")
        .arg("--output-format").arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--replay-user-messages")
        .arg("--verbose")
        .arg("--model").arg("claude-opus-4-6")
        .arg("--thinking-display").arg("summarized")
        .arg("--max-thinking-tokens").arg("31999")
        .arg("--resume").arg(&resume_arg)
        .arg("--add-dir").arg(format!("{}/code", home));

    if yolo {
        cmd.arg("--dangerously-skip-permissions");
    } else {
        let mcp_config_path = std::env::temp_dir()
            .join(format!("easement-wicket-{}.json", slug));
        let mcp_config = serde_json::json!({
            "mcpServers": {
                "wicket": {
                    "type": "http",
                    "url": format!("http://localhost:6502/mcp/{}", slug)
                }
            }
        });
        if let Err(e) = std::fs::write(&mcp_config_path, mcp_config.to_string()) {
            ws_emit_error(ws_tx, &format!("cannot write mcp config: {}", e));
            return;
        }
        cmd.arg("--permission-prompt-tool").arg("mcp__wicket__wicket_approve")
            .arg("--mcp-config").arg(&mcp_config_path)
            .arg("--disallowed-tools").arg("Bash,Write,Edit");
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            ws_emit_error(ws_tx, &format!("cannot spawn claude: {}", e));
            return;
        }
    };

    let mut child_stdin = child.stdin.take().expect("stdin was piped");
    let child_stdout = child.stdout.take().expect("stdout was piped");
    let child_stderr = child.stderr.take().expect("stderr was piped");

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

    ws_emit(ws_tx, "lifecycle", serde_json::json!("round_started"));

    let kickoff = format_user_message(message);
    if child_stdin.write_all(kickoff.as_bytes()).await.is_err() {
        ws_emit_error(ws_tx, "failed to send kickoff message");
        return;
    }
    let _ = child_stdin.flush().await;

    // Read first stdout event for session ID.
    let mut stdout_reader = BufReader::new(child_stdout);
    let mut first_line = String::new();
    let session_id: String;

    match stdout_reader.read_line(&mut first_line).await {
        Ok(0) => {
            ws_emit_error(ws_tx, "claude exited without output");
            return;
        }
        Ok(_) => {
            let trimmed = first_line.trim();
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                session_id = data.get("session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if session_id.is_empty() {
                    ws_emit_error(ws_tx, "first event has no session_id");
                    return;
                }
                tracing::info!(session_id = %session_id, "captured session id");
                ws_emit(ws_tx, "meta", serde_json::json!({ "session_id": session_id }));

                // Forward the first event as stdout (for stream_event detection).
                let is_stream = data.get("type").and_then(|v| v.as_str()) == Some("stream_event");
                if is_stream {
                    if let Some(event) = data.get("event") {
                        ws_emit(ws_tx, "delta", event.clone());
                    }
                }
            } else {
                ws_emit_error(ws_tx, "first event is not valid JSON");
                return;
            }
        }
        Err(e) => {
            ws_emit_error(ws_tx, &format!("failed to read claude stdout: {}", e));
            return;
        }
    }

    if let Some(ref tmp) = temp_transcript {
        let _ = std::fs::remove_file(tmp);
    }

    // Start transcript tailer.
    let projects_dir = PathBuf::from(&home).join(".claude").join("projects");
    let (transcript_tx, mut transcript_rx) = mpsc::channel::<PathBuf>(1);
    let target_name = format!("{}.jsonl", session_id);

    let watch_target = target_name.clone();
    let watch_projects_dir = projects_dir.clone();
    tokio::spawn(async move {
        watch_for_transcript(watch_projects_dir, watch_target, transcript_tx).await;
    });

    let (tailer_stop_tx, tailer_stop_rx) = mpsc::channel::<()>(1);
    let mut tailer_stop_rx = Some(tailer_stop_rx);
    let mut tailer_started = false;
    let mut tailer_handle: Option<tokio::task::JoinHandle<usize>> = None;
    let mut transcript_path: Option<PathBuf> = None;

    // Drain gate.
    let mut gate = DrainGate::new();
    let mut round_done = false;

    // Read stdout in the current task (not spawned) so we can manage the drain gate.
    let mut line = String::new();
    loop {
        tokio::select! {
            Some(path) = transcript_rx.recv(), if !tailer_started => {
                ws_emit(ws_tx, "meta", serde_json::json!({
                    "transcript_path": path.to_string_lossy()
                }));
                transcript_path = Some(path.clone());
                let stop_rx = tailer_stop_rx.take().unwrap();
                let tailer_ws = ws_tx.clone();
                tailer_handle = Some(tokio::spawn(async move {
                    tail_transcript(path, tailer_ws, stop_rx).await
                }));
                tailer_started = true;
            }
            result = stdout_reader.read_line(&mut line) => {
                match result {
                    Ok(0) => break,
                    Ok(_) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                let event_type = data.get("type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");

                                let is_stream = event_type == "stream_event";
                                if is_stream {
                                    if let Some(event) = data.get("event") {
                                        ws_emit(ws_tx, "delta", event.clone());
                                    }
                                } else if let Ok(event) = serde_json::from_value::<StdoutEvent>(data.clone()) {
                                    match &event {
                                        StdoutEvent::Assistant { uuid, .. } => {
                                            if let Some(uuid) = uuid {
                                                ws_emit(ws_tx, "boundary", serde_json::json!({ "uuid": uuid }));
                                            }
                                        }
                                        StdoutEvent::User { is_replay: true, .. } => {
                                            gate.replayed += 1;
                                        }
                                        StdoutEvent::Result { .. } => {
                                            if let Some(usage) = data.get("usage") {
                                                ws_emit(ws_tx, "usage", usage.clone());
                                            }
                                            if gate.is_drained() {
                                                round_done = true;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        line.clear();
                    }
                    Err(_) => break,
                }
            }
        }
        if round_done {
            break;
        }
    }

    // Close stdin so Claude exits cleanly.
    drop(child_stdin);

    // Wait for claude to exit.
    let status = child.wait().await;
    match status {
        Ok(s) => {
            let code = s.code().unwrap_or(-1);
            tracing::info!(exit_code = code, "claude exited");
            ws_emit_log(ws_tx, "info", "claude exited", serde_json::json!({
                "exit_code": code,
            }));
            ws_emit(ws_tx, "meta", serde_json::json!({ "exit_code": code }));
        }
        Err(e) => {
            tracing::error!(error = %e, "error waiting for claude");
            ws_emit_error(ws_tx, &format!("error waiting for claude: {}", e));
        }
    }

    // Stop tailer and read remainder.
    let _ = tailer_stop_tx.send(()).await;
    let lines_emitted = match tailer_handle {
        Some(handle) => handle.await.unwrap_or(0),
        None => 0,
    };

    if let Some(ref path) = transcript_path {
        if let Ok(file) = std::fs::File::open(path) {
            use std::io::BufRead;
            let mut remainder: usize = 0;
            for line in std::io::BufReader::new(file)
                .lines()
                .skip(lines_emitted)
                .flatten()
            {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                        ws_emit(ws_tx, "transcript", data);
                        remainder += 1;
                    }
                }
            }
            tracing::info!(remainder, "final transcript lines emitted");
        }
    }

    ws_emit(ws_tx, "lifecycle", serde_json::json!("round_completed"));
    tracing::info!("round completed");
}

// -- Main --

#[tokio::main]
async fn main() {
    let _guard = init_tracing();

    let home = match std::env::var("HOME") {
        Ok(h) => h,
        Err(_) => {
            eprintln!("HOME not set");
            std::process::exit(1);
        }
    };

    // Read bootstrap from stdin: one line of JSON with slug and timestamp.
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut bootstrap_line = String::new();
    match reader.read_line(&mut bootstrap_line).await {
        Ok(0) => std::process::exit(0),
        Ok(_) => {}
        Err(_) => {
            eprintln!("failed to read bootstrap from stdin");
            std::process::exit(1);
        }
    }
    let bootstrap: serde_json::Value = match serde_json::from_str(bootstrap_line.trim()) {
        Ok(v) => v,
        Err(_) => {
            // Backward compat: plain slug string.
            serde_json::json!({ "slug": bootstrap_line.trim() })
        }
    };
    let slug = bootstrap.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let timestamp = bootstrap.get("timestamp").and_then(|v| v.as_str()).map(|s| s.to_string());
    if slug.is_empty() {
        eprintln!("empty slug");
        std::process::exit(1);
    }

    tracing::info!(slug = %slug, timestamp = ?timestamp, "easement starting");

    // Set up working directory.
    let pane_dir = PathBuf::from(&home).join("pane").join(&slug);
    let _ = std::fs::create_dir_all(&pane_dir);

    // Connect to Wicket.
    let wicket_url = "ws://localhost:6502";
    let (ws_stream, _) = match tokio_tungstenite::connect_async(wicket_url).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot connect to wicket");
            eprintln!("cannot connect to wicket: {}", e);
            std::process::exit(1);
        }
    };

    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Send connect payload.
    let connect = serde_json::json!({
        "slug": slug,
        "protocol": "easement",
        "timestamp": timestamp
    });
    if ws_sink.send(Message::text(connect.to_string())).await.is_err() {
        tracing::error!("failed to send connect payload");
        std::process::exit(1);
    }

    tracing::info!("connected to wicket");

    // Outbound channel: handlers send envelopes here, writer task drains to WebSocket.
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<String>();

    // Writer task.
    tokio::spawn(async move {
        while let Some(msg) = ws_rx.recv().await {
            if ws_sink.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
        let _ = ws_sink.close().await;
    });

    // Envelope loop.
    let slug_owned = slug.clone();
    while let Some(result) = ws_stream.next().await {
        match result {
            Ok(Message::Text(text)) => {
                let envelope: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "bad envelope from wicket");
                        continue;
                    }
                };
                let stream = envelope.get("stream").and_then(|v| v.as_str()).unwrap_or("");
                let data = envelope.get("data").cloned().unwrap_or_default();

                match stream {
                    "claude" => {
                        handle_claude_turn(&ws_tx, &slug_owned, data).await;
                    }
                    "zsh" => {
                        handle_zsh(&ws_tx, &slug_owned, data).await;
                    }
                    "apply_patch" => {
                        handle_apply_patch(&ws_tx, &slug_owned, data).await;
                    }
                    "shutdown" => {
                        tracing::info!("shutdown requested");
                        break;
                    }
                    other => {
                        tracing::debug!(stream = %other, "ignoring unknown envelope");
                    }
                }
            }
            Ok(Message::Close(_)) => {
                tracing::info!("wicket closed connection");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "websocket read error");
                break;
            }
            _ => {}
        }
    }

    tracing::info!("easement shutting down");
}
