// Easement: a lightweight binary that wraps the Claude CLI for local or remote
// execution. Receives a JSON payload on stdin, spawns claude with the right
// flags, multiplexes output streams (claude stdout, transcript, errors) into
// typed JSON envelopes on stdout, and passes messages from stdin through to
// claude. Runs identically whether invoked locally or over SSH.
//
// The payload is one line of JSON. Everything after that line is the NDJSON
// message stream from Wicket, passed through to claude's stdin.
//
// Output is NDJSON envelopes: {"stream":"stdout","data":{...}} for claude
// events, {"stream":"transcript","data":{...}} for JSONL entries,
// {"stream":"meta","data":{...}} for session info, {"stream":"error",...}
// for failures.
//
// Approval is handled by Wicket at http://localhost:6502/mcp/<slug>.
// Easement writes the MCP config pointing Claude to that endpoint.
// On remote machines, ssh -R 6502:localhost:6502 tunnels the port.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
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

// -- Payload --

#[derive(Debug, Deserialize)]
struct Payload {
    slug: String,
    #[serde(default)]
    yolo: bool,
    message: String,
    session_id: Option<String>,
    transcript: Option<Vec<serde_json::Value>>,
}

// -- Output envelopes --

#[derive(Debug, Serialize)]
struct Envelope {
    stream: &'static str,
    data: serde_json::Value,
}

fn emit(stream: &'static str, data: serde_json::Value) {
    if let Ok(line) = serde_json::to_string(&Envelope { stream, data }) {
        // Ignoring write errors — if stdout is broken, we're done anyway.
        let _ = std::io::Write::write_all(&mut std::io::stdout().lock(), line.as_bytes());
        let _ = std::io::Write::write_all(&mut std::io::stdout().lock(), b"\n");
    }
}

fn emit_error(message: &str) {
    emit("error", serde_json::json!({ "message": message }));
}

fn emit_log(level: &str, message: &str, fields: serde_json::Value) {
    emit("log", serde_json::json!({
        "level": level,
        "message": message,
        "fields": fields,
    }));
}

fn emit_meta(data: serde_json::Value) {
    emit("meta", data);
}

// -- Stdin message for claude --

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

// -- Transcript discovery --
//
// Two paths: for session-id resumes the transcript file already exists
// under ~/.claude/projects/<cwd-slug>/<session-id>.jsonl. We search
// the projects directory for it. For forks (resume from file path) the
// CLI creates a new file — we watch for creation with notify.

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

    // Register the watch first — any creation events from this point
    // forward will be captured.
    if watcher
        .watch(&projects_dir, RecursiveMode::Recursive)
        .is_err()
    {
        return;
    }

    // Now scan. If the file was created before the watcher registered,
    // this catches it. If it was created after, the watcher has it.
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
            emit_error("transcript watcher ended without finding transcript");
        }
        Err(_) => {
            tracing::error!(target_name, "timeout waiting for transcript after 15 seconds");
            emit_error("timeout waiting for transcript after 15 seconds");
        }
    }
}

// -- Transcript tailer --
//
// Once we know the transcript path, tail it. Read from the current position,
// emit each line as a transcript envelope. Watch for modifications with notify
// and read new content when it arrives. Returns the number of lines emitted so
// the caller can do a final deterministic read after claude exits.

async fn tail_transcript(path: PathBuf, stop: mpsc::Receiver<()>) -> usize {
    use tokio::fs::File;
    let mut stop = stop;
    let mut lines_emitted: usize = 0;

    // Wait for the file to exist.
    loop {
        if path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let file = match File::open(&path).await {
        Ok(f) => f,
        Err(e) => {
            emit_error(&format!("cannot open transcript: {}", e));
            return 0;
        }
    };

    let mut reader = BufReader::new(file);
    let mut line = String::new();

    // Read existing content first.
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                        emit("transcript", data);
                        lines_emitted += 1;
                    }
                }
            }
            Err(_) => break,
        }
    }

    // Now tail: wait for modifications, read new lines.
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
                                    emit("transcript", data);
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
//
// The Claude CLI requires hasTrustDialogAccepted in ~/.claude.json for
// each working directory. Puzzle handles this locally; Easement handles
// it on remote machines where Puzzle can't reach the config.

fn ensure_trust(config_path: &Path, directory: &str) -> Result<(), String> {
    // Acquire mkdir-based lock matching the CLI's proper-lockfile protocol.
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

        // Check if trust is already set.
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

// MCP approval is now handled by Wicket directly at
// http://localhost:6502/mcp/<slug>. Easement just writes the MCP config
// pointing Claude to that endpoint.

// -- Sandbox --
//
// Reads ~/.local/state/puzzle/<slug>/sandbox.conf and builds a
// platform-specific sandbox command. On macOS, sandbox-exec with an
// SBPL policy. On Linux, bwrap with bind mounts.

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
    // Build the SBPL policy.
    let mut policy = String::new();
    policy.push_str("(version 1)\n");
    policy.push_str("(deny default)\n");

    // Process management.
    policy.push_str("(allow process-exec)\n");
    policy.push_str("(allow process-fork)\n");
    policy.push_str("(allow signal (target same-sandbox))\n");
    policy.push_str("(allow process-info* (target same-sandbox))\n");

    // Read access to everything.
    policy.push_str("(allow file-read*)\n");

    // Write access to writable roots.
    for path in &config.writable {
        policy.push_str(&format!("(allow file-write* (subpath \"{}\"))\n", path));
    }

    // /dev/null, /tmp, and temp dirs.
    policy.push_str("(allow file-write* (subpath \"/tmp\"))\n");
    policy.push_str("(allow file-write* (subpath \"/private/tmp\"))\n");
    policy.push_str(&format!(
        "(allow file-write* (subpath \"{}\"))\n",
        std::env::temp_dir().display()
    ));
    policy.push_str("(allow file-write-data (require-all (path \"/dev/null\") (vnode-type CHARACTER-DEVICE)))\n");

    // PTY support.
    policy.push_str("(allow pseudo-tty)\n");
    policy.push_str("(allow file-read* file-write* file-ioctl (literal \"/dev/ptmx\"))\n");
    policy.push_str("(allow file-read* file-write* (regex #\"^/dev/ttys[0-9]+\"))\n");
    policy.push_str("(allow file-ioctl (regex #\"^/dev/ttys[0-9]+\"))\n");

    // Sysctls for basic operation.
    policy.push_str("(allow sysctl-read)\n");

    // Mach services for basic operation.
    policy.push_str("(allow mach-lookup)\n");

    // Network: allow all (for now — Wicket is on localhost:6502, Claude API is remote).
    policy.push_str("(allow network-outbound)\n");
    policy.push_str("(allow network-inbound)\n");
    policy.push_str("(allow system-socket)\n");

    // IPC.
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

#[tokio::main]
async fn main() {
    let _guard = init_tracing();

    let home = match std::env::var("HOME") {
        Ok(h) => h,
        Err(_) => {
            emit_error("HOME not set");
            std::process::exit(1);
        }
    };

    // Read the payload — one line of JSON from stdin.
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut payload_line = String::new();

    let payload_bytes = reader.read_line(&mut payload_line).await;
    match payload_bytes {
        Ok(0) => std::process::exit(0),
        Ok(_) => {}
        Err(_) => {
            emit_error("failed to read payload from stdin");
            std::process::exit(1);
        }
    }

    let payload: Payload = match serde_json::from_str(payload_line.trim()) {
        Ok(p) => p,
        Err(e) => {
            emit_error(&format!("invalid payload: {}", e));
            std::process::exit(1);
        }
    };

    tracing::info!(
        slug = %payload.slug,
        yolo = payload.yolo,
        has_session_id = payload.session_id.is_some(),
        has_transcript = payload.transcript.is_some(),
        "payload received"
    );
    emit_log("info", "payload received", serde_json::json!({
        "slug": &payload.slug,
        "yolo": payload.yolo,
        "has_session_id": payload.session_id.is_some(),
    }));

    // Set up the working directory.
    let pane_dir = PathBuf::from(&home).join("pane").join(&payload.slug);
    if std::fs::create_dir_all(&pane_dir).is_err() {
        emit_error(&format!("cannot create {}", pane_dir.display()));
        std::process::exit(1);
    }
    if std::env::set_current_dir(&pane_dir).is_err() {
        emit_error(&format!("cannot cd to {}", pane_dir.display()));
        std::process::exit(1);
    }

    // Ensure trust for the pane directory so the CLI skips its approval
    // dialog. Same protocol as Puzzle's config.rs — locked read-modify-write
    // of ~/.claude.json with mkdir-based locking.
    let config_path = PathBuf::from(&home).join(".claude.json");
    let pane_dir_str = pane_dir.to_str().unwrap_or("");
    if let Err(e) = ensure_trust(&config_path, pane_dir_str) {
        tracing::warn!("failed to ensure trust: {}", e);
    }

    // Determine the resume target.
    let resume_arg: String;
    let mut temp_transcript: Option<PathBuf> = None;

    if let Some(sid) = &payload.session_id {
        resume_arg = sid.clone();
    } else if let Some(ref entries) = payload.transcript {
        // Write transcript entries to a temp file, one per line.
        let tmp_path = std::env::temp_dir()
            .join(format!("easement-{}.jsonl", payload.slug));
        let mut content = String::new();
        for entry in entries {
            if let Ok(line) = serde_json::to_string(entry) {
                content.push_str(&line);
                content.push('\n');
            }
        }
        if std::fs::write(&tmp_path, &content).is_err() {
            emit_error("cannot write transcript temp file");
            std::process::exit(1);
        }
        resume_arg = tmp_path.to_string_lossy().to_string();
        temp_transcript = Some(tmp_path);
    } else {
        emit_error("payload must have session_id or transcript");
        std::process::exit(1);
    }

    tracing::info!(resume_arg = %resume_arg, "spawning claude");
    emit_log("info", "spawning claude", serde_json::json!({
        "resume_arg": &resume_arg,
    }));

    // Build the command.
    let mut cmd = Command::new("claude");
    cmd.arg("--print")
        .arg("--input-format").arg("stream-json")
        .arg("--output-format").arg("stream-json")
        .arg("--replay-user-messages")
        .arg("--verbose")
        .arg("--max-thinking-tokens").arg("31999")
        .arg("--resume").arg(&resume_arg)
        .arg("--add-dir").arg(format!("{}/code", home));

    if payload.yolo {
        cmd.arg("--dangerously-skip-permissions");
    } else {
        // Write the MCP config pointing Claude to Wicket's HTTP endpoint.
        // Wicket serves approval requests at /mcp/<slug>. On remote machines,
        // ssh -R 6502:localhost:6502 tunnels the port back to the Mac.
        let mcp_config_path = std::env::temp_dir()
            .join(format!("easement-wicket-{}.json", payload.slug));
        let mcp_config = serde_json::json!({
            "mcpServers": {
                "wicket": {
                    "type": "http",
                    "url": format!("http://localhost:6502/mcp/{}", payload.slug)
                }
            }
        });
        if let Err(e) = std::fs::write(&mcp_config_path, mcp_config.to_string()) {
            emit_error(&format!("cannot write mcp config: {}", e));
            std::process::exit(1);
        }
        tracing::info!(path = %mcp_config_path.display(), "wrote mcp config");

        cmd.arg("--permission-prompt-tool").arg("mcp__wicket__wicket_approve")
            .arg("--mcp-config").arg(&mcp_config_path)
            .arg("--disallowed-tools").arg("Bash,Write,Edit");
    }

    // Capture stderr so MCP initialization errors and other diagnostics
    // are visible in the log rather than swallowed.
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            emit_error(&format!("cannot spawn claude: {}", e));
            std::process::exit(1);
        }
    };

    let mut child_stdin: Option<tokio::process::ChildStdin> = Some(child.stdin.take().expect("stdin was piped"));
    let child_stdout = child.stdout.take().expect("stdout was piped");
    let child_stderr = child.stderr.take().expect("stderr was piped");

    // Log stderr lines so MCP errors and Claude diagnostics are visible.
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

    // Send the kickoff message.
    let kickoff = format_user_message(&payload.message);
    if let Some(ref mut stdin) = child_stdin {
        if stdin.write_all(kickoff.as_bytes()).await.is_err() {
            emit_error("failed to send kickoff message");
            std::process::exit(1);
        }
        let _ = stdin.flush().await;
    }

    // Read the first stdout event to capture the session ID.
    let mut stdout_reader = BufReader::new(child_stdout);
    let mut first_line = String::new();
    let session_id: String;

    match stdout_reader.read_line(&mut first_line).await {
        Ok(0) => {
            emit_error("claude exited without output");
            std::process::exit(1);
        }
        Ok(_) => {
            let trimmed = first_line.trim();
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                session_id = data
                    .get("session_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                if session_id.is_empty() {
                    emit_error("first event has no session_id");
                    std::process::exit(1);
                }

                tracing::info!(session_id = %session_id, "captured session id from first event");
                emit_log("info", "session id captured", serde_json::json!({
                    "session_id": &session_id,
                }));
                emit_meta(serde_json::json!({ "session_id": session_id }));
                emit("stdout", data);
            } else {
                emit_error("first event is not valid JSON");
                std::process::exit(1);
            }
        }
        Err(e) => {
            emit_error(&format!("failed to read claude stdout: {}", e));
            std::process::exit(1);
        }
    }

    // Clean up the temp transcript file — claude has already read it.
    if let Some(ref tmp) = temp_transcript {
        let _ = std::fs::remove_file(tmp);
    }

    // Find or watch for the transcript file. For session-id resumes,
    // Claude writes to the original session file — use the payload's
    // session ID. For forks (transcript payload), a new file is created
    // under the stdout session ID.
    let projects_dir = PathBuf::from(&home).join(".claude").join("projects");
    let (transcript_tx, mut transcript_rx) = mpsc::channel::<PathBuf>(1);
    let target_name = if let Some(ref sid) = payload.session_id {
        tracing::info!(payload_sid = %sid, stdout_sid = %session_id, "using payload session id for transcript");
        format!("{}.jsonl", sid)
    } else {
        tracing::info!(stdout_sid = %session_id, "using stdout session id for transcript (fork)");
        format!("{}.jsonl", session_id)
    };

    // Start the watcher first, then scan. If the file was created before
    // the watcher registered, the scan catches it. If it's created after,
    // the watcher catches it.
    let watch_target = target_name.clone();
    let watch_projects_dir = projects_dir.clone();
    tokio::spawn(async move {
        watch_for_transcript(watch_projects_dir, watch_target, transcript_tx).await;
    });

    // Start the tailer once we discover the transcript path. The stop channel
    // tells the tailer to stop watching. After claude exits we join the handle
    // to get the line count, then do a final deterministic read of the file.
    let (tailer_stop_tx, tailer_stop_rx) = mpsc::channel::<()>(1);
    let mut tailer_stop_rx = Some(tailer_stop_rx);
    let mut tailer_started = false;
    let mut tailer_handle: Option<tokio::task::JoinHandle<usize>> = None;
    let mut transcript_path: Option<PathBuf> = None;

    // Spawn a task to read claude's stdout and emit envelopes.
    let (stdout_done_tx, mut stdout_done_rx) = mpsc::channel::<()>(1);

    tokio::spawn(async move {
        let mut line = String::new();
        let mut stdout_lines: usize = 0;
        loop {
            line.clear();
            match stdout_reader.read_line(&mut line).await {
                Ok(0) => {
                    tracing::debug!(total_lines = stdout_lines, "claude stdout EOF");
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            let event_type = data.get("type")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            tracing::debug!(event_type, "stdout event");
                            stdout_lines += 1;
                            emit("stdout", data);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "stdout read error");
                    break;
                }
            }
        }
        let _ = stdout_done_tx.send(()).await;
    });

    // Stdin passthrough state. These stay in the main loop rather than a
    // spawned task so that when child.wait() fires and the loop breaks,
    // the stdin reading stops naturally. A spawned task would block
    // runtime shutdown waiting on a synchronous stdin read from Wicket's
    // pipe, deadlocking if Wicket is waiting for us to exit.
    let mut stdin_line = String::new();

    // Main loop: wait for transcript discovery, stdout completion, stdin
    // passthrough, or child exit.
    loop {
        tokio::select! {
            Some(path) = transcript_rx.recv(), if !tailer_started => {
                emit_meta(serde_json::json!({
                    "transcript_path": path.to_string_lossy()
                }));
                transcript_path = Some(path.clone());
                let stop_rx = tailer_stop_rx.take().unwrap();
                tailer_handle = Some(tokio::spawn(async move {
                    tail_transcript(path, stop_rx).await
                }));
                tailer_started = true;
            }
            Some(()) = stdout_done_rx.recv() => {
                // Claude's stdout closed. Nothing to do — child.wait()
                // will fire next.
            }
            result = reader.read_line(&mut stdin_line), if child_stdin.is_some() => {
                match result {
                    Ok(0) => {
                        tracing::debug!("puzzle stdin EOF, closing claude stdin");
                        child_stdin.take();
                    }
                    Ok(_) => {
                        // Parse envelope to check if this is a Wicket tool request.
                        let stream = stdin_line.trim()
                            .strip_prefix('{')
                            .and_then(|_| serde_json::from_str::<serde_json::Value>(stdin_line.trim()).ok())
                            .and_then(|v| v.get("stream").and_then(|s| s.as_str()).map(|s| s.to_string()));

                        match stream.as_deref() {
                            Some("zsh") => {
                                if let Ok(env) = serde_json::from_str::<serde_json::Value>(stdin_line.trim()) {
                                    let data = env.get("data").cloned().unwrap_or_default();
                                    let command = data.get("command")
                                        .and_then(|c| c.as_str())
                                        .unwrap_or("");
                                    let sandboxed = data.get("sandboxed")
                                        .and_then(|v| v.as_bool())
                                        .unwrap_or(true);
                                    tracing::info!(command = %command, sandboxed, "zsh exec request");
                                    emit_log("info", "zsh exec", serde_json::json!({
                                        "command": command,
                                        "sandboxed": sandboxed,
                                    }));

                                    let output = if sandboxed {
                                        let sandbox_config = read_sandbox_config(&payload.slug);
                                        let mut cmd = build_sandbox_command(command, &sandbox_config);
                                        cmd.output().await
                                    } else {
                                        tracing::warn!(command = %command, "running unsandboxed (escalation approved)");
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
                                            emit("zsh_result", serde_json::json!({
                                                "output": combined,
                                                "exit_code": code,
                                            }));
                                        }
                                        Err(e) => {
                                            emit("zsh_result", serde_json::json!({
                                                "output": format!("failed to execute: {}", e),
                                                "exit_code": 1,
                                            }));
                                        }
                                    }
                                }
                            }
                            Some("apply_patch") => {
                                if let Ok(env) = serde_json::from_str::<serde_json::Value>(stdin_line.trim()) {
                                    let data = env.get("data").cloned().unwrap_or_default();
                                    let patch = data.get("patch")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("");
                                    tracing::info!("apply_patch request");

                                    // Check sandbox before applying: parse the patch
                                    // to get file paths and verify they're writable.
                                    let sandbox_config = read_sandbox_config(&payload.slug);
                                    let cwd = std::env::current_dir()
                                        .unwrap_or_else(|_| PathBuf::from("."));

                                    match codex_apply_patch::parse_patch(patch) {
                                        Ok(parsed) => {
                                            // Check all paths against sandbox config.
                                            let mut denied_path = None;
                                            for hunk in &parsed.hunks {
                                                let path = match hunk {
                                                    codex_apply_patch::Hunk::AddFile { path, .. } => path,
                                                    codex_apply_patch::Hunk::DeleteFile { path } => path,
                                                    codex_apply_patch::Hunk::UpdateFile { path, .. } => path,
                                                };
                                                let abs = cwd.join(path)
                                                    .to_string_lossy().to_string();
                                                let is_writable = sandbox_config.writable.iter()
                                                    .any(|root| abs.starts_with(root));
                                                if !is_writable {
                                                    denied_path = Some(abs);
                                                    break;
                                                }
                                            }

                                            if let Some(denied) = denied_path {
                                                emit("zsh_result", serde_json::json!({
                                                    "output": format!("patch denied: {} is not inside a writable root", denied),
                                                    "exit_code": 1,
                                                }));
                                            } else {
                                                // Apply the patch.
                                                let mut stdout_buf = Vec::new();
                                                let mut stderr_buf = Vec::new();
                                                match codex_apply_patch::apply_patch(
                                                    patch, &mut stdout_buf, &mut stderr_buf,
                                                ) {
                                                    Ok(()) => {
                                                        let output = String::from_utf8_lossy(&stdout_buf);
                                                        emit("zsh_result", serde_json::json!({
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
                                                        emit("zsh_result", serde_json::json!({
                                                            "output": output,
                                                            "exit_code": 1,
                                                        }));
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            emit("zsh_result", serde_json::json!({
                                                "output": format!("patch parse error: {}", e),
                                                "exit_code": 1,
                                            }));
                                        }
                                    }
                                }
                            }
                            _ => {
                                if let Some(ref mut stdin) = child_stdin {
                                    tracing::debug!("stdin passthrough");
                                    if stdin.write_all(stdin_line.as_bytes()).await.is_err() {
                                        tracing::warn!("stdin write to claude failed");
                                    } else {
                                        let _ = stdin.flush().await;
                                    }
                                }
                            }
                        }
                        stdin_line.clear();
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "stdin read error");
                    }
                }
            }
            status = child.wait() => {
                match status {
                    Ok(s) => {
                        let code = s.code().unwrap_or(-1);
                        tracing::info!(exit_code = code, "claude exited");
                        emit_log("info", "claude exited", serde_json::json!({
                            "exit_code": code,
                        }));
                        emit_meta(serde_json::json!({
                            "exit_code": code
                        }));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "error waiting for claude");
                        emit_error(&format!("error waiting for claude: {}", e));
                    }
                }

                // Stop the tailer and get the number of lines it already
                // emitted. The transcript is fully flushed on disk now that
                // claude has exited, so we read the remainder directly.
                let _ = tailer_stop_tx.send(()).await;
                let lines_emitted = match tailer_handle {
                    Some(handle) => handle.await.unwrap_or(0),
                    None => 0,
                };

                tracing::info!(lines_emitted, "tailer stopped, reading remainder");

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
                                    emit("transcript", data);
                                    remainder += 1;
                                }
                            }
                        }
                        tracing::info!(remainder, "final transcript lines emitted");
                    }
                } else {
                    tracing::warn!("no transcript path discovered");
                }

                // Exit immediately. The tokio runtime cannot shut down
                // cleanly because stdin is backed by a blocking thread
                // pool read that will never complete while Wicket holds
                // the pipe open. Bypassing runtime Drop is the only way
                // to avoid the deadlock.
                std::process::exit(0);
            }
        }
    }
}
