// Easement: a lightweight binary that wraps the Claude CLI for local or remote
// execution. Receives a JSON payload on stdin, spawns claude with the right
// flags, multiplexes output streams (claude stdout, transcript, errors) into
// typed JSON envelopes on stdout, and passes messages from stdin through to
// claude. Runs identically whether invoked locally or over SSH.
//
// The payload is one line of JSON. Everything after that line is the NDJSON
// message stream from Puzzle, passed through to claude's stdin.
//
// Output is NDJSON envelopes: {"stream":"stdout","data":{...}} for claude
// events, {"stream":"transcript","data":{...}} for JSONL entries,
// {"stream":"meta","data":{...}} for session info, {"stream":"error",...}
// for failures.

use std::path::PathBuf;
use std::process::Stdio;

use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

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
// When claude resumes from a file path, it forks into a new session and writes
// the transcript under ~/.claude/projects/<cwd-slug>/<uuid>.jsonl. We don't
// know the path until the CLI creates it. The notify crate watches for file
// creation in the projects directory. When a file matching the session ID
// appears, we have the transcript path.

async fn watch_for_transcript(
    projects_dir: PathBuf,
    session_id: String,
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

    // Watch the projects directory recursively — the transcript may land in a
    // subdirectory we haven't seen yet.
    if watcher
        .watch(&projects_dir, RecursiveMode::Recursive)
        .is_err()
    {
        return;
    }

    let target = format!("{}.jsonl", session_id);
    let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while let Some(path) = notify_rx.recv().await {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name == target {
                    return Some(path);
                }
            }
        }
        None
    })
    .await;

    match result {
        Ok(Some(path)) => {
            let _ = tx.send(path).await;
        }
        Ok(None) => {
            emit_error("transcript watcher ended without finding transcript");
        }
        Err(_) => {
            emit_error("timeout waiting for transcript after 15 seconds");
        }
    }
}

// -- Transcript tailer --
//
// Once we know the transcript path, tail it. Read from the current position,
// emit each line as a transcript envelope. Watch for modifications with notify
// and read new content when it arrives.

async fn tail_transcript(path: PathBuf, stop: mpsc::Receiver<()>) {
    use tokio::fs::File;
    let mut stop = stop;

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
            return;
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
        Err(_) => return,
    };

    if let Some(parent) = path.parent() {
        if watcher.watch(parent, RecursiveMode::NonRecursive).is_err() {
            return;
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
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            _ = stop.recv() => {
                // Final read to flush anything remaining.
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            let trimmed = line.trim();
                            if !trimmed.is_empty() {
                                if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                    emit("transcript", data);
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                return;
            }
        }
    }
}

#[tokio::main]
async fn main() {
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

    if reader.read_line(&mut payload_line).await.is_err() {
        emit_error("failed to read payload from stdin");
        std::process::exit(1);
    }

    let payload: Payload = match serde_json::from_str(payload_line.trim()) {
        Ok(p) => p,
        Err(e) => {
            emit_error(&format!("invalid payload: {}", e));
            std::process::exit(1);
        }
    };

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
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            emit_error(&format!("cannot spawn claude: {}", e));
            std::process::exit(1);
        }
    };

    let mut child_stdin = child.stdin.take().expect("stdin was piped");
    let child_stdout = child.stdout.take().expect("stdout was piped");

    // Send the kickoff message.
    let kickoff = format_user_message(&payload.message);
    if child_stdin.write_all(kickoff.as_bytes()).await.is_err() {
        emit_error("failed to send kickoff message");
        std::process::exit(1);
    }
    let _ = child_stdin.flush().await;

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

    // Start watching for the transcript file.
    let projects_dir = PathBuf::from(&home).join(".claude").join("projects");
    let (transcript_tx, mut transcript_rx) = mpsc::channel::<PathBuf>(1);
    let watch_session_id = session_id.clone();

    tokio::spawn(async move {
        watch_for_transcript(projects_dir, watch_session_id, transcript_tx).await;
    });

    // Start the tailer once we discover the transcript path. The stop channel
    // lets us tell the tailer to flush and exit when claude is done.
    let (tailer_stop_tx, tailer_stop_rx) = mpsc::channel::<()>(1);
    let mut tailer_stop_rx = Some(tailer_stop_rx);
    let mut tailer_started = false;

    // Spawn a task to read claude's stdout and emit envelopes.
    let (stdout_done_tx, mut stdout_done_rx) = mpsc::channel::<()>(1);

    tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match stdout_reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            emit("stdout", data);
                        }
                    }
                }
                Err(_) => break,
            }
        }
        let _ = stdout_done_tx.send(()).await;
    });

    // Pass stdin through to claude. Everything after the payload line is the
    // message stream from Puzzle. When Puzzle closes its end, cat exits,
    // child_stdin drops, claude sees EOF.
    let mut _stdin_done = false;

    tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if child_stdin.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                    let _ = child_stdin.flush().await;
                }
                Err(_) => break,
            }
        }
        // child_stdin drops here, closing claude's stdin.
    });

    // Main loop: wait for transcript discovery, stdout completion, or child exit.
    loop {
        tokio::select! {
            Some(path) = transcript_rx.recv(), if !tailer_started => {
                emit_meta(serde_json::json!({
                    "transcript_path": path.to_string_lossy()
                }));
                let stop_rx = tailer_stop_rx.take().unwrap();
                tokio::spawn(async move {
                    tail_transcript(path, stop_rx).await;
                });
                tailer_started = true;
            }
            Some(()) = stdout_done_rx.recv() => {
                _stdin_done = true;
            }
            status = child.wait() => {
                match status {
                    Ok(s) => {
                        emit_meta(serde_json::json!({
                            "exit_code": s.code().unwrap_or(-1)
                        }));
                    }
                    Err(e) => {
                        emit_error(&format!("error waiting for claude: {}", e));
                    }
                }
                // Tell the tailer to flush and stop.
                let _ = tailer_stop_tx.send(()).await;
                // Give the tailer a moment to flush.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                break;
            }
        }
    }
}
