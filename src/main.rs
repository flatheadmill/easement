// Easement: WebSocket bus and HTTP MCP gateway on port 6502.
//
// Clients such as Wicket and Shotgun connect over WebSocket and advertise tool manifests.
// Interactive Claude Code connects to /mcp/<slug>; Easement routes tool calls onto the bus
// and returns the claimed result.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::OnceLock;

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
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

// NDJSON log. One channel, one file, queryable with jq. The broadcast channel sheds load
// automatically and reports how many messages were dropped via RecvError::Lagged(n).

#[derive(Clone, Serialize)]
struct LogRecord {
    who: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    whom: Option<String>,
    what: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    r#where: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    why: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    how: Option<String>,
    noise: u8,
    r#with: Value,
}

#[derive(Serialize)]
struct LogEntry {
    when: String,
    who: &'static str,
    what: LogRecord,
}

static LOG: OnceLock<broadcast::Sender<LogRecord>> = OnceLock::new();

fn log(record: LogRecord) {
    if let Some(tx) = LOG.get() {
        let _ = tx.send(record);
    }
}

macro_rules! log_fields {
    ($record:ident, $with:ident,) => {};
    ($record:ident, $with:ident, whom: $value:expr, $($rest:tt)*) => {
        $record.whom = Some(($value).to_string());
        log_fields!($record, $with, $($rest)*);
    };
    ($record:ident, $with:ident, where: $value:expr, $($rest:tt)*) => {
        $record.r#where = Some(($value).to_string());
        log_fields!($record, $with, $($rest)*);
    };
    ($record:ident, $with:ident, why: $value:expr, $($rest:tt)*) => {
        $record.why = Some(($value).to_string());
        log_fields!($record, $with, $($rest)*);
    };
    ($record:ident, $with:ident, how: $value:expr, $($rest:tt)*) => {
        $record.how = Some(($value).to_string());
        log_fields!($record, $with, $($rest)*);
    };
    ($record:ident, $with:ident, $key:ident: $value:expr, $($rest:tt)*) => {
        $with.insert(stringify!($key).to_string(), serde_json::json!($value));
        log_fields!($record, $with, $($rest)*);
    };
}

macro_rules! log_record {
    ($noise:expr, $who:expr, $what:expr $(, $key:tt: $value:expr)* $(,)?) => {{
        let mut record = LogRecord {
            who: ($who).to_string(),
            whom: None,
            what: ($what).to_string(),
            r#where: None,
            why: None,
            how: None,
            noise: $noise,
            r#with: Value::Null,
        };
        #[allow(unused_mut)]
        let mut details = serde_json::Map::new();
        log_fields!(record, details, $($key: $value,)*);
        record.r#with = Value::Object(details);
        record
    }};
}

macro_rules! trace {
    ($who:expr, $what:expr $(, $key:ident: $value:expr)* $(,)?) => {
        crate::log(log_record!(0, $who, $what $(, $key: $value)*))
    };
}

macro_rules! error {
    ($who:expr, $what:expr, $error:expr $(, $key:ident: $value:expr)* $(,)?) => {
        crate::log(log_record!(0, $who, $what, why: $error.to_string() $(, $key: $value)*))
    };
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn fatal(message: impl std::fmt::Display) -> ! {
    eprintln!("fatal: {}", message);
    std::process::abort();
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

    let (tx, _) = broadcast::channel::<LogRecord>(4096);
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
                        who: "easement",
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
                        who: "easement",
                        what: log_record!(0, "log", "shed",
                            why: "the log writer fell behind its channel",
                            count: n,
                        ),
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

struct ToolResult {
    output: String,
    exit_code: i32,
    changes: Option<Value>,
}

#[derive(Serialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Dispatch {
    Tool {
        slug: String,
        #[serde(flatten)]
        event: ToolDispatch,
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

fn dispatch(tx: &mpsc::UnboundedSender<String>, msg: Dispatch) {
    if let Ok(json) = serde_json::to_string(&msg) {
        let _ = tx.send(json);
    }
}

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
    let json = serde_json::to_string(&resp)
        .unwrap_or_else(|e| fatal(format!("json response serialization failed: {}", e)));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(json)))
        .unwrap_or_else(|e| fatal(format!("json response build failed: {}", e)))
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
        trace!("wicket", "spawn",
            whom: "wicket",
            where: host,
            how: "local",
        );
        let mut c = tokio::process::Command::new("wicket");
        c.arg(&wicket_url).arg(host);
        c
    } else if let Some(gcp) = parse_gcp_internal_host(host) {
        trace!(
            "wicket",
            "spawn",
            whom: "wicket",
            where: host,
            how: "gcloud",
            instance: gcp.instance,
            zone: gcp.zone,
            project: gcp.project,
        );
        let mut c = tokio::process::Command::new("gcloud");
        c.arg("compute")
            .arg("ssh")
            .arg(&gcp.instance)
            .arg(format!("--project={}", gcp.project))
            .arg(format!("--zone={}", gcp.zone))
            .arg(format!("--ssh-flag=-R {}:localhost:{}", port, port))
            .arg("--ssh-flag=-A")
            .arg(format!(
                "--command=PATH=\"$HOME/.local/bin:$PATH\" exec wicket {} {}",
                wicket_url, host
            ));
        c
    } else {
        trace!("wicket", "spawn",
            whom: "wicket",
            where: host,
            how: "ssh",
        );
        let mut c = tokio::process::Command::new("ssh");
        // Forward the ssh agent so the remote Wicket can authenticate onward
        // (git pushes, further ssh hops) with the operator's keys.
        c.arg("-A");
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
    trace!("mcp", "request",
        how: "http",
        slug: slug,
        transcript: transcript,
        method: req.method().to_string(),
    );
    if req.method() != hyper::Method::POST {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::new()))
            .unwrap_or_else(|e| fatal(format!("method-not-allowed response build failed: {}", e)));
    }

    let body = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("failed to read body")))
                .unwrap_or_else(|e| fatal(format!("bad-request response build failed: {}", e)));
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

            trace!("mcp", "call",
                how: "http",
                tool: params.name,
                arguments: params.arguments,
            );

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
                let text = serde_json::to_string(&text).unwrap_or_else(|e| {
                    fatal(format!("approval response serialization failed: {}", e))
                });
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
                    tool: f.clone(),
                    args: json!({ "who": who, "f": f, "args": args }),
                    reply: reply_tx,
                }),
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
                error!("websocket", "fail_upgrade", e,
                    whom: "client",
                    where: peer,
                    how: "websocket",
                );
                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from(format!("upgrade error: {}", e))))
                    .unwrap_or_else(|e| {
                        fatal(format!("websocket upgrade response build failed: {}", e))
                    }))
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
                .unwrap_or_else(|e| fatal(format!("missing-slug response build failed: {}", e))))
        } else {
            let slug = parts[0].to_string();
            let transcript = parts.get(1).map(|s| s.to_string());
            Ok(handle_mcp(req, &slug, transcript.as_deref(), main_tx).await)
        }
    } else if path == "/health" {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("ok")))
            .unwrap_or_else(|e| fatal(format!("health response build failed: {}", e))))
    } else {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("not found")))
            .unwrap_or_else(|e| fatal(format!("not-found response build failed: {}", e))))
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
                        let Some(top) = heap.peek() else {
                            fatal("timer heap missing after push");
                        };
                        sleep.as_mut().reset(top.when);
                    }
                }
                () = &mut sleep => {
                    let now = tokio::time::Instant::now();
                    while let Some(top) = heap.peek() {
                        if top.when > now {
                            break;
                        }
                        let Some(deadline) = heap.pop() else {
                            fatal("timer heap missing after peek");
                        };
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

// Collapse the tool manifests from every connected socket into one discovery
// listing, deduplicated by (who, f). Several Wickets on different hosts each
// advertise the same `who: "wicket"` manifest; the caller selects the host with
// the `where` argument to `call`, so a capability should appear once here rather
// than once per connected instance. Dedup on (who, f) rather than collapsing by
// who alone so that two clients sharing a name but differing in capabilities
// would still surface the union, each function listed a single time.
fn collect_tools<'a>(toolsets: impl Iterator<Item = &'a ToolSet>) -> Vec<Value> {
    let mut seen: HashSet<(&str, &str)> = HashSet::new();
    let mut out = Vec::new();
    for ts in toolsets {
        for t in &ts.tools {
            if seen.insert((ts.who.as_str(), t.f.as_str())) {
                out.push(json!({ "who": ts.who, "f": t.f, "description": t.description }));
            }
        }
    }
    out
}

#[derive(serde::Deserialize)]
#[serde(tag = "what", rename_all = "snake_case")]
enum Packet {
    Tool(ToolPacket),
    Socket(SocketPacket),
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
    Response {
        call_id: String,
        output: String,
        #[serde(default)]
        exit_code: i32,
        #[serde(default)]
        changes: Option<Value>,
    },
    BackgroundOutput {
        job_id: String,
        output_path: String,
        line: String,
    },
    Notification {
        slug: String,
        #[serde(default)]
        transcript: String,
        message: String,
        #[serde(default)]
        meta: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_entries_use_the_record_envelope_grammar() {
        let render = |record| {
            serde_json::to_string(&LogEntry {
                when: "2026-07-16T12:00:00.000Z".to_string(),
                who: "easement",
                what: record,
            })
            .unwrap()
        };

        let dispatch = render(log_record!(0, "tool", "dispatch",
            whom: "wicket",
            where: "localhost",
            why: "the tool call matched a connected client",
            how: "websocket",
            call_id: "call-1",
            client_id: 7,
            f: "zsh",
        ));
        let spawn = render(log_record!(0, "wicket", "spawn",
            whom: "wicket",
            where: "yolo@orb",
            how: "ssh",
        ));
        let connect = render(log_record!(0, "websocket", "connect",
            whom: "wicket",
            where: "localhost",
            why: "the client advertised its tool manifest",
            how: "websocket",
            client_id: 7,
            tools: 8,
        ));

        assert_eq!(
            dispatch,
            r#"{"when":"2026-07-16T12:00:00.000Z","who":"easement","what":{"who":"tool","whom":"wicket","what":"dispatch","where":"localhost","why":"the tool call matched a connected client","how":"websocket","noise":0,"with":{"call_id":"call-1","client_id":7,"f":"zsh"}}}"#
        );
        assert_eq!(
            spawn,
            r#"{"when":"2026-07-16T12:00:00.000Z","who":"easement","what":{"who":"wicket","whom":"wicket","what":"spawn","where":"yolo@orb","how":"ssh","noise":0,"with":{}}}"#
        );
        assert_eq!(
            connect,
            r#"{"when":"2026-07-16T12:00:00.000Z","who":"easement","what":{"who":"websocket","whom":"wicket","what":"connect","where":"localhost","why":"the client advertised its tool manifest","how":"websocket","noise":0,"with":{"client_id":7,"tools":8}}}"#
        );

        for line in [dispatch, spawn, connect] {
            println!("{line}");
        }
    }

    #[test]
    fn dispatch_tool_omits_transcript() {
        let msg = Dispatch::Tool {
            slug: "puzzle".to_string(),
            event: ToolDispatch::Run {
                call_id: "call-1".to_string(),
                args: json!({ "f": "zsh", "where": "localhost" }),
            },
        };
        let value = serde_json::to_value(msg).unwrap();

        assert_eq!(value.get("what").and_then(|v| v.as_str()), Some("tool"));
        assert_eq!(value.get("why").and_then(|v| v.as_str()), Some("run"));
        assert_eq!(value.get("slug").and_then(|v| v.as_str()), Some("puzzle"));
        assert!(value.get("transcript").is_none());
    }

    #[test]
    fn notification_decodes_with_and_without_transcript() {
        let with_transcript = json!({
            "what": "tool",
            "why": "notification",
            "slug": "puzzle",
            "transcript": "",
            "message": "Background job exited."
        });
        let without_transcript = json!({
            "what": "tool",
            "why": "notification",
            "slug": "puzzle",
            "message": "Background job exited."
        });

        let with: Packet = serde_json::from_value(with_transcript).unwrap();
        let without: Packet = serde_json::from_value(without_transcript).unwrap();

        match with {
            Packet::Tool(ToolPacket::Notification { transcript, .. }) => {
                assert_eq!(transcript, "");
            }
            _ => panic!("expected tool notification"),
        }
        match without {
            Packet::Tool(ToolPacket::Notification { transcript, .. }) => {
                assert_eq!(transcript, "");
            }
            _ => panic!("expected tool notification"),
        }
    }
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
        tool: String,
        args: Value,
        reply: oneshot::Sender<ToolResult>,
    },
    ToolCallEnsured {
        call_id: String,
        slug: String,
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
    ToolCallTimeout {
        call_id: String,
    },
    WicketSpawnTimeout {
        host: String,
    },
}

#[tokio::main]
async fn main() {
    init_log();

    let port = easement_port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            trace!("lifecycle", "start",
                where: addr,
                how: "tcp",
                port: port,
            );
            l
        }
        Err(e) => {
            fatal(format!("failed to bind {}: {}", addr, e));
        }
    };

    let (main_tx, mut main_rx) = mpsc::unbounded_channel::<MainEvent>();
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
        args: Value,
    }
    struct ToolCall {
        claim: ToolClaim,
        client_id: u64,
    }
    let shutdown = false;
    let mut tool_claims: HashMap<String, ToolClaim> = HashMap::new();
    let mut tool_calls: HashMap<String, ToolCall> = HashMap::new();
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
                        error!("lifecycle", "fail_accept", e,
                            whom: "client",
                            where: addr,
                            how: "tcp",
                        );
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
                        error!("websocket", "fail_connection", e,
                            whom: "client",
                            where: peer,
                            how: "http",
                        );
                    }
                });
            }
            Some(event) = main_rx.recv() => {
                match event {
                    MainEvent::Listen { ws, peer } => {
                        let ws_stream = match ws.await {
                            Ok(s) => s,
                            Err(e) => {
                                error!("websocket", "fail_upgrade", e,
                                    whom: "client",
                                    where: peer,
                                    how: "websocket",
                                );
                                continue;
                            }
                        };

                        let client_id = next_client_id;
                        next_client_id += 1;
                        trace!("websocket", "connect",
                            whom: "client",
                            where: peer,
                            how: "websocket",
                            client_id: client_id,
                        );

                        let (mut sink, mut stream) = ws_stream.split();

                        // Writer task: socket_rx -> WebSocket.
                        let (socket_tx, mut socket_rx) = mpsc::unbounded_channel::<String>();
                        sockets.insert(client_id, Socket { tx: socket_tx, who: None, r#where: "localhost".to_string(), tools: None });

                        tokio::spawn(async move {
                            while let Some(msg) = socket_rx.recv().await {
                                if sink.send(Message::text(msg)).await.is_err() {
                                    break;
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
                                                error!("websocket", "reject", e,
                                                    whom: "client",
                                                    where: peer,
                                                    how: "json",
                                                    client_id: client_id,
                                                );
                                            }
                                        }
                                    }
                                    Ok(Message::Close(_)) => break,
                                    Err(e) => {
                                        error!("websocket", "fail_stream", e,
                                            whom: "client",
                                            where: peer,
                                            how: "websocket",
                                            client_id: client_id,
                                        );
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            trace!("websocket", "exit",
                                whom: "client",
                                where: peer,
                                how: "websocket",
                                client_id: client_id,
                            );
                            let _ = main_tx.send(MainEvent::Disconnected { client_id });
                        });
                    }
                    MainEvent::ToolCheck { event } => {
                        if shutdown {
                            // Shutdown is not currently settable. If it becomes real,
                            // handle every event that can be stashed behind ToolCheck,
                            // including ToolCallEnsured drained after a Wicket reconnect;
                            // otherwise this branch will panic during shutdown cleanup.
                            match *event {
                                MainEvent::ToolCall { reply, .. } => {
                                    let _ = reply.send(ToolResult {
                                        output: "[meta] MCP tools and assistant harness shutting down, please end your turn".to_string(),
                                        exit_code: 1,
                                        changes: None,
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
                                let tools = collect_tools(sockets.values().filter_map(|s| s.tools.as_ref()));
                                let _ = reply.send(serde_json::to_string_pretty(&tools).unwrap_or_else(|_| "[]".to_string()));
                            }
                            Some(wicket @ Wicket { state: WicketState::Starting, .. }) => {
                                wicket.stashed.push(MainEvent::ToolsQuery { reply });
                            }
                            None => {
                                match spawn_wicket(&host) {
                                    Ok(mut child) => {
                                        trace!("wicket", "spawn",
                                            whom: "wicket",
                                            where: host,
                                            why: "tool discovery found no wicket process",
                                        );
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
                                        error!("wicket", "fail_spawn", e,
                                            whom: "wicket",
                                            where: host,
                                        );
                                        let tools = collect_tools(sockets.values().filter_map(|s| s.tools.as_ref()));
                                        let _ = reply.send(serde_json::to_string_pretty(&tools).unwrap_or_else(|_| "[]".to_string()));
                                    }
                                }
                            }
                        }
                    }
                    MainEvent::ToolCall { call_id, slug, tool, args, reply } => {
                        let who = args.get("who").and_then(|v| v.as_str()).unwrap_or("");

                        if who != "wicket" {
                            let _ = main_tx.send(MainEvent::ToolCallEnsured {
                                call_id, slug, tool, args, reply,
                            });
                            continue;
                        }

                        let r#where = match args.get("args").and_then(|a| a.get("where")).and_then(|v| v.as_str()) {
                            Some(w) => w.to_string(),
                            None => {
                                let _ = reply.send(ToolResult {
                                    output: "wicket tool call missing required where argument".to_string(),
                                    exit_code: 1,
                                    changes: None,
                                });
                                continue;
                            }
                        };
                        let ensured = MainEvent::ToolCallEnsured {
                            call_id: call_id.clone(), slug: slug.clone(), tool, args, reply,
                        };

                        match wickets.get_mut(&r#where) {
                            Some(Wicket { state: WicketState::Connected { .. }, .. }) => {
                                let _ = main_tx.send(ensured);
                            }
                            Some(wicket @ Wicket { state: WicketState::Starting, .. }) => {
                                trace!("tool", "stash",
                                    whom: "wicket",
                                    where: r#where,
                                    why: "the wicket process is starting",
                                    call_id: call_id,
                                );
                                wicket.stashed.push(ensured);
                            }
                            Some(Wicket { state: WicketState::Disconnected, .. }) => {
                                if let MainEvent::ToolCallEnsured { reply, .. } = ensured {
                                    let _ = reply.send(ToolResult {
                                        output: format!("wicket on {} disconnected", r#where),
                                        exit_code: 1,
                                        changes: None,
                                    });
                                }
                            }
                            None => {
                                match spawn_wicket(&r#where) {
                                    Ok(mut child) => {
                                        trace!("wicket", "spawn",
                                            whom: "wicket",
                                            where: r#where,
                                            why: "a tool call found no wicket process",
                                        );
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
                                        error!("wicket", "fail_spawn", e,
                                            whom: "wicket",
                                            where: r#where,
                                        );
                                        if let MainEvent::ToolCallEnsured { reply, .. } = ensured {
                                            let _ = reply.send(ToolResult {
                                                output: format!("failed to spawn wicket on {}: {}", r#where, e),
                                                exit_code: 1,
                                                changes: None,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    MainEvent::ToolCallEnsured { call_id, slug, tool, args, reply } => {
                        trace!("tool", "ensure",
                            call_id: call_id,
                            slug: slug,
                            tool: tool,
                        );

                        // The tool path now dispatches directly. The retired print engine
                        // used to receive queued steers before tools ran; CCCLI owns the
                        // conversation now, so Easement only inserts the claim and steers it.
                        tool_claims.insert(call_id.clone(), ToolClaim { reply, slug, args });
                        let _ = main_tx.send(MainEvent::ToolSteered { call_id });
                    }
                    MainEvent::ToolSteered { call_id } => {
                        trace!("tool", "steer",
                            call_id: call_id,
                        );
                        let claim = match tool_claims.remove(&call_id) {
                            Some(c) => c,
                            None => {
                                trace!("tool", "reject_steer",
                                    why: "no pending tool claim matched the steer",
                                    call_id: call_id,
                                );
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

                        let socket = sockets.iter()
                            .find(|(_, s)| s.who.as_deref() == Some(who) && s.r#where == r#where)
                            .map(|(client_id, s)| (*client_id, &s.tx));

                        match socket {
                            Some((client_id, tx)) => {
                                trace!("tool", "dispatch",
                                    whom: who,
                                    where: r#where,
                                    why: "the tool call matched a connected client",
                                    how: "websocket",
                                    call_id: call_id,
                                    client_id: client_id,
                                    f: f,
                                );
                                let mut flat_args = inner_args.as_object().cloned().unwrap_or_default();
                                flat_args.insert("f".to_string(), json!(f));
                                dispatch(tx, Dispatch::Tool {
                                    slug: claim.slug.clone(),
                                    event: ToolDispatch::Run {
                                        call_id: call_id.clone(),
                                        args: Value::Object(flat_args),
                                    },
                                });
                                tool_calls.insert(call_id.clone(), ToolCall { claim, client_id });
                                let _ = timer_tx.send(Delayed {
                                    ms: 86_400_000,
                                    event: MainEvent::ToolCallTimeout { call_id },
                                });
                            }
                            None => {
                                trace!("tool", "reject",
                                    whom: who,
                                    where: r#where,
                                    why: "no connected socket matched the tool target",
                                    call_id: call_id,
                                );
                                let _ = claim.reply.send(ToolResult {
                                    output: format!("no connected client for who={} where={}", who, r#where),
                                    exit_code: 1,
                                    changes: None,
                                });
                            }
                        }
                    }
                    // Tool runs are directed to one socket by (who, where). Current clients
                    // answer with a response for that call_id; the old broadcast-and-claim
                    // lifecycle is retired.
                    MainEvent::Packet { client_id, data } => {
                        match serde_json::from_value::<Packet>(data) {
                            Ok(Packet::Tool(ToolPacket::Response { call_id, output, exit_code, changes })) => {
                                if let Some(call) = tool_calls.remove(&call_id) {
                                    let claim = call.claim;
                                    trace!("tool", "respond",
                                        client_id: client_id,
                                        call_id: call_id,
                                        exit_code: exit_code,
                                    );
                                    if changes.is_some() {
                                        trace!("tool", "ignore_changes",
                                            why: "patch-change processing is retired",
                                            call_id: call_id,
                                        );
                                    }
                                    let _ = claim.reply.send(ToolResult { output, exit_code, changes });
                                } else {
                                    trace!("tool", "reject_response",
                                        why: "no in-flight tool call matched the response",
                                        client_id: client_id,
                                        call_id: call_id,
                                    );
                                }
                            }
                            Ok(Packet::Tool(ToolPacket::BackgroundOutput { job_id, output_path, line })) => {
                                trace!("tool", "receive_output",
                                    client_id: client_id,
                                    job_id: job_id,
                                    output_path: output_path,
                                    line: line,
                                );
                            }
                            Ok(Packet::Tool(ToolPacket::Notification {
                                slug,
                                transcript,
                                message,
                                meta,
                            })) => {
                                trace!(
                                    "tool",
                                    "notify",
                                    client_id: client_id,
                                    slug: slug,
                                    transcript: transcript,
                                    message: message,
                                    has_meta: meta.is_some(),
                                );
                            }
                            Ok(Packet::Socket(SocketPacket::Connect { who, r#where, tools })) => {
                                let toolset = ToolSet { who: who.clone(), tools };
                                let resolved_where = r#where.clone().unwrap_or_else(|| "localhost".to_string());
                                trace!("websocket", "connect",
                                    whom: who,
                                    where: resolved_where,
                                    why: "the client advertised its tool manifest",
                                    how: "websocket",
                                    client_id: client_id,
                                    tools: toolset.tools.len(),
                                );
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
                                        trace!("wicket", "drain",
                                            whom: "wicket",
                                            where: r#where,
                                            why: "the wicket connected with work stashed",
                                            stashed: drained.len(),
                                        );
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
                                error!("websocket", "reject", e,
                                    whom: "client",
                                    how: "json",
                                    client_id: client_id,
                                );
                            }
                        }
                    }
                    MainEvent::Disconnected { client_id } => {
                        let disconnected_where = sockets
                            .remove(&client_id)
                            .map(|socket| socket.r#where)
                            .unwrap_or_else(|| "unknown".to_string());
                        for wicket in wickets.values_mut() {
                            if matches!(wicket.state, WicketState::Connected { client_id: cid } if cid == client_id) {
                                trace!("wicket", "disconnect",
                                    whom: "wicket",
                                    where: disconnected_where,
                                    why: "the websocket disconnected",
                                    how: "websocket",
                                    client_id: client_id,
                                );
                                wicket.state = WicketState::Disconnected;
                            }
                        }
                        // A dispatched call belongs to the socket it was sent to. Once that
                        // socket hangs up, the command may have completed, partially run, or
                        // never started; Easement cannot know. Do not replay it on reconnect.
                        // Fail it honestly and let the model/operator decide whether retrying is
                        // safe.
                        let hung_up: Vec<String> = tool_calls.iter()
                            .filter_map(|(call_id, call)| {
                                (call.client_id == client_id).then(|| call_id.clone())
                            })
                            .collect();
                        for call_id in hung_up {
                            if let Some(call) = tool_calls.remove(&call_id) {
                                let claim = call.claim;
                                let who = claim.args.get("who")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown");
                                let r#where = claim.args.get("args")
                                    .and_then(|a| a.get("where"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or(&disconnected_where);
                                let output = if who == "wicket" {
                                    format!(
                                        "wicket on {} hung up while running tool call; command may or may not have completed",
                                        r#where
                                    )
                                } else {
                                    format!(
                                        "client who={} where={} hung up while running tool call; operation may or may not have completed",
                                        who, r#where
                                    )
                                };
                                trace!("websocket", "hang_up",
                                    whom: who,
                                    where: r#where,
                                    why: "the socket closed while a tool call was in flight",
                                    how: "websocket",
                                    client_id: client_id,
                                    call_id: call_id,
                                );
                                let _ = claim.reply.send(ToolResult {
                                    output,
                                    exit_code: 1,
                                    changes: None,
                                });
                            }
                        }
                        trace!("websocket", "disconnect",
                            whom: "client",
                            where: disconnected_where,
                            how: "websocket",
                            client_id: client_id,
                        );
                    }
                    MainEvent::WicketExited { host, exit_code } => {
                        trace!("wicket", "exit",
                            whom: "wicket",
                            where: host,
                            how: "process",
                            exit_code: exit_code,
                        );
                        if let Some(wicket) = wickets.remove(&host) {
                            for event in wicket.stashed {
                                let _ = main_tx.send(MainEvent::ToolCheck { event: Box::new(event) });
                            }
                        }
                    }
                    MainEvent::ToolCallTimeout { call_id } => {
                        if let Some(call) = tool_calls.remove(&call_id) {
                            let claim = call.claim;
                            trace!("tool", "timeout",
                                why: "the tool call did not respond before its deadline",
                                call_id: call_id,
                            );
                            let _ = claim.reply.send(ToolResult {
                                output: "tool call timed out".to_string(),
                                exit_code: 1,
                                changes: None,
                            });
                        }
                    }
                    MainEvent::WicketSpawnTimeout { host } => {
                        if let Some(wicket) = wickets.get_mut(&host) &&
                             matches!(wicket.state, WicketState::Starting) {
                            trace!("wicket", "timeout_spawn",
                                whom: "wicket",
                                where: host,
                                why: "the wicket did not connect before its deadline",
                            );
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
