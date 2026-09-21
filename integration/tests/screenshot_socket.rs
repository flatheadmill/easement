//! Real Easement HTTP/WebSocket pump with a simulated Shotgun and the actual
//! Wicket worker. All sockets and state belong to this disposable test instance.
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use wicket::screenshot_wire::{Completion, Request, Worker};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
struct Server {
    child: Child,
    home: PathBuf,
    port: u16,
}
impl Server {
    async fn start() -> Self {
        let home =
            std::env::temp_dir().join(format!("easement-socket-test-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(home.join("pane/test"))
            .await
            .unwrap();
        let home = tokio::fs::canonicalize(home).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let child = Command::new(
            std::env::var_os("EASEMENT_TEST_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/easement")
                }),
        )
        .env("HOME", &home)
        .env("EASEMENT_PORT", port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
        let server = Self { child, home, port };
        for _ in 0..100 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("isolated Easement did not listen");
    }
    async fn connect(&self, who: &str) -> Socket {
        let (mut socket, _) = connect_async(format!("ws://127.0.0.1:{}/", self.port))
            .await
            .unwrap();
        send(&mut socket, json!({ "what": "socket", "why": "connect", "who": who, "where": "localhost", "tools": [] })).await;
        socket
    }
    async fn logs(&self) -> String {
        tokio::fs::read_to_string(self.home.join(format!(
            ".local/state/easement/easement-{}.jsonl",
            self.port
        )))
        .await
        .unwrap_or_default()
    }
    async fn registered(&self, count: usize) {
        for _ in 0..100 {
            let seen = self
                .logs()
                .await
                .lines()
                .filter_map(|s| serde_json::from_str::<Value>(s).ok())
                .filter(|v| {
                    v["what"]["who"] == "websocket"
                        && v["what"]["what"] == "connect"
                        && v["what"]["with"].get("tools").is_some()
                })
                .count();
            if seen >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("socket registration did not reach pump");
    }
    fn call(&self, f: &str, args: Value) -> tokio::task::JoinHandle<Value> {
        let port = self.port;
        let f = f.to_owned();
        tokio::spawn(async move {
            let body = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "call", "arguments": { "who": "shotgun", "f": f, "args": args } } }).to_string();
            let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let request = format!(
                "POST /mcp/test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: \
                 application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut output = Vec::new();
            stream.read_to_end(&mut output).await.unwrap();
            let output = String::from_utf8(output).unwrap();
            serde_json::from_str(output.split_once("\r\n\r\n").unwrap().1).unwrap()
        })
    }
    async fn stop(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
        tokio::fs::remove_dir_all(&self.home).await.unwrap();
    }
}
async fn send(socket: &mut Socket, value: Value) {
    socket.send(Message::text(value.to_string())).await.unwrap();
}
async fn recv(socket: &mut Socket) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).unwrap();
        }
    }
}
fn source(call: &Value, why: &str, mut fields: Value) -> Value {
    fields["what"] = json!("screenshot_save");
    fields["why"] = json!(why);
    fields["call_id"] = call["call_id"].clone();
    fields["operation_id"] = call["operation_id"].clone();
    fields["attempt_id"] = json!("attempt-1");
    fields
}
async fn bridge(
    writer: &mut Socket,
    worker: &Worker,
    rx: &mut mpsc::Receiver<Completion>,
    drop_reply: bool,
) -> Value {
    let command = recv(writer).await;
    assert!(
        worker
            .submit(Request::parse(&command.to_string()).unwrap())
            .is_ok()
    );
    let completion = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    for record in completion.observations {
        send(
            writer,
            json!({ "what": "log", "why": "write", "whom": "wicket", "with": record }),
        )
        .await;
    }
    if !drop_reply {
        send(writer, completion.packet.clone()).await;
    }
    completion.packet
}

#[tokio::test]
async fn websocket_route_preserves_ownership_receipts_credit_and_quiet_logs() {
    let server = Server::start().await;
    let mut shotgun = server.connect("shotgun").await;
    let mut writer = server.connect("wicket").await;
    server.registered(2).await;
    // The old wrong-socket bug must be fixed in the actual ordinary tool path.
    let mut ordinary = server.call("noop", json!({}));
    let call = recv(&mut shotgun).await;
    send(&mut writer, json!({ "what": "tool", "why": "response", "call_id": call["call_id"], "output": "forged", "exit_code": 0 })).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut ordinary)
            .await
            .is_err()
    );
    let broad = "ordinary image-result-sized output ".repeat(6000);
    send(&mut shotgun, json!({ "what": "tool", "why": "response", "call_id": call["call_id"], "output": broad, "exit_code": 0 })).await;
    let response = tokio::time::timeout(Duration::from_secs(10), ordinary)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response["result"]["content"][0]["text"], broad);

    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 1, 1);
        encoder.set_color(png::ColorType::Rgba);
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&[42, 17, 88, 255]).unwrap();
        writer.finish().unwrap();
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let (tx, mut rx) = mpsc::channel(8);
    let worker = Worker::start(server.home.clone(), tx);
    for lost_reply in [false, true] {
        let op = if lost_reply { "lost-reply" } else { "saved" };
        let path = server.home.join(format!("pane/test/{op}.png"));
        let manifest = json!({ "destination": path, "bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes)), "mode": "viewport", "source_url": "https://example.test/", "captured_at": "now" });
        let mcp = server.call(
            "screenshot_save",
            json!({ "operation_id": op, "destination": path }),
        );
        let call = recv(&mut shotgun).await;
        for (why, fields) in [
            ("begin", json!({ "where": "localhost", "intent": manifest })),
            ("chunk", json!({ "offset": 0, "data": encoded })),
            ("finish", json!({})),
        ] {
            send(&mut shotgun, source(&call, why, fields)).await;
            let response =
                bridge(&mut writer, &worker, &mut rx, lost_reply && why == "finish").await;
            if lost_reply && why == "finish" {
                assert_eq!(response["why"], "stored");
                writer.close(None).await.unwrap();
                assert_eq!(recv(&mut shotgun).await["why"], "unresolved");
            } else {
                let response = recv(&mut shotgun).await;
                assert_eq!(
                    response["why"],
                    match why {
                        "begin" => "ready",
                        "chunk" => "ack",
                        _ => "stored",
                    }
                );
                if why == "finish" {
                    send(&mut shotgun, json!({ "what": "tool", "why": "response", "call_id": call["call_id"], "output": response.to_string(), "exit_code": 0 })).await;
                }
            }
        }
        let response = tokio::time::timeout(Duration::from_secs(10), mcp)
            .await
            .unwrap()
            .unwrap();
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains(if lost_reply { "unresolved" } else { "stored" })
        );
        assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
    }
    writer = server.connect("wicket").await;
    server.registered(3).await;
    let mcp = server.call("screenshot_save", json!({ "operation_id": "lost-reply", "destination": server.home.join("pane/test/lost-reply.png") }));
    let call = recv(&mut shotgun).await;
    send(
        &mut shotgun,
        source(&call, "status", json!({ "where": "localhost" })),
    )
    .await;
    bridge(&mut writer, &worker, &mut rx, false).await;
    let saved = recv(&mut shotgun).await;
    assert_eq!(saved["why"], "stored");
    assert_ne!(saved["receipt"]["attempt"]["call_id"], call["call_id"]);
    send(&mut shotgun, json!({ "what": "tool", "why": "response", "call_id": call["call_id"], "output": saved.to_string(), "exit_code": 0 })).await;
    tokio::time::timeout(Duration::from_secs(10), mcp)
        .await
        .unwrap()
        .unwrap();
    // Withhold the first ack and send a second chunk. The real ingress/pump
    // must quarantine the source and fence its temporary receive, not buffer
    // further chunks or describe the timeout as an artifact publication failure.
    let path = server.home.join("pane/test/credit.png");
    let mcp = server.call(
        "screenshot_save",
        json!({ "operation_id": "credit", "destination": path }),
    );
    let call = recv(&mut shotgun).await;
    let manifest = json!({ "destination": path, "bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes)), "mode": "viewport", "source_url": "https://example.test/", "captured_at": "now" });
    send(
        &mut shotgun,
        source(
            &call,
            "begin",
            json!({ "where": "localhost", "intent": manifest }),
        ),
    )
    .await;
    bridge(&mut writer, &worker, &mut rx, false).await;
    assert_eq!(recv(&mut shotgun).await["why"], "ready");
    send(&mut shotgun, source(&call, "chunk", json!({ "offset": 0, "data": base64::engine::general_purpose::STANDARD.encode(&bytes[..4]) }))).await;
    assert_eq!(
        bridge(&mut writer, &worker, &mut rx, true).await["why"],
        "ack"
    );
    send(&mut shotgun, source(&call, "chunk", json!({ "offset": 4, "data": base64::engine::general_purpose::STANDARD.encode(&bytes[4..8]) }))).await;
    assert_eq!(
        bridge(&mut writer, &worker, &mut rx, false).await["why"],
        "aborted"
    );
    let rejection = tokio::time::timeout(Duration::from_secs(10), mcp)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rejection["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("rejected")
    );
    assert!(!path.exists());
    let log = server.logs().await;
    assert!(!log.contains(&encoded));
    let facts: Vec<Value> = log
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(
        facts
            .iter()
            .filter(|v| v["who"] == "wicket" && v["what"]["what"] == "store")
            .count(),
        2
    );
    assert!(
        !facts
            .iter()
            .any(|v| v["what"]["what"] == "chunk" || v["what"]["what"] == "ack")
    );
    rx.close();
    worker.shutdown().await;
    server.stop().await;
}
