//! Narrow screenshot-save routing state, owned exclusively by the main pump.
//! Receiving is temporary. Once finish is forwarded, contact loss is unknown.
//! Deadlines live in the owner (one generation per transition), not in an
//! ever-growing queue of per-chunk timer events. No bytes enter observations.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{Value, json};

pub const MAX_PACKET_BYTES: usize = 96 * 1024;
pub const MAX_CHUNK_BYTES: u64 = 64 * 1024;
pub const MAX_PNG_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CALLS: usize = 4;
pub const INGRESS_CAPACITY: usize = 8;
pub const SOCKET_QUEUE_CAPACITY: usize = 32;
const ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);
const CHUNK_TIMEOUT: Duration = Duration::from_secs(15);
const FINISH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum Effect {
    Send { socket: u64, packet: Value },
    Complete { call_id: String, packet: Value },
    Close { socket: u64 },
    Observe(Value),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    AwaitingBegin,
    Opening,
    Ready,
    Chunk(u64),
    Querying,
    Finishing,
    Cancelling,
    Done,
}

struct Route {
    source: u64,
    writer: Option<u64>,
    slug: String,
    operation: String,
    destination: Option<String>,
    attempt: String,
    intent: Option<Value>,
    phase: Phase,
    offset: u64,
    finish_sent: bool,
    sequence: u64,
    deadline: Instant,
    result: Option<Value>,
}

#[derive(Default)]
pub struct Router {
    calls: HashMap<String, Route>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePacket {
    what: String,
    why: String,
    call_id: String,
    operation_id: String,
    attempt_id: String,
    #[serde(default)]
    r#where: Option<String>,
    #[serde(default)]
    intent: Option<Value>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    data: Option<String>,
}

impl Router {
    /// Called only when dispatching an ordinary tool call. Caller-known IDs
    /// are required before capture so a lost tool response remains queryable.
    pub fn register(
        &mut self,
        call_id: &str,
        source: u64,
        slug: &str,
        args: &Value,
        now: Instant,
    ) -> Result<(), &'static str> {
        if args["who"] != "shotgun" || args["f"] != "screenshot_save" {
            return Err("not a Shotgun screenshot_save call");
        }
        let operation = args["args"]["operation_id"]
            .as_str()
            .ok_or("screenshot_save requires a caller-known operation_id")?;
        if !valid_id(call_id) || !valid_id(slug) || !valid_id(operation) {
            return Err("invalid screenshot operation identity");
        }
        if self.calls.len() >= MAX_CALLS {
            return Err("screenshot route capacity reached");
        }
        self.calls.insert(
            call_id.into(),
            Route {
                source,
                writer: None,
                slug: slug.into(),
                operation: operation.into(),
                destination: args["args"]["destination"].as_str().map(str::to_owned),
                attempt: String::new(),
                intent: None,
                phase: Phase::AwaitingBegin,
                offset: 0,
                finish_sent: false,
                sequence: 0,
                deadline: now + ADMISSION_TIMEOUT,
                result: None,
            },
        );
        Ok(())
    }

    pub fn contains(&self, call_id: &str) -> bool {
        self.calls.contains_key(call_id)
    }
    pub fn remove(&mut self, call_id: &str) {
        self.calls.remove(call_id);
    }

    pub fn packet(
        &mut self,
        socket: u64,
        data: Value,
        local_writers: &[u64],
        now: Instant,
    ) -> Vec<Effect> {
        if serde_json::to_vec(&data).map_or(true, |s| s.len() > MAX_PACKET_BYTES) {
            return violation(socket, "screenshot packet exceeds limit");
        }
        if data.get("binding").is_some() {
            return self.writer_reply(socket, data, now);
        }
        let packet: SourcePacket = match serde_json::from_value(data) {
            Ok(p) => p,
            Err(_) => return violation(socket, "invalid screenshot source packet"),
        };
        let Some(route) = self.calls.get(&packet.call_id) else {
            // Late packets from a reaped call cannot affect a replacement on
            // this same socket. Ingress bounds still apply to every packet.
            return Vec::new();
        };
        if route.source != socket
            || route.operation != packet.operation_id
            || !valid_id(&packet.attempt_id)
            || packet.what != "screenshot_save"
        {
            return violation(socket, "screenshot source/call/operation binding differs");
        }
        let mut superseded = Vec::new();
        let mut released = 0;
        if packet.why == "begin" {
            for (id, other) in &self.calls {
                if id == &packet.call_id
                    || other.slug != route.slug
                    || other.operation != route.operation
                    || other.phase == Phase::Done
                {
                    continue;
                }
                if other.finish_sent || matches!(other.phase, Phase::Querying | Phase::Cancelling) {
                    return self.end(
                        &packet.call_id,
                        "operation has an outstanding finish/status/cancel; query status",
                        true,
                    );
                }
                if other
                    .intent
                    .as_ref()
                    .is_some_and(|intent| Some(intent) != packet.intent.as_ref())
                    || other.attempt == packet.attempt_id
                {
                    return self.end(
                        &packet.call_id,
                        "replacement conflicts with the live intent or needs a new attempt ID",
                        true,
                    );
                }
                released += other
                    .intent
                    .as_ref()
                    .and_then(|v| v["bytes"].as_u64())
                    .unwrap_or(0);
                superseded.push(id.clone());
            }
        }
        let total: u64 = self
            .calls
            .values()
            .filter(|r| r.phase != Phase::Done)
            .map(|r| {
                r.intent
                    .as_ref()
                    .and_then(|v| v["bytes"].as_u64())
                    .unwrap_or(0)
            })
            .sum();
        let route = self.calls.get_mut(&packet.call_id).expect("checked above");
        if route.phase == Phase::Done {
            return Vec::new();
        }
        if packet.why != "begin" && packet.why != "status" && packet.attempt_id != route.attempt {
            return Vec::new();
        }
        let mut fields = json!({});
        let phase = match packet.why.as_str() {
            "begin" if matches!(route.phase, Phase::AwaitingBegin | Phase::Ready) => {
                // A status observation can complete its own call, but cannot
                // stand in for the outcome of a subsequent capture attempt.
                route.result = None;
                let Some(intent) = packet.intent else {
                    return violation(socket, "begin needs a PNG manifest");
                };
                if packet.offset.is_some()
                    || packet.data.is_some()
                    || packet.r#where.as_deref() != Some("localhost")
                {
                    return violation(socket, "invalid screenshot begin fields or writer location");
                }
                if let Err(reason) = validate_intent(&intent) {
                    return self.end(&packet.call_id, reason, true);
                }
                if route.destination.is_none()
                    || route
                        .destination
                        .as_ref()
                        .is_some_and(|d| intent["destination"].as_str() != Some(d))
                {
                    return self.end(
                        &packet.call_id,
                        "destination differs from the dispatched save",
                        true,
                    );
                }
                if route.intent.as_ref().is_some_and(|old| old != &intent) {
                    return self.end(
                        &packet.call_id,
                        "operation already binds a different manifest",
                        true,
                    );
                }
                let old_size = route
                    .intent
                    .as_ref()
                    .and_then(|v| v["bytes"].as_u64())
                    .unwrap_or(0);
                if total - released - old_size
                    + intent["bytes"].as_u64().expect("validated manifest")
                    > MAX_TOTAL_BYTES
                {
                    return self.end(
                        &packet.call_id,
                        "screenshot aggregate byte limit reached",
                        true,
                    );
                }
                if route.phase == Phase::Ready && packet.attempt_id == route.attempt {
                    return violation(socket, "replacement requires a new attempt ID");
                }
                route.attempt = packet.attempt_id;
                route.intent = Some(intent.clone());
                route.offset = 0;
                fields["intent"] = intent;
                Phase::Opening
            }
            "status" if route.phase == Phase::AwaitingBegin => {
                if packet.intent.is_some()
                    || packet.offset.is_some()
                    || packet.data.is_some()
                    || packet.r#where.as_deref() != Some("localhost")
                {
                    return violation(socket, "invalid status fields");
                }
                route.attempt = packet.attempt_id;
                route.result = None;
                Phase::Querying
            }
            "chunk" if route.phase == Phase::Ready => {
                if packet.intent.is_some() || packet.r#where.is_some() {
                    return violation(socket, "invalid chunk fields");
                }
                let Some(data) = packet.data else {
                    return violation(socket, "missing chunk data");
                };
                let Ok(length) = decoded_chunk_len(&data) else {
                    return violation(socket, "invalid or oversized base64 chunk");
                };
                let end = route.offset + length;
                if packet.offset != Some(route.offset)
                    || end
                        > route.intent.as_ref().expect("ready has manifest")["bytes"]
                            .as_u64()
                            .expect("validated")
                {
                    return violation(socket, "chunk offset or manifest byte count differs");
                }
                fields["offset"] = json!(route.offset);
                fields["data"] = json!(data);
                Phase::Chunk(end)
            }
            "finish" if route.phase == Phase::Ready => {
                if packet.intent.is_some()
                    || packet.offset.is_some()
                    || packet.data.is_some()
                    || packet.r#where.is_some()
                    || route.offset
                        != route.intent.as_ref().expect("ready has manifest")["bytes"]
                            .as_u64()
                            .expect("validated")
                {
                    return violation(
                        socket,
                        "finish requires exactly the admitted bytes and no extra fields",
                    );
                }
                route.finish_sent = true;
                Phase::Finishing
            }
            "cancel"
                if matches!(
                    route.phase,
                    Phase::Opening | Phase::Ready | Phase::Chunk(_) | Phase::Finishing
                ) =>
            {
                if packet.intent.is_some()
                    || packet.offset.is_some()
                    || packet.data.is_some()
                    || packet.r#where.is_some()
                {
                    return violation(socket, "invalid cancel fields");
                }
                Phase::Cancelling
            }
            _ => {
                return violation(
                    socket,
                    "screenshot command violates phase or one-chunk credit",
                );
            }
        };
        if route.writer.is_none() {
            if local_writers.len() != 1 {
                return self.end(
                    &packet.call_id,
                    "screenshot requires exactly one connected local Wicket",
                    true,
                );
            }
            route.writer = Some(local_writers[0]);
        }
        route.phase = phase;
        advance(route, now);
        let command = command(&packet.call_id, route, &packet.why, fields);
        let writer = route.writer.expect("selected above");
        let mut effects = Vec::new();
        for id in superseded {
            let old = self.calls.get_mut(&id).expect("superseded route exists");
            let result = result_packet(
                &id,
                old,
                "rejected",
                Some("temporary receive superseded by a fresh retry"),
            );
            old.result = Some(result.clone());
            old.phase = Phase::Done;
            advance(old, now);
            // Do not cancel the operation: Wicket replaces/fences its old
            // ticket atomically when it processes the following begin.
            effects.push(Effect::Send {
                socket: old.source,
                packet: result.clone(),
            });
            effects.push(Effect::Complete {
                call_id: id.clone(),
                packet: result,
            });
            effects.push(Effect::Observe(json!({ "who": "screenshot", "what": "reject_route", "why": "temporary receive superseded by retry", "with": { "call_id": id, "replacement_call_id": packet.call_id } })));
        }
        effects.push(Effect::Send {
            socket: writer,
            packet: command,
        });
        effects
    }

    fn writer_reply(&mut self, socket: u64, data: Value, now: Instant) -> Vec<Effect> {
        let Some(call_id) = data["binding"]["attempt"]["call_id"]
            .as_str()
            .map(str::to_owned)
        else {
            return violation(socket, "writer reply lacks call binding");
        };
        let Some(route) = self.calls.get_mut(&call_id) else {
            // A reply can arrive after a route deadline/disconnect reaped it.
            // It has no authority over any new call and must not be relayed.
            return Vec::new();
        };
        if route.writer != Some(socket) {
            return violation(socket, "reply is not from the selected writer");
        }
        let Some(sequence) = data["sequence"].as_u64() else {
            return violation(socket, "writer reply lacks sequence");
        };
        if sequence < route.sequence {
            return Vec::new();
        }
        if data["what"] != "screenshot_save" || data["binding"] != binding(&call_id, route) {
            return violation(socket, "reply attempt binding differs");
        }
        if sequence != route.sequence || route.phase == Phase::Done {
            return violation(socket, "writer reply has no outstanding command");
        }
        if data.as_object().is_none_or(|m| {
            m.keys().any(|k| {
                ![
                    "what",
                    "why",
                    "binding",
                    "sequence",
                    "next_offset",
                    "receipt",
                    "reason",
                ]
                .contains(&k.as_str())
            })
        }) {
            return violation(socket, "writer reply has unexpected fields");
        }
        let Some(why) = data["why"].as_str() else {
            return violation(socket, "writer reply lacks disposition");
        };
        let mut result = result_packet(&call_id, route, why, None);
        match why {
            "ready" if route.phase == Phase::Opening && data["next_offset"] == 0 => {
                route.phase = Phase::Ready;
                result["next_offset"] = json!(0);
            }
            "ack" => {
                let Phase::Chunk(end) = route.phase else {
                    return violation(socket, "ack has no outstanding chunk");
                };
                if data["next_offset"] != end {
                    return violation(socket, "ack offset differs from forwarded chunk");
                }
                route.offset = end;
                route.phase = Phase::Ready;
                result["next_offset"] = json!(end);
            }
            "unknown" | "receiving" if route.phase == Phase::Querying => {
                route.phase = Phase::AwaitingBegin;
                if why == "receiving" {
                    let Some(offset) = data["next_offset"].as_u64().filter(|n| *n <= MAX_PNG_BYTES)
                    else {
                        return violation(socket, "invalid receiving offset");
                    };
                    result["next_offset"] = json!(offset);
                }
            }
            "stored" | "published" | "failed" | "aborted" => {
                if !matches!(
                    route.phase,
                    Phase::Opening
                        | Phase::Querying
                        | Phase::Finishing
                        | Phase::Cancelling
                        | Phase::Chunk(_)
                ) || !valid_receipt(&data["receipt"], why, route)
                {
                    return violation(
                        socket,
                        "writer terminal receipt does not match the operation",
                    );
                }
                result["receipt"] = data["receipt"].clone();
                route.phase = Phase::Done;
            }
            "rejected" | "unresolved" => {
                // A refusal is about this command. Once finish was sent it
                // cannot establish nonpublication of the operation.
                if route.finish_sent {
                    result["why"] = json!("unresolved");
                }
                result["reason"] = json!(if why == "rejected" {
                    "Wicket refused the screenshot command"
                } else {
                    "Wicket could not establish a terminal outcome; reconcile by operation ID"
                });
                route.phase = Phase::Done;
            }
            _ => return violation(socket, "writer reply violates screenshot phase"),
        }
        if route.phase == Phase::Done || matches!(why, "unknown" | "receiving") {
            route.result = Some(result.clone());
        }
        advance(route, now);
        vec![Effect::Send {
            socket: route.source,
            packet: result,
        }]
    }

    /// Ordinary tool responses carry exactly the writer packet previously
    /// relayed to Shotgun. Never pass a screenshot tool's unchecked raw output.
    pub fn tool_result(&self, call_id: &str, output: &str) -> Result<Value, &'static str> {
        let route = self.calls.get(call_id).ok_or("unknown screenshot call")?;
        let expected = route
            .result
            .as_ref()
            .ok_or("no writer outcome has been received")?;
        if output.len() > MAX_PACKET_BYTES
            || serde_json::from_str::<Value>(output).ok().as_ref() != Some(expected)
        {
            return Err("tool response differs from the writer outcome");
        }
        Ok(expected.clone())
    }

    pub fn disconnected(&mut self, socket: u64) -> Vec<Effect> {
        let calls: Vec<_> = self
            .calls
            .iter()
            .filter(|(_, r)| r.source == socket || r.writer == Some(socket))
            .map(|(id, _)| id.clone())
            .collect();
        calls
            .into_iter()
            .flat_map(|id| self.end(&id, "screenshot route endpoint disconnected", true))
            .collect()
    }

    pub fn expired(&mut self, now: Instant) -> Vec<Effect> {
        let timers: Vec<_> = self
            .calls
            .iter()
            .filter(|(_, r)| r.deadline <= now)
            .map(|(id, r)| (id.clone(), r.sequence))
            .collect();
        timers
            .into_iter()
            .flat_map(|(id, generation)| self.timeout(&id, generation, now))
            .collect()
    }

    fn timeout(&mut self, call_id: &str, generation: u64, now: Instant) -> Vec<Effect> {
        if self
            .calls
            .get(call_id)
            .is_none_or(|r| r.sequence != generation || r.deadline > now)
        {
            return Vec::new();
        }
        self.end(call_id, "screenshot route deadline elapsed", true)
    }

    pub fn end(&mut self, call_id: &str, reason: &str, relay: bool) -> Vec<Effect> {
        let Some(mut route) = self.calls.remove(call_id) else {
            return Vec::new();
        };
        let result = route.result.clone().unwrap_or_else(|| {
            result_packet(
                call_id,
                &route,
                if route.finish_sent || route.phase == Phase::Querying {
                    "unresolved"
                } else {
                    "rejected"
                },
                Some(reason),
            )
        });
        let mut effects = Vec::new();
        if let Some(writer) = route.writer
            && route.result.is_none()
            && !route.attempt.is_empty()
            && !matches!(route.phase, Phase::Querying | Phase::AwaitingBegin)
        {
            route.sequence += 1;
            effects.push(Effect::Send {
                socket: writer,
                packet: command(call_id, &route, "cancel", json!({})),
            });
        }
        effects.push(Effect::Observe(json!({ "who": "screenshot", "what": "reject_route", "why": reason,
            "how": "websocket", "with": { "call_id": call_id, "operation_id": route.operation,
            "source_socket": route.source, "writer_socket": route.writer, "disposition": result["why"] } })));
        if relay {
            effects.push(Effect::Send {
                socket: route.source,
                packet: result.clone(),
            });
        }
        effects.push(Effect::Complete {
            call_id: call_id.into(),
            packet: result,
        });
        effects
    }
}

fn advance(route: &mut Route, now: Instant) {
    route.sequence += 1;
    route.deadline = now
        + match route.phase {
            Phase::Finishing => FINISH_TIMEOUT,
            Phase::Ready | Phase::Chunk(_) | Phase::Cancelling => CHUNK_TIMEOUT,
            _ => ADMISSION_TIMEOUT,
        };
}

fn binding(call_id: &str, route: &Route) -> Value {
    json!({ "operation": { "slug": route.slug, "caller": "shotgun", "operation_id": route.operation },
        "attempt": { "call_id": call_id, "attempt_id": route.attempt }, "source_socket": route.source, "writer_socket": route.writer })
}

fn command(call_id: &str, route: &Route, why: &str, mut fields: Value) -> Value {
    fields["what"] = json!("screenshot_save");
    fields["why"] = json!(why);
    fields["binding"] = binding(call_id, route);
    fields["sequence"] = json!(route.sequence);
    fields
}

fn result_packet(call_id: &str, route: &Route, why: &str, reason: Option<&str>) -> Value {
    let mut result = json!({ "what": "screenshot_save", "why": why, "call_id": call_id,
        "operation_id": route.operation, "attempt_id": route.attempt });
    if let Some(reason) = reason {
        result["reason"] = json!(reason);
    }
    result
}

fn violation(socket: u64, reason: &str) -> Vec<Effect> {
    vec![
        Effect::Observe(
            json!({ "who": "screenshot", "what": "reject_route", "why": reason,
        "how": "websocket", "with": { "socket": socket } }),
        ),
        Effect::Close { socket },
    ]
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

fn validate_intent(intent: &Value) -> Result<(), &'static str> {
    let keys = [
        "destination",
        "bytes",
        "sha256",
        "mode",
        "source_url",
        "captured_at",
    ];
    if intent
        .as_object()
        .is_none_or(|m| m.len() != keys.len() || keys.iter().any(|k| !m.contains_key(*k)))
    {
        return Err("invalid screenshot manifest fields");
    }
    let path = intent["destination"]
        .as_str()
        .ok_or("missing destination")?;
    let hash = intent["sha256"].as_str().ok_or("missing hash")?;
    if !path.starts_with('/')
        || path.len() > 4096
        || intent["bytes"]
            .as_u64()
            .is_none_or(|n| n == 0 || n > MAX_PNG_BYTES)
        || hash.len() != 64
        || !hash
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        || !["viewport", "full_page"]
            .iter()
            .any(|m| intent["mode"] == *m)
        || intent["source_url"].as_str().is_none_or(|s| s.len() > 8192)
        || intent["captured_at"].as_str().is_none_or(|s| s.len() > 128)
        || serde_json::to_vec(intent).map_or(true, |s| s.len() > 16 * 1024)
    {
        return Err("screenshot manifest exceeds limits or is malformed");
    }
    Ok(())
}

fn valid_receipt(receipt: &Value, why: &str, route: &Route) -> bool {
    let keys = [
        "operation",
        "attempt",
        "event_id",
        "requested",
        "destination",
        "verified",
        "accepted_at",
        "observed_at",
        "outcome",
    ];
    if receipt
        .as_object()
        .is_none_or(|m| m.len() != keys.len() || keys.iter().any(|k| !m.contains_key(*k)))
        || validate_intent(&receipt["requested"]).is_err()
        || receipt["operation"]
            != json!({ "slug": route.slug, "caller": "shotgun", "operation_id": route.operation })
        || !receipt["event_id"].is_string()
        || !receipt["destination"].is_string()
        || route
            .intent
            .as_ref()
            .is_some_and(|i| receipt["requested"] != *i)
        || route
            .destination
            .as_ref()
            .is_some_and(|d| receipt["requested"]["destination"].as_str() != Some(d))
    {
        return false;
    }
    match why {
        "stored" => receipt["outcome"] == "stored" && receipt["accepted_at"].is_string(),
        "published" => {
            receipt["outcome"]["published_durability_unconfirmed"].is_object()
                && receipt["accepted_at"].is_string()
        }
        "failed" => receipt["outcome"]["failed"].is_object(),
        "aborted" => receipt["outcome"] == "aborted",
        _ => false,
    }
}

/// Validate canonical padded base64 and its decoded size without assembling or
/// allocating decoded bytes in Easement. Wicket independently decodes each chunk.
fn decoded_chunk_len(data: &str) -> Result<u64, ()> {
    let bytes = data.as_bytes();
    if bytes.is_empty()
        || !bytes.len().is_multiple_of(4)
        || bytes.len() > (MAX_CHUNK_BYTES as usize).div_ceil(3) * 4
    {
        return Err(());
    }
    let padding = bytes.iter().rev().take_while(|b| **b == b'=').count();
    if padding > 2 {
        return Err(());
    }
    let digit = |b| match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    if bytes[..bytes.len() - padding]
        .iter()
        .any(|b| digit(*b).is_none())
    {
        return Err(());
    }
    let last = digit(bytes[bytes.len() - padding - 1]).ok_or(())?;
    if padding == 1 && last & 3 != 0 || padding == 2 && last & 15 != 0 {
        return Err(());
    }
    let length = (bytes.len() / 4 * 3 - padding) as u64;
    if length > MAX_CHUNK_BYTES {
        return Err(());
    }
    Ok(length)
}

/// Inspect the envelope without materializing a possibly oversized data field.
/// Preserve tungstenite's ordinary 64 MiB message / 16 MiB frame defaults.
pub fn ingress(text: &str) -> Result<Value, &'static str> {
    #[derive(Deserialize)]
    struct Header {
        what: String,
    }
    let header: Header = serde_json::from_str(text).map_err(|_| "invalid JSON envelope")?;
    if header.what == "screenshot_save" && text.len() > MAX_PACKET_BYTES {
        return Err("screenshot packet exceeds limit");
    }
    serde_json::from_str(text).map_err(|_| "invalid JSON envelope")
}

#[cfg(test)]
mod tests;
