use super::*;

fn args(op: &str) -> Value {
    json!({ "who": "shotgun", "f": "screenshot_save", "args": { "operation_id": op, "destination": "/safe/capture.png" } })
}
fn intent(bytes: u64) -> Value {
    json!({ "destination": "/safe/capture.png", "bytes": bytes, "sha256": "0".repeat(64), "mode": "viewport", "source_url": "https://example.test/", "captured_at": "now" })
}
fn source(why: &str, mut fields: Value) -> Value {
    fields["what"] = json!("screenshot_save");
    fields["why"] = json!(why);
    fields["call_id"] = json!("call-1");
    fields["operation_id"] = json!("op-1");
    fields["attempt_id"] = json!("attempt-1");
    fields
}
fn writer(command: &Value, why: &str, mut fields: Value) -> Value {
    fields["what"] = json!("screenshot_save");
    fields["why"] = json!(why);
    fields["binding"] = command["binding"].clone();
    fields["sequence"] = command["sequence"].clone();
    fields
}
fn sent(effects: Vec<Effect>, socket: u64) -> Value {
    effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send { socket: s, packet } if s == socket => Some(packet),
            _ => None,
        })
        .expect("expected routed packet")
}
fn registered(now: Instant) -> Router {
    let mut router = Router::default();
    router
        .register("call-1", 1, "test", &args("op-1"), now)
        .unwrap();
    router
}
fn begin(router: &mut Router, now: Instant, count: u64) -> Value {
    sent(
        router.packet(
            1,
            source(
                "begin",
                json!({ "where": "localhost", "intent": intent(count) }),
            ),
            &[2],
            now,
        ),
        2,
    )
}
fn ready(router: &mut Router, now: Instant, count: u64) -> Value {
    let command = begin(router, now, count);
    sent(
        router.packet(
            2,
            writer(&command, "ready", json!({ "next_offset": 0 })),
            &[2],
            now,
        ),
        1,
    )
}
fn assert_closed(effects: &[Effect], socket: u64) {
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Close { socket: s } if *s == socket))
    );
}
fn completed(effects: &[Effect]) -> &Value {
    effects
        .iter()
        .find_map(|e| match e {
            Effect::Complete { packet, .. } => Some(packet),
            _ => None,
        })
        .unwrap()
}

#[test]
fn only_the_dispatched_shotgun_call_can_begin_and_binding_is_router_owned() {
    let now = Instant::now();
    let mut router = registered(now);
    let packet = source(
        "begin",
        json!({ "where": "localhost", "intent": intent(3) }),
    );
    assert_closed(&router.packet(9, packet.clone(), &[2], now), 9);
    let mut forged = packet.clone();
    forged["slug"] = json!("other");
    assert_closed(&router.packet(1, forged, &[2], now), 1);
    let command = sent(router.packet(1, packet, &[2], now), 2);
    assert_eq!(
        command["binding"]["operation"],
        json!({ "slug": "test", "caller": "shotgun", "operation_id": "op-1" })
    );
    assert_eq!(command["binding"]["source_socket"], 1);
    assert_eq!(command["binding"]["writer_socket"], 2);
    let mut other = args("op-2");
    other["who"] = json!("wicket");
    assert!(router.register("call-2", 3, "test", &other, now).is_err());
    other["who"] = json!("shotgun");
    other["f"] = json!("screenshot");
    assert!(router.register("call-2", 3, "test", &other, now).is_err());
}

#[test]
fn absent_ambiguous_and_remote_writers_are_not_admitted() {
    let now = Instant::now();
    for writers in [vec![], vec![2, 3]] {
        let mut router = registered(now);
        let effects = router.packet(
            1,
            source(
                "begin",
                json!({ "where": "localhost", "intent": intent(3) }),
            ),
            &writers,
            now,
        );
        assert_eq!(completed(&effects)["why"], "rejected");
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::Send { socket: 2 | 3, .. }))
        );
    }
    let mut router = registered(now);
    assert_closed(
        &router.packet(
            1,
            source("begin", json!({ "where": "remote", "intent": intent(3) })),
            &[2],
            now,
        ),
        1,
    );
}

#[test]
fn only_selected_writer_can_acknowledge_and_offsets_must_match() {
    let now = Instant::now();
    let mut router = registered(now);
    let command = begin(&mut router, now, 3);
    let reply = writer(&command, "ready", json!({ "next_offset": 0 }));
    assert_closed(&router.packet(3, reply.clone(), &[2, 3], now), 3);
    sent(router.packet(2, reply, &[2], now), 1);
    let command = sent(
        router.packet(
            1,
            source("chunk", json!({ "offset": 0, "data": "YWJj" })),
            &[2],
            now,
        ),
        2,
    );
    assert_closed(
        &router.packet(
            2,
            writer(&command, "ack", json!({ "next_offset": 2 })),
            &[2],
            now,
        ),
        2,
    );
    assert_eq!(
        sent(
            router.packet(
                2,
                writer(&command, "ack", json!({ "next_offset": 3 })),
                &[2],
                now
            ),
            1
        )["next_offset"],
        3
    );
}

#[test]
fn one_unacknowledged_chunk_and_exact_finish_count_are_enforced() {
    let now = Instant::now();
    let mut router = registered(now);
    ready(&mut router, now, 6);
    assert_closed(&router.packet(1, source("finish", json!({})), &[2], now), 1);
    let chunk = source("chunk", json!({ "offset": 0, "data": "YWJj" }));
    sent(router.packet(1, chunk.clone(), &[2], now), 2);
    assert_closed(&router.packet(1, chunk, &[2], now), 1);
    assert_closed(&router.packet(1, source("finish", json!({})), &[2], now), 1);
}

#[test]
fn base64_limits_are_canonical_and_do_not_allocate_decoded_artifacts() {
    for data in [
        "", "a", "a===", "ab==", "abc=\n", "a=b=", "____", "abc", "AAB=", "YWJj=",
    ] {
        assert!(decoded_chunk_len(data).is_err(), "{data:?}");
    }
    for (count, data) in [
        (1, "Kg==".to_owned()),
        (2, "Kio=".to_owned()),
        (3, "Kioq".to_owned()),
        (65535, "Kioq".repeat(21845)),
        (65536, "Kioq".repeat(21845) + "Kg=="),
    ] {
        assert_eq!(decoded_chunk_len(&data), Ok(count));
    }
    assert!(decoded_chunk_len(&("Kioq".repeat(21845) + "Kio=")).is_err());
}

#[test]
fn concurrency_aggregate_and_manifest_limits_hold_before_forwarding() {
    let now = Instant::now();
    let mut router = Router::default();
    for index in 0..MAX_CALLS {
        router
            .register(
                &format!("call-{index}"),
                1,
                "test",
                &args(&format!("op-{index}")),
                now,
            )
            .unwrap();
    }
    assert!(
        router
            .register("extra", 1, "test", &args("extra"), now)
            .is_err()
    );
    for index in 0..2 {
        let mut packet = source(
            "begin",
            json!({ "where": "localhost", "intent": intent(MAX_PNG_BYTES) }),
        );
        packet["call_id"] = json!(format!("call-{index}"));
        packet["operation_id"] = json!(format!("op-{index}"));
        sent(router.packet(1, packet, &[2], now), 2);
    }
    let mut packet = source(
        "begin",
        json!({ "where": "localhost", "intent": intent(1) }),
    );
    packet["call_id"] = json!("call-2");
    packet["operation_id"] = json!("op-2");
    assert_eq!(
        completed(&router.packet(1, packet, &[2], now))["why"],
        "rejected"
    );
    for n in [0, MAX_PNG_BYTES + 1] {
        assert!(validate_intent(&intent(n)).is_err());
    }
    let mut wrong = intent(3);
    wrong["source_url"] = json!("\0".repeat(8192));
    assert!(validate_intent(&wrong).is_err());
}

#[test]
fn stale_attempt_packets_and_late_reply_cannot_mutate_replacement() {
    let now = Instant::now();
    let mut router = registered(now);
    let old = begin(&mut router, now, 3);
    sent(
        router.packet(
            2,
            writer(&old, "ready", json!({ "next_offset": 0 })),
            &[2],
            now,
        ),
        1,
    );
    let mut replacement = source(
        "begin",
        json!({ "where": "localhost", "intent": intent(3) }),
    );
    replacement["attempt_id"] = json!("attempt-2");
    let current = sent(router.packet(1, replacement, &[2], now), 2);
    assert!(
        router
            .packet(
                2,
                writer(&old, "ready", json!({ "next_offset": 0 })),
                &[2],
                now
            )
            .is_empty()
    );
    assert!(
        router
            .packet(1, source("cancel", json!({})), &[2], now)
            .is_empty()
    );
    assert_eq!(
        sent(
            router.packet(
                2,
                writer(&current, "ready", json!({ "next_offset": 0 })),
                &[2],
                now
            ),
            1
        )["attempt_id"],
        "attempt-2"
    );
}

#[test]
fn a_fresh_call_replaces_receiving_without_cancelling_or_reusing_its_attempt() {
    let now = Instant::now();
    let mut router = registered(now);
    ready(&mut router, now, 3);
    router
        .register("call-2", 1, "test", &args("op-1"), now)
        .unwrap();
    let mut query = source("status", json!({ "where": "localhost" }));
    query["call_id"] = json!("call-2");
    query["attempt_id"] = json!("query-2");
    let query = sent(router.packet(1, query, &[2], now), 2);
    sent(
        router.packet(
            2,
            writer(&query, "receiving", json!({ "next_offset": 0 })),
            &[2],
            now,
        ),
        1,
    );
    let mut replacement = source(
        "begin",
        json!({ "where": "localhost", "intent": intent(3) }),
    );
    replacement["call_id"] = json!("call-2");
    replacement["attempt_id"] = json!("attempt-2");
    let effects = router.packet(1, replacement, &[2], now);
    assert_eq!(completed(&effects)["call_id"], "call-1");
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::Send { packet, .. } if packet["why"] == "cancel"))
    );
    let command = sent(effects, 2);
    assert!(
        router
            .packet(1, source("cancel", json!({})), &[2], now)
            .is_empty()
    );
    router.remove("call-1");
    assert!(
        router
            .packet(
                1,
                source("chunk", json!({ "offset": 0, "data": "YWJj" })),
                &[2],
                now
            )
            .is_empty()
    );
    sent(
        router.packet(
            2,
            writer(&command, "ready", json!({ "next_offset": 0 })),
            &[2],
            now,
        ),
        1,
    );
    let mut chunk = source("chunk", json!({ "offset": 0, "data": "YWJj" }));
    chunk["call_id"] = json!("call-2");
    chunk["attempt_id"] = json!("attempt-2");
    assert_eq!(sent(router.packet(1, chunk, &[2], now), 2)["why"], "chunk");
}

#[test]
fn disconnect_and_deadline_before_finish_are_rejections_after_finish_are_unknown() {
    for after_finish in [false, true] {
        for disconnected in [false, true] {
            let now = Instant::now();
            let mut router = registered(now);
            ready(&mut router, now, 3);
            if after_finish {
                let command = sent(
                    router.packet(
                        1,
                        source("chunk", json!({ "offset": 0, "data": "YWJj" })),
                        &[2],
                        now,
                    ),
                    2,
                );
                sent(
                    router.packet(
                        2,
                        writer(&command, "ack", json!({ "next_offset": 3 })),
                        &[2],
                        now,
                    ),
                    1,
                );
                sent(router.packet(1, source("finish", json!({})), &[2], now), 2);
            }
            let effects = if disconnected {
                router.disconnected(2)
            } else {
                router.expired(now + FINISH_TIMEOUT + ADMISSION_TIMEOUT)
            };
            assert_eq!(
                completed(&effects)["why"],
                if after_finish {
                    "unresolved"
                } else {
                    "rejected"
                }
            );
            assert_eq!(completed(&effects)["operation_id"], "op-1");
            assert!(!effects.iter().any(|e| matches!(e, Effect::Observe(v) if v["what"] == "store" || v["what"] == "fail_publish")));
        }
    }
}

#[test]
fn stale_deadline_does_not_expire_a_new_phase() {
    let now = Instant::now();
    let mut router = registered(now);
    let opening = begin(&mut router, now, 3);
    let generation = router.calls["call-1"].sequence;
    let later = now + ADMISSION_TIMEOUT - Duration::from_secs(1);
    sent(
        router.packet(
            2,
            writer(&opening, "ready", json!({ "next_offset": 0 })),
            &[2],
            later,
        ),
        1,
    );
    assert!(
        router
            .timeout("call-1", generation, now + ADMISSION_TIMEOUT)
            .is_empty()
    );
    assert!(router.contains("call-1"));
}

#[test]
fn screenshot_ingress_is_bounded_without_shrinking_ordinary_image_results() {
    let oversized = source(
        "chunk",
        json!({ "offset": 0, "data": "A".repeat(MAX_PACKET_BYTES) }),
    )
    .to_string();
    assert_eq!(
        ingress(&oversized).unwrap_err(),
        "screenshot packet exceeds limit"
    );
    let ordinary = json!({ "what": "tool", "why": "response", "call_id": "image", "output": "A".repeat(2 * MAX_PACKET_BYTES) }).to_string();
    assert!(ingress(&ordinary).is_ok());
    let malformed = "{\"what\":\"screenshot_save\",\"data\": RAW_SCREENSHOT_SENTINEL}";
    assert!(
        !ingress(malformed)
            .unwrap_err()
            .contains("RAW_SCREENSHOT_SENTINEL")
    );
}

#[test]
fn violations_never_echo_raw_data_into_observations_or_model_results() {
    let now = Instant::now();
    let mut router = registered(now);
    ready(&mut router, now, 3);
    let sentinel = "RAW_SCREENSHOT_SENTINEL";
    let effects = router.packet(
        1,
        source("chunk", json!({ "offset": 0, "data": sentinel })),
        &[2],
        now,
    );
    assert!(!format!("{effects:?}").contains(sentinel));
    assert!(!format!("{:?}", router.disconnected(1)).contains(sentinel));
}

#[test]
fn a_status_retry_preserves_the_writer_receipt_and_checks_the_tool_wrapper() {
    let now = Instant::now();
    let mut router = registered(now);
    ready(&mut router, now, 3);
    let command = sent(
        router.packet(
            1,
            source("chunk", json!({ "offset": 0, "data": "YWJj" })),
            &[2],
            now,
        ),
        2,
    );
    sent(
        router.packet(
            2,
            writer(&command, "ack", json!({ "next_offset": 3 })),
            &[2],
            now,
        ),
        1,
    );
    sent(router.packet(1, source("finish", json!({})), &[2], now), 2);
    assert_eq!(completed(&router.disconnected(2))["why"], "unresolved");
    router
        .register("call-2", 3, "test", &args("op-1"), now)
        .unwrap();
    let mut query = source("status", json!({ "where": "localhost" }));
    query["call_id"] = json!("call-2");
    query["attempt_id"] = json!("retry-2");
    let command = sent(router.packet(3, query, &[4], now), 4);
    let receipt = json!({ "operation": { "slug": "test", "caller": "shotgun", "operation_id": "op-1" },
        "attempt": { "call_id": "call-1", "attempt_id": "attempt-1" }, "event_id": "terminal-fact", "requested": intent(3),
        "destination": "/safe/capture.png", "verified": { "mime_type": "image/png", "bytes": 3, "sha256": "0".repeat(64), "pixel_width": 1, "pixel_height": 1 },
        "accepted_at": "accepted", "observed_at": "observed", "outcome": "stored" });
    let saved = sent(
        router.packet(
            4,
            writer(&command, "stored", json!({ "receipt": receipt })),
            &[4],
            now,
        ),
        3,
    );
    assert_eq!(saved["receipt"]["attempt"]["call_id"], "call-1");
    assert_eq!(saved["call_id"], "call-2");
    assert!(router.tool_result("call-2", "invented success").is_err());
    assert_eq!(
        router.tool_result("call-2", &saved.to_string()).unwrap(),
        saved
    );
    let effects = router.disconnected(3);
    assert_eq!(completed(&effects)["receipt"], saved["receipt"]);
}

#[test]
fn status_only_unknown_and_receiving_are_exact_tool_results_until_begin() {
    for why in ["unknown", "receiving"] {
        let now = Instant::now();
        let mut router = registered(now);
        let query = sent(
            router.packet(
                1,
                source("status", json!({ "where": "localhost" })),
                &[2],
                now,
            ),
            2,
        );
        let fields = if why == "receiving" {
            json!({ "next_offset": 2 })
        } else {
            json!({})
        };
        let result = sent(router.packet(2, writer(&query, why, fields), &[2], now), 1);
        assert_eq!(
            router.tool_result("call-1", &result.to_string()).unwrap(),
            result
        );
        assert!(
            router
                .tool_result("call-1", &json!({ "why": why }).to_string())
                .is_err()
        );
        begin(&mut router, now, 3);
        assert!(router.tool_result("call-1", &result.to_string()).is_err());
        let ended = router.end("call-1", "lost after begin", false);
        assert_eq!(completed(&ended)["why"], "rejected");
    }
}

#[test]
fn a_rejected_begin_cannot_fall_back_to_an_earlier_unknown_status() {
    let now = Instant::now();
    let mut router = registered(now);
    let query = sent(
        router.packet(
            1,
            source("status", json!({ "where": "localhost" })),
            &[2],
            now,
        ),
        2,
    );
    sent(
        router.packet(2, writer(&query, "unknown", json!({})), &[2], now),
        1,
    );
    let mut wrong = intent(3);
    wrong["destination"] = json!("/safe/other.png");
    let effects = router.packet(
        1,
        source("begin", json!({ "where": "localhost", "intent": wrong })),
        &[2],
        now,
    );
    assert_eq!(completed(&effects)["why"], "rejected");
}
