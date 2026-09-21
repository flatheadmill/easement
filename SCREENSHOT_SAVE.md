# Internal screenshot-save route

This is the implemented Easement/Wicket boundary. Shotgun does not implement it
yet. It is specific to `who=shotgun`, `f=screenshot_save` calls and one local
Wicket. The router and worker keep owned state; filesystem work runs on one
blocking Wicket worker. Wicket opens its store lazily.

The original tool arguments must carry a caller-known `operation_id`. A begin
also requires `destination` in those original arguments. Status-only calls can
omit destination. Shotgun additionally requires explicit action=capture|retry|status;
all actions query status first, and only an explicit new capture may capture
after unknown. Easement supplies the slug, logical caller, call ID, and
selected source/writer socket IDs. Socket identities remain self-declared; this
change enforces connection ownership, not authentication.

All source packets have `what: screenshot_save`, `why`, `call_id`, `operation_id`,
and `attempt_id`. Begin/status additionally select `where: localhost` (the
writer location, distinct from the original tool dispatch's source location).

| why | Additional source fields | Expected response |
| --- | --- | --- |
| begin | `where`, `intent` | ready or an existing outcome |
| chunk | `offset`, `data` (canonical padded base64) | ack with `next_offset` |
| finish | none | stored, published, failed, or unresolved |
| status | `where` | unknown, receiving, or an existing outcome |
| cancel | none | aborted, existing outcome, or unresolved |

`intent` contains `destination`, `bytes`, lowercase `sha256`, `mode` (viewport or
full_page), `source_url`, and `captured_at`. Wicket preserves the exact PNG bytes;
source URL, mode, and capture time remain sender claims in its receipt.

For example, after receiving a dispatched tool call:

```json
{
  "what": "screenshot_save",
  "why": "begin",
  "call_id": "the-dispatched-call-id",
  "operation_id": "the-caller-known-save-id",
  "attempt_id": "a-new-attempt-id",
  "where": "localhost",
  "intent": {
    "destination": "/an/allowed/existing/parent/capture.png",
    "bytes": 1234,
    "sha256": "the-64-character-lowercase-sha256",
    "mode": "viewport",
    "source_url": "https://example.test/",
    "captured_at": "2026-09-20T00:00:00Z"
  }
}
```

Easement forwards a separate envelope containing `what`, `why`, `binding`,
`sequence`, and the action's fields. The binding is:

```json
{
  "operation": { "slug": "invoking-slug", "caller": "shotgun", "operation_id": "save-id" },
  "attempt": { "call_id": "dispatched-call-id", "attempt_id": "attempt-id" },
  "source_socket": 1,
  "writer_socket": 2
}
```

Wicket echoes binding and sequence. Only the selected writer socket can answer.
Easement strips those internal fields before relaying a reply to Shotgun, adding
the source call, operation and attempt IDs instead. Shotgun's ordinary tool
response must serialize that relayed packet as its `output`; Easement checks
it against the writer outcome. An invalid wrapper is discarded. If Wicket's
outcome is already known, that authoritative outcome still reaches the MCP
caller. Historical receipts retain their original attempt/call identity while
the outer reply identifies the current status call. A status-only unknown or
receiving reply can also be carried as the exact ordinary tool result. Beginning
a subsequent receive clears that saved status result before validating the new
begin, so it cannot mask the new attempt's failure or outcome.

## Ownership, retries and cancellation

`ready` and `ack` describe temporary receiving, not durable acceptance. Wicket
accepts only inside finish, after complete byte/hash/PNG verification and
synchronization of the stage, parent and acceptance journal. It returns stored
only after publication and a recoverable immutable terminal receipt.

Loss before finish forwarding produces a route rejection and best-effort
receive cancellation. Loss after finish forwarding is unresolved and includes
the operation ID. Cancellation requested is never itself reported as aborted:
only Wicket's fenced cancellation receipt can say that. A cancellation that
loses to publication returns the stored receipt.

A fresh call can query status before recapturing. Unknown means there is no
durable operation known to this store; receiving still needs begin to bind a
live attempt. A retry with the same intent and a new attempt ID can replace a
temporary receive, including on a new tool call/socket. Easement retires the
old source binding and Wicket fences the old ticket before ready. Old packets
and old replies cannot cancel or advance the replacement. An outstanding
finish cannot be replaced. It requires status reconciliation.

Unresolved operations are never automatically republished. On explicit status,
an idle Wicket capability can reopen its store to reconcile a failed journal
barrier. It retains live receiving tickets until those attempts end. A store
open/reopen error disables screenshot work without preventing ordinary tools.

The core's pre-acceptance failure/abort receipts are process-local. Accepted
records and receipts are durable. This route does not add a browser spool,
durable router state, receipt pruning, or recovery of lost browser capture bytes.

## Bounds and observation

There are at most four screenshot calls, 32 MiB per PNG, and 64 MiB of aggregate
admitted receive bytes. One unacknowledged chunk per attempt is permitted;
chunks decode to at most 64 KiB. Screenshot JSON packets are capped at 96 KiB,
with metadata limits matching Wicket. Easement validates base64 shape/size
without decoding or assembling the image. Wicket independently decodes chunks.

Socket ingress has an eight-entry bounded channel, separate from internal
lifecycle events. Easement's socket output queues have 32 entries. Wicket's
worker command and completion queues have eight entries. A current-attempt
credit violation quarantines the source socket immediately. Tungstenite's
ordinary 64 MiB message and 16 MiB frame limits remain in place for existing
image/tool results; the screenshot cap does not replace those broad limits.

Admission/status/result delivery have 30-second deadlines, receive/ack/cancel
have 15 seconds, and finish has 60 seconds. The pump checks stored deadlines
every 250 ms. Each transition changes a generation; stale timers/replies have
no authority. There is no growing timer queue per chunk.

Chunks and ordinary screenshot protocol packets bypass raw-wire logging.
Easement records exceptional routing facts only. Wicket drains core observations
as opaque `what=log, why=write, whom=wicket` packets to the shared logger.
Receipts correlate those facts with tool responses without emitting a second
store assertion. Published-but-not-synchronized is a distinct `published` reply
with the core's `PublishedDurabilityUnconfirmed` receipt and `fail_sync` fact;
it is neither normal stored success nor failed publication. Logging is best
effort and never participates in transaction commit.

## Verification

From Easement:

```sh
cargo test --offline --locked
cargo build --offline --locked
cargo test --offline --manifest-path integration/Cargo.toml
```

The isolated integration crate requires the adjacent Wicket checkout. It starts
a disposable Easement binary from `target/debug/easement` (or
`EASEMENT_TEST_BIN`), uses temporary state and localhost sockets, and connects a
simulated Shotgun to the real Wicket worker. It covers ordinary wrong-socket
responses, normal image-sized results, publication, lost terminal replies,
status recovery, credit violations, and log contents. Keeping this harness
separate leaves Easement's normal build and dependency graph self-contained.

From Wicket, `cargo test --offline` also runs its actual binary against a
disposable WebSocket server, checking lazy store creation, exact publication,
cancellation after publication, and both logging paths. No test contacts a
browser or requires changes to Shotgun.
