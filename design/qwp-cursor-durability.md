# Design: porting Java durable-ack (`request_durable_ack`) to the Go client

Status: **complete & green (Java-aligned)** — config/options, wire decode, the
`qwpDurableTracker` state machine, handshake + mismatch (terminal
PROTOCOL_VIOLATION foreground / retryable drainer), send-loop wiring, keepalive,
`SenderProgressHandler`, `Total{DurableAcks,DurableTrimAdvances}` accessors, the
`BackgroundDrainer` durable trim + `QwpBackgroundDrainerListener` (16-attempt cap
+ `.failed` quarantine), and a faithful object-storage-delay E2E (fake locally;
enterprise sidecar `STATS durableAcks/durableTrim` + durable-mode `AWAIT_ACKED`
for the real cluster). ·
Target: `github.com/questdb/go-questdb-client/v4` ·
Sources: java-questdb-client (source of truth) — `CursorWebSocketSendLoop.java`,
`AckWatermark.java`, `WebSocketResponse.java`, `BackgroundDrainer.java`,
`SenderProgressHandler.java`, `WebSocketClient.java`, `QwpWebSocketSender.java`.

This is a **large port** and follows the project convention: design first, build
foundations before dependents. It is the durability design doc referenced but
missing from `CLAUDE.md`.

> Inline `file:line` references below are **indicative** — this doc was written
> before implementation and the code has since shifted. The code is authoritative;
> grep the named function/identifier rather than trusting a line number.

## 1. Goal & scope

Port Java's **durable acknowledgement** mode to the Go QWP sender. Today the Go
client is fully **OK-ACK-driven**: the server confirms a batch when it is
committed to the local [WAL](https://questdb.io/docs/concepts/write-ahead-log/),
and the client trims store-and-forward (SF) segments and advances its ack
watermark on that OK ACK. `request_durable_ack=on` is **rejected at
construction** (`conf_parse.go:593`) and `STATUS_DURABLE_ACK` frames are parsed
at the transport layer but dropped by the send loop (`qwp_sf_send_loop.go:1231`).

Durable-ack changes *what "acknowledged" means*: with Enterprise primary
replication, the server emits per-table cumulative `STATUS_DURABLE_ACK` frames as
WAL data reaches **object storage**, and an opted-in client advances its
**trim/replay/await** watermark only on those durable frames — not on the
ordinary OK ACK. This is the difference between **settled** (committed to the
primary's WAL, survivable only while the primary's disk survives) and **durable**
(uploaded to object storage, survives a primary loss).

**In scope (full parity):** the wire frame, upgrade-header negotiation +
mismatch handling, the durable-trim state machine, the keepalive PING, the
persisted durable watermark, reconnect/replay semantics, the `BackgroundDrainer`
durable mode, and the `await` / progress API.

**Non-goals:** changing the `.sfa` on-disk segment format (durable-ack only
changes *which watermark gates deletion*, not the bytes — Java `MmapSegment`
parity is preserved); durable-ack over HTTP/TCP (WebSocket QWP only, same as
Java).

**Phasing** (§9): an MVP delivers the foreground sender's durable path; the
`BackgroundDrainer` durable mode and the progress-handler API are follow-ups —
the same seam Go deferred at originally.

## 2. Background: what exists on each side

### 2.1 The semantic (settled vs durable)

| Watermark | Server event | Survives | Go today | Java `request_durable_ack=on` |
|---|---|---|---|---|
| **settled** | WAL commit → `STATUS_OK fsn=N` | primary disk | yes (the only mode) | still emitted; no longer trims |
| **durable** | object-storage upload → `STATUS_DURABLE_ACK` per table | primary loss | **ignored** | drives trim/replay/await |

`STATUS_DURABLE_ACK` is **cumulative per table**: `(table=T, seqTxn=N)` means
every transaction on `T` up to `N` is durable. It carries **no wire sequence /
FSN** — it is keyed by `(table name, seqTxn)`, so the client must map durable
table-watermarks back to the wire-seq of the batches that produced them.

### 2.2 Java (source of truth)

| Piece | Role |
|---|---|
| `WebSocketResponse` `STATUS_DURABLE_ACK=0x02` | wire frame: `status(1) + tableCount(2) + [nameLen(2)+name+seqTxn(8)]*` (no sequence field; min 3 bytes) |
| `WebSocketClient` `X-QWP-Request-Durable-Ack` / `X-QWP-Durable-Ack` | upgrade opt-in request header + server echo |
| `QwpDurableAckMismatchException` | terminal: requested but the bound endpoint didn't advertise durable-ack |
| `CursorWebSocketSendLoop.durableAckMode` + `durableTableWatermarks` + `pendingDurable` FIFO | the trim state machine (~380 LOC) |
| `AckWatermark` (`.ack-watermark` sidecar) | persisted single cumulative FSN high-water mark |
| `BackgroundDrainer.durableAckMode` + `BackgroundDrainerListener` | orphan drain in durable mode + mismatch-retry + operator callbacks |
| `SenderProgressHandler` | async `onAcked(fsn)` callback; settled-vs-durable contract |
| `awaitAckedFsn` | under durable mode, `targetFsn` advances only after durable upload |

### 2.3 Go (current state — the scaffolding that already exists)

Go was built anticipating this port; the seams are in place:

| Already present | Where |
|---|---|
| `QwpStatusDurableAck = 0x02` | `qwp_constants.go:226` |
| Transport parse of the durable frame shape + per-table trailer walk | `qwp_transport.go:595` (`readAck`, durable case `:629`), `:654` (`validateAckTableEntries`) — **entries validated then discarded** |
| ACK size constants incl. durable | `qwp_transport.go:88` (`qwpAckDurableMinSize`, `qwpAckDurableTablesOff`) |
| Both config keys in the allow-list + parsed | `conf_parse.go:75,91`; `request_durable_ack` case `:570` (**rejects `on` at `:593`**); `durable_ack_keepalive_interval_millis` case `:598` (accepted, inert) |
| `.ack-watermark` sidecar file (single 8-byte FSN, magic-stamped, best-effort) | `qwp_sf_ack_watermark.go:36` — doc already says "durably-acknowledged FSN" |
| The single send-loop hook | `qwp_sf_send_loop.go:1226` — `case QwpStatusDurableAck { continue }` |
| Test helper `buildAckDurable()` | `qwp_transport_test.go:73` |

**The gap:** the send-loop state machine, the keepalive PING, the handshake
negotiation, the drainer's durable mode, and the progress API — everything that
turns the parsed-but-dropped frame into trim/replay/await behavior.

## 3. Wire protocol

### 3.1 `STATUS_DURABLE_ACK` frame

```
OK frame:           status(1)=0x00 | sequence(8) | tableCount(2) | [ nameLen(2) name(N) seqTxn(8) ]*
DURABLE_ACK frame:  status(1)=0x02 |              | tableCount(2) | [ nameLen(2) name(N) seqTxn(8) ]*
```

The durable frame **omits the 8-byte sequence** — `readAck`'s durable branch
already reads `tableCount` at `qwpAckDurableTablesOff = 1` (vs `9` for OK). Both
frames share the identical per-table trailer, already walked by
`validateAckTableEntries`. **The only new decode work is exposing the `(name,
seqTxn)` pairs** the walker currently discards, for both OK and durable frames
(the OK frame's pairs are needed to build a pending entry; the durable frame's
pairs advance the table watermarks).

### 3.2 Handshake negotiation

Durable-ack is negotiated on the WebSocket upgrade; a client must not assume it:

- **Request:** the opted-in client sends `X-QWP-Request-Durable-Ack: true` on the
  upgrade (Go: add to the request headers in `qwp_transport.go`'s connect path).
- **Response:** the server echoes `X-QWP-Durable-Ack: enabled` iff it will emit
  durable frames (only a replication-configured **primary** does). Go parses this
  off the upgrade response.
- **Mismatch (NORMATIVE):** if the client requested durable-ack but the bound
  endpoint did **not** advertise it, the endpoint is **unusable** — the client
  must not silently fall back to OK-only trimming (that would drop data it
  believes durable). The foreground sender raises a terminal
  `*QwpDurableAckMismatchError` classified as `PROTOCOL_VIOLATION` (never
  user-configurable → always HALT). The drainer tolerates it transiently (§5.8).

## 4. Central decision: one watermark, not two

Java advances a **single** `engine.acknowledge(...)` watermark that, in
`durableAckMode`, is only moved by durable frames. Go must do the same:
**`engineAckedFsn` remains the one watermark; in durable mode only the durable
drain advances it.** OK ACKs no longer call `engineAcknowledge`.

Rejected alternative — a second parallel `durableAckedFsn`: it would double the
ring/notify machinery (`qwp_sf_ring.go`), fork every replay anchor
(`engineAckedFsn()+1` at `qwp_sf_send_loop.go:696,1492,1595,1617,1638`), and
force `AwaitAckedFsn` to choose a watermark by mode. The single-watermark model
falls out cleanly and matches Java bit-for-bit.

**Consequences of one watermark (all correct, all intended):**

1. **`AwaitAckedFsn` / `AckedFsn` become durable confirmation for free** — they
   poll `engineAckedFsn` (`qwp_sender_cursor.go:888,918`), which now only
   advances on durable. Matches Java's `awaitAckedFsn` semantic shift with no API
   fork.
2. **Reconnect replays from the durable watermark** — `positionCursorForStart`
   and `swapClient` restart at `engineAckedFsn()+1`, i.e. they **re-send
   OK-but-not-yet-durable data**. This is *required*: a settled-only batch is not
   safe to forget across a primary failover.
3. **Trim slows to the durable rate** — `drainTrimmable` frees ring space on
   `ackedFsn` (`qwp_sf_ring.go:493`); gating it on durable means the ring holds
   more, so `engineAppendBlocking` backpressures sooner. Correct: durable mode
   deliberately retains until object-storage upload.
4. **The persisted `.ack-watermark` auto-becomes the durable watermark** —
   `SegmentManager`'s equivalent (`qwp_sf_manager.go:456`) already persists
   `engineAckedFsn`; no sidecar change, so a restart re-replays exactly the
   not-yet-durable tail.
5. **`Close` blocks until durable, not just settled** — `waitCursorDrain`
   (`qwp_sender_cursor.go` close path) polls `ackedFsn >= publishedFsn`, so in
   durable mode `Close` waits for object-storage upload, bounded by
   `close_flush_timeout` (then returns the "data may be lost" drain-timeout).
   Matches Java; document it — durable mode makes clean shutdown slower.

What stays OK-driven: **flow control and reconnect *targeting*.**
`serverAckedSeq` / `highestFullySent` (`qwp_sf_send_loop.go:215`) still track the
OK stream (they bound `replayTargetFsn` and prevent acking past what was sent).
Durable mode only re-points the **trim/persist/await** watermark
(`engineAcknowledge`), never the send/replay bookkeeping.

## 5. Proposed Go design

### 5.1 Files / layout

| File | Change |
|---|---|
| `qwp_sf_durable.go` **(new)** | the durable-trim state machine: `qwpDurableTracker` (per-table watermarks, pending FIFO, pooled entries, `enqueueOk`/`applyDurable`/`drain`) |
| `qwp_transport.go` | expose per-table `(name, seqTxn)` from `readAck`; send/parse the two upgrade headers |
| `qwp_sf_send_loop.go` | replace the `:1231` `continue`; branch `receiverLoop` on `durableAckMode`; keepalive PING in `senderLoop`'s select; `clearDurableAckTracking` on `swapClient` |
| `qwp_sf_ack_watermark.go` | reuse as-is (already persists the single watermark) |
| `conf_parse.go` | flip `on` from reject to accept; store the flag + keepalive on `lineSenderConfig` |
| `sender.go` / options | `WithRequestDurableAck(bool)`, `WithDurableAckKeepaliveInterval(d)`, thread into `lineSenderConfig` |
| `sender_error.go` | `*QwpDurableAckMismatchError` + `CategoryProtocolViolation` classification |
| `qwp_sf_drainer.go` | thread durable mode; mismatch-retry loop + `.failed` sentinel |
| `sender_progress_handler.go` **(new, phase 2)** | `SenderProgressHandler` + `WithProgressHandler` over the existing `qwpDispatcher[T]` |
| `qwp_sender.go` | doc `AwaitAckedFsn` durable semantics; (phase 2) progress accessors |

### 5.2 Config

`conf_parse.go:593`: replace the hard rejection of `request_durable_ack=on` with
storing `conf.requestDurableAck = true`. The `durable_ack_keepalive_interval_millis`
case (`:598`) is already accepted — store it (default
`qwpDurableAckKeepaliveDefault = 200ms`, matching Java's
`DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS`). Both stay WebSocket-only (the
non-QWP reject at `:577` is retained). Options mirror the keys
(`WithRequestDurableAck`, `WithDurableAckKeepaliveInterval`), with the
option-wins-over-string precedence used everywhere else.

### 5.3 Handshake + mismatch error

In `qwp_transport.go`'s connect path: when `opts.requestDurableAck`, add
`X-QWP-Request-Durable-Ack: true` to the upgrade request; after a successful
upgrade, read `X-QWP-Durable-Ack` from the response and record
`serverDurableAckEnabled` on the transport. The round-walk (`qwp_sf_round_walk.go`)
enforces the mismatch: **requested-but-not-advertised → the endpoint does not
bind**, surfaced as a terminal `*QwpDurableAckMismatchError` (host/port/role;
`role==""` ⇒ "primary upgraded but silent", non-empty ⇒ "only replicas reached").
The foreground connect classifies it `PROTOCOL_VIOLATION` (HALT, non-retryable);
the drainer catches it for retry (§5.8).

### 5.4 The durable-trim state machine (`qwp_sf_durable.go`) — the core

`qwpDurableTracker`, owned by the send loop, reset on every reconnect:

```
type qwpDurableTracker struct {
    watermarks map[string]int64      // per-table cumulative durable seqTxn (advance via max)
    pending    []*qwpPendingDurable  // FIFO in wire-seq order, awaiting durable coverage
    pool       []*qwpPendingDurable  // free list — 0-alloc steady state
}

type qwpPendingDurable struct {     // one OK-acked batch not yet durable
    wireSeq int64
    tables  []string
    seqTxns []int64                  // isCovered(w): every tables[i] has w[tables[i]] >= seqTxns[i]
}                                    // len(tables)==0 (empty batch) ⇒ trivially covered
```

`receiverLoop` (`qwp_sf_send_loop.go:1207`) branches on `durableAckMode`:

- **OK ACK, durable mode** — instead of `applyAckWatermark`/`engineAcknowledge`,
  `enqueueOk()` reads the **current OK frame's** `(table, seqTxn)` entries into a
  pending entry at the capped wire-seq (Java: `enqueuePendingOk` reads `response`
  in place), then `drain()`. (Legacy non-durable mode: unchanged — advance
  `engineAcknowledge` directly.)
- **`STATUS_DURABLE_ACK`, durable mode** — replace the `:1231` `continue` with
  `applyDurable(frame)`: for each entry advance `watermarks[name] =
  max(cur, seqTxn)`, then `drain()`. A durable frame received **without** opt-in
  (a server bug) is logged once and ignored, never fatal (Java parity).
- **Rejection, durable mode** — never touches the tracker or the watermark
  (NACK policy v2, `design/qwp-nack-policy-v2.md`): a retriable rejection
  recycles the connection (the tracker resets on swap) and replays from
  `ackedFsn+1`; a terminal rejection latches with the bytes preserved on disk.
  There is no drop path and no empty placeholder entry.

**Why `drain()` on every enqueue, not just on a durable frame:** a durable ACK
may arrive *before* the OK ACK for the same batch (independent server events), so
a freshly-enqueued entry may already be covered; draining on enqueue keeps
`ackedFsn` current with no extra wire round-trip.

`drain()` pops every **head** entry whose tables are all covered by `watermarks`
(FIFO, so cumulative ordering holds), and if any popped, calls
`engineAcknowledge(fsnAtZero + highestPoppedWireSeq)` **once** — the single point
that advances the trim/persist/await watermark in durable mode. Popped entries
return to `pool`.

**Allocation.** The tracker runs on the **receiver** goroutine, not the producer
pipeline, so it does **not** touch `BenchmarkQwpSenderSteadyState` (which pins the
`Table→Symbol→Column→At` producer path — `CLAUDE.md` §Testing); that gate is
unaffected. Its own steady-state allocation is a separate concern: pending entries
and their `seqTxns`/`tables` slices are pooled, and table names are **interned** —
canonicalized to one stable string per distinct table, reused as both the
`watermarks` map key and the pending-entry reference. Table names can **not** be
reused-buffer slices: a name is a live map key across many frames, so a rewound
receive buffer would corrupt the map. A distinct table is allocated once (cold),
never per frame. Pin this with a dedicated `BenchmarkQwpDurableSteadyState`.

### 5.5 Keepalive PING

The OSS server flushes pending durable-ack frames only in response to inbound
recv events, so an idle opted-in client with unconfirmed data must prod it.
`senderLoop`'s idle `select` gains a ticker case: when `durableAckMode &&
keepaliveInterval > 0 && tracker.hasPending()` and no other work is due, send a
WebSocket ping (`sendDurableKeepalive`). Go realises Java's `lastFrameOrPingNanos`
throttle structurally rather than as a field on the tracker: a reusable
`time.Timer` in `senderLoop` is reset on every real frame (`drainResetTimer` in
the `didWork` branch) and after each ping, so a busy sender never also pings; and
because `runOneConnection` spawns a fresh `senderLoop` (fresh timer) per
connection, the throttle resets on reconnect for free — `qwpDurableTracker.reset()`
only needs to clear watermarks + pending. `interval <= 0` disables (Java parity).

### 5.6 Persisted watermark

No new artifact. `.ack-watermark` already persists `engineAckedFsn`
(`qwp_sf_ack_watermark.go`, written from the maintenance pass at
`qwp_sf_manager.go:456`). Under one-watermark durable mode that value *is* the
durable watermark, so a restart seeds `ackedFsn = max(watermark, baseSeed)` and
re-replays exactly the not-yet-durable tail — the whole point of persisting it.
Verify the restart seed clamps to `publishedFsn` and removes a stale
fully-drained sidecar (existing logic).

### 5.7 Reconnect / replay

`swapClient` (`:1487`) calls `tracker.reset()` (`clearDurableAckTracking`): a
fresh connection re-emits cumulative durable state from scratch, so stale
per-table watermarks and pending entries must be dropped, and the keepalive
throttle reset. Replay restarts at `engineAckedFsn()+1` (the durable watermark)
— unchanged code, correct meaning under §4.

### 5.8 `BackgroundDrainer` durable mode + listener (phase 2)

The drainer (`qwp_sf_drainer.go`) inherits `durableAckMode` and the keepalive
interval and runs the same state machine, so it trims an orphan slot only on
durable acks. **Asymmetry with the foreground sender (NORMATIVE):** a foreground
sender that lands on a non-advertising endpoint fails loud (§5.3); the drainer
**tolerates it transiently** because its source data is pinned on disk (nothing
trims without durable acks) — it retries. On `*QwpDurableAckMismatchError` the
connect walk retries with capped backoff, firing
`OnDurableAckUnavailable(dir, attempt)` per miss; at
`qwpMaxDurableAckMismatchAttempts = 16` consecutive capability-gap sweeps it
fires `OnDurableAckPersistentFailure(dir, attempts, elapsed)`, drops the
`.failed` sentinel (`qwp_sf_orphan.go`), and ends `FAILED`. The settle budget
counts ONLY capability-gap sweeps (Invariant B): a transport outage never
consumes it, and an all-replica role-reject sweep resets it (topology churn)
while firing `OnPrimaryUnavailable(dir, attempt)` — there is no wall-clock
give-up on the connect walk. The callbacks form
`QwpBackgroundDrainerListener` (mirrors the existing `SenderConnectionListener`
dispatcher pattern; runs off the drainer goroutine, must not block).

### 5.9 Await / progress API

- **`AwaitAckedFsn` / `AckedFsn`** need no behavioral fork (§4 consequence 1) —
  only a doc update noting that under `request_durable_ack=on` the returned FSN
  is a *durable* watermark. Pair with `FlushAndGetSequence` as today.
- **`SenderProgressHandler`** (phase 2, net-new public API) — a monotonic
  `onAcked(fsn int64)` callback delivered async over a reused `qwpDispatcher[T]`,
  fired from `drain()` when the watermark advances. Its doc carries the
  **settled-vs-durable warning**: in the default (non-durable) mode `onAcked`
  fires on OK/settled, which does not by itself prove durability —
  cross-reference the `SenderErrorHandler`. (NACK policy v2 restored watermark
  purity: the FSN stream reflects only genuine server OKs, never a drop.)
  Registered via `WithProgressHandler`; adding it to the
  `QwpSender` interface is safe pre-release (`var _ QwpSender` guards internal
  impls; no published `/v4` tag ships it — see the facade doc's precedent).

## 6. Config keys (single source of truth: `conf_parse.go`)

| Key | Option | Default | Notes |
|---|---|---|---|
| `request_durable_ack` | `WithRequestDurableAck(bool)` | `off` | WebSocket-only; `on` flips trim/replay/await to durable; non-QWP rejected |
| `durable_ack_keepalive_interval_millis` | `WithDurableAckKeepaliveInterval(d)` | `200` | paces the keepalive ping; `0` disables; inert unless `request_durable_ack=on` |

## 7. Decisions (resolved)

1. **One watermark, not two** (§4) — `engineAckedFsn` gated on durable in durable
   mode; no parallel watermark, no ring/await fork.
2. **Mismatch is terminal for the foreground sender, retryable for the drainer**
   (§5.3, §5.8) — a foreground fallback to OK-only trimming would silently drop
   believed-durable data; a drainer can wait because its source is pinned.
3. **`STATUS_DURABLE_ACK` is keyed by `(table, seqTxn)`, so a pending FIFO is
   mandatory** (§5.4) — the OK frame's table entries populate each pending entry;
   a durable frame advances per-table watermarks; `drain()` maps coverage back to
   a wire-seq. There is no shortcut that avoids the FIFO.
4. **Progress handler is a phase-2 net-new API** (§5.9) — the durable *trim*
   works without it; it is observability. `AwaitAckedFsn` already gives durable
   confirmation.
5. **`.sfa` format unchanged; only the trim-gating watermark and the meaning of
   the `.ack-watermark` sidecar change** — Java `MmapSegment` on-disk parity is
   preserved.

## 8. Hazard checklist (NORMATIVE — Java's stepped-on pits → Go defenses)

- **A — silent OK-only fallback on a non-primary.** A requested-but-unadvertised
  endpoint MUST NOT bind for the foreground sender. Defense: §5.3 mismatch →
  `PROTOCOL_VIOLATION` HALT. Test: httptest server that omits `X-QWP-Durable-Ack`.
- **B — trimming settled-but-not-durable data.** In durable mode `engineAcknowledge`
  MUST be reachable *only* from `drain()`, never from the OK-ack or the
  `DROP_AND_CONTINUE` path. Test: OK ACK alone leaves `AckedFsn` unchanged;
  `drainTrimmable` frees nothing until a covering durable frame arrives.
- **C — reordered / partial durable frames.** Per-table watermarks advance by
  `max` only; a batch is durable only when **all** its tables are covered; empty
  batches are trivially covered. Test: interleave two tables at different durable
  rates; assert FIFO drain stops at the first uncovered entry.
- **D — DROP advancing past unconfirmed good data.** A `DROP_AND_CONTINUE` in
  durable mode enqueues an empty entry, never a direct `engineAcknowledge`.
  Test: OK(seq0, table T@5) then DROP(seq1); watermark must not pass seq1 until
  T@5 is durable.
- **E — stale durable state across reconnect.** `swapClient` (and
  `positionCursorForStart`) MUST `tracker.reset()` (watermarks + pending); the
  keepalive throttle resets for free via the per-connection `senderLoop` timer.
  Test: durable frame on connection 1 must not satisfy a pending entry re-sent on
  connection 2.
- **F — restart re-replays already-durable data / forgets not-yet-durable data.**
  The persisted watermark is the durable one; seed `max(watermark, baseSeed)`,
  clamp to `publishedFsn`. Test: crash after settled-but-not-durable, restart,
  assert the tail replays.
- **G — keepalive pings on a busy or a non-durable sender.** Ping only when
  `durableAckMode && interval>0 && pending nonEmpty && idle`. Test: a steady
  producer emits no pings; an idle sender with pending data pings at the interval.
- **H — allocation.** The producer 0-alloc gate is unaffected (the tracker is off
  that path, §5.4), so `BenchmarkQwpSenderSteadyState` stays 0 allocs/op untouched.
  The tracker's own steady state is pooled, and table names are **interned** (a
  live map key must not be a reused buffer, §5.4). Gate: a new
  `BenchmarkQwpDurableSteadyState` pins the pending-pool + intern-cache reuse.
- **I — drainer deletes `.sfa` before durable.** The drainer trims only on
  durable; on persistent mismatch it drops `.failed` and stops, never unlinks
  un-durable segments. Test: drainer against a non-advertising server retries then
  fails, leaving the slot intact.

## 9. Implementation order (foundation-first)

**Phase 1 — MVP (foreground durable path):**

1. **Wire decode** — expose `(name, seqTxn)` from `readAck` for OK + durable
   frames (§3.1). Pure, unit-testable against `buildAckOK*` / `buildAckDurable`.
2. **Config + options** — accept `on`, store flag + keepalive (§5.2). Flip the
   `qwp_sf_conf_test.go` rejection tests to acceptance.
3. **Handshake + mismatch error** — headers + `*QwpDurableAckMismatchError` +
   round-walk enforcement (§5.3).
4. **`qwpDurableTracker`** — the state machine, pooled (§5.4). Heaviest unit.
5. **Send-loop wiring** — branch `receiverLoop`, `reset()` on `swapClient` (§5.4,
   §5.7).
6. **Keepalive PING** — `senderLoop` ticker (§5.5).
7. **Restart** — verify the sidecar seeds the durable watermark (§5.6).

**Phase 2 — parity follow-ups:**

8. **`BackgroundDrainer` durable mode + `QwpBackgroundDrainerListener`** (§5.8).
9. **`SenderProgressHandler` + `WithProgressHandler`** (§5.9).
10. **E2E** — Enterprise primary-replication path (§10).

Hazards A–I (§8) are the acceptance bar; each phase-1 step lands with its unit
tests.

## 10. Testing strategy

**Unit (pure + `newQwpTestServer` httptest WS stand-in):** the tracker
(coverage, FIFO drain, empty-batch, DROP entry, per-table max) as a table test;
the send-loop branches with a fake server emitting OK-then-durable and
OK-then-DROP-then-durable; reconnect `reset()`; keepalive cadence (assert pings
only when idle+pending); handshake mismatch (server omits the echo header →
terminal `*QwpDurableAckMismatchError`). Flip the existing
`qwp_sf_conf_test.go` / `qwp_sf_classify_test.go` durable-ack assertions from
"rejected/unknown" to first-class.

**Fuzz:** extend `qwp_egress`-style frame fuzzers with random durable frames
(random table sets / seqTxns) asserting the tracker never advances past coverage
and never leaks pending entries.

**Benchmark (gate):** `BenchmarkQwpSenderSteadyState` stays 0 allocs/op (untouched
— the tracker is off the producer path); add a `BenchmarkQwpDurableSteadyState`
pinning the durable path's pending-pool + table-name intern-cache reuse.

**E2E (phase 2, Docker + Enterprise):** a primary-replication cluster (or a
faithful fake that emits durable frames after a simulated upload delay); the
`system_test/enterprise_e2e` sidecar gains an `AWAIT_DURABLE_ACKED` command and a
durable stat, mirroring the existing `AWAIT_ACKED` / `STATS acked=`.

**Gates:** `go vet`, `staticcheck`, `gofmt`, the 0-alloc benchmark, and the
`export_test.go` re-export invariant (any new `QwpSender` method added there).
