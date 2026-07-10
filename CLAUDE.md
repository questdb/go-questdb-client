# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository. It captures invariants and "where to look" pointers —
for specifics (file contents, constants, config-key catalog, error categories)
read the code, which is authoritative.

## Project

Go client library for QuestDB ingestion. Three transports:

- **HTTP / HTTPS** and **TCP / TCPS** — the legacy InfluxDB Line Protocol (ILP).
- **WS / WSS (QWP)** — QuestDB's binary columnar wire protocol over WebSocket.
  The only transport exposing the full type system (int8/16/32, float32, char,
  date, timestamp-nanos, uuid, varchar, geohash, int64 arrays). **QWP is not a
  version of ILP** — distinct framing, codecs, and server handshake.

Module path: `github.com/questdb/go-questdb-client/v4` — the `/v4` segment is
load-bearing when importing within this repo. Minimum Go: 1.23 (go.mod pins
`go 1.23` with a `1.24.4` toolchain).

## Commands

```bash
# Required for interop_test.go.
git submodule update --init --recursive

# Static analysis (run by CI).
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

# Tests. Integration suites spin up QuestDB containers via
# testcontainers-go, so Docker must be running for those.
go test -v ./...

# Single suite — testify suites dispatch via the top-level
# Test*Suite entry point plus the method name. The QWP live-server
# tests are plain TestQwpIntegration* functions, not a testify suite.
go test -v -run TestIntegrationSuite/TestE2EValidWrites .
go test -v -run TestQwpIntegration .

# Allocation-tracked benchmark on the QWP hot path.
go test -v -bench BenchmarkQwpSenderSteadyState -benchmem -run ^$ .
```

`examples/` ships compilable `main.go` files referenced by
`examples.manifest.yaml`, which questdb.io uses to render docs — keep paths and
filenames stable.

## Architecture

Public surface: `LineSender` interface in `sender.go`. Every transport satisfies
it. `QwpSender` (in `qwp_sender.go`) is a superset for QWP-only column types —
callers wanting them must type-assert.

Two entry points: `LineSenderFromConf(ctx, "schema::addr=...;key=value")`
(parser in `conf_parse.go`; schemas: `http`, `https`, `tcp`, `tcps`, `ws`,
`wss`) and `NewLineSender(ctx, opts...)` (requires one of `WithHttp`, `WithTcp`,
`WithQwp`). Both funnel through `lineSenderConfig` and `newLineSender` in
`sender.go`. **`conf_parse.go` is the single source of truth for supported
config keys.**

### ILP (HTTP / TCP)

Three protocol versions: V1 text-only, V2 adds binary `float64` and
n-dimensional float arrays, V3 adds decimals. Each transport has three concrete
structs — `httpLineSender{,V2,V3}` in `http_sender.go`, `tcpLineSender{,V2,V3}`
in `tcp_sender.go`.

HTTP auto-negotiates the version; TCP requires `WithProtocolVersion(...)` or
`protocol_version=2|3`. **Adding a new ILP column type or feature touches all
six structs**, the `LineSender` interface, `buffer.go` (raw ILP encoding), and
the `Messages` / `MsgCount` / `BufLen` / `ProtocolVersion` switch helpers in
`export_test.go`. Keep those switches exhaustive.

### QWP (WebSocket columnar protocol)

Everything QWP lives in `qwp_*.go`. The buffer (`qwp_buffer.go`), encoder
(`qwp_encoder.go`), wire primitives (`qwp_wire.go`), and transport
(`qwp_transport.go`) form the columnar codec stack. The sender (`qwp_sender.go`
+ `qwp_sender_cursor.go`) implements `LineSender` and `QwpSender` on top of it.

**All wire I/O — memory-backed *and* disk-backed — goes through the cursor
engine + send loop** in `qwp_sf_*.go`. `sf_dir` empty selects memory-backed
segments; set selects disk-backed under `<sf_dir>/<sender_id>/*.sfa` (that
per-sender directory is itself the slot — there is no extra slot level),
on-disk-compatible with the Java client's `MmapSegment.java`. The producer
encodes a batch into `qwpSfCursorEngine` via `engineAppendBlocking`; the
`qwpSfSendLoop` goroutine drains it to the WebSocket, parses ACKs, advances
`engineAckedFsn`, and owns reconnect + replay from `engineAckedFsn() + 1`.

**Cursor frames carry a self-sufficient schema** — full inline column
definitions on every frame — which keeps reconnect/replay/orphan-adoption
schema-safe against a fresh server connection. The symbol dictionary is
**delta-encoded** (each id sent once per connection); a reconnect re-registers
the whole dictionary via a send-loop catch-up frame before replay, and SF mode
persists it to a per-slot `.symbol-dict` side-file so a recovered /
orphan-drained slot can rebuild it. See "Delta symbol dictionary" below and
`design/qwp-delta-symbol-dict.md`.

**Invariant B (store-and-forward robustness):** a running sender, async
initial connect, and every background/orphan drainer retry transport outages
and all-replica role-reject windows **indefinitely** with capped exponential
backoff — no wall-clock give-up, no terminal latch, no `.failed` quarantine
for transport-class failures. `reconnect_max_duration_millis` bounds only the
blocking sync initial connect; on a running sender it does not bound reconnect —
it doubles as the poison-frame episode floor and the drainer no-progress wedge
budget. Sanctioned terminals: auth reject, 404/426
upgrade reject (at sweep exhaustion), durable-ack capability-gap settle
exhaustion (capability-gap sweeps only; transport windows pause, role-reject
sweeps reset), poison-frame escalation (strikes + episode floor), and — on
drainers — a failed slot recovery / engine open or a no-progress wedge past the
budget (both `.failed` quarantines). The only producer-visible error from a
running drain path is SF-out-of-space backpressure. Enforced by the review-pr skill checklist.

**The wire carries no schema id and no schema mode byte.** A table block is
`table_name, row_count, col_count, inline columns, column data`; the inline
column definitions are the authoritative schema, repeated on every frame. There
is no `nextSchemaId` accumulator on the sender, no per-table `schemaId` field on
the table buffer, no schema-change detection, and no reference mode. (QWP once
carried a mode byte + schema id plus a schema-reference optimisation; it was
removed across the server and all clients.) On egress, the decoder parses the
schema from the first `RESULT_BATCH` of a query (`batch_seq == 0`) into
`qwpQueryDecoder.querySchema` and reuses it for that query's continuation
batches; `qwpEgressIO.dispatcherRun` calls `resetQuerySchema` at the start of
every query so a schema never leaks across query boundaries.

**Delta symbol dictionary** (`design/qwp-delta-symbol-dict.md`). The dict is
delta-encoded: `symbolDeltaBaseline()` returns `maxSentSymbolId` (delta mode) or
`-1` (full-dict fallback), passed as the encoder's `maxSentId`, so each frame
carries only ids above the sent watermark. `deltaDictEnabled` (on the producer
and the send loop) comes from `engineDeltaDictEnabled()` — always in memory
mode, SF only when the per-slot `.symbol-dict` side-file (`qwp_sf_symbol_dict.go`)
opened; otherwise the sender falls back to full self-sufficient frames
(`maxSentId=-1`), byte-identical to the old behaviour. `maxSentSymbolId` is
monotonic (never reset — it survives the wire boundary); `batchMaxSymbolId` is
the `batchMaxId` arg bounding `writeDeltaDict`, rewound by `resetAfterFlush` to
`maxSentSymbolId` (never `-1`). Both `enqueueCursor` and the per-table split
`enqueueCursorSplit` emit deltas and advance the baseline **per frame** — the
split path MUST stay delta, since a full-dict frame there would be skipped by the
send-loop mirror and gap the reconnect catch-up.

On reconnect the fresh server has an empty dictionary, so the send loop keeps an
I/O-goroutine-owned mirror of every symbol it has sent (`sentDictBytes` /
`sentDictCount`, extended by `accumulateSentDict` after each send) and
re-registers the whole dictionary via table-less **catch-up frame(s)**
(`setWireBaselineWithCatchUp` → `sendDictCatchUp`, split by the server batch cap)
before replay. Catch-up frames occupy wire seqs `0..k-1` mapping to already-acked
FSNs (`fsnAtZero = replayStart - k`), so ack alignment holds and — being
table-less — they are trivially durable; they bump `nextWireSeq` /
`highestFullySent` but never `framesSentOnConn` (the poison-strike gate). SF mode
write-ahead persists a frame's new symbols before publishing it; a host-crash
tear (frame delta start > recovered dict size) is caught pre-send by the
**torn-dict guard**, a terminal `PROTOCOL_VIOLATION` ("resend required").

`WithInFlightWindow(n)` / `in_flight_window=n` is **retained but a no-op** in
the cursor architecture — backpressure is governed by the engine's segment-ring
+ `engineAppendBlocking` deadline.

### Java-parity QWP knobs (not in connect-string.md)

These connect-string keys are recognised by the Java client
(`Sender.java`) but are not listed in the
[native-client spec](https://github.com/questdb/questdb-enterprise/blob/main/questdb/docs/qwp/connect-string.md).
We accept them for Java-parity portability — a connect string that
works on the Java client must work here. None should ever be
considered for removal without a matching change in Java:

- `gorilla=on|off` — gates the Gorilla timestamp encoding in
  `qwp_encoder.go` (FLAG_GORILLA). Default `on`.
- `in_flight_window=N` — see the "retained but a no-op" note above.

`close_timeout=N` (millisecond integer) was a v4.0–v4.5 Go-only key
for the memory-mode close path. The cursor architecture unified
memory and SF onto `close_flush_timeout_millis`, which the spec
also defines. The parser now rejects `close_timeout=` with a
migration hint pointing at `close_flush_timeout_millis`.
`WithCloseTimeout(d)` is retained as a deprecated alias that routes
positive durations through `close_flush_timeout_millis`; new code
should use `WithCloseFlushTimeout` directly.

Flush semantics: `Flush` / `FlushAndGetSequence` **never wait for the server
ACK** — they return once the batch is published into the cursor engine (in-RAM
for memory mode, on-disk for SF) and the send loop delivers + replays it in the
background. This matches the Java spec ("flush() never waits for ACK; ACKs are
async") and is uniform
across both the pending-rows and zero-pending branches and auto-flush — all
route through `enqueueCursor`; explicit `Flush` only additionally surfaces a
latched send-loop error eagerly. (`Flush` was an ACK barrier
through v4.2.0; that contract was dropped when the cursor/SF architecture made
local persistence, not the ACK, the durability guarantee.) `FlushAndGetSequence` returns the
published FSN — the upper bound of any `SenderError.ToFsn` for that batch;
**pair it with `AwaitAckedFsn` for server-ACK confirmation** (the dedicated
primitive now that `Flush` no longer blocks on ACKs).

Durable-ack (`request_durable_ack` / `WithRequestDurableAck`, QWP-only) shifts the
trim/replay/await watermark from the WAL-commit OK ACK to the server's
`STATUS_DURABLE_ACK` (object-storage upload), so under it `AckedFsn` /
`AwaitAckedFsn` / `Close`-drain confirm **durability**, not just commit. The trim
state machine is `qwpDurableTracker` (`qwp_sf_durable.go`, mutex-guarded so the
receiver and the send-side last-frame re-drive can both touch it); the send loop
stashes each OK ack and releases it only once covering durable frames arrive. A
dropped/coalesced OK-ack sequence HALTs fail-closed (`qwpDurableTracker.seqGap` /
`durableOnOk`), and connecting to a non-durable endpoint fails with a
`PROTOCOL_VIOLATION` `*QwpDurableAckMismatchError` rather than silently falling
back. An idle `durable_ack_keepalive_interval_millis` ping
re-elicits pending durable frames.

Orphan-slot adoption (SF mode, `drain_orphans=on`) is implemented in
`qwp_sf_orphan.go` + `qwp_sf_drainer.go` + `qwp_sf_round_walk.go`; drainers run
in dedicated goroutines and are visible via `QwpSender.BackgroundDrainers()`.

### Error handling (NACK policy v2 — no drop, no lists, no dead senders)

QWP server rejections surface as `*SenderError` (`sender_error.go` is canonical
for categories + policy enum). Two paths: async callback registered via
`WithErrorHandler`, and producer-side typed error via `errors.As` after `Flush`
/ `FlushAndGetSequence`.

**There is no drop policy** by design. Three
policies: `RETRIABLE` (WRITE_ERROR, INTERNAL_ERROR, UNKNOWN fail-open) recycles
the connection and replays from `ackedFsn+1` through the wire-failure reconnect
machinery — dispatch is informational, nothing dropped, no watermark advance;
`RETRIABLE_OTHER` (NOT_WRITABLE, reserved wire byte 0x0C) same with endpoint
rotation; `TERMINAL` (SCHEMA_MISMATCH, PARSE_ERROR, SECURITY_ERROR,
PROTOCOL_VIOLATION) latches — reserved for rejections deterministic under
byte-identical replay, bytes preserved in the SF log.

Policy resolution precedence (highest first): `WithErrorPolicyResolver` →
`WithErrorPolicy(category, ...)` → connect-string `on_*_error` →
`on_server_error` → spec defaults. `PROTOCOL_VIOLATION` is forced TERMINAL and
`UNKNOWN` is forced RETRIABLE (fail open); user overrides for those two are
ignored.

**WS close codes carry no policy semantics** — every close is
reconnect-eligible (`qwpSfIsTerminalCloseCode` is diagnostics-only). The
guarded case — a frame that deterministically kills the connection without a
NACK — is caught behaviorally by the **poison-frame detector**: a retriable
NACK or non-orderly close (not 1000/1001) after at least one send, at the same
head-of-line FSN with no ack progress, counts a strike. Escalation to a typed
PROTOCOL_VIOLATION terminal naming the FSN requires **both**
`max_frame_rejections` (default 4; `WithMaxFrameRejections`) consecutive strikes
**and** an episode lasting at least `reconnect_max_duration_millis` — the
duration floor stops a transient rejection burst from burning every strike in a
second; below it the sender keeps recycling with capped backoff (a loud stall,
never a terminal). An ack **covering** the poisoned FSN resets the counter; a
lower ack (durable-mode replay re-OK-ing predecessors) does not. In durable mode
the head-of-line anchor is the highest OK-acked frame, not the durable
watermark, so replay re-OKs cannot reset the episode (`highestOkAckedFsn`).
`ackedFsn` advances **only** on server OKs (durable acks under
`request_durable_ack`) — never on any rejection.

A TERMINAL latches the typed error on the I/O loop; `sendLoopCheckError()`
surfaces it on the next producer call. The sender does not auto-resume — close
+ rebuild is the supported recovery (matches Java).

### Connection pooling

`sender_pool.go` (`LineSenderPool`) is the legacy **HTTP-only** pool — TCP/QWP
configs are rejected with `errHttpOnlySender`.

QWP pooling lives behind the **`QuestDB` facade** (`questdb.go`), ported from the
Java client. `Connect` /
`NewQuestDB(ctx, conf, opts...)` take one `ws`/`wss` cluster config and own two
elastic pools — `qwpSenderPool` (`qwp_sender_pool.go`) and `qwpQueryPool`
(`qwp_query_pool.go`) — plus a reaper (`qwp_pool_housekeeper.go`).
`BorrowSender` leases a `LineSender`; `BorrowQuery` leases a `*Query` that
delegates to the cursor/iterator API; `Close` on a lease returns it to the pool,
the real disconnect waits for `QuestDB.Close`. Leases are **generation-stamped**
so a stale handle can't corrupt a re-borrowed slot.

- **`lazy_connect=true`** (facade-only `Side.POOL` key; standalone clients
  accept-but-ignore it) tolerates a down server at startup: ingest gets
  `initial_connect_retry=async` injected and the read pool defaults to
  `query_pool_min=0` (connects on first borrow). `build()` rejects the two
  conflicts (non-`async` `initial_connect_retry`; explicit `query_pool_min>0`).
- **SF-in-pool** (`sf_dir` set): each slot gets `sender_id=<base>-<index>`
  (`slotInUse` bitmap), every pooled sender fences the in-range slot set out of
  orphan adoption (`lineSenderConfig.orphanDrainExclude` →
  `qwpSfScanOrphans`'s exclusion predicate), and crash-stranded in-range slots
  are recovered by binding an async self-recovering sender to each at
  construction (no dedicated recoverer; build never blocks). Hazard checklist
  A–I in the design doc §4.4 is the bar.
- Pool/facade connect-string keys: `sender_pool_min/max`, `query_pool_min/max`,
  `acquire_timeout_ms`, `idle_timeout_ms`, `max_lifetime_ms`,
  `housekeeper_interval_ms`, `lazy_connect` (all in `poolKeys`, `conf_parse.go`).

`connect_timeout` (COMMON key) bounds the TCP connect on HTTP + QWP dials (inert
on TCP, Java parity); `WithConnectionListener` + `connection_listener_inbox_capacity`
add a `SenderConnectionListener` event stream (`sender_connection_listener.go`)
over the generic `qwpDispatcher[T]` (`qwp_dispatcher.go`).

## Testing

QWP unit tests use `httptest.Server` to stand in for the QuestDB WebSocket
endpoint (`newQwpTestServer` in `qwp_sender_test.go`). ILP unit tests are pure.

`*_integration_test.go` files need Docker — they spin up real QuestDB via
testcontainers-go; HTTP/TCP suites sometimes launch haproxy via
`test/haproxy.cfg`.

Cross-language conformance: `interop_test.go` +
`test/interop/questdb-client-test` (submodule) — ILP vectors shared across
QuestDB client libraries.

`BenchmarkQwpSenderSteadyState` in `qwp_bench_test.go` asserts **0 allocs/op**
on the Table→Symbol→Column→At pipeline after warmup (pinned in
`TestQwpSenderSteadyStateZeroAllocs`). Preserve this: any new allocation in that
hot path moves to a reusable scratch buffer on `qwpLineSender` (see
`encodeInfoBuf` for the pattern).

`export_test.go` re-exports unexported identifiers (including `QwpSenderType`)
into the `questdb` package for black-box tests in package `questdb_test`. When
adding internals tests must reach, extend this file rather than making
production code public.

## Conventions

- Every `.go` file starts with the QuestDB Apache-2.0 license banner; preserve
  it when creating new files.
- Column/table/symbol name validation: ILP in `buffer.go`, QWP in
  `qwp_buffer.go`. The disallowed-character set is documented on each
  `LineSender` method.
- **Errors on the fluent API latch** — `Table` / `Symbol` / `*Column` keep
  returning the sender; the latched error surfaces on the next `At` / `AtNow` /
  `Flush`. Preserve this when adding methods.
