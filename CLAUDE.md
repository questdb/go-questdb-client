# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository. It captures invariants and "where to look" pointers —
for specifics (file contents, constants, config-key catalog, error categories)
read the code, which is authoritative.

## Project

Go client library for QuestDB ingestion, across two protocol families:

- **ILP** — the legacy InfluxDB Line Protocol, over **HTTP / HTTPS** and **TCP /
  TCPS**. **Feature-frozen: new column types and protocol features land in QWP
  only**; ILP is maintained for compatibility, not extended.
- **QWP** — QuestDB's binary columnar wire protocol over **WS / WSS**, and where
  active development happens. The only transport exposing QuestDB's full type
  system — the numeric, temporal, uuid, varchar, geohash and array types ILP
  text can't express. **QWP is not a version of ILP** — distinct framing,
  codecs, and server handshake.

Module path: `github.com/questdb/go-questdb-client/v4` — the `/v4` segment is
required in imports within this repo. go.mod deliberately pins `go 1.23` with
**no `toolchain` directive** (CI guards with `GOTOOLCHAIN=local`); don't let
`go mod tidy` regrow one.

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

Two entry points: `LineSenderFromConf(ctx, "schema::addr=...;key=value")` (the
schema selects the transport) and `NewLineSender(ctx, opts...)` (one of
`WithHttp` / `WithTcp` / `WithQwp`). **`conf_parse.go` is the single source of
truth for supported config keys and schemas.**

### ILP (HTTP / TCP) — legacy, feature-frozen

Maintained for compatibility; new column types and features go to QWP, not here.
ILP protocol versions (client-selected, ILP-only): V1 text, V2 adds binary
`float64` + float arrays, V3 adds decimals. HTTP auto-negotiates the version; TCP
requires `protocol_version=2|3` (or `WithProtocolVersion`) for the binary types.
Senders in `http_sender.go` / `tcp_sender.go`, encoding in `buffer.go`.

### QWP (WebSocket columnar protocol)

Everything QWP lives in `qwp_*.go`: a columnar codec stack (buffer, encoder,
wire, transport) under the sender (`qwp_sender.go` + `qwp_sender_cursor.go`),
which implements `LineSender` and `QwpSender`.

**Three separate "version" numbers live around QWP — don't conflate them:** the
QWP protocol version (one version, negotiated at the WS upgrade via
`X-QWP-Version`; the `protocol_version=` key is ILP-only and rejected for QWP),
the endpoint **path names** `/write/v4` (ingest) and `/read/v1` (egress) in
`qwp_transport.go` — literal paths, not versions — and the `/v4` Go module
version. The `4` in `/write/v4` is unrelated to the module's `/v4`.

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
delta-encoded; see "Delta symbol dictionary" below.

**Invariant B (store-and-forward robustness):** a running sender, async
initial connect, and every background/orphan drainer retry transport outages
and all-replica role-reject windows **indefinitely** with capped exponential
backoff — no wall-clock give-up, no terminal error, no `.failed` quarantine
for transport-class failures. `reconnect_max_duration_millis` bounds only the
blocking sync initial connect; on a running sender it does not bound reconnect
(it also serves as the poison-frame episode floor and the drainer no-progress
budget). The sanctioned terminals — auth reject, upgrade reject, durable-ack
capability-gap exhaustion, poison-frame escalation, and drainer slot-recovery
failure — are enumerated and enforced by the review-pr skill checklist. The only
producer-visible error from a running drain path is SF-out-of-space backpressure.

**A table block is self-describing** — the inline column definitions are its
authoritative schema. On egress, the decoder parses the schema from the first
`RESULT_BATCH` of a query and reuses it for that query's continuation batches,
resetting it at the start of every query so a schema never leaks across query
boundaries (see `resetQuerySchema` in the egress dispatcher).

**Delta symbol dictionary.** The dictionary is delta-encoded — each symbol id is
sent once per connection, so a frame carries only ids above the last-sent
watermark (monotonic, never reset). Delta mode is always on in memory mode; in
SF mode it depends on the per-slot `.symbol-dict` side-file
(`qwp_sf_symbol_dict.go`) being open, else the frame falls back to a full
self-sufficient dictionary.

On reconnect the fresh server's dictionary is empty, so the send loop keeps its
own mirror of every symbol sent and re-registers the whole dictionary via
table-less **catch-up frames** before replaying; the frames map onto
already-acked FSNs, so ack accounting stays aligned. SF mode persists a frame's
new symbols before publishing and rebuilds from side-file plus surviving frames
on recovery. If recovery leaves a frame needing symbols the dictionary lacks,
the **torn-dict guard** fails it before send with a terminal `PROTOCOL_VIOLATION`
("resend required"). Mechanics: `sendDictCatchUp` and the torn-dict guard in
`qwp_sf_send_loop.go`.

Flush semantics: `Flush` / `FlushAndGetSequence` **never wait for the server
ACK** — they return once the batch is published into the cursor engine (in-RAM
for memory mode, on-disk for SF) and the send loop delivers + replays it in the
background. This matches the Java spec ("flush() never waits for ACK; ACKs are
async"); every path — pending-rows, zero-pending, and auto-flush — routes through
`enqueueCursor`, and explicit `Flush` additionally surfaces a pending send-loop
error eagerly. `FlushAndGetSequence` returns the published FSN, the upper bound
of any `SenderError.ToFsn` for that batch; **pair it with `AwaitAckedFsn` for
server-ACK confirmation.**

Durable-ack (`request_durable_ack` / `WithRequestDurableAck`, QWP-only) shifts the
trim/replay/await watermark from the WAL-commit OK ACK to the server's
`STATUS_DURABLE_ACK` (object-storage upload), so under it `AckedFsn` /
`AwaitAckedFsn` / `Close`-drain confirm **durability**, not just commit. The trim
state machine is `qwpDurableTracker` in `qwp_sf_durable.go`: the send loop holds
each OK ack until covering durable frames arrive, a dropped OK-ack sequence fails
closed, and connecting to a non-durable endpoint fails with a `PROTOCOL_VIOLATION`
(`*QwpDurableAckMismatchError`) rather than silently falling back. An idle
`durable_ack_keepalive_interval_millis` ping re-elicits pending durable frames.

Orphan-slot adoption (SF mode, `drain_orphans=on`, in `qwp_sf_orphan.go` +
siblings): drainers run in dedicated goroutines, observable via
`QwpSender.BackgroundDrainers()`.

### Error handling (no drop, no lists, no dead senders)

QWP server rejections surface as `*SenderError` (`sender_error.go` is canonical
for categories + policy enum). Two paths: async callback registered via
`WithErrorHandler`, and producer-side typed error via `errors.As` after `Flush`
/ `FlushAndGetSequence`.

**There is no drop policy** by design. Three policies, with the category→policy
mapping canonical in `sender_error.go`: `RETRIABLE` recycles the connection and
replays from `ackedFsn+1` through the wire-failure reconnect machinery (nothing
dropped, no watermark advance); `RETRIABLE_OTHER` does the same with endpoint
rotation; `TERMINAL` stops the sender, and is reserved for rejections that are
deterministic under byte-identical replay (their bytes are preserved in the SF
log).

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
`PROTOCOL_VIOLATION` naming the FSN requires **both** `max_frame_rejections`
(`WithMaxFrameRejections`) consecutive strikes **and** an episode lasting at
least `reconnect_max_duration_millis`, so a transient rejection burst can't spend
every strike in a second; below that floor the sender keeps recycling with capped
backoff. An ack **covering** the poisoned FSN resets the counter; a lower ack
(durable-mode replay re-acking predecessors) does not. `ackedFsn` advances
**only** on server OKs — never on any rejection.

A TERMINAL latches the typed error on the I/O loop; `sendLoopCheckError()`
surfaces it on the next producer call. The sender does not auto-resume — close
+ rebuild is the supported recovery (matches Java).

### Connection pooling

`sender_pool.go` (`LineSenderPool`) is the legacy **HTTP-only** pool — TCP/QWP
configs are rejected with `errHttpOnlySender`.

QWP pooling lives behind the **`QuestDB` facade** (`questdb.go`), ported from the
Java client. `Connect` / `NewQuestDB(ctx, conf, opts...)` take one `ws`/`wss`
cluster config and own two elastic pools (senders + queries) plus a reaper.
`BorrowSender` leases a `LineSender`; `BorrowQuery` leases a `*Query` over the
cursor/iterator API; `Close` on a lease returns it to the pool, and the real
disconnect waits for `QuestDB.Close`. Leases are **generation-stamped** so a
stale handle can't corrupt a re-borrowed slot.

- **`lazy_connect=true`** (facade-only `Side.POOL` key; standalone clients
  accept-but-ignore it) tolerates a down server at startup: ingest gets
  `initial_connect_retry=async` injected and the read pool defaults to
  `query_pool_min=0` (connects on first borrow). `build()` rejects the two
  conflicts (non-`async` `initial_connect_retry`; explicit `query_pool_min>0`).
- **SF-in-pool** (`sf_dir` set): each slot gets `sender_id=<base>-<index>`,
  every pooled sender fences its in-range slots out of orphan adoption, and
  crash-stranded in-range slots are recovered by binding an async
  self-recovering sender to each at construction (build never blocks). Hazard
  checklist A–I in the design doc §4.4 is the bar.
- Pool/facade connect-string keys live in `poolKeys` (`conf_parse.go`).

`connect_timeout` (COMMON key) bounds the TCP connect on HTTP + QWP dials (inert
on TCP, Java parity); `WithConnectionListener` + `connection_listener_inbox_capacity`
add a `SenderConnectionListener` event stream (`sender_connection_listener.go`)
over the generic `qwpDispatcher[T]` (`qwp_dispatcher.go`).

## Testing

QWP unit tests use `httptest.Server` to stand in for the QuestDB WebSocket
endpoint (`newQwpTestServer` in `qwp_sender_test.go`). ILP unit tests are pure.

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
