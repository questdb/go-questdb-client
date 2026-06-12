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
# Test*Suite entry point plus the method name.
go test -v -run TestIntegrationSuite/TestE2EValidWrites .
go test -v -run TestQwpIntegrationSuite .

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

**Cursor frames are self-sufficient** — full schema definitions plus the full
symbol dictionary from id 0, every flush. This is what makes
reconnect/replay/orphan-adoption safe across a fresh server connection.

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

Symbol-dict tracking (`maxSentSymbolId`, `batchMaxSymbolId`) is still in place,
and both fields are load-bearing. The encoder always passes `-1` as the
`maxSentId` arg of `encodeMultiTableWithDeltaDict` to force "full dict from id
0", but `batchMaxSymbolId` is the separate `batchMaxId` arg and bounds the dict
actually written: `writeDeltaDict` emits `globalDict[0..batchMaxSymbolId]`, so
dropping it would silently truncate the symbol dict. `maxSentSymbolId` is the
cross-flush high-water mark that `resetAfterFlush` rewinds `batchMaxSymbolId` to
(never to `-1`), so a later batch reusing only earlier symbols still writes the
full dict its rows reference. Both are also read by tests and external
observers, but that is incidental to their wire role.

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
background. This matches the Java spec (`design/qwp-cursor-durability.md`
decision #1: "flush() never waits for ACK; ACKs are async") and is uniform
across both the pending-rows and zero-pending branches and auto-flush — all
route through `enqueueCursor`; explicit `Flush` only additionally surfaces a
latched send-loop error eagerly. (`Flush` was an ACK barrier
through v4.2.0; that contract was dropped when the cursor/SF architecture made
local persistence, not the ACK, the durability guarantee.) `FlushAndGetSequence` returns the
published FSN — the upper bound of any `SenderError.ToFsn` for that batch;
**pair it with `AwaitAckedFsn` for server-ACK confirmation** (the dedicated
primitive now that `Flush` no longer blocks on ACKs).

Orphan-slot adoption (SF mode, `drain_orphans=on`) is implemented in
`qwp_sf_orphan.go` + `qwp_sf_drainer.go` + `qwp_sf_round_walk.go`; drainers run
in dedicated goroutines and are visible via `QwpSender.BackgroundDrainers()`.

### Error handling

QWP server rejections surface as `*SenderError` (`sender_error.go` is canonical
for categories + policy enum). Two paths: async callback registered via
`WithErrorHandler`, and producer-side typed error via `errors.As` after `Flush`
/ `FlushAndGetSequence`.

Policy resolution precedence (highest first): `WithErrorPolicyResolver` →
`WithErrorPolicy(category, ...)` → connect-string `on_*_error` →
`on_server_error` → spec defaults. `PROTOCOL_VIOLATION` and `UNKNOWN` are never
user-configurable — always HALT.

A HALT latches the typed error on the I/O loop; `sendLoopCheckError()` surfaces
it on the next producer call. The sender does not auto-resume — close + rebuild
is the supported recovery (matches Java).

### Connection pooling

`sender_pool.go` (`LineSenderPool`) is **HTTP-only by design** — TCP/QWP configs
are rejected with `errHttpOnlySender`. QWP has its own concurrency model and
doesn't participate.

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
