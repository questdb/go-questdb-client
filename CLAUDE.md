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
`go 1.23` with no `toolchain` directive; CI's `1.23.x`/`1.24.x` matrix runs
under `GOTOOLCHAIN=local`).

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

**`Flush` / `FlushAndGetSequence` never wait for the server ACK** — they return
once the batch is published to the cursor engine (in-RAM for memory mode,
on-disk for SF); delivery and replay to the server run in the background. For
server-ACK confirmation, pair `FlushAndGetSequence` with `AwaitAckedFsn`.

### Error handling

QWP server rejections surface as `*SenderError` (`sender_error.go` is canonical
for the categories + policy enum). `PROTOCOL_VIOLATION` and `UNKNOWN` are never
user-configurable — always HALT. A HALT records the typed error on the I/O loop
and surfaces it on the next producer call; the sender does not auto-resume, so
close + rebuild is the supported recovery. Policy precedence, highest first:
`WithErrorPolicyResolver` → `WithErrorPolicy` → connect-string `on_*_error` →
`on_server_error` → spec defaults.

### Connection pooling

`sender_pool.go` (`LineSenderPool`) is **HTTP-only by design** — TCP/QWP configs
are rejected with `errHttpOnlySender`. QWP has its own concurrency model and
doesn't participate.

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
