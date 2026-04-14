# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Go client library for QuestDB ingestion. Three transports are supported:

- **HTTP / HTTPS** — InfluxDB Line Protocol (ILP), recommended for most workloads.
- **TCP / TCPS** — ILP over raw TCP, kept for low-overhead deployments.
- **WS / WSS (QWP)** — QuestDB's binary **columnar** wire protocol over
  WebSocket. Higher throughput than ILP for wide rows, and the only
  transport that exposes the full QuestDB type system (int8/int16/int32,
  float32, char, date, timestamp-nanos, uuid, varchar, geohash, int64
  arrays).

Module path: `github.com/questdb/go-questdb-client/v4` — the `/v4` segment
is load-bearing; keep the suffix when importing within this repo. Minimum
Go version: **1.23** (go.mod pins `go 1.23` with a `1.24.4` toolchain
directive).

## Commands

```bash
# Fetch the interop test vectors (required for interop_test.go).
git submodule update --init --recursive

# Static analysis (run by CI).
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.4.3 ./...

# Full test suite. Integration tests (both ILP and QWP) spin up QuestDB
# containers via testcontainers-go, so Docker must be running locally.
go test -v ./...

# Run a single test or suite. testify suites are dispatched via the
# top-level Test*Suite entry point plus the method name.
go test -v -run TestIntegrationSuite/TestE2EValidWrites .
go test -v -run TestQwpIntegrationSuite .
go test -v -run TestHttpHappyCasesFromConf .

# Benchmarks — the QWP hot path is allocation-tracked.
go test -v -bench BenchmarkQwpSenderSteadyState -benchmem -run ^$ .
```

There is no Makefile or build step — consumers import the package
directly. The `examples/` tree (`from-conf`, `http/...`, `tcp/...`) holds
compilable sample `main.go` files referenced by `examples.manifest.yaml`,
which questdb.io uses to render docs, so keep paths and filenames stable
when editing examples.

## Architecture

The public surface is the `LineSender` interface defined in `sender.go`.
All fluent builder methods (`Table`, `Symbol`, `*Column`, `At`, `AtNow`,
`Flush`, `Close`) are declared there; every transport implementation must
satisfy it. QWP adds a **superset** interface `QwpSender` (in
`qwp_sender.go`) with the extra column types listed above — callers that
want QWP-only columns must type-assert the returned sender to
`QwpSender`.

### Transports and protocol versions

Two factories are the only entry points:

- `LineSenderFromConf(ctx, "schema::addr=...;key=value;...")` — parses
  the config string in `conf_parse.go`. Supported schemas: `http`,
  `https`, `tcp`, `tcps`, `ws`, `wss`.
- `NewLineSender(ctx, opts...)` — functional options. One of `WithHttp`,
  `WithTcp`, or `WithQwp` is required; a missing sender type returns
  *"sender type is not specified: use WithHttp, WithTcp, or WithQwp"*.
  `NewLineSender` makes two passes over the options: the first discovers
  the transport so per-transport defaults can be applied, the second
  applies every option against the seeded config.

Both funnel through `lineSenderConfig` and `newLineSender` in
`sender.go`, which dispatches to per-transport sanitizers
(`sanitizeHttpConf`, `sanitizeTcpConf`, `sanitizeQwpConf`) and
constructors (`newHttpLineSender`, `newTcpLineSender`,
`newQwpLineSenderFromConf`).

**ILP protocol versions.** HTTP and TCP transports each have three
concrete structs, one per ILP protocol version: V1 is text-only, V2 adds
binary `float64` and n-dimensional `float64` arrays, V3 adds decimals.

- `httpLineSender`, `httpLineSenderV2`, `httpLineSenderV3` — `http_sender.go`
- `tcpLineSender`, `tcpLineSenderV2`, `tcpLineSenderV3` — `tcp_sender.go`

HTTP auto-negotiates the protocol version with the server; TCP requires
`WithProtocolVersion(ProtocolVersion2|3)` or
`protocol_version=2|3` in the config string. When adding a new column
type or ILP feature, expect to touch all six ILP structs, the
`LineSender` interface, `buffer.go` (raw ILP encoding), and the
`Messages` / `MsgCount` / `BufLen` / `ProtocolVersion` switch helpers in
`export_test.go`.

### QWP (WebSocket columnar protocol)

QWP is not a version of ILP — it is a distinct binary protocol with its
own framing, codecs, and server handshake. Everything QWP lives in
`qwp_*.go`:

- `qwp_constants.go` — magic (`"QWP1"`), header flags (LZ4, Zstd, Gorilla
  timestamp encoding, delta symbol dictionary), type codes, and ACK
  status codes.
- `qwp_wire.go` + `qwp_varint.go` — low-level wire primitives; little-
  endian fixed-width writers and varint encoding.
- `qwp_buffer.go` — `qwpColumnBuffer` (per-type columnar storage,
  bit-packed booleans, offset+data for strings, separate null bitmap)
  and `qwpTableBuffer` (gap-fill, row cancel, per-table schema). This
  replaces the ILP text buffer for QWP senders; the same hot-path
  discipline applies but the data is stored in columnar form until the
  encoder serializes a batch.
- `qwp_encoder.go` — builds a multi-table QWP message from a set of
  table buffers in one flush.
- `qwp_hash.go` — XXHash64-based schema hashing. The sender caches
  `(tableName, schemaHash)` keys for schemas the server has already
  ACKed; subsequent batches for that table use *reference mode* and
  skip re-sending the schema.
- `qwp_transport.go` — WebSocket transport built on
  `github.com/coder/websocket` (the only non-stdlib runtime dependency
  for QWP). Reads 9-byte ACK frames (1-byte status + 8-byte cumulative
  sequence number). Supports an optional dump writer that records all
  outgoing bytes including the HTTP upgrade handshake.
- `qwp_errors.go` — `QwpError` with typed status codes parsed from ACKs.
- `qwp_sender.go` — `qwpLineSender` (implements both `LineSender` and
  `QwpSender`), with *double-buffered* encoders so async mode can encode
  batch N+1 while batch N is flying. Sync mode uses only `encoders[0]`.
- `qwp_sender_async.go` — `qwpAsyncState`, the dedicated I/O goroutine
  (`ioLoop`), and the non-blocking-enqueue / blocking-drain split.
  Cancellable via context; `Close()` waits up to `closeTimeout`
  (default 5s) before force-cancelling. Schema and symbol caches are
  *only* advanced after the server ACKs the batch, so retries after
  failure don't produce dangling references.

Async mode is enabled by `WithInFlightWindow(n)` or `in_flight_window=n`
in the config, with `n > 1`. `WithInFlightWindow(1)` (the default)
keeps the sender fully synchronous: each `Flush` blocks until the ACK
arrives.

Delta symbol dictionaries send only new symbols since the last ACK;
this is why symbol/schema cache updates must be gated on ACK — a
rejected batch must not mark symbols as "known to the server."

### Config string reference

`conf_parse.go` is the single source of truth for supported keys.
Non-obvious behaviors:

- `username`, `password`: Basic auth for HTTP **and QWP**; for TCP,
  `username` is the ECDSA key ID and `token` is the secret (`D`) value.
- `token`: Bearer token for HTTP and QWP; ECDSA secret for TCP.
- `in_flight_window`, `close_timeout` (ms): QWP-only.
- `protocol_version=auto|1|2|3`: ILP-only.
- `tls_roots`, `tls_roots_password`: explicitly rejected — the Go
  client uses the system cert pool via `crypto/tls` defaults.

### Connection pooling

`sender_pool.go` provides `LineSenderPool` (`PoolFromConf`,
`NewLineSenderPool`). It is HTTP-only by design — constructors reject
TCP configs with `errHttpOnlySender`. QWP has its own in-flight-window
concurrency model and does not participate in the pool. The HTTP
transport itself is shared across all `httpLineSender*` instances via
the `globalTransport` singleton, which closes idle connections when the
last sender is released.

### Value types

- `decimal.go` — QuestDB's arbitrary-precision `Decimal`, the
  `ShopspringDecimal` adapter, and `NewDecimalFromString` /
  `NewDecimalFromFloat` constructors. Used by both ILP V3
  (`DecimalColumn*` methods) and QWP (which transmits the fixed-width
  Decimal64/128/256 wire forms).
- `ndarray.go` — generic `NdArray[T]` used by `Float64ArrayNDColumn`.
  1D/2D/3D convenience methods wrap it. `MaxArrayElements` (`(1 << 28)
  - 1`) caps total element count. QWP additionally supports
  `Int64Array{1,2,3}DColumn` via the same columnar buffer machinery.

## Testing layout

- `buffer_test.go`, `conf_test.go`, `tcp_sender_test.go`,
  `http_sender_test.go`, `sender_pool_test.go`, `ndarray_test.go`,
  `qwp_buffer_test.go`, `qwp_encoder_test.go`, `qwp_sender_test.go`,
  `qwp_sender_async_test.go`, `qwp_wire_test.go`, `qwp_varint_test.go`,
  `qwp_errors_test.go`, `qwp_transport_test.go` — pure unit tests, no
  Docker required. QWP unit tests use `httptest.Server` to stand in for
  the QuestDB WebSocket endpoint (`newQwpTestServer` in
  `qwp_sender_test.go`).
- `integration_test.go`, `http_integration_test.go`,
  `tcp_integration_test.go`, `qwp_integration_test.go` — boot real
  QuestDB via testcontainers-go (HTTP/TCP suites sometimes also launch
  haproxy via `test/haproxy.cfg`). These require Docker and pull
  images on first run.
- `interop_test.go` + `test/interop/questdb-client-test` (git submodule)
  — cross-language ILP conformance vectors shared across all QuestDB
  client libraries.
- `qwp_bench_test.go` — `BenchmarkQwpSenderSteadyState` asserts **0
  allocs/op** on the Table→Symbol→Column→At pipeline after warmup.
  Preserve this invariant: any new allocation in that hot path should
  be moved to a reusable scratch buffer on `qwpLineSender` (see
  `encodeInfoBuf`, `pendingSchemaKeysBuf` for the pattern).
- `export_test.go` re-exports unexported identifiers (including
  `QwpSenderType`) into the `questdb` package for black-box tests in
  package `questdb_test`. When adding internals tests must reach,
  extend this file rather than making production code public. The
  `Messages` / `MsgCount` / `BufLen` / `ProtocolVersion` helpers switch
  across all concrete sender types — keep them exhaustive.

## Conventions

- Every `.go` file starts with the QuestDB Apache-2.0 license banner;
  preserve it when creating new files.
- Column/table/symbol names have an explicit disallowed-character set
  documented on each `LineSender` method. ILP validation lives in
  `buffer.go`; QWP validation lives in `qwp_buffer.go`.
- Errors returned from ILP methods are **latched on the buffer** — the
  fluent API keeps returning the same sender, and the error surfaces on
  the next `At`/`AtNow`/`Flush`. QWP follows the same pattern on its
  per-row builder. Preserve this when adding methods.
- QWP schema/symbol cache updates are gated on server ACK. Never advance
  them speculatively from the enqueue side — a lost batch after a cache
  bump will corrupt every subsequent message for that table.
