# CLAUDE.md

This file provides repository invariants and navigation pointers. Read the code
for implementation facts and the public documentation for API guarantees. For
shutdown, start with [README: QWP shutdown and ownership](README.md#qwp-shutdown-and-ownership)
and the relevant method, error, and callback Go docs. Flag disagreements between
contracts, implementation, tests, and PR claims.

Paths and symbols below are starting points, not exhaustive inventories. Discover
callers, interface implementations, wrappers, test helpers, and build-tagged code
before changing behavior. Do not treat a default, a particular execution path,
or a test fixture as a universal contract.

## Project

Go client library for QuestDB ingestion and querying:

- **ILP** is the legacy InfluxDB Line Protocol over HTTP/HTTPS and TCP/TCPS.
  **Feature-frozen:** new column types and protocol features belong in QWP;
  ILP is maintained for compatibility, not extended.
- **QWP** is QuestDB's binary columnar protocol over WS/WSS, with ingestion,
  querying, and QWP-only column types. **QWP is not a version of ILP.** Resolve
  supported types and protocol capabilities from public APIs and codec tests.

Module path: `github.com/questdb/go-questdb-client/v4`; the `/v4` suffix is required
in imports of this module. `go.mod` and CI define supported Go versions. Preserve
the minimum-version checks with `GOTOOLCHAIN=local` and the intentional absence of
a `toolchain` directive; do not let `go mod tidy` add one.

## Commands and validation

```bash
# Required for the shared ILP interoperability vectors.
git submodule update --init --recursive

go vet ./...

# Full tests; inspect fixture prerequisites and skips as described below.
go test -v ./...

# Examples of targeted selection; verify the intended tests actually ran.
go test -v -run TestIntegrationSuite/TestE2EValidWrites .
go test -v -run TestQwpIntegration .

# Allocation reporting for one QWP steady-state workload.
go test -bench BenchmarkQwpSenderSteadyState -benchmem -run '^$' .
```

Resolve Staticcheck's pinned version and invocation from
`.github/workflows/build.yml`, rather than maintaining another version pin here.
Inspect the relevant CI jobs for platform coverage, environment, and toolchain
requirements. A local pass does not validate an unexecuted platform job.

Integration fixtures differ. Container-backed suites need Docker; QWP live-server
fixtures can use an external server or launch a private JVM. Start with
`qwp_integration_test.go` and `qwp_fuzz_fixture_test.go` for resolution through
`QDB_FUZZ_ADDR`, `QDB_JAR`, `QDB_REPO`, or a sibling checkout, and for
`QDB_FUZZ_STRICT` skip-versus-fail behavior. Inspect the actual fixture used by the
selected suite: a successful command with skipped tests is not integration proof.

## Architecture and configuration

`LineSender` in `sender.go` is the common ingestion interface. `QwpSender` in
`qwp_sender.go` extends it with QWP-specific operations. Check the actual transport
before asserting the superset. Interface changes must cover discovered concrete
implementations, pooled leases, wrappers, and test adapters—not just the underlying
transport structs.

Standalone ingestion entry points are `LineSenderFromConf` and `NewLineSender`.
The `QuestDB` facade in `questdb.go` owns sender and query pools; standalone query
APIs live in `qwp_query_client.go`.

Configuration responsibilities are split:

- `conf_parse.go`: ingestion parsing and shared direction/pool key inventories.
- `qwp_query_conf.go`: query parsing, defaults, and validation.
- `questdb.go`: facade options, effective pool configuration, and startup validation.
- Public option setters and Go docs: programmatic configuration and precedence.

A key accepted for sharing between clients may be intentionally ignored in one
direction. Check all affected parsers, defaults, validators, options, and docs
when changing configuration; no single parser defines every client's behavior.

### ILP

Start with `http_sender.go`, `tcp_sender.go`, and `buffer.go`. HTTP supports
protocol negotiation; TCP requires explicit protocol selection for binary types.
Resolve version-specific behavior from configuration, encoding code, and tests.
Keep existing compatibility; do not add QWP features to ILP.

### QWP ingest and query

The `qwp_*.go` files contain the codec, transport, ingest cursor engine, and query
machinery; shared APIs, errors, configuration, and the facade also live elsewhere.

Do not conflate the negotiated QWP protocol version, endpoint path names, and Go
module version. Handshake headers and endpoint defaults live in `qwp_transport.go`;
`protocol_version` is an ILP-only configuration key. An endpoint's path suffix does
not determine the Go module or negotiated protocol version.

Ingest batch delivery uses the cursor engine and send loop in `qwp_sf_*.go` for
both memory-backed and disk-backed buffering. `qwp_sender_cursor.go` connects
encoding/publication to that engine. Query I/O is separate: start with
`qwp_query_io.go`, `qwp_query_decoder.go`, and `qwp_query_failover.go`.

Ingest table blocks carry full inline column definitions so replay is schema-safe
on a fresh connection. Symbol dictionary dependencies are separate; do not infer
a self-contained dictionary from a self-contained schema. Egress schema state
must not leak across query boundaries.

### Publication, ACKs, and errors

QWP `Flush` / `FlushAndGetSequence` publish without waiting for the batch's server
ACK. This does **not** mean publication is nonblocking or syscall-free: encoding,
backpressure, dictionary persistence, and segment rotation have their own costs
and failure paths. Read each method's docs for cancellation and error behavior.
Pair the FSN returned by `FlushAndGetSequence` with `AwaitAckedFsn` for server
confirmation; publication alone is not confirmation of delivery or durability.

**Acknowledgement governs trim and replay; rejection never advances the
watermark.** With durable ACK requested, ordinary OKs alone do not confirm the
required durability. Check the selected mode's covering acknowledgements in
`qwp_sf_durable.go` and `qwp_sf_send_loop.go`; do not silently fall back when the
server lacks the requested capability.

**There is no server-rejection drop policy.** Retriable `SenderError` notifications
are informational and asynchronous; terminal errors additionally latch for
producer-side reporting. A terminal sender does not auto-resume: close and rebuild
is the supported recovery. Check the public method docs for reporting points.

`sender_error.go` defines categories and policy semantics; `qwp_sf_classify.go`
defines default classification and policy resolution. Public setters and parsers
supply overrides. Preserve documented precedence and forced policies; do not turn
default policy choices into unoverrideable rules. See README's error-handling
section and the relevant policy/reconnect tests.

WS close codes alone do not select rejection policy. Repeated-frame-failure
escalation must satisfy its evidence and timing gates; a transport interruption
is not itself proof of a poisoned frame. Inspect strike attribution, progress
resets, and episode accounting in the send loop, including durable-mode replay.

### Store-and-forward safety

**Invariant B:** running senders, asynchronous initial connection, and
background/orphan drain paths retry transport outages and all-replica role-reject
windows **indefinitely with capped exponential backoff**. Transport-class failures
must not become terminal or quarantine a slot through a total retry-duration or
attempt-count limit. Bounded synchronous initial connection, sanctioned terminal
episodes, and explicit shutdown are distinct lifecycle cases. Resolve their exact
conditions from public docs, error policy, and the reconnect/drainer tests—not
from a duplicated terminal-category list.

**Environmental storage faults are not corruption evidence.** A local I/O failure
opening a slot must remain retriable without condemning its bytes. Recovery and
deletion decisions require evidence from the slot and its committed boundaries.
Legacy slots without committed boundaries cannot assume corrupt files were already
delivered. Start with `qwp_sf_recovery.go`, `qwp_sf_manifest.go`, and their tests.

Active-tail recovery retains the valid frame prefix and zeroes the unreadable
suffix in place after chain validation, even when the prefix is empty. This is
a deliberate discard policy: later intact frames can be unreachable and are
not preserved as evidence. Do not extend this policy to missing required
segments, chain gaps, or unvalidated sealed data. Keep descriptor-first writes
and the retry-marker durability barriers in `qwp_sf_segment.go`.

For disk-backed slots, preserve cross-client segment and dictionary format
compatibility. Use the referenced protocol/format specifications and compatibility
tests, not an unversioned assertion that behavior "matches Java". Legacy Go-slot
migration has an operational restriction: **downgrading after migration is
unsupported**.

Quarantined evidence is operator-owned; the client must not reclaim it to regain
capacity. `.corrupt` files inside live slots count against `sf_max_total_bytes`;
whole-slot copies under `quarantined/` do not. See README's "Quarantined slots" for
locations and operational recovery. Failed scans retain the previous accounting;
periodic reconciliation is not a hard detection-latency guarantee. Inspect manager
code and tests for scheduling and accounting details.

`ErrBackpressureTimeout` and `ErrSfDurability` identify non-terminal backpressure
and local-storage failures, not an exhaustive list of producer errors. Terminal
server errors and internal failures can also reach the producer. A segment-manager
worker stopped by an internal panic is terminal; do not wrap that failure in a
non-terminal sentinel merely to fit this list. Conversely, failure to match either
sentinel does not by itself establish that an error is terminal.
Unpublished rows remain pending for retry after these non-terminal errors; a failed
flush can already have published part of a batch. Do not resend published work as
though nothing happened.
An idle poll or unrelated successful operation must not hide unresolved storage
maintenance. Derive error persistence/clearing from the public contract and the
specific outstanding work in `qwp_sf_manager.go` and `qwp_sf_errors.go`.

Disk-backed control-point durability barriers remain required even with
`sf_durability=memory`. Do not remove ordering barriers on creation, rotation,
trim, or cleanup based on a steady-state performance assumption. Inspect the
manifest, segment, and platform fsync/directory-barrier implementations and tests
for the actual ordering and crash model. **Process restart, kernel crash, and
power loss are not equivalent guarantees.** Windows directory barriers are a
no-op; Darwin fsync does not force the drive cache as `F_FULLFSYNC` would. Do not
claim a uniform host-crash or power-loss guarantee across platforms.

### Symbol dictionaries and replay

Delta encoding avoids repeating dictionary entries during normal publication;
it is not an exactly-once-per-connection delivery guarantee. In SF mode, missing
or failed dictionary persistence can require full-dictionary frames, even while
a side-file handle remains open. See `qwp_sender_cursor.go` and
`qwp_sf_symbol_dict.go` for mode selection and failure handling.

Recovery must preserve symbol-id/name associations using trusted dictionary and
frame evidence; it must not invent entries or renumber ids referenced by retained
frames. Inspect `qwp_sf_recovered_dict.go`, dictionary recovery, and their tests
before changing truncation, reconstruction, or compatibility limits.

Reconnect must supply the dictionary needed by replay on a fresh connection.
Catch-up and replay can repeat entries. Preserve ACK/FSN alignment and keep
catch-up frames out of data-frame poison-strike accounting. The send loop owns
these wire interactions; inspect its actual handoffs rather than assuming a fixed
goroutine topology.

### Pooling and shutdown ownership

`LineSenderPool` in `sender_pool.go` is the legacy **HTTP-only** pool. QWP pooling
uses the `QuestDB` facade, with implementation entry points in
`qwp_sender_pool.go` and `qwp_query_pool.go`. Stale leases must not mutate or return
a subsequently borrowed slot; inspect generation guards and forwarding methods.

Startup behavior depends on the complete effective configuration, including pool
minimums, lazy connection, initial-connect mode, and persistent-slot recovery.
`lazy_connect` permits deferred connectivity, not disabled functionality; inspect
its validation and defaults in `questdb.go`. SF recovery connections can be
asynchronous while eager prewarming and local-storage construction still block.
Do not infer a nonblocking constructor from one flag or recovery path.

Pool-managed SF slot identity, orphan-adoption exclusion, recovery, and capacity
accounting must agree. Discover all lifecycle obligations, including construction,
leases, returns, and retained cleanup. Start with the sender pool, engine, manager,
and orphan-drainer implementations and tests.

Shutdown policy belongs in README's linked shutdown section and the public Close,
slot-release, error, and callback docs. Neither a cancelled wait nor a finished
attempt proves resource release. Ownership must survive timeouts and failures;
verify strong references, quiescence before unmap/reuse, and truthful results.
For implementation navigation, start with `qwp_sf_cleanup.go`, `qwp_sf_engine.go`,
`qwp_sf_manager.go`, sender/query pools, query client, and facade. Follow actual
ownership transfers rather than preserving a particular cleanup topology.

## Testing and conventions

- **QWP logging:** Route diagnostics through `qwpEffectiveLogger`, or a logger
  obtained from it, not package-level slog functions or raw configured loggers.
  Do not use diagnostic delivery or success as a condition for error
  classification or cleanup ownership. Review new logging sites for this
  convention; source-scanning tests are not intended to prove exhaustive
  compliance. This is not a guarantee of progress with blocking or re-entrant
  handlers; see the public logger-option docs for restrictions.
- Discover relevant unit, integration, interoperability, race, platform, and
  performance tests. Mock WebSocket fixtures are useful for protocol unit tests;
  they do not substitute for live-server coverage.
- Shared ILP conformance starts at `interop_test.go` and
  `test/interop/questdb-client-test`. Check applicable compatibility fixtures when
  changing wire or persistent formats.
- **Preserve QWP steady-state zero-allocation guarantees.** Start with
  `qwp_bench_test.go` and discover affected workloads, including pooled and
  edge-case variants. Benchmarks report allocations; tests assert regression
  thresholds and may have instrumentation-specific exclusions. Measure the actual
  workload, not just one convenient case. Choose allocation remedies based on
  ownership and measured costs, not a prescribed scratch field.
- Extend test-only exposure (starting with `export_test.go`) when tests need
  internals; do not make production APIs public solely for tests.
- Every new `.go` file needs the QuestDB Apache-2.0 license banner.
- Name validation starts in `buffer.go` for ILP and `qwp_buffer.go` for QWP.
  Preserve the restrictions documented on the public methods.
- **Fluent API errors latch:** `Table` / `Symbol` / `*Column` return the sender;
  the latched error surfaces on the next `At` / `AtNow` / `Flush`. Preserve both
  the reporting point and buffer-state safety.
- `Hazard` letters are local to their comment/test family, not a global taxonomy.
  Read the nearby definition and trace the actual hazard before changing guards.
- Use `WithCloseFlushTimeout` / `close_flush_timeout_millis`. `WithCloseTimeout`
  is a deprecated alias; the `close_timeout=` connect-string key is rejected.
- `examples.manifest.yaml` references compilable examples rendered by questdb.io.
  Keep referenced paths and filenames stable and build affected examples.
