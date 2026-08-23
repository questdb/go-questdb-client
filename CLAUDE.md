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

# Static analysis (run by CI; build.yml owns the pinned version).
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
ILP protocol versions are client-selected and ILP-only. HTTP auto-negotiates
one; TCP requires an explicit `protocol_version` (or `WithProtocolVersion`)
before the binary types are reachable at all. Senders in `http_sender.go` /
`tcp_sender.go`, encoding in `buffer.go`.

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

Disk slots are manifest-backed: `sf-manifest.bin` holds the committed oldest
and active segment bases, `.ack-watermark` the cumulative ACK FSN. Recovery
validates the complete segment chain against those committed boundaries before
quarantining anything — a head or tail the manifest says is required fails
closed, while a file proven stale or stray may be removed or renamed. The
delete site (`qwpSfDiscardOpened`) re-verifies the boundary it is handed: a
head that matches no kept segment base (and is not the explicit
nothing-delivered sentinel) fails closed instead of deleting, and a head the
legacy migration synthesized licenses no torn-file deletion below it. A
foreground sender preserves a fail-closed slot under `<sf_dir>/quarantined/` and
starts a fresh slot; an orphan drainer writes the reason to a `.failed` sentinel
instead. Legacy unflagged Go slots migrate in place on first recovery, and
downgrading after that migration is unsupported. Quarantined `.corrupt` files
inside a live slot count against `sf_max_total_bytes` — when they exhaust the
budget no new segment is minted and the producer sees `ErrBackpressureTimeout`;
the operator regains space by deleting the evidence, the client never does.

Segment and manifest control points are durable even with
`sf_durability=memory`: initial creation, rotation, each trim batch, and a fully
drained close use header/manifest fsync plus directory barriers. Frame
publication and ordinary ACK cadence stay syscall-free; watermark sync happens
only when it covers a trim or final drain.

Close treats manager-worker quiescence as a cleanup barrier: a timed-out
manager join does not release worker-reachable mappings, side files, or the slot
flock — cleanup transfers to the worker exit, and `engineCloseCompleted()`
becomes true only once that cleanup has released the flock. Every incomplete
terminal close installs a per-engine retry owner, and those retries have no
deadline: a persistent local-disk fault holds the flock and any pool capacity
reservation until storage recovers or the process exits, because releasing
ownership early could race retained files. Deferred-close pool slots stay
reserved and count against capacity until re-probed, so
`housekeeper_interval_ms=0` does not leak capacity, and pool shutdown reports an
error wrapping `ErrSfCleanupPending` while any slot cleanup is still pending.
That report is a "not yet", not a verdict: `QuestDB.Close` re-probes the sender
pool on every later call and returns nil once the last slot lock is gone, which
is the only re-check left once the housekeeper has stopped. A standalone
sender's `Close` returns nil in the same situation, so **a nil `Close` does not
by itself mean the slot lock is released** — reopening the same slot can fail,
naming this process as the holder.

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
failure — are enumerated and enforced by the review-pr skill checklist. The
producer-visible errors from a running drain path are all local: SF-out-of-space
backpressure (`ErrBackpressureTimeout`) and local-storage durability failures
(`ErrSfDurability` — a rotation that cannot commit its header or manifest, or a
run of failed slot maintenance). Both are non-terminal: the rows stay pending
and the same call can be retried.

Drainer quarantine covers a slot proved inconsistent
(`qwpSfErrRecoveryFailClosed`) plus the drainer's own give-ups — auth reject,
durable-ack settle exhaustion, the no-progress watchdog, and a panic.
**Local-storage analog of Invariant B: an environmental fault never condemns a
slot — fail-closed comes only from what the slot's bytes prove.** A local I/O
fault while opening a slot — a full disk, a read-only mount, an exhausted fd
table, a vanished mount — leaves no `.failed` sentinel and quarantines
nothing: a drainer run just fails and the next foreground scan adopts the slot
again, while a foreground construction fails with a retriable error wrapping
`ErrSfDurability` and succeeds once the fault clears, all rows intact.
Availability waits on the operator; preservation wins by design. Legacy
(manifest-less) slots fail closed on any corrupt segment that could still hold
frames: with no committed boundaries nothing can show them delivered.

**A table block is self-describing** — the inline column definitions are its
authoritative schema. On egress the decoder parses the schema from a query's
first `RESULT_BATCH` and reuses it across that query's continuation batches,
resetting at every query start so a schema never leaks across query
boundaries.

**Delta symbol dictionary.** The dictionary is delta-encoded — each symbol id is
sent once per connection, so a frame carries only ids above the last-sent
watermark (monotonic, never reset). Delta mode is always on in memory mode; in
SF mode it depends on the per-slot `.symbol-dict` side-file
(`qwp_sf_symbol_dict.go`) being open, else the frame falls back to a full
self-sufficient dictionary.

On reconnect the fresh server has an empty dictionary, so the send loop keeps
an I/O-goroutine-owned mirror of every symbol it has sent and re-registers the
whole dictionary through table-less **catch-up frames** before any replay. Those
frames occupy wire seqs that map onto already-acked FSNs, so ack alignment
holds, and they stay outside the poison-strike gate — a catch-up frame must
never count as a send attempt at the head-of-line FSN. SF mode write-ahead
persists a frame's new symbols before publishing it.

The `.symbol-dict` chunk format is byte-compatible with the Java client's —
a hard cross-client constraint, not a local implementation choice. Recovery
trusts only the run of chunks whose checksums match and physically truncates the
untrusted tail, then rebuilds whatever ids came after it out of the surviving
frames themselves and writes the difference back while those frames are still on
disk. Content that cannot be trusted supplies no ids and is left byte-identical
on disk, so the frame scan alone decides whether the slot is recoverable. A
residual hole under a frame still waiting to be sent fails recovery before
connecting, and the pre-send **torn-dict guard** catches the same condition as a
terminal `PROTOCOL_VIOLATION`. The recovery parser deliberately accepts more
entries than the append-time cap, so a slot written by an older client keeps
every id at its own position.

Flush semantics: `Flush` / `FlushAndGetSequence` **never wait for the server
ACK** — they return once the batch is published into the cursor engine (in-RAM
for memory mode, on-disk for SF) and the send loop delivers + replays it in the
background. This matches the Java spec ("flush() never waits for ACK; ACKs are
async"). Pending-rows, zero-pending and auto-flush all take the same path, and
explicit `Flush` additionally surfaces a pending send-loop error eagerly.
`FlushAndGetSequence` returns the published FSN, the upper bound
of any `SenderError.ToFsn` for that batch; **pair it with `AwaitAckedFsn` for
server-ACK confirmation.**

Durable-ack (`request_durable_ack` / `WithRequestDurableAck`, QWP-only) shifts the
trim/replay/await watermark from the WAL-commit OK ACK to the server's
`STATUS_DURABLE_ACK` (object-storage upload), so under it `AckedFsn` /
`AwaitAckedFsn` / `Close`-drain confirm **durability**, not just commit. The trim
state machine is `qwpDurableTracker` in `qwp_sf_durable.go`: the send loop holds
each OK ack until covering durable frames arrive, a dropped OK-ack sequence fails
closed, and connecting to a non-durable endpoint fails with a `PROTOCOL_VIOLATION`
(`*QwpDurableAckMismatchError`) rather than silently falling back.

Orphan-slot adoption (SF mode, `drain_orphans=on`) lives in `qwp_sf_orphan.go`
+ siblings; each drainer runs in its own goroutine.

### Error handling (no drop, no lists, no dead senders)

QWP server rejections surface as `*SenderError`, both asynchronously and as a
producer-side typed error on the next flush. `sender_error.go` is canonical for
the categories and the policy enum.

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
  accept-but-ignore it) tolerates a down server at startup by mutating config
  under the caller: ingest gets `initial_connect_retry=async` injected and the
  read pool defaults to `query_pool_min=0` (connects on first borrow). `build()`
  rejects anything that contradicts either.
- **SF-in-pool** (`sf_dir` set): each slot gets `sender_id=<base>-<index>`,
  every pooled sender fences its in-range slots out of orphan adoption, and
  crash-stranded in-range slots are recovered by binding an async
  self-recovering sender to each at construction (build never blocks).
- Pool/facade connect-string keys live in `poolKeys` (`conf_parse.go`).

`WithConnectionListener` adds a `SenderConnectionListener` event stream
(`sender_connection_listener.go`) over the generic `qwpDispatcher[T]`
(`qwp_dispatcher.go`).

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
- **`Hazard A`–`Hazard I`** is shared vocabulary across the pool / SF comments
  and tests for the concurrency hazards those designs guard against. No document
  defines the letters; each is explained inline at its use sites.
- Use `WithCloseFlushTimeout` / `close_flush_timeout_millis`. `WithCloseTimeout`
  is a deprecated alias and the `close_timeout=` connect-string key is rejected.
