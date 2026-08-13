# QWP implementation review

Date: 2026-08-13

Reviewed revisions:

- Go client: `4f2723e` (`main`, equal to `origin/main`)
- Java client reference: `2489b243` (`main`)

Reference checkout: `/home/jara/devel/oss/java-questdb-client`

## Recommendation

The Go client implements bidirectional QWP and connection pooling, but disk
store-and-forward should remain experimental. It has three high-severity
correctness defects that must be fixed before the implementation is described as
production-safe or byte-compatible with the Java client:

1. Recovery can silently discard a corrupt boundary segment.
2. Shutdown can release a store-and-forward slot while its manager still uses it.
3. Go and Java use incompatible version-1 `.symbol-dict` formats.

Fix them in that order: fail-closed segment recovery, shutdown cleanup ownership,
then symbol-dictionary format alignment and migration.

## Capability summary

| Capability | Verdict | Notes |
| --- | --- | --- |
| Ingress | Supported, with type gaps | `QwpSender` implements binary QWP ingestion, acknowledgements, replay, failover, and extended column types. BINARY and IPv4 are not supported for ingestion. |
| Egress | Supported | `QwpQueryClient` implements streaming queries, `Exec`, typed binds, cancellation, compression, result decoding, and failover. BINARY and IPv4 results are decoded. |
| Connection pooling | Supported | `QuestDB` owns separate elastic ingest and query pools. Store-and-forward sender slots receive distinct identities. |
| Store-and-forward | Implemented but not safe enough for production | Disk segments, restart replay, acknowledgement watermarks, orphan draining, and durable-ACK negotiation exist. Recovery and shutdown violate data-safety invariants. |

## 1. Ingress

The public `QwpSender` interface extends `LineSender` with QWP-specific types and
acknowledgement/replay observability:

- `qwp_sender.go:37-214`
- `qwp_sender_cursor.go`
- `qwp_encoder.go`
- `qwp_sf_send_loop.go`

The implementation supports the main fixed-width types, strings, symbols,
decimals, geohashes, arrays, UUID, LONG256, and nanosecond timestamps. Flush
publishes a frame to the local cursor engine; it does not wait for a server ACK.
Callers that require confirmation use `FlushAndGetSequence` followed by
`AwaitAckedFsn`.

### Ingress parity gap

BINARY and IPv4 are decoder-only in Go:

- `qwp_constants.go:62-75`

The current Java sender supports both:

- `core/src/main/java/io/questdb/client/Sender.java:314-357`
- `core/src/main/java/io/questdb/client/Sender.java:638-662`
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java:984-1038`
- `core/src/main/java/io/questdb/client/cutlass/qwp/client/QwpWebSocketSender.java:2135-2196`

This is a capability gap rather than a corruption bug, but it means ingress is
not at full Java type parity.

## 2. Egress

Egress is a real public implementation rather than a protocol stub:

- Constructors: `qwp_query_client.go:557-578`
- `Query` and `Exec`: `qwp_query_client.go:900-980`
- Streaming cursor lifecycle: `qwp_query_client.go:1180-1245`
- Column-major result batches: `qwp_query_batch.go:213-285`
- BINARY accessor: `qwp_query_batch.go:557`
- IPv4 representation through `Int32`: `qwp_query_batch.go:370`

It supports typed bind values, streaming result batches, flow-control credits,
cancel-and-drain behavior, zstd decoding, query/exec kind checks, and transparent
SELECT replay after eligible transport failures. Connection-desynchronization is
surfaced and causes pooled workers to be evicted rather than reused.

## 3. Connection pooling

The high-level `QuestDB` facade owns independent pools for ingest and query:

- Facade and concurrent borrow contract: `questdb.go:55-68`
- Pool creation: `questdb.go:229-374`
- Borrow APIs: `questdb.go:404-414`
- Default pool sizes: `questdb.go:39-40` (`min=1`, `max=4`)
- Ingest pool: `qwp_sender_pool.go:41-55`
- Query lease: `qwp_query_pool.go:499-510`

Both pools are elastic, support acquisition timeouts and idle/lifetime reaping,
and use generation-stamped leases so stale handles cannot act on a reborrowed
slot. The ingest pool assigns a distinct sender ID to each store-and-forward
slot and fences live sibling slots from orphan adoption.

The shutdown bug below weakens the store-and-forward ownership guarantee, but
pooling itself is implemented and covered by focused tests.

## 4. Store-and-forward

The implementation contains the expected major components:

- Cursor engine and append backpressure: `qwp_sf_engine.go`
- Segment ring and restart recovery: `qwp_sf_ring.go`
- Background spare allocation and ACK-driven trim: `qwp_sf_manager.go`
- WebSocket replay loop: `qwp_sf_send_loop.go`
- ACK watermark: `qwp_sf_ack_watermark.go`
- Persisted delta-symbol dictionary: `qwp_sf_symbol_dict.go`
- Slot ownership: `qwp_sf_lock.go`
- Orphan discovery and draining: `qwp_sf_orphan.go`, `qwp_sf_drainer.go`

With `sf_dir` configured, frames are published into mmap-backed files before
wire transmission. Recovery reopens surviving frames and replays the unacknowledged
tail. Durable ACK negotiation can make the trim watermark follow server-durable
rather than ordinary ACKs.

### Durability qualification

Go currently accepts only `sf_durability=memory`:

- `conf_parse.go:720-736`

Despite the name, an `sf_dir` still uses disk-backed mmap files. `memory` means
the client relies on OS page-cache writeback: it is intended to survive a client
process restart but does not guarantee host/power-loss durability.

Current Java additionally supports periodic storage barriers:

- `core/src/main/java/io/questdb/client/Sender.java:2895-2948`

## Correctness findings

### High: corrupt boundary segments are silently discarded

`qwpSfOpenRing` skips files classified as `qwpSfErrSegmentCorrupt`:

- `qwp_sf_ring.go:163-235`

It checks FSN contiguity only between the surviving segments:

- `qwp_sf_ring.go:264-297`

That detects a missing middle segment, but it cannot detect that a skipped file
was the oldest or newest load-bearing segment. Recovery then seeds `ackedFsn`
from the lowest surviving base sequence:

- `qwp_sf_engine.go:286-305`

Consequences:

- A corrupt newest segment makes its persisted tail disappear with no gap.
- A corrupt oldest segment can make its frames look previously trimmed/ACKed.

An isolated regression probe created two one-frame segments at FSNs 0 and 1,
corrupted the newest segment's magic, and reopened the ring. Recovery logged the
skip and succeeded with:

```text
unsafe recovery succeeded after dropping newest persisted segment: publishedFsn=0
```

The current Java implementation records durable head and active boundaries in
an `SfManifest`. It defers corrupt-file quarantine until the surviving chain has
been validated, and fails closed when a corrupt file may be load-bearing:

- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/SegmentRing.java:255-413`

#### Required correction

Port the manifest-era Java recovery contract, including boundary records,
manifest-required segment marking, fail-closed handling for missing/corrupt
boundary members, and a deliberate legacy migration path. Merely changing every
corrupt file to a fatal error would be safe but would lose Java's distinction
between proven stray files and load-bearing corruption.

### High: close releases the slot before manager quiescence

`segmentManagerClose` sets `closed`, waits at most five seconds for the worker,
then returns without reporting whether the worker stopped:

- `qwp_sf_manager.go:161-193`

The worker performs segment creation and deletion outside the manager mutex:

- `qwp_sf_manager.go:323-372`
- `qwp_sf_manager.go:384-507`

`engineCloseInternal` assumes that an owned manager has joined. It then closes
the ring and watermark, removes residual files, and releases the slot lock:

- `qwp_sf_engine.go:705-794`

An isolated probe blocked the manager inside segment allocation. After the
five-second grace expired, engine close returned and a second owner acquired the
same slot while the first manager was still inside slot I/O:

```text
slot lock was released while the manager worker was still inside slot I/O
```

This opens a window where a replacement engine and a stale worker operate on the
same directory. Depending on where the old worker resumes, it can remove or
mutate files belonging to the replacement, or touch resources the closing
engine already unmapped.

The current Java implementation treats worker quiescence as a cleanup barrier.
If the bounded join expires, it transfers ring, watermark, segment-file, and
slot-lock cleanup to the worker's exit path. The slot remains locked until that
cleanup completes:

- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/CursorSendEngine.java:1020-1148`

#### Required correction

Make manager close report exact worker-quiescence state. When the worker has not
stopped, retain every worker-reachable resource and the slot lock, and transfer
terminal cleanup to the worker's exit path. A shared manager needs the analogous
per-ring service-pass barrier.

### High under cross-client recovery: `.symbol-dict` version 1 is incompatible

Go's version-1 format is:

```text
[magic u32][version u8][reserved 3]
repeated [symbolLength varint][UTF-8 bytes]
```

See `qwp_sf_symbol_dict.go:50-68` and its recovery parser at
`qwp_sf_symbol_dict.go:166-204`.

Current Java's version-1 format is chunked and checksummed:

```text
[magic u32][version u8][reserved 3]
repeated [entryCount varint][entryBytes varint][entries][crc32c u32]
```

See:

- `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/PersistedSymbolDict.java:57-75`

Because both formats use the same magic and version, Go does not reliably reject
Java bytes. An isolated probe encoded Java's one-symbol chunk for `"x"`. Go
accepted it with the wrong ID map:

```text
[]string{"\x02", "x"}
```

This can produce incorrect dictionary registration or a replay rejection when a
Java-created slot is adopted by Go. It also violates the explicit requirement to
remain byte-compatible with the merged Java format:

- `design/qwp-delta-symbol-dict.md:511-512`

#### Required correction

Adopt the current Java chunked CRC-32C format. Since the existing Go files use
the same version number, the migration must distinguish old Go bytes from current
Java bytes without silently guessing. If that cannot be proved unambiguously,
fail closed and require an explicit migration tool or a new versioned format.

## Validation performed

### Passed

```text
go vet ./...
```

Focused store-and-forward, pool, query, and result-batch tests:

```text
go test . -count=1 -run \
  '^(TestQwpSf|TestSfConf|TestSfDurability|TestQwpSenderPool|TestQwpQueryPool|TestQwpQueryHappyPath|TestQwpExecHappyPath|TestQwpColumnBatch)'
```

Result:

```text
ok github.com/questdb/go-questdb-client/v4 7.888s
```

### Aggregate-suite limitation

`go test ./...` exercised the QWP and integration tests but ended in `FAIL`
because both legacy interop tests could not open this missing fixture:

```text
./test/interop/questdb-client/ilp-client-interop-test.json
```

The aggregate output was large and truncated by the runner, so this review does
not claim a clean full-suite result.

### Regression probes

The three probes described above were run from an isolated temporary copy and
were intentionally written as expectations that a safe implementation would
satisfy. All three failed against `4f2723e`, confirming:

1. The corrupt newest segment is accepted and omitted.
2. Java dictionary bytes are accepted with the wrong symbol IDs.
3. The slot lock is released while the manager remains inside slot I/O.

The temporary copy was removed after validation; no probe files were added to
this repository.
