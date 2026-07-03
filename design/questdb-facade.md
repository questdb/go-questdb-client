# QuestDB facade — pooled ingest + query

The `QuestDB` handle (`questdb.go`) owns two elastic connection pools over a
single `ws`/`wss` cluster config: `qwpSenderPool` (ingest) and `qwpQueryPool`
(egress), plus a reaper (`qwpPoolHousekeeper`). `Connect` / `NewQuestDB` build
it; `BorrowSender` / `BorrowQuery` lease from it; `Close` drains it.

This is a Go port of the Java client's pooled `Sender` / query facade. The wire
behaviour, connect-string keys, and lease semantics match; the concurrency
model is re-expressed in Go idiom (goroutines + channels + `sync.Mutex`, no
thread pool).

## 1. Handle

`QuestDB` is safe for concurrent use. `BorrowSender` returns a `LineSender`
(type-assertable to `QwpSender`); `BorrowQuery` returns a `*Query`. A lease's
`Close` returns the slot to the pool — the underlying connection is not torn
down until `QuestDB.Close`. Leases are generation-stamped (§4.4 Hazard B).

## 2. Configuration

The facade parses one connect string through **both** the ingest and egress
parsers, so a shared `ws`/`wss` string validates for both sides. Facade-owned
`Side.POOL` keys (`sender_pool_min/max`, `query_pool_min/max`,
`acquire_timeout_ms`, `idle_timeout_ms`, `max_lifetime_ms`,
`housekeeper_interval_ms`, `lazy_connect`) live in `poolKeys` and are
accepted-but-ignored by both standalone parsers. Pool-config resolution
precedence: `With*` option → connect-string key → default.

`lazy_connect=true` tolerates a down server at startup: ingest gets
`initial_connect_retry=async` injected (writes buffer via store-and-forward /
the cursor engine) and the read pool defaults to `query_pool_min=0` (connects
on first borrow). `build()` rejects the two conflicts: a non-`async`
`initial_connect_retry`, and an explicit `query_pool_min>0`.

## 3. Pool lifecycle

Both pools prewarm to `min`, grow to `max`, serve borrows with an acquire
timeout, and reap idle / over-age slots back to `min`. All slot bookkeeping
(`all`, `available`, the `slotInUse` reservation bitmap, generation counters,
in-flight-creation and pending-teardown counters) is guarded by one mutex per
pool; waiters block on a broadcast channel re-checked in a loop.

`Close` marks the pool closing, waits a bounded interval for outstanding leases
to return (acquire timeout, hard-capped at 5 s), then tears every slot down.
A lease that never returns is leaked with a log line rather than freed under a
live producer (§4.4 Hazard E). Teardown of a returning borrower's slot runs on
that borrower's goroutine, tracked so `Close` cannot return mid-teardown.

## 4. Store-and-forward in the pool

When `sf_dir` is set each slot gets `sender_id=<base>-<index>`; the per-sender
directory `<sf_dir>/<base>-<index>/` is the slot, on-disk-compatible with the
Java `MmapSegment` layout. Crash-stranded in-range slots (a previous run of the
same base left unacked `.sfa` data) are recovered at construction by binding an
async self-recovering sender to each — no dedicated recoverer goroutine, and
the build never blocks on a down server.

### 4.4 Hazard checklist

The SF-in-pool design is safe against the following hazards. Each is the bar a
change to the pool, the facade, or orphan adoption must still clear; the code
tags the guard site with the matching letter.

- **A — slot-directory collision.** Two live senders must never share a slot
  dir. Each pooled slot owns a distinct `<base>-<index>`, and
  `allocateSlotIndexLocked` reserves the index in the `slotInUse` bitmap before
  any dial. The reservation bitmap — not the advisory `flock` — is the primary
  defence: a crash-recovered sender reserves its index so a later borrow cannot
  re-pick it. `flock` is the cross-process backstop.

- **B — stale lease.** A handle to a returned-then-reborrowed slot must not
  corrupt the borrow that now owns it, nor double-return. Leases are
  generation-stamped: `giveBack` bumps the slot generation under the pool mutex,
  so a stale lease's return is dropped (idempotent `Close`) and its fluent calls
  no-op / error via the `live()` generation check. Concurrent use of one lease
  from multiple goroutines is a `LineSender` contract violation and outside this
  guarantee; the generation check is a stale-handle filter, not a mutex.

- **C — crash-recovery integrity.** A slot recovered after a crash must replay
  exactly its not-yet-acknowledged tail and never re-ship acknowledged data.
  Recovery seeds the engine from the persisted watermark and replays from
  `engineAckedFsn()+1` (the durable watermark under `request_durable_ack`); a
  rejection never advances the watermark, so the rejected frame replays rather
  than being skipped.

- **D — down server at construction.** Building the pool (or a crash-recovery
  sender) must not block on an unreachable server. Recovery senders connect
  async; `build()` returns immediately and the send loop retries the outage
  indefinitely with capped backoff.

- **E — teardown under a live producer.** `Close` must never tear down a slot a
  producer is still inside. It waits a bounded interval for outstanding leases,
  runs a returning slot's teardown on the borrower's own goroutine, and leaks
  (logs, does not free) a lease that never comes back.

- **F — capacity over-allocation.** The pool must never exceed `maxSize`.
  `capUsedLocked` counts available + in-use + in-flight-creation + closing +
  pending-teardown slots; a new creation is admitted only while that sum is
  below `maxSize`, so closing / leaked / in-flight slots cannot let it
  over-allocate.

- **G — draining a live sibling.** Orphan adoption must never drain a slot a
  live pooled sibling owns. Every pooled sender fences the pool's whole in-range
  slot set (`inRangeFence`) out of `qwpSfScanOrphans`. Out-of-range same-base
  dirs (a previous larger run) and foreign dirs stay drainable.

- **H — down server mid-life.** A running pool must survive a server outage
  without failing borrows or losing buffered data. The send loop and drainers
  retry transport outages indefinitely (Invariant B); the only producer-visible
  error from a running drain path is store-and-forward backpressure.

- **I — partial-build teardown.** A failure partway through `build()` must not
  leak the resources already built. The heavy build paths (`createSlotAt`,
  `createWorker`) are panic-guarded so a fault converts to an error the caller
  can clean up, and `build()` closes what it already built in reverse order on
  any failure.

## 5. Query pool

`BorrowQuery` leases a `*Query` delegating to the cursor/iterator query API.
A lease is generation-stamped like a sender lease. A worker is evicted (not
recycled) when its client latched a transport-terminal error
(`terminalError()`), exhausted failover (`*QwpFailoverExhaustedError`), or left
its single-stream wire desynced by an abandoned cleanup drain (`execDesynced()`).

## 6. Housekeeper

One reaper goroutine per handle sweeps both pools on `housekeeper_interval_ms`
(explicit `0` disables it), reaping idle-past-`idle_timeout_ms` and
over-`max_lifetime_ms` slots back to `min`. A memory-mode slot still holding
unacknowledged rows is spared so a transient outage does not destroy its in-RAM
tail. Reap victims are closed concurrently so a sweep fits the housekeeper's
join budget on `QuestDB.Close`.
