# QWP delta symbol dictionary (port of java-questdb-client PR #66)

Status: **implemented** — landed across `qwp_sf_symbol_dict.go`,
`qwp_sf_engine.go`, `qwp_sender{,_cursor}.go`, `qwp_sf_send_loop.go` with tests
in `qwp_sf_symbol_dict_test.go` / `qwp_delta_dict_test.go`.
The original port was reviewed against the checked-out Java branch
`qwp-delta-symbol-dict` at `de86197`; post-merge parity fixes from PR #66's
final `37d4b0a` state are recorded in §12.
Parity target: `java-questdb-client` PR #66 "feat(qwp): stop resending the
full symbol dictionary on every message".

## 1. Motivation

Today every QWP ingress frame carries the **entire** symbol dictionary from
id 0 (`encodeMultiTableWithDeltaDict(..., maxSentId=-1, batchMaxId=…)` in
`qwp_sender_cursor.go:564,655`). For a connection that ingests many distinct
symbols this re-transmits the whole dictionary on every flush. The saving from
delta-encoding grows with symbol cardinality × frame count; for low-cardinality
or short-lived connections it is negligible.

This is a **client-side bandwidth optimization only**. The wire format and the
server are unchanged:

- The Go encoder already sets `FLAG_DELTA_SYMBOL_DICT` (`0x08`,
  `qwp_constants.go:214`) on every frame and already writes the dict as
  `[deltaStart varint][deltaCount varint][symbols…]` (`writeDeltaDict`,
  `qwp_encoder.go:179`). Sending the full dict is just the special case
  `deltaStart=0` every frame.
- The server therefore already interprets the section as a delta and
  accumulates ids across frames on a connection, resetting on a fresh
  connection. Java's own file-mode fallback (send full frames when the
  side-file can't open) proves full-dict frames stay valid forever, so there
  is **no forced migration** and full wire interop with any server is kept.

The optimization delta-encodes: each symbol id is registered with the server
**once per connection**; subsequent frames carry only new ids. Because a delta
frame is **not self-sufficient** (it references ids registered by earlier
frames), reconnect/replay/orphan-drain — which land on a **fresh** server with
an empty dictionary — must first **re-register the whole dictionary** before
replaying delta frames. Two mechanisms provide that: an in-memory **catch-up
frame** on reconnect (both modes) and a per-slot **persisted dictionary**
side-file (SF/file mode only).

## 2. Invariant being relaxed

CLAUDE.md records: *"Cursor frames are self-sufficient — full schema plus the
full symbol dictionary from id 0, every flush. This is what makes
reconnect/replay/orphan-adoption safe across a fresh server connection."*

This design **narrows** that invariant to the *schema* only. After the change:

- **Schema stays fully self-sufficient** — every table block still repeats its
  full inline column definitions on every frame. Untouched.
- **The symbol dictionary becomes delta-encoded.** Self-sufficiency for the
  dictionary is reconstructed at the wire boundary by the catch-up frame
  (memory + SF) and, for on-disk frames that outlive the process, by the
  persisted side-file (SF).

CLAUDE.md's "Symbol-dict tracking" paragraph is updated: the encoder no longer
"always passes `-1`"; in delta mode it passes the producer's sent watermark.

## 3. Wire format (unchanged — reference)

Frame = 12-byte header + delta-dict section + table blocks.

```
Header (12 bytes, little-endian):
  [0..4)  magic  u32 = qwpMagic (0x31505751)
  [4]     version u8 = qwpVersion (0x01)
  [5]     flags   u8 = FLAG_DELTA_SYMBOL_DICT(0x08) | FLAG_GORILLA(0x04)
  [6..8)  tableCount u16
  [8..12) payloadLen u32   (bytes after the header)

Delta-dict section (immediately after header):
  [deltaStart varint][deltaCount varint]
  deltaCount × [len varint][utf8 bytes]     # ids deltaStart .. deltaStart+deltaCount-1
```

A **catch-up frame** is a well-formed frame with `tableCount=0` carrying the
whole dictionary as one delta chunk `[deltaStart=chunkStart][deltaCount=n][…]`
(possibly split across several frames, §5.3). The server accumulates chunks
exactly as it would per-frame deltas. This is the same shape as the existing
`tableCount=0` commit frame, which the server already acks and (durable mode)
trivially confirms.

## 4. Enablement gating (`deltaDictEnabled`)

Mirror Java `CursorSendEngine.isDeltaDictEnabled()`:

- **Memory mode** (`sfDir == ""`): always enabled. Reconnect replays from the
  in-process ring; the send loop's in-RAM mirror rebuilds the dictionary via a
  catch-up frame. No persistence needed.
- **SF/file mode** (`sfDir != ""`): producer delta encoding is enabled **iff**
  the persisted dict side-file opened. If it is absent or unusable, newly
  published frames fall back to full self-sufficient dictionary sections. On
  recovery the engine first folds the side-file's trusted prefix together with
  every surviving frame (§6.4), so an old delta backlog can still be proven
  safe and drained even when the side-file itself disappeared.

The flag is resolved once at engine bind and controls only the producer
(`qwpLineSender`). The send loop parses and mirrors every delta-formatted frame
independently of that flag: recovered delta frames and new full-dictionary
fallback frames can coexist in the same slot.

No new connect-string key. Java exposes no knob; parity = always-on where
available. (Optional future safety valve `delta_symbol_dict=on|off` is noted in
§10 but **not** in scope — adding one would diverge from Java.)

## 5. Memory-mode design

### 5.1 Producer: emit deltas

`qwpLineSender` already carries the exact state needed
(`qwp_sender.go:281-288`):

- `globalSymbolList []string` — id→name, dense/ascending/stable (`id =
  len(list)` at add, `qwp_sender.go:568-580`).
- `maxSentSymbolId int` — cross-flush high-water, **never reset** (init −1 at
  `:443`; advanced only at append, `qwp_sender_cursor.go:589,678`). This is
  already Java's monotonic `sentMaxSymbolId` baseline; it correctly survives a
  reconnect because the producer goroutine has no reconnect notion.
- `batchMaxSymbolId int` — highest id used this batch; `resetAfterFlush` rewinds
  it to `maxSentSymbolId` (`qwp_sender.go:1416`), never to −1.

Change: add `deltaDictEnabled bool`; introduce
`symbolDeltaBaseline() int { if deltaDictEnabled { return maxSentSymbolId } else { return -1 } }`
and pass it where `-1` is hardcoded today:

- `enqueueCursor` (`qwp_sender_cursor.go:564`)
- `enqueueCursorSplit` (`:655`)

For the **combined path** (`enqueueCursor`) this is all that is needed:
`writeDeltaDict` writes `globalDict[maxSentId+1 .. batchMaxId]`; the post-append
advance of `maxSentSymbolId` (`:589-591`) becomes the baseline for the next
frame. In full mode `symbolDeltaBaseline()` returns −1 → byte-identical to today.

**Split path (`enqueueCursorSplit`) — needs more than a call-site swap (deep-review
finding).** Go's split differs structurally from Java's: it emits **one frame per
table** (`qwp_sender_cursor.go:654`, `maxSentId=-1`) and advances `maxSentSymbolId`
**once, after the loop** (`:678-679`). A naive port that only swaps the `-1` while
keeping the end-of-loop advance would make **every** per-table frame carry the same
delta `[maxSent+1 .. batchMax]` (baseline unchanged within the loop) — correct on
the wire (server re-registers idempotently) but wasteful. The **required** changes:

1. Pass `symbolDeltaBaseline()` at `:655` instead of a hard-coded `-1`, so the
   split path follows the same producer mode as the combined path. In delta
   mode the first split frame introduces the new suffix; in full-dictionary
   fallback every split frame begins at zero. The send-loop mirror handles both
   exact continuation and partial/full overlap, appending only the unseen tail.
2. Advance `maxSentSymbolId` **per successfully-appended frame** inside the loop
   (mirroring Java `flushPendingRowsSplit`'s per-frame `advanceSentMaxSymbolId()`),
   so table 1's frame carries the batch's new ids and tables 2..N carry empty
   deltas referencing ids already registered. Correct under retain-on-error: a
   later table failing after an earlier one advanced the baseline is fine — the
   earlier frame was sent, so the mirror holds its symbols and the catch-up
   re-registers them; the retried tail re-encodes against the advanced baseline.
3. (SF) write-ahead persist (§6.3) runs per-frame, before each
   `engineAppendBlocking` in the loop.

`resetSymbolDictStateForNewConnection` has no Go analogue needed: nothing on the
Go producer resets `maxSentSymbolId` (verified — only set at init `:443`, advanced
at append), which is exactly the required "baseline survives the wire boundary"
behavior.

### 5.2 Send loop: in-RAM sent-dictionary mirror

The send loop (`qwpSfSendLoop`) is a pure byte pump with no encoder or symbol
list — but it does not need them. The mirror stores the raw
`[len varint][utf8]` **symbol bytes** exactly as they appear in a frame's delta
section; those bytes *are* the payload of a catch-up frame. Building a catch-up
frame is then just: header + `[deltaStart][deltaCount]` varints + copied mirror
bytes.

New `qwpSfSendLoop` fields (I/O-goroutine-owned, no locks):

```
hasReplayDictionaryDependency bool
sentDictBytes                 []byte // concatenated [len][utf8] in id order
sentDictCount                 int    // mirrored ids 0..count-1
```

- **`accumulateSentDict(payload)`** — after each successful real frame send in
  `trySendOne` (`qwp_sf_send_loop.go:1323`), parse the payload's delta section.
  Reject a true gap (`deltaStart > sentDictCount`); for an exact continuation
  append the complete range, and for an overlap skip the entries already held
  and append only the unseen suffix. A completely covered range or empty delta
  is a no-op, making replay and full-dictionary fallback idempotent.
- Seeded on construction from the engine's recovered dictionary snapshot
  (SF recovery/drain, §6.4); empty for a fresh sender.
- `hasReplayDictionaryDependency` is initially true for a delta-producing
  engine or when recovered unacked frames contain a non-zero delta start. It
  gates reconnect catch-up, not mirror accumulation.
- No explicit free needed (GC owns the slice); nil it on loop exit for hygiene.

### 5.3 Catch-up on reconnect

The Go analogue of Java `setWireBaselineWithCatchUp` slots into **`swapClient`**
(`qwp_sf_send_loop.go:2059`) and **`positionCursorForStart`** (`:841`), which
today do:

```
replayStart := engineAckedFsn() + 1
fsnAtZero.Store(replayStart)
nextWireSeq.Store(0)
```

Replace the last two lines with `setWireBaselineWithCatchUp(ctx, replayStart)`:

```
if transport != nil && hasReplayDictionaryDependency && sentDictCount > 0 {
    nextWireSeq.Store(0)
    k := sendDictCatchUp(ctx)            // sends k table-less frames, each nextWireSeq++
    fsnAtZero.Store(replayStart - k)     // catch-up frames map to already-acked FSNs
} else {
    fsnAtZero.Store(replayStart)
    nextWireSeq.Store(0)
}
```

**Why this is correct (the FSN-alignment argument).** A catch-up frame consumes
wire seq `j ∈ [0,k)`. With `fsnAtZero = replayStart - k`, that frame maps to
`fsnSent = fsnAtZero + j = replayStart - k + j ∈ [replayStart-k, replayStart-1]`
— all **already-acked** FSNs. The first *real* replayed frame lands at wire seq
`k`, i.e. `fsnAtZero + k = replayStart`. So the wire-seq→FSN map used on both
send (`:1321`) and ack (`applyAckWatermark :1417`, `durableDrain :1606`) stays
consistent; the server's acks for catch-up frames are harmless re-acks of
already-acked FSNs (monotonic `ring.acknowledge` treats them as no-ops). This is
exactly Java's design — catch-up frames are **in** wire-seq space and `fsnAtZero`
is shifted back to absorb them; nothing is sent "out of band."

**`sendDictCatchUp`** (Java `sendDictCatchUp`/`sendCatchUpChunk`): walk
`sentDictBytes` entry-by-entry, packing into chunks bounded by the server's
advertised batch cap (see §5.4), and for each chunk build+send a `tableCount=0`
frame `[deltaStart=chunkStartId][deltaCount=chunkSymbols][chunk bytes]` via
`transport.sendMessage(ctx, frame)`. Each chunk does `nextWireSeq++`. Returns
the number of frames sent. On a send error, `fail`/recycle as any wire failure
(indefinite retry per Invariant B). An entry larger than the cap is a terminal
config error (matches Java `LineSenderException`).

Frame builder is byte-level, no encoder: write the 12-byte header (`magic`,
`version`, `flags = FLAG_DELTA_SYMBOL_DICT | FLAG_GORILLA`, `tableCount=0`,
`payloadLen`), then the two varints, then `copy` the chunk's mirror bytes. Reuse
`qwp_wire.go` varint helpers / a scratch `[]byte`.

### 5.4 Server batch cap for the split (plumbing confirmed)

Java splits the catch-up so no frame exceeds `client.getServerMaxBatchSize()`
(advertised `X-QWP-Max-Batch-Size`). **The Go plumbing already exists**
(deep-review): `qwpTransport.serverMaxBatchSize int32` is parsed from the
handshake header at `qwp_transport.go:496-501`, and the send loop already holds
its transport (`l.transport atomic.Pointer`, stored `:452`). So `sendDictCatchUp`
reads the cap from `l.transport.Load().serverMaxBatchSize` (same-package field
access, or a small `serverMaxBatchSize()` accessor) — the direct analogue of
Java's `client.getServerMaxBatchSize()`, no new handshake threading. The
producer's own `serverMaxBatchSize atomic.Int32` (`qwp_sender.go:345`) is a
separate mirror for the ingress cap check and is not what the send loop reads.
If the server advertises no cap (`0`), send the whole dictionary in one frame
(Java behavior). Budget per frame = `cap - headerSize(12) - 16` (room for the two
varints), min 1 (Java `sendDictCatchUp`).

## 6. SF/file-mode design (adds persistence)

Delta frames written to `.sfa` segments outlive the process; a recovered or
orphan-drained slot has no in-memory dictionary. The per-slot persisted
dictionary lets such a slot rebuild what its frames reference.

### 6.1 `PersistedSymbolDict` → new file `qwp_sf_symbol_dict.go`

Clone the shape of `qwp_sf_ack_watermark.go` (`qwpSfAckWatermark`), which is
already a tested Java-parity per-slot side-file. Differences: the dict **grows**
(append-only) rather than being a fixed 16-byte mmap, so use buffered `pwrite`
at an append offset (like Java `Files.write(fd, …, appendOffset)`), not a fixed
mmap.

- **Path/name:** `<slotDir>/.symbol-dict` (dot-prefixed constant, like
  `.ack-watermark` at `qwp_sf_ack_watermark.go:108`). The `*.sfa`-suffix filters
  in `qwpSfScanOrphans`/`qwpSfHasAnySegmentFile` (`qwp_sf_orphan.go:99,116`) and
  `qwpSfUnlinkAllSegmentFiles` skip it automatically — same as `.ack-watermark`.
- **Format (LE):** `magic u32 = 'SYD1'(0x31445953)`, `version u8 = 1`, 3 reserved
  bytes, then Java-compatible chunks
  `[entryCount varint][entryBytes varint][entries][crc32c u32]`, where `entries`
  is `[len varint][utf8]` repeated `entryCount` times and the CRC covers both
  header varints plus the entry region. Id = entry position across chunks (ids
  are dense from 0), so no id is stored. The checksum-free Go prototype used
  the same version byte and is therefore indistinguishable from a corrupt first
  chunk; recovery preserves it but does not trust it. Surviving frames may still
  reconstruct the dictionary, otherwise replay fails safely with `resend
  required` instead of accepting structurally plausible bytes as ids.
- **Fresh open:** always begin with an empty file; never inherit ids from a
  previous lifecycle. A surviving file must be truncated successfully or sender
  construction fails, because running full-dictionary next to stale bytes would
  let a later recovery trust the wrong generation. Only a provably absent file
  that cannot be created may degrade safely to producer full-dictionary mode.
- **Recovered open:** never recreate or overwrite an existing file. Return its
  trusted prefix when possible; absence or proven content damage returns no
  prefix while preserving the original bytes, and transient stat/open/read or
  truncate failures abort recovery. The frame fold (§6.4), not the header
  parser alone, decides whether the recovered backlog is safe.
- **`appendSymbols(names)`** — write one checksummed chunk per producer append,
  in one positioned write at the append offset. **No fsync** (matches SF:
  process-crash durable via page cache, not host-crash durable). Assigns the
  next dense ids implicitly.
- **`loadedSymbols() []string`** / **`size() int`** — for seeding.
- **`removeOrphan(slotDir)`** / **`close()`** — mirror
  `qwpSfAckWatermarkRemoveOrphan` (`:206`) / `close` (`:277`).

Single-writer (producer goroutine); append and close are mutex-serialised so a
shutdown racing an in-flight flush cannot write through a closed descriptor.

### 6.2 Engine wiring (`qwp_sf_engine.go`)

- Constructor (`qwpSfNewCursorEngineWithManager`, `:210`): in disk mode open the
  side-file alongside the watermark (recovery branch near `:313`; fresh-slot
  branch near `:348`, preceded by `removeOrphan` for stale-file hygiene like
  `qwpSfAckWatermarkRemoveOrphan`). Store on the engine.
- Accessors: `engineDeltaDictEnabled() bool` (`sfDir == "" || persistedDict != nil`),
  `enginePersistedSymbolDict()`, `engineRecoveredFromDisk()` (already have
  `recoveredFromDisk` at `:273`).
- `engineClose` (`:672`): `close()` the dict next to the watermark close
  (`:690-694`); on `fullyDrained`, `removeOrphan` next to
  `qwpSfAckWatermarkRemoveOrphan` (`:702`). Drainers inherit via their
  `defer engineClose()` (`qwp_sf_drainer.go:390`).

### 6.3 Producer: write-ahead persist before publish

In `enqueueCursor`/`enqueueCursorSplit`, **before** `engineAppendBlocking`
publishes the frame, append the ids this frame introduces
(`[maxSentSymbolId+1 .. batchMaxSymbolId]`) to the persisted dict (SF+delta
only; no-op in memory mode and when the frame adds no new symbols). Write-ahead
ordering ⇒ the on-disk dict is always a superset of every recoverable frame's
references (Java `persistNewSymbolsBeforePublish`). The producer holds the
engine's persisted-dict handle (pushed at bind).

Ordering note: persist happens **before** publish, and `maxSentSymbolId`
advances **after** a successful append (unchanged). If `engineAppendBlocking`
fails/backpressures, the pre-written dict entries are harmless (a superset;
never referenced by an unpublished frame) and the next attempt re-derives the
same range.

If a side-file append itself fails, the current flush returns the I/O error
without publishing its frame and latches producer mode to full-dictionary. A
retry therefore re-encodes self-sufficiently instead of wedging every future
flush on the same side-file failure. The send loop continues mirroring both old
deltas and new start-zero frames.

### 6.4 Recovery / orphan-drain analysis and seeding

Before registering a recovered ring with the segment manager or exposing it to
either consumer, the engine performs a single FSN-ordered fold over all
CRC-validated surviving frames:

1. Start with the side-file's trusted positional prefix, or an empty prefix if
   the file is absent/unusable.
2. Parse every symbol delta. Exact continuation extends coverage; partial or
   full overlap is validated and only its unseen suffix is appended.
3. A gap in an unacked frame is terminal before any network connection starts:
   the missing ids were trimmed from the log and are not proved by the
   side-file, so recovery reports `resend required`. An already-acked gap may be
   abandoned only when a later self-sufficient start-zero frame establishes a
   new provable epoch for replay.
4. Record the largest non-zero delta start in the replay range so the send loop
   knows that every fresh connection requires catch-up.
5. If frames prove a suffix beyond a valid short side-file prefix, append that
   suffix immediately to heal future recovery. A heal failure degrades the
   current producer to full-dictionary mode but keeps the proven in-memory
   snapshot.

The result is one immutable `recoveredSymbols` slice. A foreground producer
seeds `globalSymbolList` / `globalSymbols` and resumes new ids above its tip;
the send loop — including orphan drainers, which have no producer — seeds
`sentDictBytes` / `sentDictCount` from the same slice. This shared snapshot
prevents the producer from reusing id 0 while replay still assigns id 0 to an
older value. Mirror accumulation remains deliberately independent of the
producer's `deltaDictEnabled` flag.

### 6.5 Torn-dictionary guard (host-crash safety)

Because the dict is not fsync'd, a **host/power** crash can lose recently
persisted entries out of order relative to the segment frames (both are only
page-cache durable). A recovered delta frame could then reference an id beyond
the recovered dict. Sending it would corrupt the table (server null-pads missing
ids). Guard in `trySendOne` **before** the wire write (Java's guard, `:2233`):

```
start := frameDeltaStart(payload)              // -1 if no delta section
if start > sentDictCount {                     // gap ⇒ torn dict
    recordFatal(<PROTOCOL_VIOLATION: recovered SF symbol dictionary
        incomplete (likely host crash): frame delta start N exceeds
        recovered dictionary size M; resend required>)
    return false
}
```

The construction-time fold catches persistent recovery gaps before the wire;
this send-time guard remains defense in depth for state changes and frames
encountered after construction. A true gap is a sanctioned TERMINAL
(deterministic under byte-identical replay; bytes stay in the SF log;
recovery is close+rebuild+resend). It cannot occur in normal memory mode, and
normal SF write-ahead ordering keeps the dictionary a superset.

## 7. Durable-ack interaction (analyzed — safe, Java parity confirmed)

Durable mode (`request_durable_ack`) stashes each OK-ack until covering
`STATUS_DURABLE_ACK` frames arrive (`qwp_sf_durable.go`). Catch-up frames are
`tableCount=0` ⇒ their OK-ack trailer has no table entries ⇒ the pending entry
has empty `tables` ⇒ `coveredBy` returns **true** trivially
(`qwp_sf_durable.go:85-93`, "an entry with no tables is trivially durable").
So catch-up frames drain immediately once sent — **no wedge**, exactly like the
existing `tableCount=0` commit frame.

**Java parity (deep-review):** Java enqueues durable entries **ack-side**, keyed
by the OK-ack wire seq — `enqueuePendingOk(wireSeq)` (`CursorWebSocketSendLoop`
~`:1497`, comment: "Only the OK path enqueues") reads the OK frame's per-table
entries; a tableCount=0 catch-up frame yields a 0-table entry that Java's
`isDurableUnder` treats as trivially durable. This is byte-for-byte the same
shape as Go's `durableOnOk` → `enqueueOk` → `coveredBy`. Neither client
special-cases catch-up frames in the durable path; the empty-trailer mechanism
handles them in both. So the Go design needs **no** durable-specific code for
catch-up.

Wire-seq bookkeeping stays consistent:

- `durable.reset()` runs at swap (`:2075`) before catch-up; catch-up frames then
  enqueue OK-acks at seqs `0,1,…,k-1`, dense from `lastSeq=-1` ⇒ `seqGap` sees
  no gap (`:110-121`). Ordering holds because the server acks in send order and
  WS preserves it, so the receiver sees `0,1,…` monotonically.
- `durableOnOk` overrun check `seq <= nextWireSeq-1` (`:1520`) holds because
  each catch-up chunk does `nextWireSeq++` before its ack can arrive.
- Poison anchor: `highestOkAckedFsn` (durable) = `fsnAtZero + min(lastOkSeq,
  assigned)` = `(replayStart-k) + (k-1) = replayStart-1`, so head-of-line anchor
  = `replayStart` (the first real replayed frame). Catch-up acks do **not**
  advance the anchor past real frames. ✓

**Decisions (poison/munmap counters):**
- Catch-up frames bump `nextWireSeq` and `totalFramesSent` (observability) but
  **not** `framesSentOnConn` (the poison-strike gate — verified
  `sentSomething := l.framesSentOnConn.Load() > 0` at `run():1067`). A server
  drop right after catch-up, before any real frame, must leave `sentSomething`
  false so no strike lands on a real head-of-line FSN that never went out.
- Catch-up frames **do** bump `highestFullySent` (the munmap/durable-drain
  ceiling, `:1339,1605,1409`) to their wire seq. Safe: catch-up FSNs are
  already-acked and non-segment-backed, so advancing the ceiling frees nothing
  in flight; the benefit is the trivially-durable catch-up entries drain
  **promptly** instead of lingering until the first real frame (a lingering
  entry would keep `hasPending()` true and drive spurious durable keepalive
  pings on an otherwise-idle post-reconnect connection). Re-confirm the exact
  bump sites against the live code at implementation.

## 7.1 Threading: catch-up send has no concurrent socket writer (Go-specific)

Java sends the catch-up on its single I/O thread, so the send is trivially the
sole writer. Go splits the loop into `run()` (outer) + `senderLoop` +
`receiverLoop` (inner, per connection). The catch-up must therefore not race a
concurrent `senderLoop` `transport.sendMessage`. It does not:
`runOneConnection` spawns the two inner loops and blocks on `inner.Wait()`
(`qwp_sf_send_loop.go:1148`) for the whole connection lifetime; `run()` only
reaches the reconnect path (`connectWithBackoff` → `swapClient`) **after**
`runOneConnection` returns, i.e. after both inner loops have exited, and
**before** the next `runOneConnection` spawns fresh ones. So `swapClient` (and
the initial `positionCursorForStart` / async-initial `connectWithBackoff`) run
on the `run()` goroutine with no inner loop alive — the catch-up `sendMessage`
is the sole writer. Preserve this ordering; put the catch-up emit inside
`swapClient`/`positionCursorForStart` (never inside `senderLoop`).

## 8. File-by-file change list

Dependencies first (build order):

1. **`qwp_sf_symbol_dict.go`** (new) — `qwpSfSymbolDict` side-file
   (open/append/loadedSymbols/size/removeOrphan/close). License banner. §6.1.
2. **`qwp_constants.go`** — `qwpSfSymbolDictFileName = ".symbol-dict"`, magic,
   header size, version. (Flag constant already exists.)
3. **`qwp_sf_engine.go` / `qwp_sf_recovered_dict.go`** — open/close/removeOrphan
   the dict; fold every recovered frame into one validated positional snapshot;
   expose producer mode, recovered symbols, and replay dependency. §6.2, §6.4.
4. **`qwp_sender.go` / `qwp_sender_cursor.go`** — `deltaDictEnabled` +
   `symbolDeltaBaseline()`; swap the two `-1` call sites; write-ahead persist
   before publish; seed producer dict on recovery. §5.1, §6.3, §6.4.
5. **`qwp_sf_send_loop.go`** — mirror fields + `accumulateSentDict`;
   `setWireBaselineWithCatchUp` in `swapClient` + `positionCursorForStart`;
   `sendDictCatchUp`/`sendCatchUpChunk` + byte-level frame builder;
   `frameDeltaStart` + torn-dict guard; seed the mirror from the engine recovery
   snapshot. §5.2, §5.3, §6.4, §6.5.
6. **`export_test.go`** — re-export any internals the black-box tests reach
   (e.g. `qwpSfSymbolDict`, mirror inspectors).
7. **`CLAUDE.md`** — update the "self-sufficient frames" and "Symbol-dict
   tracking" paragraphs per §2; note the `.symbol-dict` side-file and delta mode.

## 9. Test plan (port Java's three suites + Go-specific)

Unit (httptest WS stub, `newQwpTestServer`):

- **Delta encoding (memory):** consecutive flushes with new symbols carry
  `deltaStart = prevMax+1`, not 0; a flush reusing only old symbols carries an
  empty delta. Reuse the server-side accumulator from Java `CatchUpHandler`
  (per-connection dict, null-pad to `deltaStart`).
- **Reconnect catch-up (memory)** — Java `DeltaDictCatchUpTest`
  `testReconnectCatchUpRebuildsDictionary`: symbol on conn 1, drop, symbol on
  conn 2; assert conn 2 saw a `tableCount=0` catch-up frame and its rebuilt
  dictionary is `[alpha, beta]` gap-free.
- **Catch-up split** — `testReconnectCatchUpSplitsLargeDictionaryAcrossFrames`:
  advertise a small `X-QWP-Max-Batch-Size`, register N symbols, drop, assert the
  catch-up spans ≥2 zero-table frames and reassembles gap-free.
- **Persisted dict** — Java `PersistedSymbolDictTest`: append/reopen round-trip;
  bad-magic recreated empty; empty-symbol round-trip; `removeOrphan` deletes;
  torn trailing entry self-heals.
- **SF recovery** — Java `DeltaDictRecoveryTest`: a recovered file-mode slot
  replays delta frames against a fresh server and rebuilds the dict; a torn
  (host-crash) dict fails cleanly with the resend-required PROTOCOL_VIOLATION
  instead of corrupting.
- **Full-dict fallback still holds** — Java `SelfSufficientFramesTest`: memory
  mode still emits from id 0 when the mirror is empty; SF with side-file
  disabled falls back to full frames.
- **Durable + catch-up** (Go-specific, §7): durable-ack sender, force a
  reconnect, assert the catch-up frame drains trivially and durable trimming
  keeps advancing (no wedge, no PROTOCOL_VIOLATION).
- **Zero-alloc guard** — `TestQwpSenderSteadyStateZeroAllocs`: the steady-state
  hot path must stay 0 allocs/op. `symbolDeltaBaseline()` is branch-only; the
  write-ahead persist is SF-only and off the memory-mode bench path. Verify.

Integration (Docker): extend a QWP failover suite to assert lossless ingest
across reconnect with high symbol cardinality (Java's
`SqlFailoverQwpClientLosslessTest` analogue).

## 10. Risks & open items

- **~~G1 — server batch-cap plumbing~~ RESOLVED (§5.4):** `qwpTransport.serverMaxBatchSize`
  is already parsed at handshake; the send loop reads it from its transport. No
  new plumbing.
- **G2 — zero-alloc hot path:** keep `symbolDeltaBaseline` allocation-free; keep
  all persist/mirror work out of the memory-mode steady-state path. Bench-gated
  by `TestQwpSenderSteadyStateZeroAllocs`.
- **Split-path correctness (§5.1):** in delta mode the split path must advance
  the baseline per successfully published frame; full-dictionary fallback must
  remain start-zero and overlap-safe. Cover both with split-then-reconnect tests
  that assert a gap-free server dictionary.
- **Counter bump sites (§7):** re-verify the exact
  `framesSentOnConn` / `highestFullySent` / `totalFramesSent` bump points against
  the live send loop when wiring the catch-up path.
- **Optional, out of scope:** a `delta_symbol_dict=on|off` safety valve. Java has
  none; adding one diverges from parity. Defer unless a reason emerges.
- **Java PR #66 merged on 2026-08-05.** Post-review changes that affect the Go
  implementation are tracked in §12; keep both clients' wire behavior aligned.

## 11. Deep-review deltas (vs. the pre-review draft)

Verified against Java branch `qwp-delta-symbol-dict` (`de86197`) and the current
Go tree:

1. **Split path was mis-described as "naturally correct."** It is not — Go's
   per-table split advances `maxSentSymbolId` once at end-of-loop and passes
   `-1`. Corrected to a firm requirement (§5.1): emit deltas + advance per-frame
   + per-frame write-ahead persist. Root cause of a real corruption path (mirror
   gap) if done naively.
2. **G1 resolved, not open** (§5.4): the server batch cap is already parsed into
   `qwpTransport` and reachable from the send loop.
3. **Durable parity confirmed** (§7): Java enqueues durable entries ack-side
   (`enqueuePendingOk`), and tableCount=0 catch-up frames are trivially durable
   in both clients — no durable-specific Go code needed.
4. **`highestFullySent` decision flipped** (§7): catch-up frames now bump it (was
   "leave untouched"), so trivially-durable catch-up entries drain promptly and
   don't drive spurious keepalive pings.
5. **New threading section** (§7.1): the Go loop is multi-goroutine (unlike
   Java's single I/O thread); verified the catch-up send runs between connection
   lifecycles with no concurrent `senderLoop` writer (`inner.Wait()` at
   `:1148`).
6. **Poison gate identified** (`sentSomething`/`framesSentOnConn` at `run():1067`)
   — catch-up must not bump it.

## 12. Post-merge parity fixes

The final Java PR added several guards after the original Go port was written.
The Go implementation now mirrors the directly corresponding wire and
producer semantics:

1. A sender refuses the 1,000,001st distinct symbol before assigning an id;
   existing values remain usable at the exact cap. The dictionary is global to
   one sender and shared across every table and SYMBOL column, matching Java PR
   #66 and the Java client's `MAX_SYMBOL_DICTIONARY_SIZE = 1_000_000`
   compatibility contract. Newer servers may accept more entries, but retaining
   the Java baseline keeps the same sender input portable.
2. Wire status `0x0D` is exposed as `DICTIONARY_GAP` and defaults to retriable,
   because recycling the connection and running catch-up repairs server-side
   dictionary coverage.
3. A delta that partially overlaps the send-loop mirror is valid: the held
   prefix is skipped and only the unseen tail is accumulated. Only
   `deltaStart > sentDictCount` is a torn-dictionary gap.
4. Reopening `.symbol-dict` truncates bytes beyond the last trusted chunk
   before another append. This prevents a shorter retry from leaving a suffix
   that a later recovery could misparse as a ghost symbol id.
5. The side-file now uses the latest Java per-append chunk layout with CRC-32C,
   so an interior corruption cannot remain structurally parseable and silently
   shift the dense id mapping. The checksum-free prototype format is not trusted
   on recovery because it cannot be distinguished safely from a corrupt first
   Java-format chunk; frame analysis can still recover a provable contiguous
   dictionary without it.
6. Producer fallback and send-side recovery are separated: disabling new delta
   frames no longer freezes the mirror for recovered delta frames. Construction
   folds the trusted side-file prefix plus all surviving frames, then producer
   and send loop seed from the same snapshot; an unacked gap fails before the
   network starts, while a contiguous side-file-free backlog remains drainable.
7. Resetting a successfully handled/dropped batch reclaims only symbol ids above
   both the published watermark and the persisted write-ahead size, matching
   Java's `truncateTo` safety floor.
8. A side-file append failure withholds the current frame, switches subsequent
   producer retries to full-dictionary mode, and leaves send-side delta tracking
   active. This preserves retryability instead of permanently wedging flush.
9. A fresh slot uses an explicit clean open. It refuses to proceed if a
   previous generation's side-file exists but cannot be truncated, closing the
   stale-id-space recovery hole fixed by Java's `openClean` path.

## 13. Remaining differences from Java `main`

The correctness fixes above do not make the two SF implementations identical.
The latest Java client has broader machinery that is intentionally tracked as
follow-up work rather than hidden under the PR #66 parity claim:

1. In full-dictionary fallback, Java can pre-register an over-cap dictionary in
   deferred table-less chunks and then publish the data against an empty delta.
   Go still splits only by table and repeats the full dictionary in each frame,
   so sufficiently high cardinality can make a full-dict fallback batch
   unshippable even though each individual symbol fits the server cap.
2. Java includes the physical `.symbol-dict` bytes in `sf_max_total_bytes` and
   has a two-segment liveness floor. Go's SF manager currently budgets segment
   bytes only.
3. Java exposes quarantined SF data as typed `DATA_LOSS` / `ABANDONED`
   notifications. Go retains its existing `.failed` sentinel and protocol-error
   reporting model.

Go now implements the relevant no-transaction equivalent of Java's
`RecoveredFrameAnalysis`: it reconstructs a missing suffix, distinguishes a
self-sufficient start-zero recovery epoch from a true gap, and rejects unsafe
unacked replay before connecting. Java's orphaned deferred-transaction-tail and
mmap-fault containment branches are specific to its transaction/storage model
and have no direct one-for-one Go equivalent.
