# QWP store-and-forward hardening plan

Date: 2026-08-13
Baseline: Go `4f2723e` (`main`). Java reference: `2489b243` at
`/home/jara/devel/oss/java-questdb-client` (spot-check there when in doubt; this
document is intended to be self-sufficient).

Input: `design/qwp-implementation-review.md` (same date). This plan turns its
three "Required correction" items into an implementable sequence:

1. **Fix 1** — manifest-based, fail-closed segment recovery.
2. **Fix 2** — close must treat manager-worker quiescence as a cleanup barrier;
   the slot lock is never released while the worker can still touch the slot.
3. **Fix 3** — adopt Java's chunked CRC-32C `.symbol-dict` format with a
   fail-closed migration for legacy flat files.

Ship as three commits on the current branch, in this order, each passing the
validation gates before the next begins. Fix 1 and Fix 2 both edit
`engineCloseInternal`; Fix 1 adds a close-drain block that Fix 2 then moves
into the new `engineFinishClose` — expect that mechanical relocation.

All Go line numbers below refer to `4f2723e`.

---

## Fix 1 — manifest-based fail-closed recovery

Today `qwpSfOpenRing` (`qwp_sf_ring.go:181-298`) skips any
`qwpSfErrSegmentCorrupt` file with a WARN (`:213-224`) and validates FSN
contiguity only between survivors (`:276-285`). A corrupt newest segment
silently amputates the persisted tail; a corrupt oldest segment makes its
frames look already-acked (the engine seeds `ackedFsn` from the lowest
survivor, `qwp_sf_engine.go:287-368`). The fix ports Java's `SfManifest`
boundary record and its recovery decision tree.

### 1.1 New file `qwp_sf_manifest.go` — the boundary record

On-disk: `<slot>/sf-manifest.bin`, exactly **8192 bytes**. Two independent
64-byte records at offsets **0** (slot 0) and **4096** (slot 1); the rest of
each 4 KiB half is zero. Record layout, little-endian:

| offset | size | field |
| --- | --- | --- |
| 0 | u32 | magic `0x314d4653` (`'S','F','M','1'`) |
| 4 | u32 | version = 1 (note: u32, unlike the segment header's u8) |
| 8 | i64 | `generation`, strictly > 0 in a valid record |
| 16 | i64 | `headBase` — baseSeq of the oldest retained (untrimmed) segment |
| 24 | i64 | `activeBase` — baseSeq of the active segment |
| 32 | 28 B | reserved, zero |
| 60 | u32 | CRC-32C over record bytes `[0, 60)` |

CRC dialect is plain `crc32.Checksum(rec[:60], crc32.MakeTable(crc32.Castagnoli))`
(Java's `~seed … ~crc` with seed 0 is exactly that).

Semantics to implement (each is load-bearing):

- **Slot alternation**: record for generation `g` is written at
  `(g & 1) * 4096`. `create` starts at generation 0 and immediately writes one
  update, so **the first record ever written is generation 1 at offset 4096**.
  Rationale: a torn sector can never destroy both the in-flight update and the
  previously committed record.
- **Validity predicate** (all must hold, else the record is "absent"): full
  64-byte read; magic; version; CRC match; `generation > 0`; `headBase >= 0`;
  `activeBase >= headBase`.
- **Selection**: slot0 invalid → slot1; slot1 invalid or `gen0 > gen1` → slot0;
  else slot1.
- **`update(newHead, newActive)`** (mutex-guarded): error if closed. When
  `generation > 0`, clamp **each field independently** monotonically
  (`newHead = max(newHead, head)`, same for active) — rotation (producer) and
  trim (manager) compute their arguments from snapshots the other may have
  passed. Then validate `newHead >= 0 && newActive >= newHead`. **No-op
  short-circuit**: if `generation > 0` and both post-clamp values are
  unchanged, return without writing or fsyncing. Otherwise write the 64-byte
  record at `((generation+1) & 1) * 4096`, **fsync the fd on every update**,
  and only then commit the in-memory generation/head/active. Use a
  preallocated 64-byte scratch on the struct (trim path must not allocate).
- **`qwpSfManifestCreate(dir, head, active)`**: `O_CREAT|O_EXCL|O_RDWR` create;
  preallocate/extend to 8192 (reuse `qwpSfAllocate`); construct with
  `generation=0, head=-1, active=-1`; call `update(head, active)` (→ gen 1 at
  4096, fsynced); fsync the directory. On any failure: close and remove the
  file, return the error.
- **`qwpSfManifestOpen(dir)`**: absent → `(nil, nil)`. Size != 8192 →
  **quarantine as creation debris** (rename to `sf-manifest.bin.corrupt`, else
  remove, else error — leftover debris would wedge every later create) and
  return `(nil, nil)`; this is safe because `create` reaches full size via
  allocate before writing any record, and segments are flagged
  manifest-required only after `create` returns. Both records invalid → close
  fd first, quarantine, `(nil, nil)` (every update rewrites only one slot, so a
  torn update always leaves the sibling intact; zero valid records proves a
  creation crash). Otherwise return a manifest seeded from the selected record.
- **`qwpSfManifestRemove(dir) bool`**: `remove || !exists` → true ("confirmed
  gone").

### 1.2 `.ack-watermark` format upgrade (shared record codec)

Go's watermark is a 16-byte magic+i64 file with no CRC
(`qwp_sf_ack_watermark.go:107-124`); Java calls that the *legacy* format and
uses an 8192-byte file with the **same dual-slot 64-byte record shape as the
manifest**: magic `0x31574B41` (`'AKW1'`), u32 version 1, i64 generation @8,
i64 `fsn` @16, CRC-32C of `[0,60)` @60, validity additionally requires
`fsn >= -1`. Port it now — the manifest and watermark share one record codec,
and Java resets any wrong-sized watermark on open (so Go's 16-byte files are
self-healed by either client; the `max()` seed clamp makes that safe).

- Rewrite `qwp_sf_ack_watermark.go` on the shared codec: mmap the 8192-byte
  file for the engine's lifetime; `write(fsn)` is a pure store of the next
  record into the mapping (alternating slot, generation+1, CRC) — no syscall;
  add a separate `sync()` = msync whole file + fsync fd. `open` resets any
  wrong-sized or unparseable file by clean recreate (returns error only on
  create/allocate failure); `read()` returns the selected record's fsn or the
  INVALID sentinel (`math.MinInt64`, already `qwpSfAckWatermarkInvalid`).
- Recovered-slot branch: the watermark becomes **required** — a nil/failed
  open in `qwp_sf_engine.go:326` is now an operational recovery error (Java:
  "could not open required ack watermark"), because trim ordering below relies
  on it. Fresh-slot branch keeps remove-then-open (`qwp_sf_engine.go:382-383`).
- Seeding math is unchanged (`candidate = max(lowestBase-1, watermark.read())`,
  reject `> publishedFsn` — `qwp_sf_engine.go:348-364` already matches Java).

### 1.3 Segment changes (`qwp_sf_segment.go`)

1. **`MANIFEST_REQUIRED` flag**: bit `0x01` of the flags byte at header offset
   5. Add `segmentManifestRequired() bool` (false for memory-backed) and
   `markManifestRequired()` = set bit + `syncHeader()`. Add `syncHeader()` =
   msync of the 24-byte header + fsync fd; failure is an error (operational).
   The flag is a durable promise that `sf-manifest.bin` exists; it is stamped
   only after the manifest is durably on disk.
2. **Reclassify unsupported version**: today it is `qwpSfErrSegmentCorrupt`
   (`:280-285`). Make it a plain (operational) error — a different client
   build wrote it; quarantining would strand its frames. Corruption stays:
   bad magic, size < 24, negative baseSeq.
3. **Whole-suffix torn-tail detection**: `qwpSfDetectTornTail` (`:551-565`)
   inspects only up to 8 bytes at the bail-out position; Java inspects the
   **entire** `[lastGood, fileSize)` suffix so an all-zero bad frame header
   cannot hide non-zero payload further on. Scan the whole suffix.
4. **`sanitizeTornTail()`**: new method. No-op if `tornTailBytes == 0` or
   already sanitized; precondition `appendCursor == fileSize - tornTailBytes`
   (else error); zero the suffix, msync the mapping + fsync fd (failure =
   operational error, aborts recovery); idempotent.

### 1.4 Recovery — replace `qwpSfOpenRing`

Move recovery into a new `qwp_sf_recovery.go` implementing Java's decision
tree. Signature: `qwpSfRecoverRing(sfDir string, maxBytesPerSegment int64)
(*qwpSfSegmentRing, *qwpSfManifest, error)` returning `(nil, nil, nil)` for
EMPTY; the ring takes ownership of the manifest (rotation updates it; ring
close closes it). Decision tree (implement faithfully; comments should carry
the rationale):

**Step 0/1 — enumerate.** Missing dir → EMPTY. Enumeration error →
operational error. Collect `*.sfa` names only (`.corrupt` files are invisible;
the `.`-prefixed side files don't match).

**Step 2 — open every candidate.** `qwpSfOpenSegment` per file:
- `qwpSfErrSegmentCorrupt` → exclude from `all`, append path to
  `corruptPaths`, **defer quarantine** until the surviving chain validates.
  Log the deferral.
- Any other error (open/stat/mmap, unsupported version) → **abort**
  (operational): the file may be intact; dropping it could lose durable
  frames. (This branch exists today at `:225-235`; keep its comment.)
- Drop the current zero-frame-file cleanup block (`:252-261`) — extras are
  handled by the tree below.

**Step 3 — open the manifest** (after all segments): `qwpSfManifestOpen`.

**Step 4 — nothing readable (`len(all) == 0`).**
- `corruptPaths` non-empty and manifest present → **fail closed**:
  "every SF segment is corrupt but sf-manifest.bin references durable data"
  (no boundary check — mere presence of a manifest fails closed here).
- `corruptPaths` non-empty, no manifest → quarantine the corrupt files
  (rename to `<path>.corrupt`), EMPTY (legacy semantics).
- No `.sfa` at all, manifest present with `headBase != activeBase` → **fail
  closed**: manifest references durable data but no segment files exist.
- No `.sfa`, manifest with collapsed boundaries (`head == active`) → the
  clean-drain / fresh-start crash window: WARN, close manifest, remove it
  (failure to remove → operational error), EMPTY.

**Step 5 — partition.** `data` = opened segments with `frameCount > 0`,
sorted by **unsigned** baseSeq (existing sort is already unsigned).
`requiresManifest` = OR of `segmentManifestRequired()` over **all** opened
segments, including frameless spares. If `manifest == nil && requiresManifest`
→ **fail closed**: "new-format SF segment exists but sf-manifest.bin is
missing".

**Step 6A — manifest path** (`manifest != nil`):

```
head, active := manifest.headBase, manifest.activeBase
for seg in data (ascending):
    end := seg.baseSeq + seg.frameCount
    if seg.baseSeq < head:
        if end > head: FAIL "segment overlaps committed SF head boundary"
        continue                      // stale-but-acked extra (manifest-before-unlink crash)
    if seg.baseSeq > active: FAIL "segment exists beyond committed SF active boundary"
    chain = append(chain, seg)
if chain non-empty:
    validateContiguous(chain)         // FSN gap → fail closed (existing check text)
    if chain[0].baseSeq != head: FAIL "missing expected SF head segment at base H"
activeSeg := findActive(all, active)  // among same-baseSeq candidates prefer:
                                      // (1) frameCount>0, (2) empty with torn tail,
                                      // (3) first clean empty; duplicates are NOT an error
if activeSeg == nil:
    if chain empty && head == active && corruptPaths == nil:
        // clean-drain crash window: quarantine torn leftovers, remove clean ones
        // (remove failure → operational error), remove manifest, EMPTY
    FAIL "missing expected SF active segment at base A"
if chain empty:
    if head != active || activeSeg.frameCount != 0 || corruptPaths != nil:
        FAIL "missing SF chain between committed boundaries"
             (+ " (a corrupt segment prevents proving the empty state)" if corrupt)
    chain = [activeSeg]
else if chain.last != activeSeg:
    chainEnd := last.baseSeq + last.frameCount
    if corruptPaths == nil && activeSeg.frameCount == 0 && activeSeg.baseSeq == chainEnd:
        chain = append(chain, activeSeg)   // rotation committed, died before first frame
    else:
        FAIL "missing expected SF active/tail segment at base A"
sanitizeSealedResidue(chain, failClosedOnSight=true)
for seg in chain: seg.markManifestRequired()
```

`sanitizeSealedResidue(chain, failClosed)`: for every member except the last,
if `tornTailBytes > 0` run `sanitizeTornTail()` (durable zero). If
`failClosed` and any member was torn, **after healing** return the retry-once
sentinel error (§1.6) naming the first torn path — reaching this point proves
the residue was dead (contiguity + boundaries already validated); the error
surfaces the incident and the caller's single retry sees a clean chain.

**Step 6B — legacy path** (`manifest == nil`; every pre-upgrade Go slot):

```
if data non-empty:
    start := data[0].baseSeq
    if start != 0:
        if corruptPaths != nil:
            FAIL "cannot migrate the legacy SF chain based at N: a corrupt segment
                  of unknown identity could be its head"
        for seg in all:
            if seg.frameCount == 0 && seg.tornTailBytes > 0 && seg.baseSeq < start:
                FAIL "…segment at base B lost its frames to a torn write and sits
                      below that head, so its range cannot be shown already-acked"
    validateContiguous(data)
    chain, activeSeg = data, data.last
    head, active = chain[0].baseSeq, activeSeg.baseSeq
    sanitizeSealedResidue(chain, failClosedOnSight=false)   // zero silently
else:
    activeSeg = chooseEmptyInitial(all)   // skip frameCount!=0 or torn>0; prefer
                                          // the sf-initial.sfa path; nil if none
    if activeSeg == nil:
        // close all; torn>0 → quarantine, else remove (WARN on failure);
        // quarantine corruptPaths; EMPTY
    chain = [activeSeg]; head = active = activeSeg.baseSeq
manifest = qwpSfManifestCreate(sfDir, head, active)
for seg in chain: seg.markManifestRequired()
```

The two `start != 0` guards fire only on positive evidence of a set-aside file
— a chain based at 0 needs neither, and a positive base alone is the normal
trimmed steady state. Torn empties are never reused as the initial active
(their bytes are quarantine evidence).

**Step 7 — common tail.** For each opened segment **not** in `chain`
(validated extras: spares, stale-below-head, duplicate empties): close it (a
close failure propagates), then torn > 0 → rename `<path>.corrupt` (WARN-only
on failure), else remove (WARN-only). Then quarantine `corruptPaths` the same
way. Then **`activeSeg.sanitizeTornTail()`** — unconditional by policy once
validated: replay cannot cross the tear, and leaving valid-CRC residue past it
risks resurrection at a recycled FSN; a failed durability barrier aborts
recovery (the retry re-observes the same residue, since open never mutates).
Build the ring exactly as today (`newest = active, rest sealed`,
`nextSeq = active.baseSeq + active.frameCount`).

**Failure cleanup**: any error path closes every still-owned segment and the
manifest (keep today's deferred-cleanup pattern at `:200-204`), then returns.
The global invariant to keep in a file-top comment: a failed recovery never
mutates committed chain bytes; durable pre-failure mutations are confined to
bytes the validated chain proves dead, or to preserve-by-rename quarantines.

### 1.5 Write-path wiring

- **Fresh start** (`qwp_sf_engine.go:370-395`): order is (1) create
  `sf-initial.sfa` unflagged at baseSeq 0, (2) `syncHeader()` + fsync dir,
  (3) `qwpSfManifestCreate(sfDir, 0, 0)`, (4) `markManifestRequired()`. Every
  crash window between steps self-heals through §1.4. Also remove any stale
  manifest alongside the existing watermark/dict orphan removal
  (`:381-387`).
- **Rotation** (producer-side promote of a hot spare; `rebaseSeq` is
  `qwp_sf_segment.go:381-389`, promotion lives in the ring — locate the site
  where the spare is rebased and becomes active): (1) `rebaseSeq(actualBase)`,
  (2) `spare.syncHeader()` **before** the manifest can name it, (3) under the
  ring's lock `manifest.update(headBase, actualBase)` **first**, then
  link/seal/promote. If the update (fsync) fails, the rotation never happened
  and the producer's retry re-runs from a consistent state.
- **Trim tick** (`serviceRing`, `qwp_sf_manager.go:481-507`): the ordered
  protocol per pass with pending trims is: watermark store
  (`persistIfAdvanced`) → `watermark.sync()` → fsync slot dir (pre-barrier) →
  **one** `manifest.update(newHead, activeBase)` covering the whole
  contiguous trim batch (newHead = successor of the last trimmed segment, or
  activeBase when trimming everything sealed) → close+unlink the batch →
  fsync slot dir (post-barrier) → byte accounting. Cadence watermark writes
  with no pending trim stay store-only (no sync). This gives one manifest
  fsync per trim batch, zero per ack.
- **Close-time drain** (`engineCloseInternal`'s `fullyDrained` branch,
  `qwp_sf_engine.go:779-788`; moves into `engineFinishClose` in Fix 2): final
  covering watermark write + `sync()` first; then
  `manifest.update(activeBase, activeBase)` **before the first unlink**;
  unlink `.sfa` in cleanup-rank order (`sf-initial.sfa` first, then by
  generation), stopping at the first failure; remove the manifest **last**;
  fsync the dir; only after a fully successful unlink+fsync remove the
  watermark (partial unlink retains it so residual acked segments stay
  covered); then dict orphan removal as today.
- `qwpSfUnlinkAllSegmentFiles` (`qwp_sf_engine.go:801-823`) keeps matching
  only `.sfa`; the manifest is handled explicitly per the drain protocol.

Note on fsync policy: these are the first fsyncs in Go SF. They sit on
control-point frequency (creation, rotation, trim batch, drain) — never per
frame — and exist in `sf_durability=memory` too, matching Java. Document that
in the CLAUDE.md update (§4.4).

### 1.6 Error taxonomy and caller behavior

New sentinel errors in `qwp_sf_recovery.go`:

- `qwpSfErrRecoveryFailClosed` — wraps every FAIL above plus corruption-class
  aborts. Meaning: byte evidence of boundary loss; deterministic on retry.
- `qwpSfErrSanitizedResidue` — the retry-once verdict from
  `sanitizeSealedResidue(failClosed=true)`.
- Everything else stays a plain operational error (EMFILE, mmap, fsync,
  enumerate, un-removable leftovers): abort startup, quarantine nothing.

Caller policy:

- **Sender construction** (`qwp_sender_cursor.go:196`, async path
  `qwp_sender.go:451`): on `qwpSfErrSanitizedResidue`, log and rebuild the
  engine **once** (the residue is now durably zeroed). On
  `qwpSfErrRecoveryFailClosed`, quarantine the whole slot by renaming it to
  `<sf_dir>/quarantined/<sender_id>-<unixnano>` (create the `quarantined/`
  parent; bytes preserved for the operator) and build a fresh slot at the
  original path — with a stable `sender_id`, failing forever would otherwise
  prevent the app from even constructing a sender. Log loudly. Operational
  errors propagate as today. The async initial-connect path applies the same
  policy inside its retry loop (quarantine + fresh build is a sanctioned
  terminal outcome for the slot, not for the sender).
- **Drainers** (`qwp_sf_drainer.go:406-411`): keep the existing behavior —
  any engine-open error other than lock-busy marks the slot `.failed`. That is
  a sanctioned Invariant-B terminal ("failed slot recovery / engine open").
  Write the recovery error text into the `.failed` sentinel so the operator
  sees *why*.
- The `quarantined/` directory must be invisible to orphan scanning: it
  contains no `.sfa` directly, so `qwpSfScanOrphans` (`qwp_sf_orphan.go:59-88`)
  already skips it, but add a test pinning that.

### 1.7 Orphan-scan integration

`qwpSfIsCandidateOrphan` (`qwp_sf_orphan.go:92-100`) keys purely off `*.sfa`
presence. Extend the predicate: a slot is a candidate when it has at least one
`.sfa` **or** an `sf-manifest.bin`, and no `.failed`. Rationale: a slot whose
segments were lost but whose manifest references durable data must surface as
a `.failed` quarantine (via the drainer hitting the Step-4 fail-closed branch)
rather than stay invisible; a manifest-only slot with collapsed boundaries is
adopted, resolves to EMPTY, and gets cleaned up.

### 1.8 Tests (Fix 1)

Update the tests whose semantics flip, and add the new coverage. All SF tests
are internal (`package questdb`) — no `export_test.go` changes needed.

- `qwp_sf_ring_test.go`: `TestQwpSfRingOpenExistingSkipsCorruptStrayFile`
  becomes two cases — corrupt stray **with no manifest and chain based at 0** →
  quarantined after validation, recovery succeeds; corrupt file **with a
  manifest present and boundaries it should occupy** → fail closed.
  `…QuarantinesCorruptFirstFrame` reworks onto the extras/quarantine tail.
  `…RejectsFsnGap` stays.
- New `qwp_sf_recovery_test.go`, minimum matrix (each row is one of the
  review's probes or a Java-pinned window):
  - corrupt **newest** segment under a manifest → fail closed (this is the
    review's probe 1; it must now fail).
  - corrupt **oldest** segment under a manifest → fail closed
    ("missing expected SF head segment").
  - stale segment wholly below `headBase` → removed as validated extra;
    straddling `headBase` → fail closed.
  - segment beyond `activeBase` → fail closed.
  - flagged segment with missing manifest → fail closed.
  - manifest with data boundaries and zero `.sfa` → fail closed; collapsed
    boundaries and zero `.sfa` → EMPTY + manifest removed.
  - clean-drain crash window (chain empty, `head==active`, active file
    missing, no corrupt files) → EMPTY.
  - rotation crash: empty active at `chainEnd` appended; same with a corrupt
    file present → fail closed.
  - legacy migration: chain at 0 with a corrupt stray → migrates (stray
    quarantined); chain above 0 with a corrupt file → fail closed; chain above
    0 with a torn frameless file below head → fail closed; happy migration
    writes a manifest and stamps flags (verify bit 0x01 + reopen).
  - sealed torn residue → sentinel error, then a second open succeeds and the
    suffix is zeroed (retry-once contract, sender-side test too).
  - torn active tail → zeroed durably before ring construction.
- Manifest unit tests: record round-trip, slot alternation (first record at
  4096), independent clamps, no-op short-circuit, wrong-size quarantine,
  double-invalid quarantine, torn-record survival (corrupt one slot, recover
  from the other).
- Watermark: golden-format round-trip, legacy-16-byte reset, required-on-
  recovery failure path; keep `TestQwpSfEngineRecoveryHonoursForeignWatermark`
  / `…RejectsCorruptWatermark` semantics on the new format.
- Trim/drain ordering: extend `TestQwpSfEngineWatermarkPersistedByManager`
  and `TestQwpSfEngineFullDrainUnlinksFiles` to assert the manifest head
  advances per trim batch and the drain leaves neither manifest nor watermark.
- Drainer: manifest-only slot adoption (both boundary shapes), `.failed`
  reason text contains the recovery error.
- Sender: fail-closed recovery quarantines to `quarantined/` and rebuilds; the
  quarantined dir is not orphan-adopted.

---

## Fix 2 — close treats worker quiescence as a cleanup barrier

Today `segmentManagerClose` (`qwp_sf_manager.go:168-193`) waits up to
`qwpSfManagerCloseGrace` (5s) and returns void; `engineCloseInternal`
(`qwp_sf_engine.go:731-795`) then closes the ring/watermark/dict, unlinks
files, and releases the flock even when the worker is still inside slot I/O
(`serviceRing`'s create/watermark-write/trim-unlink at
`qwp_sf_manager.go:435-436,491,496-503` — none re-check registration except
the spare-install commit). The comment at `qwp_sf_engine.go:763-766` claiming
the worker has joined is simply false after a timeout. Port Java's contract:
**when quiescence cannot be confirmed, retain every worker-reachable resource
(including the flock) and hand terminal cleanup to the worker's exit path.**

### 2.1 Manager changes (`qwp_sf_manager.go`)

- Ring entries become pointers: `rings []*qwpSfManagerRingEntry` and the
  snapshot copies pointers (today entries are copied by value, `:111-123` —
  per-entry shared state below requires identity).
- Per-entry state machine, `state atomic.Int32`:
  `REGISTERED(0) → IN_SERVICE(1) → REGISTERED`; `deregister` maps
  `REGISTERED→DEREGISTERED(3)`, `IN_SERVICE→DEREGISTERED_IN_SERVICE(2)`;
  `finishService` maps `2→3`, else `→0`. `isInService` = {1,2}.
- `workerLoop` per-entry pass becomes:
  publish `m.inService.Store(entry)` **before** the claim CAS
  (`REGISTERED→IN_SERVICE`; CAS failure = deregistered → skip, clear
  `inService`); run `serviceRing`; in a defer: `finishService`, claim any
  handed-off cleanup via `entry.cleanup.Swap(nil)`, clear `inService`, run the
  claimed cleanup **outside** all locks (recover + log on panic).
- Worker exit block (function-level defer in `workerLoop`): under `mu` set
  `workerLoopExited = true`, take-and-nil `ownedEngineExitCleanup`; run it
  outside `mu` (recover + log: "deferred owned-engine cleanup failed on
  manager-worker exit").
- `segmentManagerClose() (workerQuiescent bool)`: set `closed`, wake, bounded
  wait on the worker's done channel with `qwpSfManagerCloseGrace`. Done →
  `workerReaped = true`, return true. Timed out: under `mu`, if
  `!workerLoopExited` return false (stuck inside a pass). If `workerLoopExited`
  (worker is only running its finite exit block), wait **once more** with the
  same fixed grace, then set `workerReaped = true` and return true regardless
  — the loop has provably exited, so the worker can no longer touch any ring;
  the second wait only avoids reporting quiescence while a *previously*
  handed-off cleanup is mid-flight. Idempotent: subsequent calls re-evaluate
  with a fresh grace.
- `deferOwnedCleanupUntilWorkerExit(f func()) bool`: under `mu` — if
  `workerLoopExited || workerReaped` return false (caller cleans inline); if a
  different cleanup is already stored, panic (programming error); store, return
  true. **An exact `false` means "worker provably past its loop"; only that
  permits inline cleanup.**
- `awaitRingQuiescence(entry) bool` (shared-manager path, test-only in
  production): if no worker, or called from the worker goroutine, return true.
  Poll `entry.isInService()` every 1 ms up to `qwpSfManagerCloseGrace`; true on
  quiesced, false on deadline. (Polling instead of Java's condvar — the pass
  is short and this path is not on any hot loop.)
- `deferUntilRingQuiescent(entry, f) bool` — the no-ownerless-gap dance on
  `entry.cleanup atomic.Pointer[func()]`:

  ```
  if !entry.isInService(): return false
  if entry.cleanup.CompareAndSwap(nil, &f):
      if entry.isInService(): return true
      return !entry.cleanup.CompareAndSwap(&f, nil)  // pass ended: reclaim, or worker took it
  return true                                        // duplicate registration keeps the owner
  ```

### 2.2 Engine close state machine (`qwp_sf_engine.go`)

New fields: `closeCompleted atomic.Bool`, `terminalCleanupClaimed atomic.Bool`,
`deferredFullyDrained atomic.Bool`, `deferredLeakSegments atomic.Bool`, and a
`deferredClose func()` **bound once in the constructor** (it calls
`engineCompleteDeferredClose`).

Split `engineCloseInternal(leakSegments)` into:

**Phase A — barrier** (replaces `:731-795` head):

1. `closed.CompareAndSwap(false, true)`; on repeat call: if `closeCompleted`
   return nil, else fall through to re-run the barrier (retried close must
   converge).
2. Take `appendMu` for the barrier phase (producer fencing as today).
3. Compute `fullyDrained` (unchanged, `:751-753`) and publish
   `deferredFullyDrained` / `deferredLeakSegments` **before** any handoff.
4. `segmentManagerDeregister(ring)`.
5. Quiescence: `ownsManager` → `quiescent = manager.segmentManagerClose()`;
   shared → `quiescent = manager.awaitRingQuiescence(entry)`.
6. If `!quiescent`: owned → `handedOff =
   manager.deferOwnedCleanupUntilWorkerExit(e.deferredClose)`; shared →
   `handedOff = manager.deferUntilRingQuiescent(entry, e.deferredClose)`.
   - `handedOff` → **log an error** naming the slot ("close handed to the
     manager worker's exit path; the slot stays locked until it completes")
     and **return nil with everything retained**: ring, watermark, dict,
     segment files, flock. `closeCompleted` stays false.
   - exact `false` → worker provably past the loop / pass finished; treat as
     quiescent, fall through.
7. `if !terminalCleanupClaimed.CompareAndSwap(false, true) { return nil }` —
   someone else (worker exit, earlier close) owns cleanup.
8. `engineFinishClose(fullyDrained, leakSegments)`.

**`engineFinishClose(fullyDrained, leakSegments)`** — terminal cleanup, only
ever entered by the CAS winner (the CAS, not a mutex, is the exclusion —
the worker path must not need the engine's locks):

1. If `fullyDrained`: final covering watermark write + `sync()` (Fix 1's
   drain protocol). On failure: `terminalCleanupClaimed.Store(false)` and
   return the error (a later `Close()` retries) — release nothing.
2. `ring.segmentRingCloseInternal(leakSegments)` (closes the manifest).
3. `watermark.close()`, `persistedSymbolDict.close()`.
4. If `fullyDrained`: the Fix-1 drain block (manifest collapse-update before
   unlink, ranked unlink, manifest removed last, dir fsync, watermark+dict
   orphan removal).
5. `slotLock.close()`; **only on success** `closeCompleted.Store(true)`
   (memory mode, `slotLock == nil`, counts as success). On failure, log; the
   kernel drops the flock at process exit. Completion is never observable
   while the flock may still be held.

`engineCompleteDeferredClose()`:
`if !terminalCleanupClaimed.CompareAndSwap(false, true) { return }` then
`engineFinishClose(deferredFullyDrained, deferredLeakSegments)` and an
info-log reporting `closeCompleted`. It runs on the worker goroutine — it
must take `appendMu` itself before touching the ring (producers may still be
spinning in backpressure; they observe `closed` and bail, as the existing
comment at `:735-743` describes; the manager worker never held `appendMu`
before, and here it takes it only after its loop has exited, so no deadlock).

Add accessor `engineCloseCompleted() bool`.

Deliberate simplifications vs Java, to record in comments: no global
flock-release retry daemon; each incomplete terminal close installs a bounded-
scope per-engine owner that retries without a deadline because releasing the
flock while cleanup is incomplete would race retained files. There is no
`reclaimLogicalSlotLock` split (Go has no separate logical-lock file); the
sync-interval sealed-segment sync (Java `finishClose` step 1) is out of scope
with `sf_durability=memory`.

### 2.3 Callers

- **Sender close** (`qwp_sender_cursor.go` close path): after
  `engineClose*`, check `engineCloseCompleted()`. Incomplete → keep the engine
  referenced, install its terminal-cleanup retry owner, and WARN that the owner
  started; do not claim that a manager worker which may already have exited
  will release the lock.
- **Drainer** (`qwp_sf_drainer.go:414-424` close defer): incomplete close →
  install the terminal-cleanup retry owner, log, and exit; the retained flock
  keeps the slot un-adoptable (scans skip locked slots silently), which is
  exactly the safe state. Note the
  interaction in a comment: `drainerPoolClose`'s 3s+1s abandon budget may
  abandon a drainer whose deferred cleanup later completes on the worker —
  that is fine, the worker goroutine is process-lifetime bounded.
- **Facade pool** (`qwp_sender_pool.go` + `qwp_pool_housekeeper.go`): when a
  pooled SF sender's close leaves `engineCloseCompleted() == false`, keep the
  slot's `slotInUse` bit set (capacity retired) and WARN; the housekeeper
  reaper re-probes retired slots each tick and restores capacity once the
  deferred cleanup has published completion. Add the retired-slot list +
  reprobe to the housekeeper.

### 2.4 Tests (Fix 2)

Run this fix's suite under `-race`.

- Port the review's probe as a real test: block the worker inside spare
  allocation (inject via a test hook on segment creation — add a
  package-level `qwpSfTestSegmentCreateHook func(path string)` guarded to
  tests, following the `beforeFlockReleaseHook` idea), call `engineClose`,
  assert: close returns, `engineCloseCompleted() == false`, **a second
  `qwpSfAcquireSlotLock` on the slot fails busy**, no slot file was removed.
  Unblock the worker; assert cleanup runs on its exit path,
  `closeCompleted` flips true, and the slot becomes acquirable.
- Owned-manager quiescent path unchanged (fast close, all released) — pin
  with a timing-free assertion (no reliance on the 5s grace).
- `deferOwnedCleanupUntilWorkerExit` returning exact false → inline cleanup
  (kill the worker between timeout and registration via the hook).
- Shared-manager (`qwp_sf_engine_test.go:276` pattern): deregister one ring
  while the worker services it; `awaitRingQuiescence` false → handoff to the
  pass's completion; sibling ring keeps being serviced.
- Repeat-`Close()` convergence: first close hands off, second close after
  worker exit returns nil with `closeCompleted` true; claimed-CAS exclusion
  (deferred cleanup and a concurrent retried close never both run
  `engineFinishClose`).
- Pool: retired-slot capacity + housekeeper reprobe restores it.
- `TestQwpEngineSurfacesManagerWorkerPanic` (`qwp_sf_close_abandon_test.go:85`)
  still passes: a panicked worker has exited its loop → close reports
  quiescent → inline cleanup (the panic path must flip `workerLoopExited`).

---

## Fix 3 — `.symbol-dict` chunked CRC-32C format

Go's flat v1 body (`qwp_sf_symbol_dict.go:50-57`) is the **pre-merge** Java
format; merged Java rewrote the body **without changing magic or version** and
has no fallback parser. Java reading a Go file truncates it to the 8-byte
header (silent total loss); Go reading a Java file yields garbage symbols with
shifted ids (the review's probe: `["\x02", "x"]`). Go must adopt the Java
format byte-identically and treat legacy flat bodies as a fail-closed
migration case.

### 3.1 Byte format (writer and reader must match this exactly)

Header unchanged: `53 59 44 31` (`'SYD1'` LE, magic `0x31445953`), version
byte `1`, 3 reserved zero bytes (not validated on read). Body = back-to-back
chunks, no alignment:

```
[entryCount : uvarint]   >= 1
[entryBytes : uvarint]   >= 1, exact byte length of the entries region
[entries    : entryBytes]  entryCount × ( [len uvarint][len bytes UTF-8] )
[crc32c     : u32 LE]    over entryCount||entryBytes||entries (NOT the CRC itself)
```

- Varints: canonical minimal unsigned LEB128 (`binary.PutUvarint`). Readers
  reject a varint longer than **6 bytes** (Java's `shift > 35` rule) — Java
  never writes more, and matching the strictness keeps validation identical.
- CRC-32C: `crc32.Checksum(chunk[:len-4], crc32.MakeTable(crc32.Castagnoli))`,
  stored little-endian.
- `len` is the UTF-8 **byte** length; a zero-length symbol is legal (1 byte),
  so `entryBytes >= 1` always holds.
- Symbol id `i` = i-th entry across all chunks; ids dense from 0, never stored.
- One chunk per `appendSymbols` call (one call per published frame carrying
  new symbols); empty appends write nothing. **No fsync ever** (write-ahead
  ordering before frame publish is the contract; a host-crash tear is caught
  by the CRC + the send loop's torn-dict guard). Go doesn't preallocate, so no
  truncate-on-close is needed.

### 3.2 Writer (`appendSymbols`, `qwp_sf_symbol_dict.go:228-250`)

Emit one chunk per call: build `uvarint(count) || uvarint(len(entries)) ||
entries || crc` in the existing `scratch`, one positional write at
`appendOffset`, advance `appendOffset`/`count` only after the full write.
Preserve resume-from-`size()` idempotency at the caller
(`persistNewSymbols`, `qwp_sender_cursor.go:532-547`): a failed publish must
not double-append. The entries region byte-encoding is unchanged, so the
producer-side encoding helpers are reusable as-is.

### 3.3 Reader — discrimination and migration

`qwpSfSymbolDictOpenExisting` (`:166-205`) is replaced by a strict chunked
scan with a three-bucket disposition. Scan from offset 8; per chunk, **stop
(end of trusted prefix)** on any of: truncated/over-long `entryCount` varint;
same for `entryBytes`; `entriesStart + entryBytes + 4 > fileLen` (torn);
CRC mismatch; `entryCount <= 0`; `entryBytes <= 0`; count/length overflow
(keep Go's existing 1 MiB entry / 1 GiB file caps — stricter than Java but
never disagree on real files); entries region not consisting of exactly
`entryCount` well-formed `[len][utf8]` records consuming it **exactly**.
Nothing after the first bad chunk is trusted.

Disposition:

1. **≥ 1 valid chunk** (CRC-proven chunked file): accept the trusted prefix.
   If `validLen < fileLen`, **ftruncate to `validLen`**; a truncate failure is
   a retriable operational error, not a degrade (Java `:939-942`). Seed
   `loaded` from the concatenated entries, `appendOffset = validLen`.
2. **File length == 8** (header only): valid empty dict in both formats.
3. **Zero valid chunks and length > 8**: ambiguous — either a legacy flat file
   (Go ≤ v4.x, or pre-merge Java) or a chunked file whose first chunk tore.
   Run the legacy flat parser (today's `:185-198`) as a *probe only*: if it
   yields ≥ 1 entry, return a new typed error,
   `qwpSfErrSymbolDictAmbiguousFormat`, whose message says the body may be
   legacy-flat data or a torn first chunk and carries remediation for both:
   use Go ≤ v4.x only when an older client wrote it; otherwise restore the
   file, or delete it to fall back to full-dict frames only when the slot has no
   unacked delta frames. If the flat probe also yields nothing, keep today's
   corrupt-dict behavior. **Never silently trust a flat parse** — a torn
   first-chunk Java file can flat-parse into garbage symbols, and registering
   those corrupts replayed rows silently; fail-closed is the review's explicit
   requirement.

Wiring of the new error: `qwpSfSymbolDictOpenRecovered`
(`qwp_sf_engine.go:334-337` caller) already treats a dict error as a fatal
recovery error — keep that; classify `qwpSfErrSymbolDictAmbiguousFormat` as
**operational** (not `qwpSfErrRecoveryFailClosed`): the bytes are healthy for
an older client, so the slot must not be auto-quarantined by the Fix-1 sender
policy; startup fails with the remediation message. Drainers `.failed` the
slot with that message (sanctioned, operator-visible). `qwpSfSymbolDictOpen`
(fresh path, `:107-120`) keeps recreate-on-corrupt but must **not** recreate
on the legacy-format error (that would silently destroy a legacy dict —
propagate instead).

No auto-migration and no tool in this fix: remediation is documented in the
error text and CHANGELOG. (Rejected alternative, for the record: trusting an
exact-consumption flat parse cannot distinguish legacy bytes from torn-Java
bytes with proof, and the failure mode is silent symbol corruption.)

### 3.4 Consumers

`loadedSymbols()` / `size()` contracts are unchanged, so the send-loop mirror
seeding (`qwp_sf_send_loop.go:503-504, 2370-2387`), catch-up
(`:2233-2245`), producer seeding (`qwp_sender_cursor.go:552-573`), and the
torn-dict guard (`qwp_sf_send_loop.go:1400-1422`) need no changes. The guard's
semantics get *stronger*: a host-crash tear now surfaces at dict open (CRC)
instead of only at replay.

### 3.5 Tests (Fix 3)

- Golden byte-compat both ways, following the
  `TestQwpSfSegmentGoldenFileJavaConformance` precedent
  (`qwp_sf_segment_test.go:390`): (a) hand-assemble the Java encoding of
  `["x"]` — header `53 59 44 31 01 00 00 00`, chunk `01 02 01 78` + CRC-32C
  over those 4 bytes, LE — and assert Go opens it as exactly `["x"]` (this is
  the review's probe 2 inverted); (b) write `["x", "", "多字节"]` with the Go
  writer and assert the raw bytes match an independently hand-computed Java
  encoding (chunk headers, entries, CRC).
- Every scan break condition from §3.3 as a table test (torn varint, torn
  chunk, CRC flip, zero count, zero bytes, entries under/overrun, over-long
  varint), asserting prefix-trust + physical truncation and that a truncate
  failure surfaces as an error.
- Legacy flat file with entries → `qwpSfErrSymbolDictAmbiguousFormat`, file left
  byte-identical (no truncate, no recreate) on both `OpenRecovered` and
  fresh `Open`; drainer marks `.failed` with the message; sender startup fails
  without quarantining the slot.
- Header-only 8-byte file → empty dict (both formats' shared boundary).
- Rewrite the existing suite (`qwp_sf_symbol_dict_test.go`) onto the chunked
  format; `…TornTrailingEntrySelfHeals` becomes torn-trailing-**chunk**
  self-heals with physical truncation; `…AppendPersistsAcrossReopen`,
  `…EmptySymbolRoundTrips` unchanged in spirit.
- End-to-end: `qwp_delta_dict_recovery_test.go` and
  `qwp_delta_dict_test.go:270` (SF slot recovery + catch-up) must pass
  unmodified in behavior — they pin that the id map survives the format swap.
- Alloc check: `appendSymbols` sits on the flush path — extend scratch reuse
  so the chunk assembly does not allocate per flush, and keep
  `TestQwpSenderSteadyStateZeroAllocs` green (symbols are interned after
  warmup, so steady state shouldn't append; verify).

---

## Cross-cutting

- **Docs**: update `CLAUDE.md` §QWP/SF for: the manifest file and its
  fail-closed recovery contract, the slot-directory file table
  (+`sf-manifest.bin`, `sf-manifest.bin.corrupt`, `quarantined/`), the new
  close/quiescence contract (`closeCompleted`, deferred cleanup), the chunked
  dict format, and the control-point fsync policy. Add an addendum to
  `design/qwp-delta-symbol-dict.md` (§on-disk format superseded by the merged
  Java chunked format). Update `design/qwp-implementation-review.md` findings
  with "addressed by <commit>" notes as each lands.
- **Invariant B**: nothing here adds a terminal to a *running* sender's
  transport path. New terminals are confined to construction/recovery
  (sanctioned: build-time errors, drainer `.failed` on recovery, the existing
  torn-dict replay guard). State this in each commit message; the review-pr
  checklist will be applied.
- **Conf surface**: no new config keys. `sf_durability=memory` remains the
  only accepted value; the new fsyncs are control-point (creation, rotation,
  trim batch, drain), matching Java's baseline behavior in the same mode.
- **Compatibility statement per commit**: Fix 1 migrates legacy Go slots in place
  (manifest created on first recovery) — one-way; older Go clients refuse the
  flagged segments? No: old clients ignore the flags byte and don't know the
  manifest — document that downgrade after upgrade is unsupported once a slot
  is migrated. Fix 3 is one-way for the dict file as well.

### Validation gates (before each commit)

```bash
git submodule update --init --recursive   # interop fixture, avoids the known FAIL
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
go test ./... -count=1
go test -race -count=1 -run 'TestQwpSf|TestSfConf' .
go test -v -bench BenchmarkQwpSenderSteadyState -benchmem -run ^$ .   # 0 allocs/op
```

### Explicit non-goals

- BINARY / IPv4 ingress parity (review §1 — separate feature work).
- `sf_durability` periodic/full barrier modes (Java `Sender.java:2895-2948`).
- Recovery via bounded positioned reads instead of mmap (Java's SIGBUS
  hardening) — worthwhile follow-up, not required for correctness here.
- A background flock-release retry daemon (Java's shared retry thread);
  Go recovers via retried `Close()`, the pool reprobe, or process exit.
- Production shared-manager support (Go production is always one manager per
  engine; the per-ring barrier exists for the test-only shared path).
