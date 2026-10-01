# QWP: accepted limits and maintainer invariants

This file is for maintainers and reviewers of the QWP store-and-forward (SF)
and shutdown code. The [README](../README.md) tells users what to do. This file
records what the design deliberately does not handle, and the rules code
changes must keep.

- A behavior listed here as an accepted limit is not a defect by itself. It
  becomes one when a change widens it, or when the conditions that bound it no
  longer hold.
- No limit here excuses false success, lost ownership, or destroyed evidence.
- When a review finding is accepted rather than fixed, record it here with the
  conditions that reach it and the reason it is accepted. The README keeps only
  what a user needs to act on.

## Shutdown and resource ownership

The user contract is the README's
[QWP shutdown and ownership](../README.md#qwp-shutdown-and-ownership) section
and the Close, slot-release, error and callback Go docs.

The client must not unmap memory while a reader can still access it, or reuse a
store-and-forward slot while old client code can still use it. The client
remains responsible for resources after timeouts and internal failures, even if
construction failed without returning a handle. A cleanup panic is not evidence
of corrupt files and does not justify quarantining a slot or marking it
`.failed`.

Cleanup must preserve saved rows and follow the acknowledgement, recovery, and
safe file-deletion rules. It does not promise an empty directory. These
guarantees do not cover faults that terminate the process or arbitrary memory
corruption. Panics caused by supported inputs and ordinary data races are still
bugs.

## Store-and-forward storage

### Disk-block reservation

Where the filesystem rejects native reservation, Go fails with
`ErrSfDurability`; the Java client falls back to sparse files. Successful
reservation does not certify existing sparse files or guarantee safety against
every later storage failure or copy-on-write allocation.

### Directory-sync epochs

On Unix, SF namespace changes are separated into directory-sync epochs: a
dependent manifest update/removal is not allowed to become durable before the
segment names it depends on. This protects recovery across an OS crash within
the platform `fsync` guarantee. Windows exposes no documented, unprivileged
equivalent of directory `fsync` (see `qwp_sf_dirsync_windows.go`); on Windows SF
protects process-restart recovery but does not promise host-crash ordering for
file creation, rename, and removal.

### Crash durability with `sf_durability=memory`

A segment rotation makes three things durable in order: every published frame
of the segment it seals (`msync` + `fsync`), the next segment's header, and
`sf-manifest.bin`. The manifest therefore never points past frames the disk
lost. The segment manager starts writing the active segment back while it fills
(`sync_file_range` on Linux, `msync(MS_ASYNC)` on other Unixes,
`FlushViewOfFile` on Windows), so a rotation usually waits only for the last
part. Before a trim deletes acknowledged frames, the manager makes
`.symbol-dict` durable, because later frames can refer to symbols those frames
introduced. See [Symbol dictionary durability](#symbol-dictionary-durability)
for the whole dictionary invariant.

Accepted limit: frames in the active segment are not fsynced as they are
appended, so an OS crash or power loss can lose the end of the active segment.
Recovery then keeps its readable beginning under the damaged-tail policy.
Process restart, host crash and power loss are not equivalent guarantees:
Darwin `fsync` is not `F_FULLFSYNC`, and the Windows directory barrier is a
no-op.

### Acknowledgement evidence at startup

Residual side files are permitted, so startup, not close, is what makes an old
`.ack-watermark` record safe. Every disk-backed construction inspects that file
under the slot lock and decides from the history recovery actually established:

- Recovery produced no ring: the record describes a previous lifecycle whose
  frames are gone, and this session restarts frame numbering at 0. An empty
  recovered ring is different: it retains its sequence base and remains part of
  its existing history even though it currently holds no frames.
- A record above the recovered history's published tip: no correctly operating
  session for this history produced it. Recovery can also produce this
  legitimately, by discarding an unreadable active tail.

In both cases the record is retired durably, by truncating the file to zero
bytes (both record slots together), rather than ignored for one run. Ignoring
it is not sufficient, because the same numbers get republished, after which the
old record looks plausible again to the next restart. Every construction then
syncs the prepared file and performs the slot-directory barrier before
allocating or mapping it, even when the file already looks empty, invalid, or
acceptable: bytes visible in the page cache are no evidence that an earlier
attempt's barrier completed. That unconditional checkpoint is the retry
mechanism; no marker file or format change is involved, and a zero-length file
is only an intermediate startup state.

If inspecting, resetting, or either barrier fails, construction fails with a
retriable storage error and keeps its cleanup obligations; only a later
allocation or write-back failure may continue without a mapped watermark, and a
nil in-memory watermark is never evidence that the file on disk is safe. Such a
failure does not authorize dropping unacknowledged frames or quarantining the
slot, and it does not promise that a failing or full disk can be started on or
drained. Discarding ACK evidence keeps the segment-derived replay position,
immediately before the lowest surviving segment rather than FSN 0, and can
replay surviving rows that were already acknowledged: SF does not provide
exactly-once delivery. The valid-prefix/zeroed-suffix policy for a damaged
active tail still applies, and discarded frames are not reconstructed.

Accepted limit: a stale record whose value still falls inside the surviving
frame range cannot be distinguished from a legitimate acknowledgement. The
shared format carries no slot-lifecycle identifier, so the client neither
detects nor repairs such pre-existing slots. These startup barriers use the
same platform guarantees as the rest of SF.

### A missing active segment can go undetected

Accepted limit: when the saved head and active boundaries in `sf-manifest.bin`
are equal and the active segment is gone, the directory looks exactly like a
fully delivered close that crashed while deleting its files, so recovery starts
the slot empty. A segment deleted by hand in that state takes its unsent rows
with it, and nothing reports them. The README tells operators to remove segment
files only together with the whole slot directory. The comments at the two
empty-slot checks in `qwp_sf_recovery.go` describe the same case.

## Symbol dictionary recovery

### Symbol dictionary durability

Invariant: a disk-backed engine always holds an open `.symbol-dict` containing
every id that any frame of its slot uses. Construction makes the whole
dictionary durable before the engine is registered with the segment manager,
which is before any frame of the session exists, and the manager makes anything
appended afterwards durable before a trim deletes frames. An id is needed from
the dictionary in only three ways, and each is covered:

- A surviving frame introduced it: a delta frame carries the symbols it
  introduces.
- The frame that introduced it was trimmed: the trim fsynced the dictionary
  first.
- No frame ever carried it, for example an id an earlier session persisted for
  a frame it never published. The producer is seeded with it and its first
  frame's delta starts above it. It was in the dictionary at construction, so
  the construction-time fsync made it durable.

So the kernel's writeback order for the active segment does not matter, and an
OS crash within the platform `fsync` guarantee cannot leave a dictionary gap.
Construction decides every recovery verdict, including the size limits below,
before it changes anything on disk. Only an accepted slot has its dictionary's
untrusted tail cut, healed, rewritten, or, for a ring with no frames,
truncated.

A store-and-forward sender never falls back to full-dictionary frames. A
dictionary that cannot be created, healed, rewritten, or fsynced at startup
fails construction with a retriable `ErrSfDurability`, and a failed append on a
flush fails that flush the same way with its rows still pending. A full or
failing disk therefore stops an SF sender or drainer rather than degrading it,
and a drainer writes no `.failed` for it.

The invariant covers slots this client writes. A slot adopted from the Java
client brings whatever its writer made durable; see
[Differences from the Java client](#differences-from-the-java-client).

### An unusable dictionary is rewritten in place

When a recovered slot that holds frames has no usable `.symbol-dict` (absent,
shorter than its header, bad magic or version, no valid chunk, a legacy flat
file, or larger than the 1 GiB read limit), and the frames pass the gap and
size checks, construction rewrites the file in place from the symbols the
frames spell out. The old bytes are discarded, not preserved: they were
unusable, and the frames, which recovery already trusts, hold everything they
held that any frame needs. A stat, open, or read failure is a retriable error,
never a reason to rewrite. A crash during the rewrite leaves the frames intact,
and the next start rewrites again. A recovered ring with no frames always
starts an empty dictionary, because no frame refers to the old ids.

### Dictionary limits refuse a slot

Accepted limit. The writer and recovery share three limits: 1M ids, 1 MiB per
symbol name, and 1 GiB measured by a conservative bound (each name's length
plus 22 bytes, plus the 8-byte header), not by raw file size. The producer
refuses a new symbol value that would break one, so no slot this client writes
can break one. A recovered slot whose dictionary breaks one is a fail-closed
verdict: it is preserved under `.unreplayable-<n>`, or marked `.failed` by a
drainer.

Released Java clients (1.3.7 and later) enforce the same 1M-id limit, but
neither the per-name limit nor the 1 GiB limit, so they can write such a slot.
For a Java slot whose frames depend on the dictionary, this client already
refused a file over 1 GiB as unusable and a name over 1 MiB as a malformed
chunk. A slot left by Java's full-dictionary fallback is refused when one of its
frames carries a symbol value over 1 MiB, because this client rebuilds a
dictionary from those frames. Both cases need extreme symbol data, and raising
the limits is a separate decision: this client reads the whole dictionary into
memory at recovery, while the Java client maps it.

### Differences from the Java client

The file format is the same; the behavior differs:

- The Java client falls back to full-dictionary frames when a dictionary write
  fails, at startup or on a flush. This client fails the operation with
  `ErrSfDurability` instead. A Go sender or drainer adopting a slot that Java's
  fallback left rebuilds the dictionary from the self-sufficient frames and
  continues with delta frames. Every connection it opens then starts with a
  dictionary catch-up before replay, even while the frames still to replay are
  self-sufficient; that costs the dictionary's bytes once per connection.
- The Java client never fsyncs the dictionary, so a host crash can leave a
  Java-written slot whose dictionary lacks ids that trimmed frames introduced.
  Recovery refuses such a slot, correctly.
- The Java writer allows a dictionary of about 2 GiB and names of any length;
  see [Dictionary limits refuse a slot](#dictionary-limits-refuse-a-slot).

### A dictionary gap inside acknowledged frames refuses a later restart

Accepted limit. `qwpSfAnalyzeRecoveredDict` in `qwp_sf_recovered_dict.go`
accepts a slot whose `.symbol-dict` is missing ids that only acknowledged
frames build on, because no frame waiting to be sent needs them. The session
that follows seeds its dictionary from the ids recovery trusts. When there are
any, every frame it publishes carries a symbol delta starting above id 0,
including frames with no symbol column. If the process restarts while the
segments holding the old acknowledged frames are still on disk, those new
unacknowledged frames sit behind the gap, and recovery refuses the slot. A
second route: when the new session adds more symbols than the gap is wide, the
name check compares the old acknowledged frames' names with the reused ids and
refuses the slot even when every frame is acknowledged. Neither refusal can
happen once the segments holding the old frames are trimmed.

Why it is accepted:

- The starting state needs damage to a middle chunk of `.symbol-dict` that its
  CRC detects, such as bit rot. An OS crash does not produce it within the
  platform `fsync` guarantee, because of the
  [dictionary durability invariant](#symbol-dictionary-durability).
- The outcome fails closed. No row is replayed with the wrong symbols, and the
  refused slot is preserved byte for byte under `.unreplayable-<n>`, or marked
  `.failed` by a drainer. Rows the second session published are in that copy
  and need an operator to deliver them.
- Recovering these slots would take a look-ahead rule in the fail-closed
  analysis (skip an acknowledged frame when a later frame has
  `0 < deltaStart <` its own `deltaStart`). That adds complexity to a safety
  check for a state that needs detected media damage.

## Quarantine and slot locking

The user rules are in the README's
[Quarantined slots](../README.md#quarantined-slots) section and the
`QuarantinedSlotPath` / `WithSenderId` Go docs.

### Naming and exclusion

The client picks the same `.unreplayable-<n>` names as the Java client and,
like it, excludes any directory whose name contains `.unreplayable-` from
automatic adoption, by name, regardless of what the directory holds. A valid
`sender_id` cannot contain a dot, so the namespace cannot collide with a
configured slot.

An existing `.failed` is kept exactly as it is, including when this client
would have written a different reason. Neither the marker's presence nor its
contents affect exclusion.

### The legacy `quarantined/` container

Earlier development builds of this client used a nested container,
`<sf_dir>/quarantined/<sender_id>-<nanos>/`. No tagged release up to v4.2.0
creates it. Existing copies are left exactly where they are: nothing flattens,
renames, scans, reclaims or resumes them. The name `quarantined` is also a
legal `sender_id`; a sender configured with it starts normally when the path is
absent, empty, or holds only ordinary slot files, and refuses to start when the
directory holds child directories or symlinks that could be an older client's
evidence.

### Byte accounting for `.corrupt` files

The manager counts `.corrupt` files when it opens the slot. While the byte limit
blocks a new segment, it rescans the slot at most once per second, so it
notices deleted `.corrupt` files within one second without scanning on every
1 ms poll. If a scan fails, it keeps the previous byte count. Whole-slot copies
under `.unreplayable-<n>`, and under the legacy `quarantined/` container, do
not count toward a slot's limit and are never reclaimed to regain capacity.

### Guarantees and limits

These bounds apply to quarantine and to the slot-name locking around it.

- **Cooperating participants only.** A slot's close → rename → recreate
  transition is serialised by a parent-anchored lock under
  `<sf_dir>/.slot-locks/`, which this client takes before opening a slot and
  before adopting an orphan. It protects processes that use this protocol, on
  filesystems providing the advisory locking and rename semantics it relies on.
  It does not protect against an operator moving files, an incompatible or older
  client that never takes the lock, or a host where advisory locks do not work.
  Stopping orphan adoption alone is not sufficient, because an older foreground
  sender can still create the legacy `quarantined/` container. This client
  deliberately reuses stale lock files rather than unlinking a pathname that
  another process may already have open. The namespace and locking layout were
  inspected at QuestDB Java client revision
  `981bdb02a471f3b290c89b8e78cbc422610e329e`; that is evidence about that
  revision, not a minimum compatible release or proof of live cross-client
  interoperability. Concurrent Go/Java transitions and every filesystem/OS
  combination have not been integration-tested.
- **Offline reuse does not change formats.** The quarantine layout does not
  alter ordinary slot payload formats, but offline reuse remains subject to the
  existing format and migration restrictions, including the unsupported
  downgrade after legacy Go-slot migration.
- **Preservation is not backup.** Quarantine does not overwrite, delete, replay
  or reclaim what it moves, but it does not repair damage recovery already
  found, and it cannot protect a copy from later storage failure, another
  program, or an operator. Recovery steps that ran before the handoff, such as
  the README's damaged-tail policy, still applied.
- **The transition is not atomic.** Renaming the old slot and creating the fresh
  one are separate steps with no rollback. A failure after the rename reports
  the destination that already exists rather than pretending nothing happened;
  the slot can be left preserved with no fresh slot in place, and cleanup may
  still own resources. Crash behaviour is bounded by the same platform
  guarantees as the rest of SF.
- **Refusals are conditional, not timed.** The 64-destination policy bounds how
  many copies one slot name may accumulate. It says nothing about how long a
  quarantine takes, how much disk it uses, or whether one succeeds at all:
  inspection, rename or barrier failures, an over-long destination name, or an
  unresolved lock can each refuse construction. Nothing is deleted to make
  progress.
- **Markers and logs are best-effort.** A `.failed` marker may be missing or
  incomplete, and diagnostics may be filtered, discarded or lost. Neither is a
  condition for excluding a preserved copy from adoption. Application logging
  handlers keep the restrictions documented on `WithLogger`.
- **Legacy-container refusals are deliberately conservative.** An unrelated
  child directory under a `quarantined` slot name causes a refusal, including
  for case variants such as `Quarantined` on a case-sensitive filesystem. That
  is a request for a human look, not a corruption verdict, and it never
  authorises the client to move or delete anything. A permission or I/O failure
  while inspecting is reported as the operational fault it is.
- **Reporting is historical.** `QuarantinedSlotPath` reports where this sender
  put bytes during its own construction. It is not a live check that the
  directory still exists, a durability receipt, or a record that survives a
  crash.
