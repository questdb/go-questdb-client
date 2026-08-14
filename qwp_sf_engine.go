/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package questdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// qwpSfEngineDefaultAppendDeadline is the default backpressure
// deadline for appendBlocking. Mirrors Java's
// CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS = 30s.
const qwpSfEngineDefaultAppendDeadline = 30 * time.Second

// qwpSfEngineParkInterval is how long appendBlocking sleeps between
// retries while waiting for the manager to free space. Mirrors
// Java's 50µs LockSupport.parkNanos.
const qwpSfEngineParkInterval = 50 * time.Microsecond

// ErrBackpressureTimeout is the sentinel a producer call
// (At / AtNow / Flush / FlushAndGetSequence) wraps when the
// store-and-forward append deadline (WithSfAppendDeadline /
// sf_append_deadline_millis) expires before the cursor engine frees
// space. The wire path is not draining — the server is slow or
// disconnected, or sf_max_total_bytes is too small. Match it with
// errors.Is; the wrapped error carries the deadline and reconnect
// diagnostics in its message.
var ErrBackpressureTimeout = errors.New(
	"qwp/sf: cursor ring backpressured — wire path is not draining (server slow / disconnected, or sf_max_total_bytes too small)")

// qwpSfTestBeforeSegmentUnlinkHook is a test seam for holding terminal cleanup
// after quiescence while a concurrent Close arrives. Production leaves it nil.
var qwpSfTestBeforeSegmentUnlinkHook atomic.Pointer[func(path string)]

// qwpSfTestEngineFinishCloseHook counts terminal-cleanup entry independently
// of how many segment files that cleanup unlinks. Production leaves it nil.
var qwpSfTestEngineFinishCloseHook atomic.Pointer[func()]

// qwpSfErrEngineClosed is returned by engineAppendBlocking when the
// engine is closed underneath an in-flight or backpressure-parked
// append. The canonical trigger is a SenderErrorHandler calling
// Close() while the producer is stalled in the backpressure spin on a
// wedged wire (a HALT stops the send loop draining, so ackedFsn never
// advances and the ring stays full). The producer gets this clean
// error instead of dereferencing a segment that engineClose's
// segmentRingClose has just nil'd + munmapped.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrEngineClosed = errors.New("qwp/sf: cursor engine closed")

// qwpSfCursorEngine is the cursor-engine facade that bundles a
// qwpSfSegmentRing with a qwpSfSegmentManager and exposes the
// user-facing API the wire-send loop calls into. Keeps SF append
// work on the user goroutine (where it belongs) and segment
// lifecycle work on the manager goroutine (where it belongs).
//
// Responsibilities:
//   - Owning the ring + manager lifecycle (open / close / startup
//     recovery).
//   - Providing a user-thread append path that handles backpressure.
//   - Exposing read accessors for the I/O thread:
//     enginePublishedFsn, engineActiveSegment, engineSealedSegments.
//   - Routing server ACKs to the ring for trim.
//
// Not in scope:
//   - Multi-producer support. Single producer (one user goroutine)
//     only.
type qwpSfCursorEngine struct {
	sfDir            string
	segmentSizeBytes int64

	manager      *qwpSfSegmentManager
	managerEntry *qwpSfManagerRingEntry
	ownsManager  bool
	slotLock     *qwpSfSlotLock
	ring         *qwpSfSegmentRing

	// watermark is the engine-owned mmap'd .ack-watermark file
	// (sf-client.md §5.4). nil in memory mode and when the file
	// could not be opened (recovery then falls back to the
	// segment-derived lowestBase-1 seed). Lifetime is tied to the
	// engine: opened in the constructor after the slot lock is
	// acquired, read once to refine the recovery seed, written
	// through by the segment manager on every tick where ackedFsn
	// advanced, closed in engineClose AFTER the manager (the sole
	// writer) is gone.
	watermark *qwpSfAckWatermark

	// persistedSymbolDict is the engine-owned .symbol-dict side-file
	// (disk mode only; nil in memory mode and when it could not open). It
	// lets a recovered / orphan-drained slot re-register the whole symbol
	// dictionary on the fresh server before replaying its non-self-
	// sufficient delta frames. Opened in the constructor alongside the
	// watermark, closed in engineClose. nil in disk mode disables delta
	// encoding for the slot (the sender keeps full self-sufficient frames).
	persistedSymbolDict *qwpSfSymbolDict

	appendDeadline time.Duration

	// recoveredFromDisk is true when the constructor recovered an
	// existing on-disk slot rather than starting fresh. Diagnostic
	// accessor for tests and observability; cursor frames are
	// self-sufficient (every frame carries full schema + full
	// symbol-dict delta), so producer-side schema reset on recovery
	// is not required at the engine level.
	recoveredFromDisk bool

	// backpressureStalls counts how many times appendBlocking
	// observed qwpSfBackpressureNoSpare on its first try and had to
	// wait. One increment per blocking-call (not per spin).
	backpressureStalls atomic.Int64

	// reconnectStatus is the (optional) snapshot getter wired in by
	// the I/O send loop after it is constructed. When nil (e.g. tests
	// using the engine standalone) the backpressure-timeout error
	// falls back to the loop-agnostic "wire path is not draining"
	// wording. When non-nil, engineAppendBlocking checks it on
	// deadline expiry to distinguish "publishing but slow" from
	// "reconnecting" per spec §16, and includes attempt count +
	// outage elapsed in the latter case.
	reconnectStatus atomic.Pointer[func() (bool, int64, time.Time)]

	// terminalError is the (optional) snapshot getter wired in by the
	// I/O send loop after it is constructed, alongside reconnectStatus.
	// It returns the loop's latched terminal error — a *SenderError on a
	// HALT, a plain error on a transport-fatal condition — or nil while
	// the loop is healthy or merely reconnecting. engineAppendBlocking
	// polls it on every backpressure
	// spin iteration: a HALT stops the send loop draining the ring (ACK-
	// driven trim ceases), so a producer parked on a full ring would
	// otherwise wait out the whole appendDeadline and return the generic
	// backpressure-timeout error instead of the real terminal cause.
	// Polling it lets the parked producer fail fast with the latched
	// error. nil (the engine used standalone in tests) disables the
	// check; the spin then relies on the deadline / ctx / closed exits
	// alone.
	terminalError atomic.Pointer[func() error]

	// closed is set by engineClose. atomic.Bool so tests / status
	// accessors can sample it from any goroutine.
	closed atomic.Bool

	// closeCompleted is published only after terminal cleanup has released the
	// slot flock (or immediately in memory mode). terminalCleanupClaimed gives
	// exactly one caller -- Close or the manager worker -- ownership of that
	// cleanup. The deferred fields are written before handoff and then read by
	// the worker without taking engine locks.
	closeCompleted          atomic.Bool
	terminalCleanupClaimed  atomic.Bool
	terminalResourcesClosed atomic.Bool
	deferredCleanupOwned    atomic.Bool
	closeRetryOwnerStarted  atomic.Bool
	deferredFullyDrained    atomic.Bool
	deferredLeakSegments    atomic.Bool
	deferredClose           func()

	// appendMu serializes the producer's ring-append path against
	// engineClose's segment teardown. The producer's only entry into
	// appendOrFsn is engineAppendBlocking, which takes this lock around
	// each ring touch (initial try and every backpressure-spin retry)
	// and re-checks closed under it; engineClose holds it across the
	// manager + ring teardown. Together they guarantee no append is
	// dereferencing the active segment while segmentRingClose nil's and
	// munmaps it, and that every append after close observes closed and
	// bails with qwpSfErrEngineClosed. Without it a Close() from a
	// SenderErrorHandler (running on the dispatcher goroutine) while the
	// producer is parked in the backpressure spin tears the segment down
	// under the producer — a nil-pointer deref in memory mode, a SIGBUS
	// on the munmapped pages in SF mode. Off the per-row hot path:
	// appendOrFsn runs once per flush, not per row.
	appendMu sync.Mutex
}

// qwpSfNewCursorEngine creates an engine with a private
// qwpSfSegmentManager (owned by the engine, closed alongside it).
// Pass sfDir = "" for memory-mode (no disk involvement); a non-empty
// sfDir places the engine in store-and-forward mode against that
// slot directory.
//
// Returns an error if the slot lock can't be acquired (another
// process is using the slot), or if recovery encounters an
// inconsistent on-disk state.
func qwpSfNewCursorEngine(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	return qwpSfNewCursorEngineWithRecoveryPolicy(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline, true)
}

func qwpSfNewCursorEngineForDrainer(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	return qwpSfNewCursorEngineWithRecoveryPolicy(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline, false)
}

func qwpSfNewCursorEngineWithRecoveryPolicy(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration, recoverForeground bool) (*qwpSfCursorEngine, error) {
	for attempt := 0; ; attempt++ {
		e, err := qwpSfNewCursorEngineOnce(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline)
		if err == nil || sfDir == "" || !recoverForeground {
			return e, err
		}
		if errors.Is(err, qwpSfErrSanitizedResidue) && attempt == 0 {
			qwpEffectiveLogger(nil).Error("qwp/sf: sealed-segment residue was sanitized; retrying recovery once", "slot", sfDir, "error", err)
			continue
		}
		if errors.Is(err, qwpSfErrRecoveryFailClosed) && attempt <= 1 {
			quarantined, quarantineErr := qwpSfQuarantineSlot(sfDir)
			if quarantineErr != nil {
				return nil, fmt.Errorf("%w; additionally could not quarantine slot: %v", err, quarantineErr)
			}
			qwpEffectiveLogger(nil).Error("qwp/sf: recovery failed closed; preserved the slot and starting fresh", "slot", sfDir, "quarantined", quarantined, "error", err)
			continue
		}
		return nil, err
	}
}

func qwpSfNewCursorEngineOnce(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	mgr, err := qwpSfNewSegmentManager(segmentSizeBytes, qwpSfManagerDefaultPoll, maxTotalBytes)
	if err != nil {
		return nil, err
	}
	mgr.segmentManagerStart()
	// Close the manager (joining its worker goroutine) on any failure
	// exit of the inner constructor — error return AND panic. The inner
	// constructor's own deferred guard releases the slot flock on the
	// same unwind; this guard covers the one resource it can't see — the
	// manager we own here. ok flips true only once the engine adopts it.
	ok := false
	defer func() {
		if !ok {
			mgr.segmentManagerClose()
		}
	}()
	e, err := qwpSfNewCursorEngineWithManager(sfDir, segmentSizeBytes, mgr, appendDeadline)
	if err != nil {
		return nil, err
	}
	e.ownsManager = true
	ok = true
	return e, nil
}

// qwpSfNewCursorEngineWithManager creates an engine that shares the
// given segment manager (must already be started). The caller
// retains ownership of the manager; engineClose will not stop it.
func qwpSfNewCursorEngineWithManager(sfDir string, segmentSizeBytes int64, mgr *qwpSfSegmentManager, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	if appendDeadline <= 0 {
		appendDeadline = qwpSfEngineDefaultAppendDeadline
	}
	memoryMode := sfDir == ""
	var (
		lock              *qwpSfSlotLock
		ring              *qwpSfSegmentRing
		watermark         *qwpSfAckWatermark
		persistedDict     *qwpSfSymbolDict
		recoveredFromDisk bool
		err               error
	)
	if !memoryMode {
		// Acquire the slot lock BEFORE touching any *.sfa files.
		// Two engines pointed at the same slot would otherwise race
		// on recovery and create overlapping FSN ranges.
		lock, err = qwpSfAcquireSlotLock(sfDir)
		if err != nil {
			return nil, err
		}
	}
	// Teardown for every failure exit — error return AND panic. ok
	// flips true only once the engine adopts these resources, so the
	// deferred guard runs cleanup on any unwind between the flock
	// acquisition above and the success return below. Skipping it on a
	// panic would strand the slot: the kernel-held flock survives the
	// process, wedging every future foreground open and orphan drainer
	// (which can no longer release a lock it never took), so the slot's
	// unacked data becomes unrecoverable.
	//
	// Release order mirrors engineClose and the Java reference: the
	// ring's segment mmaps, then the watermark's own mmap + fd, then the
	// slot flock LAST so it outlives every other cleanup. A failed
	// registration never reaches the manager's ring list, so the ring
	// needs no deregister here — and cleanup touches no manager state,
	// which keeps it safe to run on the unwind of a registration panic.
	ok := false
	cleanup := func() {
		if ring != nil {
			_ = ring.segmentRingClose()
		}
		if watermark != nil {
			_ = watermark.close()
		}
		if persistedDict != nil {
			_ = persistedDict.close()
		}
		if lock != nil {
			_ = lock.close()
		}
	}
	defer func() {
		if !ok {
			cleanup()
		}
	}()
	// Disk mode: try to recover any *.sfa files left behind by a
	// prior session before deciding to start fresh. Without this the
	// engine would create a new sf-initial.sfa at baseSeq=0,
	// overlapping FSNs already on disk and corrupting ACK
	// translation, trim, and replay.
	if !memoryMode {
		ring, _, err = qwpSfRecoverRing(sfDir, segmentSizeBytes)
		if err != nil {
			return nil, err
		}
		recoveredFromDisk = ring != nil
		if ring != nil {
			// Seed ackedFsn to one below the lowest segment's baseSeq.
			// We don't know what was actually acked before the prior
			// session crashed, but anything trimmed off the ring's
			// bottom must have been acked (trim is ack-driven).
			// Without this seed, ackedFsn stays at -1 and the I/O
			// loop's start-time positioning would walk to FSN 0 —
			// which may not exist on disk if earlier segments have
			// been trimmed, causing it to fall through to the active
			// segment's tip and skip the unacked sealed segments
			// entirely.
			first := ring.firstSealed()
			lowest := int64(0)
			if first != nil {
				lowest = first.segmentBaseSeq()
			} else if a := ring.getActiveSegment(); a != nil {
				lowest = a.segmentBaseSeq()
			}
			baseSeed := lowest - 1
			// Refine the seed with the persisted ack watermark
			// (sf-client.md §5.4 / §6.5 / §18.1). It may carry
			// durable-acks the previous sender — or another client
			// whose orphan slot this drainer adopted — received for
			// frames inside the lowest surviving sealed segment.
			// Without honouring it those frames get re-replayed on a
			// fresh connection, producing row-level duplicates against
			// a still-alive server unless the table dedupes.
			//
			// max(watermark, lowestBase-1) absorbs both orderings of
			// the manager's "persist then trim" tick:
			//   - persist crashed before trim: segments still on disk
			//     are >= lowest, watermark is correct; max picks it.
			//   - trim ran before persist: those segments are gone so
			//     lowestBase is higher, watermark is stale; max picks
			//     lowestBase-1.
			//
			watermark, err = qwpSfAckWatermarkOpenRequired(sfDir)
			if err != nil || watermark == nil {
				if err == nil {
					err = errors.New("watermark open returned nil")
				}
				return nil, fmt.Errorf("qwp/sf: could not open required ack watermark: %w", err)
			}
			// Load the persisted symbol dictionary so this recovered slot's
			// delta frames can be re-registered on a fresh server before they
			// replay. A recovered slot's dictionary is NEVER recreated: its
			// segments reference the dictionary's ids by position, so truncating
			// a corrupt/mismatched file would restart the id space and re-register
			// the wrong names. A corrupt file therefore fails the open loudly (a
			// sanctioned terminal), and an absent file leaves persistedDict nil.
			persistedDict, err = qwpSfSymbolDictOpenRecovered(sfDir)
			if err != nil {
				return nil, err
			}
			// Absent dictionary with recovered frames: those frames may be
			// delta-encoded and reference ids we can no longer re-register.
			// Leaving persistedDict nil disables delta so the producer emits full
			// self-sufficient frames, and the send loop's guard rejects any
			// recovered delta frame rather than replaying it against a gap. When
			// the ring holds no frames there is nothing to alias against, so a
			// fresh dictionary is safe and keeps delta encoding available.
			if persistedDict == nil && ring.segmentRingPublishedFsn() < 0 {
				persistedDict = qwpSfSymbolDictOpenFresh(filepath.Join(sfDir, qwpSfSymbolDictFileName))
			}
			watermarkFsn := watermark.read() // nil-safe → INVALID
			candidate := baseSeed
			if watermarkFsn > candidate {
				candidate = watermarkFsn
			}
			// Reject a watermark past publishedFsn: a correctly
			// operating prior session cannot produce one, so an
			// excess value is corruption (torn write on a non-atomic
			// FS, bit-rot, manual edit). Trusting it would seed
			// ackedFsn = publishedFsn after the ring's own clamp and
			// position the cursor past every un-acked frame — silent
			// loss of the un-acked tail. Fall back to the
			// segment-derived seed so that tail still replays.
			seed := candidate
			if seed > ring.segmentRingPublishedFsn() {
				seed = baseSeed
			}
			if seed >= 0 {
				ring.acknowledge(seed)
			}
		}
	}
	if ring == nil {
		var initial *qwpSfSegment
		var initialPath string
		if memoryMode {
			initial, err = qwpSfCreateInMemorySegment(0, segmentSizeBytes)
		} else {
			// Fresh disk slot: any stale watermark refers to a
			// fully-drained lifecycle now gone. Unlink it before
			// opening so the new session's first read() correctly
			// reports INVALID (magic=0 on a freshly zero-filled
			// file) rather than honouring an FSN with no segments
			// behind it.
			qwpSfAckWatermarkRemoveOrphan(sfDir)
			if !qwpSfManifestRemove(sfDir) {
				return nil, fmt.Errorf("qwp/sf: could not remove stale manifest in %s", sfDir)
			}
			watermark, err = qwpSfAckWatermarkOpenRequired(sfDir)
			if err != nil {
				return nil, err
			}
			// Same stale-side-file hygiene for the symbol dictionary: a
			// fresh slot starts with an empty dictionary.
			qwpSfSymbolDictRemoveOrphan(sfDir)
			persistedDict, err = qwpSfSymbolDictOpen(sfDir)
			if err != nil {
				return nil, err
			}
			initialPath = filepath.Join(sfDir, "sf-initial.sfa")
			initial, err = qwpSfCreateSegment(initialPath, 0, segmentSizeBytes)
		}
		if err != nil {
			return nil, err
		}
		if !memoryMode {
			if err := initial.syncHeader(); err != nil {
				_ = initial.close()
				return nil, err
			}
			if err := qwpSfSyncDir(sfDir); err != nil {
				_ = initial.close()
				return nil, fmt.Errorf("qwp/sf: fsync fresh slot directory: %w", err)
			}
			manifest, createErr := qwpSfManifestCreate(sfDir, 0, 0)
			if createErr != nil {
				_ = initial.close()
				return nil, createErr
			}
			if err := initial.markManifestRequired(); err != nil {
				_ = manifest.close()
				_ = initial.close()
				return nil, err
			}
			ring = qwpSfNewSegmentRing(initial, segmentSizeBytes)
			ring.manifest = manifest
		} else {
			ring = qwpSfNewSegmentRing(initial, segmentSizeBytes)
		}
	}
	managerEntry, err := mgr.segmentManagerRegisterWithWatermark(ring, sfDir, watermark)
	if err != nil {
		return nil, err
	}
	e := &qwpSfCursorEngine{
		sfDir:               sfDir,
		segmentSizeBytes:    segmentSizeBytes,
		manager:             mgr,
		managerEntry:        managerEntry,
		ownsManager:         false,
		slotLock:            lock,
		ring:                ring,
		watermark:           watermark,
		persistedSymbolDict: persistedDict,
		appendDeadline:      appendDeadline,
		recoveredFromDisk:   recoveredFromDisk,
	}
	// Bind once: manager handoff retains the engine through this closure until
	// terminal cleanup has run.
	e.deferredClose = e.engineCompleteDeferredClose
	ok = true
	return e, nil
}

// engineAcknowledge records a server ACK for cumulative FSN seq.
// Triggers background trim of any sealed segments whose every frame
// is now acknowledged. Idempotent and monotonic.
func (e *qwpSfCursorEngine) engineAcknowledge(seq int64) {
	e.ring.acknowledge(seq)
}

// engineAckedFsn returns the highest FSN safe to send.
func (e *qwpSfCursorEngine) engineAckedFsn() int64 {
	return e.ring.segmentRingAckedFsn()
}

// engineSetLogger points the segment manager's cap-reached backpressure
// diagnostic at the configured logger. The manager's worker goroutine is
// already running by the time the caller has a logger, so the store is
// through an atomic.Pointer; a nil logger leaves the slog.Default() fallback.
func (e *qwpSfCursorEngine) engineSetLogger(l *slog.Logger) {
	if e == nil || e.manager == nil {
		return
	}
	e.manager.logger.Store(l)
}

// engineAckNotify returns a channel closed the next time ackedFsn
// advances. Lets AwaitAckedFsn block until a server ACK lands instead
// of polling. See qwpSfSegmentRing.segmentRingAckNotify for the
// subscribe-then-sample ordering callers must follow.
func (e *qwpSfCursorEngine) engineAckNotify() <-chan struct{} {
	return e.ring.segmentRingAckNotify()
}

// engineActiveSegment returns the current active mmap'd segment.
// I/O thread accessor.
func (e *qwpSfCursorEngine) engineActiveSegment() *qwpSfSegment {
	return e.ring.getActiveSegment()
}

// engineSfDir returns the slot directory ("" for memory-mode).
func (e *qwpSfCursorEngine) engineSfDir() string {
	return e.sfDir
}

// engineMaxFrameBytes returns the largest frame payload a single
// segment can hold: the segment size minus the file header and the
// per-frame header. A payload above this can never be appended —
// appendOrFsn returns qwpSfPayloadTooLarge for it even against a
// freshly-rotated spare — so the producer uses this bound to (a)
// clamp its byte-size auto-flush trigger and (b) drop, rather than
// retain, an oversize batch at the flush boundary. Kept here so it
// tracks the segment header layout automatically and cannot drift
// from what tryAppend actually enforces.
func (e *qwpSfCursorEngine) engineMaxFrameBytes() int64 {
	return e.segmentSizeBytes - qwpSfHeaderSize - qwpSfFrameHeaderSize
}

// engineWasRecoveredFromDisk reports whether the engine opened
// against a pre-existing on-disk slot. Memory-mode engines and
// fresh-disk engines return false.
func (e *qwpSfCursorEngine) engineWasRecoveredFromDisk() bool {
	return e.recoveredFromDisk
}

// engineDeltaDictEnabled reports whether the sender may delta-encode the
// symbol dictionary on this engine. Always true in memory mode (a reconnect
// replays from the in-process ring and the send loop re-registers the whole
// dictionary via a catch-up frame). In disk mode it requires the persisted
// dictionary to have opened, since delta frames are not self-sufficient and
// recovery / orphan-drain must rebuild the dictionary from disk. false in disk
// mode → the sender falls back to full self-sufficient frames.
func (e *qwpSfCursorEngine) engineDeltaDictEnabled() bool {
	return e.sfDir == "" || e.persistedSymbolDict != nil
}

// enginePersistedSymbolDict returns the engine's .symbol-dict side-file, or
// nil in memory mode (and in disk mode if it failed to open).
func (e *qwpSfCursorEngine) enginePersistedSymbolDict() *qwpSfSymbolDict {
	return e.persistedSymbolDict
}

// enginePublishedFsn returns the highest FSN whose frame is fully
// written and visible to consumers (the I/O thread). -1 when nothing
// has been appended yet.
func (e *qwpSfCursorEngine) enginePublishedFsn() int64 {
	return e.ring.segmentRingPublishedFsn()
}

// engineNextSealedAfter walks one step forward in the sealed list.
func (e *qwpSfCursorEngine) engineNextSealedAfter(current *qwpSfSegment) *qwpSfSegment {
	return e.ring.nextSealedAfter(current)
}

// engineFirstSealed returns the oldest sealed segment, or nil.
func (e *qwpSfCursorEngine) engineFirstSealed() *qwpSfSegment {
	return e.ring.firstSealed()
}

// engineFindSegmentContaining returns the segment whose published
// frame range covers fsn, or nil. Used by the reconnect path to
// position the I/O thread's cursor at the first unacked frame.
func (e *qwpSfCursorEngine) engineFindSegmentContaining(fsn int64) *qwpSfSegment {
	return e.ring.findSegmentContaining(fsn)
}

// engineAppendBlocking appends payload, blocking up to the
// configured deadline when the cursor ring is at its memory/disk cap
// and waiting for ACK-driven trim to free space. Returns the
// assigned FSN on success.
//
// ctx is honoured during the backpressure spin: a cancelled or
// deadline-expired ctx returns ctx.Err() immediately, so callers
// passing a tighter deadline than e.appendDeadline get their
// deadline respected.
//
// A send-loop HALT latched while the producer is parked here (a
// sanctioned terminal — auth, poisoned frame, protocol violation)
// short-circuits the spin: the loop has stopped draining the ring, so
// the deadline would only ever expire. engineAppendBlocking returns the latched
// terminal error directly via the engineSetTerminalErrorGetter hook, so
// the parked producer fails fast with the real cause instead of a
// generic backpressure timeout.
//
// Backpressure is surfaced two ways:
//   - engineTotalBackpressureStalls() counter — incremented once per
//     blocking-call that had to wait for the manager.
//   - The error from a deadline expiry distinguishes "wire path is
//     wedged" from a genuine over-large payload.
func (e *qwpSfCursorEngine) engineAppendBlocking(ctx context.Context, payload []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	fsn, closed := e.tryAppendOrFsn(payload)
	if closed {
		return 0, qwpSfErrEngineClosed
	}
	if fsn >= 0 {
		return fsn, nil
	}
	if fsn == qwpSfPayloadTooLarge {
		return 0, qwpSfErrPayloadTooLarge
	}
	if fsn == qwpSfRotationFailed {
		return 0, e.ring.rotationError()
	}
	// First miss → record one stall (not one per spin) and start the
	// deadline clock.
	e.backpressureStalls.Add(1)
	deadline := time.Now().Add(e.appendDeadline)
	timer := time.NewTimer(qwpSfEngineParkInterval)
	defer timer.Stop()
	for {
		// A send-loop HALT stops the
		// loop draining the ring: ACK-driven trim ceases, so the
		// backpressure can never clear and the deadline would only ever
		// expire. Surface the latched terminal error immediately instead
		// of spinning it out and masking the real cause behind a generic
		// backpressure timeout.
		if err := e.engineTerminalError(); err != nil {
			return 0, err
		}
		if time.Now().After(deadline) {
			return 0, e.formatBackpressureTimeout()
		}
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		timer.Reset(qwpSfEngineParkInterval)
		fsn, closed = e.tryAppendOrFsn(payload)
		if closed {
			return 0, qwpSfErrEngineClosed
		}
		if fsn >= 0 {
			return fsn, nil
		}
		if fsn == qwpSfPayloadTooLarge {
			return 0, qwpSfErrPayloadTooLarge
		}
		if fsn == qwpSfRotationFailed {
			return 0, e.ring.rotationError()
		}
	}
}

// tryAppendOrFsn runs one ring.appendOrFsn under appendMu, re-checking
// closed first so a concurrent engineClose can never tear the active
// segment down mid-append. Returns (fsn, false) with the appendOrFsn
// sentinel/result, or (0, true) when the engine has been closed — the
// signal engineAppendBlocking turns into qwpSfErrEngineClosed so a
// parked producer unwinds cleanly instead of dereferencing a nil'd /
// munmapped segment. Lock scope is exactly the ring touch; the spin's
// park happens with the lock released so engineClose is never delayed
// by more than one in-flight append.
func (e *qwpSfCursorEngine) tryAppendOrFsn(payload []byte) (fsn int64, closed bool) {
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
	if e.closed.Load() {
		return 0, true
	}
	return e.ring.appendOrFsn(payload), false
}

// engineTotalBackpressureStalls returns the cumulative number of
// times engineAppendBlocking had to wait for the manager to free
// space. One increment per blocking-call, not per spin-park.
func (e *qwpSfCursorEngine) engineTotalBackpressureStalls() int64 {
	return e.backpressureStalls.Load()
}

// engineSetReconnectStatusGetter wires a snapshot accessor that
// reports whether the I/O loop is currently inside its
// reconnect-with-backoff phase. Called once by the QWP sender
// constructor right after the send loop is created. Pass nil to
// detach (used by tests that tear down the loop independently).
//
// The getter is invoked only on the deadline-expiry path of
// engineAppendBlocking, so the cost is paid only on a true
// backpressure timeout — never on the steady-state hot path.
func (e *qwpSfCursorEngine) engineSetReconnectStatusGetter(getter func() (bool, int64, time.Time)) {
	if getter == nil {
		e.reconnectStatus.Store(nil)
		return
	}
	e.reconnectStatus.Store(&getter)
}

// engineSetTerminalErrorGetter wires a snapshot accessor that returns
// the I/O send loop's latched terminal error (nil while healthy or
// merely reconnecting). Called once by the QWP sender constructor right
// after the send loop is created, alongside
// engineSetReconnectStatusGetter. Pass nil to detach (used by tests
// that tear down the loop independently).
//
// The getter is polled inside engineAppendBlocking's backpressure spin
// so a producer parked on a full ring unwinds the moment the send loop
// HALTs. A HALT stops the send loop, so ACK-driven trim ceases and the
// ring can never drain again; without this the producer would spin out
// the full append deadline and mask the real terminal error behind a
// generic backpressure timeout.
func (e *qwpSfCursorEngine) engineSetTerminalErrorGetter(getter func() error) {
	if getter == nil {
		e.terminalError.Store(nil)
		return
	}
	e.terminalError.Store(&getter)
}

// engineTerminalError returns the send loop's latched terminal error
// via the wired getter, or nil when no getter is installed or the loop
// is healthy. Consulted only on engineAppendBlocking's backpressure
// spin, so the getter cost is paid only while a producer is actually
// parked — never on the steady-state hot path.
func (e *qwpSfCursorEngine) engineTerminalError() error {
	if g := e.terminalError.Load(); g != nil {
		if err := (*g)(); err != nil {
			return err
		}
	}
	// A dead manager worker stops the provisioning/trim that would clear
	// backpressure, so surface it here rather than letting the producer spin to
	// the deadline behind a generic timeout.
	if e.manager != nil {
		if err := e.manager.managerWorkerError(); err != nil {
			return err
		}
	}
	return nil
}

// engineSetSendLoopWakeup wires the producer→send-loop doorbell:
// appendOrFsn invokes fn after every publish so an idle send loop
// reacts immediately instead of polling at parkInterval. Called once
// by qwpSfNewSendLoop before producing starts.
func (e *qwpSfCursorEngine) engineSetSendLoopWakeup(fn func()) {
	e.ring.setSendLoopWakeup(fn)
}

// formatBackpressureTimeout builds the LineSenderException-equivalent
// error returned by engineAppendBlocking when the deadline expires.
// Per spec §16 the message MUST distinguish "publishing but slow"
// from "reconnecting"; in the latter case it includes the per-outage
// attempt count and the wall-clock outage start.
func (e *qwpSfCursorEngine) formatBackpressureTimeout() error {
	if g := e.reconnectStatus.Load(); g != nil {
		if reconnecting, attempts, outageStart := (*g)(); reconnecting {
			return fmt.Errorf("%w (deadline %s, reconnecting: attempts=%d, outage-elapsed=%s, outage-start=%s)",
				ErrBackpressureTimeout,
				e.appendDeadline,
				attempts,
				time.Since(outageStart).Round(time.Millisecond),
				outageStart.Format(time.RFC3339Nano))
		}
	}
	return fmt.Errorf("%w (deadline %s, wire publishing but slow)", ErrBackpressureTimeout, e.appendDeadline)
}

// engineClose tears down the engine. Drains residual on-disk
// segment files when the ring confirms every published FSN has been
// acked — at that moment the slot has no recoverable work and the
// files are pure noise that would mislead the next sender's
// recovery. Best-effort: logs (via returned error) and continues on
// failures, since we're already on the close path.
//
// Order: deregister the ring from the manager (so no new spares
// arrive), close the manager if we own it, close the ring (closes
// its segments), close the ack-watermark mmap AFTER the manager (its
// sole writer) is gone, unlink residual files + the now-meaningless
// watermark if fully drained, release the slot lock LAST (so the
// kernel-held flock outlives any other cleanup work).
func (e *qwpSfCursorEngine) engineClose() error {
	return e.engineCloseInternal(false)
}

// engineCloseLeakSegments tears the engine down like engineClose but leaves the
// segment mmaps mapped, for the caller whose send loop was abandoned wedged in
// an un-cancellable page fault: unmapping under that goroutine would fault the
// host process. The address space leaks until process exit; every other
// resource (fds, watermark, slot lock) is released normally.
func (e *qwpSfCursorEngine) engineCloseLeakSegments() error {
	return e.engineCloseInternal(true)
}

func (e *qwpSfCursorEngine) engineCloseInternal(leakSegments bool) error {
	firstClose := e.closed.CompareAndSwap(false, true)
	if !firstClose && (e.closeCompleted.Load() || e.deferredCleanupOwned.Load()) {
		return nil
	}
	// A worker exit or another retry already owns terminal cleanup. Retain every
	// resource until that owner publishes completion.
	if e.terminalCleanupClaimed.Load() {
		return nil
	}
	// Serialize the manager + ring teardown against the producer's
	// append path. closed is now true, so any tryAppendOrFsn that
	// acquires appendMu after us bails before touching the ring;
	// acquiring it here drains any append currently in flight. Held
	// across segmentRingClose so the active segment is nil'd + munmapped
	// with no producer dereferencing it (a SenderErrorHandler's
	// Close() racing a producer parked in engineAppendBlocking's
	// backpressure spin). appendMu is never held by the manager
	// goroutine, so joining it under the lock cannot deadlock.
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
	if e.closeCompleted.Load() || e.deferredCleanupOwned.Load() || e.terminalCleanupClaimed.Load() {
		return nil
	}
	// Capture drain state BEFORE closing the ring — once the ring is
	// closed, its accessors aren't safe to read. The active segment
	// is never trimmed by drainTrimmable (only sealed segments are),
	// so when everything published has been acked we have to unlink
	// the residual .sfa files here.
	fullyDrained := e.sfDir != "" &&
		(e.ring.segmentRingPublishedFsn() < 0 ||
			e.ring.segmentRingAckedFsn() >= e.ring.segmentRingPublishedFsn())
	e.deferredFullyDrained.Store(fullyDrained)
	// Mapping leaks are a one-way safety decision: a retrying normal close must
	// never downgrade an earlier abandoned-send-loop close and unmap memory that
	// the abandoned goroutine may still dereference.
	if leakSegments {
		e.deferredLeakSegments.Store(true)
	}
	effectiveLeakSegments := e.deferredLeakSegments.Load()

	entry := e.manager.segmentManagerDeregister(e.ring)
	if entry != nil {
		e.managerEntry = entry
	}
	quiescent := false
	if e.ownsManager {
		quiescent = e.manager.segmentManagerClose()
	} else {
		quiescent = e.manager.awaitRingQuiescence(e.managerEntry)
	}
	if !quiescent {
		var handedOff bool
		e.deferredCleanupOwned.Store(true)
		if e.ownsManager {
			handedOff = e.manager.deferOwnedCleanupUntilWorkerExit(e.deferredClose)
		} else {
			handedOff = e.manager.deferUntilRingQuiescent(e.managerEntry, e.deferredClose)
		}
		if !handedOff {
			e.deferredCleanupOwned.Store(false)
		}
		if handedOff {
			qwpEffectiveLogger(e.manager.logger.Load()).Error(
				"qwp/sf: close handed to the manager worker's exit path; the slot stays locked until it completes",
				"slot", e.sfDir)
			return nil
		}
	}
	if !e.terminalCleanupClaimed.CompareAndSwap(false, true) {
		return nil
	}
	return e.engineFinishClose(fullyDrained, effectiveLeakSegments)
}

// engineFinishClose performs terminal cleanup after manager quiescence is
// proven. The terminalCleanupClaimed CAS, not appendMu, excludes another
// cleanup owner; callers nevertheless hold appendMu to fence producers.
func (e *qwpSfCursorEngine) engineFinishClose(fullyDrained, leakSegments bool) error {
	if hook := qwpSfTestEngineFinishCloseHook.Load(); hook != nil {
		(*hook)()
	}
	if e.terminalResourcesClosed.Load() {
		if e.slotLock != nil {
			if err := e.slotLock.close(); err != nil {
				e.terminalCleanupClaimed.Store(false)
				return err
			}
		}
		e.closeCompleted.Store(true)
		return nil
	}
	var firstErr error
	drainCleanupAllowed := fullyDrained
	if fullyDrained {
		e.watermark.persistIfAdvanced(e.ring.segmentRingAckedFsn())
		if err := e.watermark.sync(); err != nil {
			firstErr = err
			drainCleanupAllowed = false
		}
		active := e.ring.getActiveSegment()
		if drainCleanupAllowed && active != nil && e.ring.manifest != nil {
			if err := e.ring.manifest.update(active.segmentBaseSeq(), active.segmentBaseSeq()); err != nil {
				firstErr = err
				drainCleanupAllowed = false
			}
		}
		// These durability barriers precede every destructive cleanup step.
		// On failure retain the ring, side files, and flock so a later Close can
		// retry from intact state.
		if !drainCleanupAllowed {
			e.terminalCleanupClaimed.Store(false)
			return firstErr
		}
	}
	if err := e.ring.segmentRingCloseInternal(leakSegments); err != nil && firstErr == nil {
		firstErr = err
	}
	// Close the watermark mmap/fd only after the owned manager has exited or the
	// shared-manager entry is past its service pass, and before releasing the
	// slot lock. No manager write can race this close.
	if e.watermark != nil {
		if err := e.watermark.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.persistedSymbolDict != nil {
		if err := e.persistedSymbolDict.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if drainCleanupAllowed {
		if err := qwpSfUnlinkAllSegmentFiles(e.sfDir); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			drainCleanupAllowed = false
		}
		if drainCleanupAllowed && !qwpSfManifestRemove(e.sfDir) {
			if firstErr == nil {
				firstErr = fmt.Errorf("qwp/sf: remove drained manifest in %s", e.sfDir)
			}
			drainCleanupAllowed = false
		}
		if drainCleanupAllowed {
			if err := qwpSfSyncDir(e.sfDir); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				drainCleanupAllowed = false
			}
		}
		if drainCleanupAllowed {
			qwpSfAckWatermarkRemoveOrphan(e.sfDir)
			qwpSfSymbolDictRemoveOrphan(e.sfDir)
		}
	}
	e.terminalResourcesClosed.Store(true)
	if e.slotLock != nil {
		if err := e.slotLock.close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			qwpEffectiveLogger(e.manager.logger.Load()).Error("qwp/sf: could not release slot lock after close", "slot", e.sfDir, "error", err)
			e.terminalCleanupClaimed.Store(false)
			return firstErr
		}
	}
	e.closeCompleted.Store(true)
	return firstErr
}

// engineCompleteDeferredClose is invoked by the manager after it is provably
// past the worker loop or the affected ring's service pass.
func (e *qwpSfCursorEngine) engineCompleteDeferredClose() {
	e.deferredCleanupOwned.Store(false)
	if !e.terminalCleanupClaimed.CompareAndSwap(false, true) {
		return
	}
	e.appendMu.Lock()
	err := e.engineFinishClose(e.deferredFullyDrained.Load(), e.deferredLeakSegments.Load())
	e.appendMu.Unlock()
	logger := qwpEffectiveLogger(e.manager.logger.Load())
	if err != nil {
		logger.Error("qwp/sf: deferred engine close failed", "slot", e.sfDir, "error", err, "closeCompleted", e.closeCompleted.Load())
		return
	}
	logger.Info("qwp/sf: deferred engine close completed", "slot", e.sfDir, "closeCompleted", e.closeCompleted.Load())
}

func (e *qwpSfCursorEngine) engineCloseCompleted() bool {
	return e != nil && e.closeCompleted.Load()
}

// engineCloseRetryable reports an incomplete close whose cleanup has no owner.
// It is false while the manager worker or another Close is responsible, which
// lets pool reprobes stay non-blocking during a legitimately stuck service
// pass. A true result means an earlier cleanup attempt failed and may be
// retried inline.
func (e *qwpSfCursorEngine) engineCloseRetryable() bool {
	return e != nil && e.closed.Load() && !e.closeCompleted.Load() &&
		!e.deferredCleanupOwned.Load() && !e.terminalCleanupClaimed.Load()
}

func (e *qwpSfCursorEngine) engineRetryCloseIfNeeded() error {
	if !e.engineCloseRetryable() {
		return nil
	}
	return e.engineCloseInternal(e.deferredLeakSegments.Load())
}

// engineStartCloseRetryOwner supplies terminal cleanup ownership to paths that
// cannot expose a retryable object to the caller (construction unwind,
// drainer exit, and pool shutdown). The per-engine goroutine exits as soon as
// the flock is released.
func (e *qwpSfCursorEngine) engineStartCloseRetryOwner(logger *slog.Logger) {
	if e == nil || e.closeCompleted.Load() || !e.closeRetryOwnerStarted.CompareAndSwap(false, true) {
		return
	}
	go func() {
		for !e.closeCompleted.Load() {
			if e.engineCloseRetryable() {
				if err := e.engineRetryCloseIfNeeded(); err != nil {
					qwpEffectiveLogger(logger).Warn("qwp/sf: terminal cleanup retry failed; slot lock remains held",
						"slot", e.sfDir, "error", err)
				}
			}
			if e.closeCompleted.Load() {
				return
			}
			time.Sleep(time.Second)
		}
	}()
}

// qwpSfUnlinkAllSegmentFiles unlinks every .sfa file under dir.
// Called only on clean shutdown when the ring confirms every
// published FSN has been acked. Files are removed in cleanup rank order and
// removal stops at the first failure, leaving the manifest in place.
func qwpSfUnlinkAllSegmentFiles(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var paths []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sfa") {
			continue
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	sort.Slice(paths, func(i, j int) bool {
		bi, bj := filepath.Base(paths[i]), filepath.Base(paths[j])
		if bi == "sf-initial.sfa" {
			return bj != "sf-initial.sfa"
		}
		if bj == "sf-initial.sfa" {
			return false
		}
		return bi < bj
	})
	for _, path := range paths {
		if hook := qwpSfTestBeforeSegmentUnlinkHook.Load(); hook != nil {
			(*hook)(path)
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
