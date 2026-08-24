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
	"runtime/debug"
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

const qwpSfCloseRetryLogThrottle = 30 * time.Second

// qwpSfSwappableVar holds a value that tests replace while production
// goroutines are reading it. As a plain var every such swap is a data race:
// the close-retry owner, the manager worker and the send loop all read theirs
// from goroutines a test has no way to join before t.Cleanup puts the original
// back. Routing them through an atomic makes the race detector quiet for the
// right reason instead of by luck, and keeps one test calling t.Parallel from
// breaking an unrelated one.
type qwpSfSwappableVar[T any] struct {
	v atomic.Pointer[T]
}

func qwpSfSwappable[T any](initial T) *qwpSfSwappableVar[T] {
	s := &qwpSfSwappableVar[T]{}
	s.v.Store(&initial)
	return s
}

func (s *qwpSfSwappableVar[T]) load() T { return *s.v.Load() }

func (s *qwpSfSwappableVar[T]) store(value T) { s.v.Store(&value) }

// qwpSfCloseRetryInterval is swappable so panic/retry tests do not need to wait
// through production's one-second terminal-cleanup cadence.
var qwpSfCloseRetryInterval = qwpSfSwappable(time.Second)

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

// qwpSfSyncSlotDir is the platform directory-barrier abstraction. On Unix it
// makes the slot namespace durable, so a file this slot created is findable
// after an OS crash. Windows has no supported unprivileged directory-fsync
// equivalent; its platform implementation is an explicit no-op and the public
// durability contract documents that weaker guarantee. Every control point
// that publishes or retires a correctness-relevant name still goes through
// this function so the epoch ordering remains visible and testable.
func qwpSfSyncSlotDir(dir string) error {
	if hook := qwpSfTestDirSyncHook.Load(); hook != nil {
		if err := (*hook)(dir); err != nil {
			return err
		}
	}
	return qwpSfSyncDir(dir)
}

// qwpSfTestDirSyncHook observes every directory barrier and may fail one. Test
// seam only: it lets a test pin which control points make a name durable, and
// inject the storage faults that no real filesystem can be talked into on
// demand. A nil return falls through to the real barrier. Nil in production.
var qwpSfTestDirSyncHook atomic.Pointer[func(dir string) error]

// qwpSfTestAfterManagerTeardownHook fires after a close has obtained its
// manager-quiescence result but before it publishes the corresponding cleanup
// transition. Test seam only.
var qwpSfTestAfterManagerTeardownHook atomic.Pointer[func()]

// qwpSfTestEngineFinishCloseHook counts terminal-cleanup entry independently
// of how many segment files that cleanup unlinks. Production leaves it nil.
var qwpSfTestEngineFinishCloseHook atomic.Pointer[func()]

type qwpSfCleanupTestPoint uint8

const (
	qwpSfCleanupTestBeforeManagerStop qwpSfCleanupTestPoint = iota
	qwpSfCleanupTestBeforeManagerHandoff
	qwpSfCleanupTestAfterManagerHandoff
	qwpSfCleanupTestAfterTerminalClaim
	qwpSfCleanupTestDuringTerminalCleanup
	qwpSfCleanupTestRingClosePhase
	qwpSfCleanupTestWatermarkClosePhase
	qwpSfCleanupTestSymbolDictClosePhase
)

// qwpSfTestCleanupHook injects faults at ownership boundaries without adding a
// separate global seam for every phase. Production leaves it nil.
var qwpSfTestCleanupHook atomic.Pointer[func(qwpSfCleanupTestPoint)]

func qwpSfRunCleanupTestHook(point qwpSfCleanupTestPoint) {
	if hook := qwpSfTestCleanupHook.Load(); hook != nil {
		(*hook)(point)
	}
}

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

	manager *qwpSfSegmentManager
	// managerEntry is written once, by the constructor, and read from both the
	// producer goroutine (engineTerminalError) and close.
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
	// recoveredSymbols is the symbol list, in id order, that the constructor
	// rebuilt for a recovered slot: the side-file's trusted prefix followed by
	// whatever the surviving frames themselves spell out. The producer and the
	// send loop both start from this one slice and neither modifies it, so an
	// id can never get two different names. nil for a fresh slot and in memory
	// mode.
	recoveredSymbols []string
	// recoveredMaxReplayDeltaStart is the highest delta start found among the
	// frames still waiting to be sent. Anything above zero means those frames
	// refer to symbols registered earlier, so a fresh connection has to be sent
	// the dictionary before replay begins.
	recoveredMaxReplayDeltaStart int

	appendDeadline time.Duration

	// recoveredFromDisk is true when the constructor recovered an
	// existing on-disk slot rather than starting fresh. Diagnostic
	// accessor for tests and observability. Cursor frames carry their own
	// schema, so nothing has to be restored for that; symbol ids do carry
	// over, and they come from recoveredSymbols.
	recoveredFromDisk bool

	// quarantinedPath is where the constructor preserved a slot whose
	// recovery failed closed, before starting this engine on a fresh one.
	// Empty when nothing was set aside. Written once, before the engine is
	// handed to its caller.
	quarantinedPath string

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

	// cleanup is the sole authority for manager quiescence, terminal ownership,
	// retryability, completion and the drain/leak inputs carried across retries.
	// Its mutex is never held across appendMu, manager waits or resource IO.
	cleanup qwpSfCleanupControl
	// closeRetryWG makes the engine-owned retry goroutine explicitly joinable
	// by lifecycle tests. A restarted owner increments before the panicked
	// generation decrements, so one Wait covers the chain.
	closeRetryWG sync.WaitGroup

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
	return qwpSfNewCursorEngineWithOptions(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline, qwpSfEngineOpenOptions{
		recoverForeground: true,
	})
}

func qwpSfNewCursorEngineForDrainer(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration) (*qwpSfCursorEngine, error) {
	return qwpSfNewCursorEngineWithOptions(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline, qwpSfEngineOpenOptions{})
}

// qwpSfEngineOpenOptions configures recovery policy and logging during engine
// construction. Recovery and the manager worker use the configured logger.
type qwpSfEngineOpenOptions struct {
	logger            *slog.Logger
	recoverForeground bool
}

func qwpSfNewCursorEngineWithOptions(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration, options qwpSfEngineOpenOptions) (*qwpSfCursorEngine, error) {
	if options.logger != nil {
		options.logger = qwpEffectiveLogger(options.logger)
	}
	// Where the bytes of a slot this build refused went, carried onto the fresh
	// engine so the caller can find them without reading the log.
	quarantinedPath := ""
	for attempt := 0; ; attempt++ {
		e, err := qwpSfNewCursorEngineOnce(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadline, options)
		if err == nil || sfDir == "" {
			if e != nil {
				e.quarantinedPath = quarantinedPath
			}
			return e, err
		}
		// The residue retry is the second half of a recovery step that already
		// completed: the sanitization is on disk and the same bytes recover on
		// the very next pass. Every caller takes it, drainers included —
		// otherwise one slot recovers under a foreground sender and the same
		// slot is abandoned under a drainer.
		if errors.Is(err, qwpSfErrSanitizedResidue) && attempt == 0 {
			qwpEffectiveLogger(options.logger).Error("qwp/sf: sealed-segment residue was sanitized; retrying recovery once", "slot", sfDir, "error", err)
			continue
		}
		// Quarantine-and-start-fresh is a foreground-only policy: a drainer
		// exists to deliver the slot's rows, so it reports the failure and
		// leaves the bytes where they are.
		if !options.recoverForeground {
			return nil, err
		}
		if errors.Is(err, qwpSfErrRecoveryFailClosed) && attempt <= 1 {
			quarantined, quarantineErr := qwpSfQuarantineSlot(sfDir)
			if quarantineErr != nil {
				return nil, errors.Join(err,
					qwpSfDurabilityError("quarantine fail-closed slot", sfDir, quarantineErr))
			}
			qwpEffectiveLogger(options.logger).Error("qwp/sf: recovery failed closed; preserved the slot and starting fresh", "slot", sfDir, "quarantined", quarantined, "error", err)
			// Keep the first preserved directory. The loop can quarantine
			// twice, and only the first copy holds the rows the caller came
			// looking for -- the second is whatever the fresh slot managed to
			// write before failing again. Reporting the later one would send
			// the caller to a near-empty directory.
			if quarantinedPath == "" {
				quarantinedPath = quarantined
			}
			continue
		}
		return nil, err
	}
}

func qwpSfNewCursorEngineOnce(sfDir string, segmentSizeBytes, maxTotalBytes int64, appendDeadline time.Duration, options qwpSfEngineOpenOptions) (result *qwpSfCursorEngine, err error) {
	mgr, err := qwpSfNewSegmentManager(segmentSizeBytes, qwpSfManagerDefaultPoll, maxTotalBytes)
	if err != nil {
		return nil, err
	}
	// Close the manager (joining its worker goroutine) on any failure
	// exit of the inner constructor — error return AND panic. The inner
	// constructor's own deferred guard releases the slot flock on the
	// same unwind; this guard covers the one resource it can't see — the
	// manager we own here. ok flips true only once the engine adopts it.
	ok := false
	defer func() {
		if !ok {
			cleanupErr := qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupManager, "segment manager", func() error {
				mgr.segmentManagerClose()
				return nil
			})
			if cleanupErr != nil {
				err = qwpAppendCloseError(err, cleanupErr)
				result = nil
			}
		}
	}()
	if options.logger != nil {
		mgr.logger.Store(options.logger)
	}
	mgr.segmentManagerStart()
	e, err := qwpSfNewCursorEngineWithManagerOptions(sfDir, segmentSizeBytes, mgr, appendDeadline, options)
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
func qwpSfNewCursorEngineWithManager(sfDir string, segmentSizeBytes int64, mgr *qwpSfSegmentManager, appendDeadline time.Duration) (result *qwpSfCursorEngine, err error) {
	return qwpSfNewCursorEngineWithManagerOptions(sfDir, segmentSizeBytes, mgr, appendDeadline, qwpSfEngineOpenOptions{})
}

func qwpSfNewCursorEngineWithManagerOptions(sfDir string, segmentSizeBytes int64, mgr *qwpSfSegmentManager, appendDeadline time.Duration, options qwpSfEngineOpenOptions) (result *qwpSfCursorEngine, err error) {
	if appendDeadline <= 0 {
		appendDeadline = qwpSfEngineDefaultAppendDeadline
	}
	memoryMode := sfDir == ""
	var (
		lock              *qwpSfSlotLock
		ring              *qwpSfSegmentRing
		watermark         *qwpSfAckWatermark
		persistedDict     *qwpSfSymbolDict
		initial           *qwpSfSegment
		manifest          *qwpSfManifest
		recoveredSymbols  []string
		recoveredMaxStart int
		recoveredFromDisk bool
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
	cleanup := func() error {
		var cleanupErr error
		if ring != nil {
			cleanupErr = qwpAppendCloseError(cleanupErr,
				qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupRing, "segment ring", ring.segmentRingClose))
		} else {
			if initial != nil {
				cleanupErr = qwpAppendCloseError(cleanupErr,
					qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupInitial, "initial segment", initial.close))
			}
			if manifest != nil {
				cleanupErr = qwpAppendCloseError(cleanupErr,
					qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupManifest, "segment manifest", manifest.close))
			}
		}
		if watermark != nil {
			cleanupErr = qwpAppendCloseError(cleanupErr,
				qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupWatermark, "ack watermark", watermark.close))
		}
		if persistedDict != nil {
			cleanupErr = qwpAppendCloseError(cleanupErr,
				qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupSymbolDict, "symbol dictionary", persistedDict.close))
		}
		if lock != nil {
			cleanupErr = qwpAppendCloseError(cleanupErr,
				qwpSfRunConstructionCleanupPhase(qwpSfConstructionCleanupSlotLock, "slot lock", lock.close))
		}
		return cleanupErr
	}
	defer func() {
		if !ok {
			cleanupErr := cleanup()
			if cleanupErr != nil {
				err = qwpAppendCloseError(err, cleanupErr)
				result = nil
			}
		}
	}()
	// Disk mode: try to recover any *.sfa files left behind by a
	// prior session before deciding to start fresh. Without this the
	// engine would create a new sf-initial.sfa at baseSeq=0,
	// overlapping FSNs already on disk and corrupting ACK
	// translation, trim, and replay.
	if !memoryMode {
		ring, _, err = qwpSfRecoverRingWithContext(sfDir, segmentSizeBytes, qwpSfRecoveryContext{logger: options.logger})
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
			watermark, err = qwpSfAckWatermarkOpenRequiredWithLogger(sfDir, options.logger)
			if errors.Is(err, qwpSfErrAckWatermarkUnbacked) {
				// A full disk is the ordinary way to get here, and draining
				// this slot is what gives the disk its space back, so the
				// engine opens without the watermark rather than refusing the
				// slot. The seed then comes from the surviving segments alone,
				// which can re-send frames a previous session already got
				// acked.
				qwpEffectiveLogger(options.logger).Warn("qwp/sf: opening a recovered slot without its ack watermark; already-acked frames may replay",
					"dir", sfDir, "error", err)
				watermark, err = nil, nil
			}
			if err != nil {
				return nil, qwpSfDurabilityError("could not open required ack watermark", sfDir, err)
			}
			// Load the persisted symbol dictionary so this recovered slot's
			// delta frames can be re-registered on a fresh server before they
			// replay. A recovered slot's dictionary is NEVER recreated: its
			// segments reference the dictionary's ids by position, so truncating
			// a corrupt/mismatched header would restart the ids at 0 and give
			// them the wrong names. Content that cannot be trusted is left on
			// disk and contributes nothing; a file whose tail is torn
			// contributes the part its checksums cover. An I/O error fails
			// construction so the caller can retry later.
			persistedDict, err = qwpSfSymbolDictOpenRecovered(sfDir)
			if err != nil {
				return nil, err
			}
			// A missing or untrusted dictionary stops the producer from writing
			// new delta frames, but the scan below may still rebuild the
			// dictionary the waiting frames need out of the frames themselves.
			// When the ring holds no frames there are no ids to clash with, so
			// starting a fresh dictionary is safe and keeps delta encoding
			// available. The test is the frames themselves: a recovered chain
			// that was fully trimmed holds none while still reporting the
			// published sequence it reached, and a sender there would otherwise
			// send a full symbol dictionary on every frame for its whole life.
			if persistedDict == nil && !ring.segmentRingHoldsFrames() {
				var freshDictErr error
				persistedDict, freshDictErr = qwpSfSymbolDictOpenFresh(filepath.Join(sfDir, qwpSfSymbolDictFileName))
				if freshDictErr != nil {
					qwpEffectiveLogger(options.logger).Warn("qwp/sf: could not create a symbol dictionary for an empty recovered slot; falling back to full-dictionary frames",
						"dir", sfDir, "error", freshDictErr)
				}
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

			// Scan the surviving frames while the engine is still private to
			// the constructor. When the side-file is missing or torn, the
			// frames themselves can still spell out the dictionary, as long as
			// they cover every id without a hole. Producer and send loop both
			// start from this one result, so an id cannot end up naming two
			// different strings.
			prefix := []string(nil)
			if persistedDict != nil {
				prefix = persistedDict.loadedSymbols()
			}
			analysis, analyzeErr := qwpSfAnalyzeRecoveredDict(
				ring, ring.segmentRingAckedFsn(), prefix)
			if analyzeErr != nil {
				return nil, analyzeErr
			}
			recoveredSymbols = analysis.symbols
			recoveredMaxStart = analysis.maxReplayDeltaStart

			// The side-file may cover fewer ids than the frames do. Write the
			// difference back now, while those frames are still around — once
			// they are ACKed and trimmed, the only copy of those symbols is
			// gone. If the side-file can no longer be written, run this session
			// with full self-sufficient frames; the in-memory dictionary still
			// holds the recovered ids, and no delta frame that depends on them
			// gets published without a durable copy behind it.
			if persistedDict != nil && len(recoveredSymbols) > persistedDict.size() {
				from := persistedDict.size()
				if appendErr := persistedDict.appendSymbols(recoveredSymbols[from:]); appendErr != nil {
					qwpEffectiveLogger(options.logger).Warn("qwp/sf: could not heal recovered symbol dictionary; falling back to full-dictionary frames",
						"error", appendErr)
					_ = persistedDict.close()
					persistedDict = nil
				}
			}
		}
	}
	if ring == nil {
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
			if err := qwpSfManifestRemove(sfDir); err != nil {
				return nil, err
			}
			watermark, err = qwpSfAckWatermarkOpenRequiredWithLogger(sfDir, options.logger)
			if errors.Is(err, qwpSfErrAckWatermarkUnbacked) {
				qwpEffectiveLogger(options.logger).Warn("qwp/sf: opening a fresh slot without its ack watermark; a later recovery falls back to the surviving segments",
					"dir", sfDir, "error", err)
				watermark, err = nil, nil
			}
			if err != nil {
				return nil, err
			}
			// A fresh slot must never inherit a prior generation's id mapping.
			// Truncate an existing side-file in place; if that is refused, abort
			// rather than run full-dict next to stale bytes a later recovery would
			// trust. A provably absent file that cannot be created may degrade to
			// full-dictionary mode safely.
			persistedDict, err = qwpSfSymbolDictOpenClean(sfDir)
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
				return nil, err
			}
			if err := qwpSfSyncSlotDir(sfDir); err != nil {
				return nil, qwpSfDurabilityError("sync fresh slot directory", sfDir, err)
			}
			var createErr error
			manifest, createErr = qwpSfManifestCreate(sfDir, 0, 0)
			if createErr != nil {
				return nil, createErr
			}
			if err := initial.markManifestRequired(); err != nil {
				return nil, err
			}
			if hook := qwpSfTestBeforeFreshRingAdoptHook.Load(); hook != nil {
				if hookErr := (*hook)(); hookErr != nil {
					return nil, hookErr
				}
			}
			ring = qwpSfNewSegmentRing(initial, segmentSizeBytes)
			ring.manifest = manifest
		} else {
			ring = qwpSfNewSegmentRing(initial, segmentSizeBytes)
		}
	}
	if hook := qwpSfTestBeforeEngineRegisterHook.Load(); hook != nil {
		if hookErr := (*hook)(); hookErr != nil {
			return nil, hookErr
		}
	}
	managerEntry, err := mgr.segmentManagerRegisterWithWatermark(ring, sfDir, watermark)
	if err != nil {
		return nil, err
	}
	e := &qwpSfCursorEngine{
		sfDir:                        sfDir,
		segmentSizeBytes:             segmentSizeBytes,
		manager:                      mgr,
		managerEntry:                 managerEntry,
		ownsManager:                  false,
		slotLock:                     lock,
		ring:                         ring,
		watermark:                    watermark,
		persistedSymbolDict:          persistedDict,
		recoveredSymbols:             recoveredSymbols,
		recoveredMaxReplayDeltaStart: recoveredMaxStart,
		appendDeadline:               appendDeadline,
		recoveredFromDisk:            recoveredFromDisk,
	}
	ok = true
	return e, nil
}

type qwpSfConstructionCleanupPoint uint8

const (
	qwpSfConstructionCleanupRing qwpSfConstructionCleanupPoint = iota
	qwpSfConstructionCleanupInitial
	qwpSfConstructionCleanupManifest
	qwpSfConstructionCleanupWatermark
	qwpSfConstructionCleanupSymbolDict
	qwpSfConstructionCleanupSlotLock
	qwpSfConstructionCleanupManager
)

// qwpSfRunConstructionCleanupPhase keeps constructor cleanup fault injection
// inside the same continuation boundary as the real release. The hook fires
// after the release attempt so tests do not intentionally leak mappings or fds.
func qwpSfRunConstructionCleanupPhase(point qwpSfConstructionCleanupPoint, name string, fn func() error) error {
	return qwpRunCleanupPhaseGuarded(name, func() error {
		err := fn()
		if hook := qwpSfTestConstructionCleanupHook.Load(); hook != nil {
			(*hook)(point)
		}
		return err
	})
}

// qwpSfTestBeforeEngineRegisterHook forces the fully-acquired constructor down
// its unwind path. qwpSfTestConstructionCleanupHook injects a panic at one
// cleanup phase so tests can verify every later obligation still runs.
var qwpSfTestBeforeEngineRegisterHook atomic.Pointer[func() error]
var qwpSfTestBeforeFreshRingAdoptHook atomic.Pointer[func() error]
var qwpSfTestConstructionCleanupHook atomic.Pointer[func(qwpSfConstructionCleanupPoint)]

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

// engineQuarantinedSlotPath returns the directory holding the slot this engine
// refused and set aside, or "" when it started on a slot it could read.
func (e *qwpSfCursorEngine) engineQuarantinedSlotPath() string {
	if e == nil {
		return ""
	}
	return e.quarantinedPath
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

// engineRecoveredSymbols returns the symbol list, in id order, that disk
// recovery rebuilt from the side-file's trusted prefix plus the surviving
// frames. nil for a fresh slot and in memory mode. The send loop and the
// producer both read it while the sender is being built, before either
// goroutine starts, and neither one modifies it.
func (e *qwpSfCursorEngine) engineRecoveredSymbols() []string {
	return e.recoveredSymbols
}

// engineRecoveredMaxReplayDeltaStart returns the highest delta start among the
// recovered frames still waiting to be sent. Anything above zero means one of
// them refers to symbols registered earlier, so every fresh connection must be
// sent the dictionary before replay begins.
func (e *qwpSfCursorEngine) engineRecoveredMaxReplayDeltaStart() int {
	return e.recoveredMaxReplayDeltaStart
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
		return 0, e.rotationDurabilityError()
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
			return 0, e.rotationDurabilityError()
		}
	}
}

func (e *qwpSfCursorEngine) rotationDurabilityError() error {
	return qwpSfDurabilityError("rotate active segment", e.sfDir, e.ring.rotationError())
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
	// Same reasoning for a slot whose maintenance keeps failing: the trim that
	// would free ring space cannot commit, so the backpressure is local storage
	// refusing writes, not a slow or disconnected server.
	if err := e.managerEntry.entryMaintenanceError(); err != nil {
		return err
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
	_, err := e.engineCloseWithOwner(leakSegments, qwpSfCleanupOwnerClose, false)
	return err
}

// engineCloseWithOwner obtains one state-machine action, then performs it
// without holding the cleanup mutex. acted is false when another close,
// manager callback or retry owner already owns the protocol, or cleanup is
// complete. repeated public Close uses respectRetryOwner so its decision and
// the retry-owner gate are one locked observation.
func (e *qwpSfCursorEngine) engineCloseWithOwner(leakSegments bool, owner qwpSfCleanupOwner, respectRetryOwner bool) (acted bool, err error) {
	if e == nil {
		return false, nil
	}
	e.closed.Store(true)
	token, action := e.cleanup.begin(leakSegments, owner, respectRetryOwner)
	return e.runCleanupAction(token, action, owner)
}

// runCleanupAction performs the action a claim decision handed back, without
// holding the cleanup mutex.
func (e *qwpSfCursorEngine) runCleanupAction(token qwpSfCleanupToken, action qwpSfCleanupAction, owner qwpSfCleanupOwner) (acted bool, err error) {
	switch action {
	case qwpSfCleanupNoAction:
		return false, nil
	case qwpSfCleanupFinish:
		e.appendMu.Lock()
		defer e.appendMu.Unlock()
		return true, e.engineFinishCloseGuarded(token)
	case qwpSfCleanupDriveManager:
		return true, e.engineDriveClose(token, owner)
	default:
		panic("qwp/sf: unknown cleanup action")
	}
}

func (e *qwpSfCursorEngine) engineDriveClose(token qwpSfCleanupToken, owner qwpSfCleanupOwner) (err error) {
	// Until a quiescence or handoff transition consumes token, any panic leaves
	// manager safety unproven. Roll back to open so a later retry re-drives the
	// manager instead of inferring safety from stale observations.
	rollback := true
	defer func() {
		if rollback {
			e.cleanup.rollbackStopping(token)
		}
	}()
	// Serialize the manager + ring teardown against the producer's
	// append path. closed is now true, so any tryAppendOrFsn that
	// acquires appendMu after us bails before touching the ring;
	// acquiring it here drains any append currently in flight. Held
	// across segmentRingClose so the active segment is nil'd + munmapped
	// with no producer dereferencing it (a SenderErrorHandler's
	// Close() racing a producer parked in engineAppendBlocking's
	// backpressure spin).
	//
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
	// Capture drain state while terminal resources are still open, BEFORE
	// closing the ring. The state record carries this and the monotonic leak
	// decision across every retry generation.
	publishedFsn := e.ring.segmentRingPublishedFsn()
	fullyDrained := e.sfDir != "" &&
		(publishedFsn < 0 || e.ring.segmentRingAckedFsn() >= publishedFsn)
	if !e.cleanup.recordDrain(token, fullyDrained) {
		return nil
	}

	// The entry stays a local: e.managerEntry is written once, by the
	// constructor, and read by the producer through engineTerminalError. A
	// second write here would race that read.
	entry := e.manager.segmentManagerDeregister(e.ring)
	if entry == nil {
		entry = e.managerEntry
	}
	quiescent := false
	if e.ownsManager {
		qwpSfRunCleanupTestHook(qwpSfCleanupTestBeforeManagerStop)
		quiescent = e.manager.segmentManagerClose()
	} else {
		qwpSfRunCleanupTestHook(qwpSfCleanupTestBeforeManagerStop)
		quiescent = e.manager.awaitRingQuiescence(entry)
	}
	if hook := qwpSfTestAfterManagerTeardownHook.Load(); hook != nil {
		(*hook)()
	}
	if quiescent {
		claim, ok := e.cleanup.managerReadyAndClaim(token, owner)
		rollback = false
		if !ok {
			return nil
		}
		return e.engineFinishCloseGuarded(claim)
	}

	handoffToken, ok := e.cleanup.prepareManagerHandoff(token)
	rollback = false
	if !ok {
		return nil
	}
	handoffResult := e.engineRegisterManagerHandoff(entry, handoffToken)
	if handoffResult == qwpSfManagerHandoffAccepted {
		qwpEffectiveLogger(e.engineLogger()).Error("qwp/sf: close handed to the manager worker's exit path; the slot stays locked until it completes",
			"slot", e.sfDir)
		return nil
	}
	if handoffResult == qwpSfManagerHandoffBusy {
		e.cleanup.abortManagerHandoff(handoffToken)
		return errors.New("qwp/sf: manager cleanup handoff slot is busy; cleanup will retry")
	}
	claim, ok := e.cleanup.managerHandoffDeclinedAndClaim(handoffToken, owner)
	if !ok {
		return nil
	}
	return e.engineFinishCloseGuarded(claim)
}

// engineRegisterManagerHandoff makes the handoff transition retryable when a
// fault occurs before the manager has accepted the callback. Once the manager
// returns true, its exact generation token stays authoritative even if later
// diagnostics panic; the callback will either be pending or already running.
func (e *qwpSfCursorEngine) engineRegisterManagerHandoff(entry *qwpSfManagerRingEntry, token qwpSfCleanupToken) (result qwpSfManagerHandoffResult) {
	registrationReturned := false
	callback := func() { e.engineCompleteDeferredClose(token) }
	handoff := &qwpSfManagerCleanupHandoff{cleanup: callback}
	defer func() {
		if !registrationReturned && !handoff.accepted.Load() {
			e.cleanup.abortManagerHandoff(token)
		}
	}()
	qwpSfRunCleanupTestHook(qwpSfCleanupTestBeforeManagerHandoff)
	if e.ownsManager {
		result = e.manager.deferOwnedCleanupHandoffUntilWorkerExit(handoff)
	} else {
		result = e.manager.deferHandoffUntilRingQuiescent(entry, handoff)
	}
	registrationReturned = true
	if result == qwpSfManagerHandoffAccepted {
		qwpSfRunCleanupTestHook(qwpSfCleanupTestAfterManagerHandoff)
	}
	return result
}

// engineFinishCloseGuarded consumes a claim after appendMu acquisition, then
// converts any terminal-cleanup panic into a retryable state transition. The
// claimed and running tokens differ, so one claim generation can enter
// engineFinishClose at most once.
func (e *qwpSfCursorEngine) engineFinishCloseGuarded(claim qwpSfCleanupToken) (err error) {
	var run qwpSfCleanupToken
	started := false
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp/sf: terminal cleanup panicked: %v\n%s", r, debug.Stack())
			if started {
				e.cleanup.retry(run, err)
			} else {
				e.cleanup.abandonClaim(claim, err)
			}
		}
	}()
	qwpSfRunCleanupTestHook(qwpSfCleanupTestAfterTerminalClaim)
	var (
		fullyDrained        bool
		barriersCommitted   bool
		leakSegments        bool
		resourcesClosed     bool
		drainedFilesPending bool
		ok                  bool
	)
	run, fullyDrained, barriersCommitted, leakSegments, resourcesClosed, drainedFilesPending, ok = e.cleanup.startTerminal(claim)
	if !ok {
		return nil
	}
	started = true
	completed, finishErr := e.engineFinishClose(run, fullyDrained, barriersCommitted, leakSegments, resourcesClosed, drainedFilesPending)
	if completed {
		e.cleanup.complete(run)
	} else {
		e.cleanup.retry(run, finishErr)
	}
	return finishErr
}

// engineFinishClose performs terminal cleanup for a running generation after
// manager quiescence has been proven and revalidated. The caller holds
// appendMu to fence producers, but the generation token -- not appendMu -- is
// what prevents a serialized second owner from running cleanup again.
func (e *qwpSfCursorEngine) engineFinishClose(run qwpSfCleanupToken, fullyDrained, barriersCommitted, leakSegments, resourcesClosed, drainedFilesPending bool) (bool, error) {
	if hook := qwpSfTestEngineFinishCloseHook.Load(); hook != nil {
		(*hook)()
	}
	qwpSfRunCleanupTestHook(qwpSfCleanupTestDuringTerminalCleanup)
	if resourcesClosed {
		if drainedFilesPending {
			if err := e.engineFinishDrainedFileCleanup(); err != nil {
				return false, err
			}
			if !e.cleanup.markDrainedFilesComplete(run) {
				return false, errors.New("qwp/sf: stale cleanup token after drained-file cleanup")
			}
		}
		if e.slotLock != nil {
			if err := e.slotLock.close(); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	var firstErr error
	drainCleanupAllowed := fullyDrained
	if fullyDrained && !barriersCommitted {
		if _, err := e.watermark.persistIfAdvanced(e.ring.segmentRingAckedFsn()); err != nil {
			firstErr = err
			drainCleanupAllowed = false
		}
		if drainCleanupAllowed {
			if err := e.watermark.sync(); err != nil {
				firstErr = err
				drainCleanupAllowed = false
			}
		}
		active := e.ring.getActiveSegment()
		manifest := e.ring.ringManifest()
		if drainCleanupAllowed && active != nil && manifest != nil {
			if err := manifest.update(active.segmentBaseSeq(), active.segmentBaseSeq()); err != nil {
				firstErr = err
				drainCleanupAllowed = false
			}
		}
		// These durability barriers precede every destructive cleanup step.
		// On failure retain the ring, side files, and flock so a later Close can
		// retry from intact state: the barriers write through those very
		// handles, so closing them would leave nothing to retry with.
		//
		// What that retention costs is worth stating, because it is far more
		// than what the path past the barriers holds. Returning here leaves
		// the whole ring mapped -- every segment in the slot, up to
		// sf_max_total_bytes of address space -- plus the watermark and the
		// symbol dictionary, and the retry owner comes back to it once a
		// second for as long as the process runs. Past the barriers the ring
		// is closed and only the slot lock's file descriptor is still held.
		// A local disk that never recovers therefore pins the slot's whole
		// mapping, not a single descriptor.
		if !drainCleanupAllowed {
			return false, firstErr
		}
		// The barriers write through the watermark and the ring's manifest,
		// which the close phases below shut. Checkpoint them in the state
		// record so a retry generation entering after a phase fault does not
		// re-run them against closed side files and stall on a spurious error.
		if !e.cleanup.checkpointDrainBarriers(run) {
			return false, errors.New("qwp/sf: stale cleanup token after drain barriers")
		}
	}
	firstErr = qwpAppendCloseError(firstErr,
		qwpRunCleanupPhaseGuarded("segment ring", func() error {
			qwpSfRunCleanupTestHook(qwpSfCleanupTestRingClosePhase)
			return e.ring.segmentRingCloseInternal(leakSegments)
		}))
	// Close the watermark mmap/fd only after the owned manager has exited or the
	// shared-manager entry is past its service pass, and before releasing the
	// slot lock. No manager write can race this close.
	if e.watermark != nil {
		firstErr = qwpAppendCloseError(firstErr,
			qwpRunCleanupPhaseGuarded("ack watermark", func() error {
				qwpSfRunCleanupTestHook(qwpSfCleanupTestWatermarkClosePhase)
				return e.watermark.close()
			}))
	}
	if e.persistedSymbolDict != nil {
		firstErr = qwpAppendCloseError(firstErr,
			qwpRunCleanupPhaseGuarded("symbol dictionary", func() error {
				qwpSfRunCleanupTestHook(qwpSfCleanupTestSymbolDictClosePhase)
				return e.persistedSymbolDict.close()
			}))
	}
	// A phase panic is stronger than an ordinary close error: the affected
	// resource may not have finished releasing. All sibling phases above were
	// still attempted, but retain the flock and make the generation retryable;
	// the next pass skips the checkpointed barriers and re-attempts each
	// idempotent resource close before release.
	var phasePanic *qwpCleanupPanicError
	if errors.As(firstErr, &phasePanic) {
		return false, firstErr
	}
	if !e.cleanup.checkpointResourcesClosed(run, drainCleanupAllowed) {
		return false, errors.New("qwp/sf: stale cleanup token after terminal resource close")
	}
	if drainCleanupAllowed {
		if err := e.engineFinishDrainedFileCleanup(); err != nil {
			if firstErr != nil {
				return false, errors.Join(firstErr, err)
			}
			return false, err
		}
		if !e.cleanup.markDrainedFilesComplete(run) {
			return false, errors.New("qwp/sf: stale cleanup token after drained-file cleanup")
		}
	}
	if e.slotLock != nil {
		if err := e.slotLock.close(); err != nil {
			firstErr = qwpAppendCloseError(firstErr, err)
			qwpEffectiveLogger(e.engineLogger()).Error("qwp/sf: could not release slot lock after close", "slot", e.sfDir, "error", err)
			return false, firstErr
		}
	}
	return true, firstErr
}

// engineFinishDrainedFileCleanup removes only fully-acked on-disk state. It is
// deliberately separate from resource closure so a partial unlink, manifest
// removal, or directory-sync failure remains retryable without touching the
// already-closed ring mappings and side-file descriptors.
func (e *qwpSfCursorEngine) engineFinishDrainedFileCleanup() error {
	if err := qwpSfUnlinkSegmentsAndSyncDir(e.sfDir); err != nil {
		return err
	}
	if err := qwpSfRemoveManifestAndSyncDir(e.sfDir); err != nil {
		return err
	}
	// Watermark and symbol-dictionary residue is harmless without segments and
	// a manifest: a restart ignores/removes it before creating the next slot.
	// Durably emptying the directory is therefore not part of the close
	// contract, and these best-effort side-file removals need no third barrier.
	qwpSfAckWatermarkRemoveOrphan(e.sfDir)
	qwpSfSymbolDictRemoveOrphan(e.sfDir)
	return nil
}

// engineCompleteDeferredClose is invoked by the manager after it is provably
// past the worker loop or the affected ring's service pass. Only the exact
// handoff generation installed in the manager can claim terminal cleanup.
func (e *qwpSfCursorEngine) engineCompleteDeferredClose(handoff qwpSfCleanupToken) {
	claim, ok := e.cleanup.managerClaim(handoff)
	if !ok {
		return
	}
	e.appendMu.Lock()
	err := e.engineFinishCloseGuarded(claim)
	e.appendMu.Unlock()
	logger := e.engineLogger()
	if err != nil {
		// The manager has finished its one-shot handoff. If terminal cleanup
		// failed or panicked, replace it with the engine-owned retry loop before
		// reporting the error so even a panicking user logger cannot leave the
		// slot without a cleanup owner.
		e.engineStartCloseRetryOwner(logger)
		qwpEffectiveLogger(logger).Error("qwp/sf: deferred engine close failed",
			"slot", e.sfDir, "error", err, "closeCompleted", e.engineCloseCompleted())
		return
	}
	qwpEffectiveLogger(logger).Info("qwp/sf: deferred engine close completed",
		"slot", e.sfDir, "closeCompleted", e.engineCloseCompleted())
}

func (e *qwpSfCursorEngine) engineCloseCompleted() bool {
	return e != nil && e.cleanup.completed()
}

// engineRetryCloseIfNeeded runs one cleanup attempt for the engine's retry
// owner and for pool reprobes. Both ignore the scheduler-owner marker and race
// only for the generation claim in the cleanup record.
func (e *qwpSfCursorEngine) engineRetryCloseIfNeeded() error {
	_, err := e.engineCloseWithOwner(false, qwpSfCleanupOwnerRetry, false)
	return err
}

// engineMarkReadersQuiesced records that the caller owning the send loop has
// stopped it, along with whether its mappings must be left alone. Terminal
// cleanup is refused to callers who did not do that themselves until this is
// published.
func (e *qwpSfCursorEngine) engineMarkReadersQuiesced(leakMappings bool) {
	if e != nil {
		e.cleanup.markReadersQuiesced(leakMappings)
	}
}

// engineRetryRepeatedClose makes the public double-close decision and cleanup
// claim as one state read. It returns acted=false while a close, manager
// callback or retry goroutine owns the protocol, or while the send loop has not
// been shown to have stopped, and acted=true when this call either re-drives
// manager teardown or consumes a retryable terminal claim.
func (e *qwpSfCursorEngine) engineRetryRepeatedClose() (acted bool, err error) {
	if e == nil {
		return false, nil
	}
	e.closed.Store(true)
	token, action := e.cleanup.beginRepeatClose(qwpSfCleanupOwnerClose)
	return e.runCleanupAction(token, action, qwpSfCleanupOwnerClose)
}

// engineRunCloseRetryAttempt contains panics outside terminal cleanup itself,
// including manager-handoff diagnostics. It deliberately does not touch
// the cleanup state: engineFinishCloseGuarded changes only the token generation
// this attempt successfully acquired.
func (e *qwpSfCursorEngine) engineRunCloseRetryAttempt() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp/sf: terminal cleanup retry panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return e.engineRetryCloseIfNeeded()
}

// engineStartCloseRetryOwner supplies terminal cleanup ownership after a close
// path can no longer retry inline (foreground close, construction unwind,
// drainer exit, and pool shutdown). The per-engine goroutine exits as soon as
// the flock is released. It deliberately has no retry deadline: releasing the
// flock or pool slot while durable cleanup is incomplete would let another
// owner race the retained files. A persistent local-storage fault therefore
// keeps the slot reserved until the fault is corrected or the process exits.
func (e *qwpSfCursorEngine) engineStartCloseRetryOwner(logger *slog.Logger) {
	if e == nil || !e.cleanup.startRetryOwner() {
		return
	}
	e.closeRetryWG.Add(1)
	go func() {
		defer e.closeRetryWG.Done()
		// Each attempt and every diagnostic below has its own panic boundary.
		// Keep this outer boundary as the final crash story: relinquish the
		// goroutine-owner marker and replace the owner if cleanup is incomplete.
		defer func() {
			if r := recover(); r != nil {
				restart := e.cleanup.retryOwnerPanicked()
				qwpEffectiveLogger(logger).Error("qwp/sf: terminal cleanup retry owner panicked",
					"slot", e.sfDir, "panic", r, "stack", string(debug.Stack()))
				if restart {
					e.engineStartCloseRetryOwner(logger)
				}
			}
		}()
		var lastWarn time.Time
		for !e.engineCloseCompleted() {
			retryErr := e.engineRunCloseRetryAttempt()
			if retryErr != nil {
				now := time.Now()
				if qwpSfShouldLogCloseRetry(lastWarn, now) {
					lastWarn = now
					qwpEffectiveLogger(logger).Warn("qwp/sf: terminal cleanup retry failed; slot lock remains held",
						"slot", e.sfDir, "error", retryErr)
				}
			}
			if e.engineCloseCompleted() {
				return
			}
			time.Sleep(qwpSfCloseRetryInterval.load())
		}
	}()
}

// engineLogger reads the configured logger through the manager. A hand-built
// engine in tests can carry no manager, and the close paths that log run on
// such an engine too.
func (e *qwpSfCursorEngine) engineLogger() *slog.Logger {
	if e == nil || e.manager == nil {
		return nil
	}
	return e.manager.logger.Load()
}

func qwpSfShouldLogCloseRetry(last, now time.Time) bool {
	return last.IsZero() || now.Sub(last) >= qwpSfCloseRetryLogThrottle
}

// qwpSfUnlinkSegmentsAndSyncDir unlinks every .sfa file under dir and commits
// their absence as one durability epoch. The manifest must not be removed
// until this helper succeeds: its separate epoch is the proof that a missing
// manifest cannot become durable while a manifest-required segment survives.
func qwpSfUnlinkSegmentsAndSyncDir(dir string) error {
	if err := qwpSfUnlinkAllSegmentFiles(dir); err != nil {
		return err
	}
	// A slot directory that is already gone is the end state this cleanup works
	// toward, and there is no namespace left to make durable. Treating it as
	// success also prevents the retry owner from retaining the flock forever.
	if err := qwpSfSyncSlotDir(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return qwpSfDurabilityError("sync slot directory after unlinking drained segments", dir, err)
	}
	return nil
}

// qwpSfRemoveManifestAndSyncDir removes a manifest only after the caller has
// committed every namespace mutation the manifest depended on, then commits
// the manifest's own absence as a new durability epoch.
func qwpSfRemoveManifestAndSyncDir(dir string) error {
	if err := qwpSfManifestRemove(dir); err != nil {
		return err
	}
	if err := qwpSfSyncSlotDir(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return qwpSfDurabilityError("sync slot directory after removing manifest", dir, err)
	}
	return nil
}

// qwpSfUnlinkAllSegmentFiles performs the unlink portion of the drained
// segment epoch. Callers use qwpSfUnlinkSegmentsAndSyncDir rather than invoking
// it directly so the namespace mutations cannot escape without their barrier.
// Called only on clean shutdown when the ring confirms every
// published FSN has been acked. Removal stops at the first failure, leaving
// the manifest in place.
//
// Files go oldest first: sf-initial.sfa (the legacy base-0 segment), then the
// sf-<generation>.sfa files in generation order. The sort needs the explicit
// sf-initial.sfa case because plain name order would put that oldest file last
// -- "i" sorts after the hex digits of every rotated name.
//
// The caller has already committed the manifest at headBase == activeBase ==
// the active segment's base, and recovery skips every segment below headBase,
// so the active segment is the one file it still requires. Every crash point
// in this sweep leaves a directory recovery accepts, in one of three ways.
// Before the active segment is reached, it is still there and the chain starts
// at headBase. After it, what can remain is the hot spare the manager minted
// most recently -- its generation is higher, so it sorts after the active
// segment and outlives it -- which holds no frames and is either read as the
// active segment at that same base or leaves the committed boundaries with no
// chain to find; both recover as an empty slot. Last, an empty directory,
// whose collapsed manifest is removed on its own.
//
// TestQwpSfDrainedCleanupCrashEpochsRecover enumerates every persisted subset
// and ordering permitted inside the epoch.
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
