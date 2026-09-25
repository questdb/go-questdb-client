/******************************************************************************
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
 *****************************************************************************/

package questdb

import (
	"errors"
	"runtime/debug"
	"sync"
	"time"
)

// qwpSfCleanupControl records the progress and result of one cleanup worker.
// Callers can wait for updates, but only that worker releases resources and
// retries storage errors.
type qwpSfCleanupControl struct {
	once     sync.Once
	mu       sync.Mutex
	changed  chan struct{}
	done     chan struct{}
	err      error
	finished bool
	released bool
}

func (c *qwpSfCleanupControl) publish(err error, finished, released bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err, c.finished, c.released = err, finished, released
	close(c.changed)
	c.changed = make(chan struct{})
}

func (e *qwpSfCursorEngine) engineCloseCompleted() bool {
	if e == nil {
		return true
	}
	e.cleanup.mu.Lock()
	defer e.cleanup.mu.Unlock()
	return e.cleanup.released
}

func (e *qwpSfCursorEngine) cleanupResult() error {
	if e == nil {
		return nil
	}
	e.cleanup.mu.Lock()
	defer e.cleanup.mu.Unlock()
	return e.cleanup.err
}

func (e *qwpSfCursorEngine) cleanupFailure() error {
	if e == nil {
		return nil
	}
	e.cleanup.mu.Lock()
	defer e.cleanup.mu.Unlock()
	if errors.Is(e.cleanup.err, ErrCleanupFailed) {
		return e.cleanup.err
	}
	return nil
}

// engineClose starts cleanup once and waits up to qwpSfManagerCloseGrace.
// It can return nil while cleanup is still running, as standalone sender Close
// allows. The pool checks completion and failure separately.
func (e *qwpSfCursorEngine) engineClose() error { return e.engineCloseWithCause(nil) }

func (e *qwpSfCursorEngine) engineCloseWithCause(cause error) error {
	if e == nil {
		return cause
	}
	e.cleanup.once.Do(func() {
		e.closed.Store(true)
		e.cleanup.mu.Lock()
		e.cleanup.changed = make(chan struct{})
		e.cleanup.done = make(chan struct{})
		e.cleanup.mu.Unlock()
		go e.engineCleanupWorker(cause)
	})
	timer := time.NewTimer(qwpSfManagerCloseGrace.load())
	defer timer.Stop()
	for {
		e.cleanup.mu.Lock()
		err, finished, changed := e.cleanup.err, e.cleanup.finished, e.cleanup.changed
		e.cleanup.mu.Unlock()
		if finished || err != nil {
			return err
		}
		select {
		case <-changed:
		case <-timer.C:
			e.cleanup.mu.Lock()
			err = e.cleanup.err
			e.cleanup.mu.Unlock()
			return err
		}
	}
}

// Keep failed engines alive until process exit, even after their workers stop.
// This prevents garbage collection from closing files or releasing locks that
// cleanup could not safely release. This list does not retry cleanup or decide
// whether the pool can reuse a slot.
var qwpSfFailedEngines struct {
	sync.Mutex
	engines []*qwpSfCursorEngine
}

func (e *qwpSfCursorEngine) retainFailedCleanup(err error) {
	qwpSfFailedEngines.Lock()
	qwpSfFailedEngines.engines = append(qwpSfFailedEngines.engines, e)
	qwpSfFailedEngines.Unlock()
	e.cleanup.publish(err, true, false)
	qwpEffectiveLogger(e.engineLogger()).Error("qwp/sf: cleanup failed; resources remain retained", "slot", e.sfDir, "error", err)
}

func (e *qwpSfCursorEngine) engineCleanupWorker(cause error) {
	defer close(e.cleanup.done)
	defer func() {
		if r := recover(); r != nil {
			err := errors.Join(cause, &qwpCleanupPanicError{phase: "engine", cause: r, stack: debug.Stack()})
			e.retainFailedCleanup(err)
		}
	}()
	// Ask both the manager and send loop to stop before waiting for them. A
	// logical-lock-only cleanup owner has no segment manager: it exists solely
	// to retain and retry a transition lock after construction failed before an
	// engine could be built. A real manager only signals that it has stopped;
	// this worker does cleanup.
	var managerDone <-chan struct{}
	if e.manager != nil {
		managerDone = e.manager.segmentManagerStop()
	}
	if reader := e.reader.Load(); reader != nil {
		cause = errors.Join(cause, qwpRunCleanupPhaseGuarded("send loop", reader.sendLoopClose))
		// After a panic, the reader may still be running. Keep its mapped memory.
		var panicErr *qwpCleanupPanicError
		if errors.As(cause, &panicErr) {
			e.retainFailedCleanup(cause)
			return
		}
	}
	if managerDone != nil {
		<-managerDone
	}
	cause = e.closeRejectedTransports(cause)
	// A manager panic may have interrupted opening or removing a segment.
	// Keep its resources: we cannot safely retry a partly completed operation.
	if e.manager != nil {
		if err := e.manager.managerWorkerError(); err != nil {
			e.retainFailedCleanup(errors.Join(cause, ErrCleanupFailed, err))
			return
		}
	}
	if errors.Is(cause, ErrCleanupFailed) {
		e.retainFailedCleanup(cause)
		return
	}
	e.appendMu.Lock()
	defer e.appendMu.Unlock()
	qwpSfRunCleanupTestHook(qwpSfCleanupTestAfterQuiescence)
	fullyDrained := e.sfDir != "" && e.ring != nil && e.ring.segmentRingAckedFsn() >= e.ring.segmentRingPublishedFsn()
	// Remember which disk updates finished so storage-error retries can skip
	// them. A panic stops cleanup instead of retrying.
	barriersDone, segmentsGone, manifestGone := !fullyDrained, false, false
	var lastWarn time.Time
	for {
		err := e.engineCleanupAttempt(fullyDrained, &barriersDone, &segmentsGone, &manifestGone)
		result := errors.Join(cause, err)
		if errors.Is(result, ErrCleanupFailed) {
			e.retainFailedCleanup(result)
			return
		}
		if e.cleanupResourcesReleased() && (!fullyDrained || manifestGone) {
			lockErr := qwpRunCleanupPhaseGuarded("slot lock", e.slotLock.close)
			result = errors.Join(result, lockErr)
			if errors.Is(result, ErrCleanupFailed) {
				e.retainFailedCleanup(result)
				return
			}
			if e.slotLock == nil || e.slotLock.file == nil {
				// A construction that could not release the slot's pathname lock
				// handed it here. Releasing it after the directory-local lock is
				// what lets another participant take the pathname, so it happens
				// only now, with this engine's files already released.
				logicalErr := qwpRunCleanupPhaseGuarded("logical slot lock", e.releaseAdoptedLogicalLock)
				result = errors.Join(result, logicalErr)
				if errors.Is(result, ErrCleanupFailed) {
					e.retainFailedCleanup(result)
					return
				}
				if !e.logicalLockHeld() {
					// Logical lock files deliberately remain in place. Unlinking a
					// lock pathname is unsafe even while holding its flock: another
					// process can already have opened the old inode but not yet tried
					// to lock it, then race a successor that creates a new inode at
					// the freed pathname. Stale files are harmless and reusable.
					//
					// Every resource is released, so any error this attempt collected
					// came from closing a descriptor or lock that was closed anyway.
					// Such an error does not mean anything is still held; it is
					// logged, and the result keeps only cause.
					e.cleanup.publish(cause, true, true)
					if released := errors.Join(err, lockErr, logicalErr); released != nil {
						qwpEffectiveLogger(e.engineLogger()).Warn("qwp/sf: slot released; closing its files reported errors", "slot", e.sfDir, "error", released)
					}
					return
				}
			}
		}
		e.cleanup.publish(result, false, false)
		now := time.Now()
		if result != nil && qwpSfShouldLogCloseRetry(lastWarn, now) {
			lastWarn = now
			qwpEffectiveLogger(e.engineLogger()).Warn("qwp/sf: cleanup incomplete; retaining slot and retrying storage", "slot", e.sfDir, "error", result)
		}
		time.Sleep(qwpSfCloseRetryInterval.load())
	}
}

func (e *qwpSfCursorEngine) engineCleanupAttempt(drained bool, barriersDone, segmentsGone, manifestGone *bool) error {
	if hook := qwpSfTestEngineFinishCloseHook.Load(); hook != nil {
		(*hook)()
	}
	qwpSfRunCleanupTestHook(qwpSfCleanupTestDuringTerminalCleanup)
	if !*barriersDone {
		if _, err := e.watermark.persistIfAdvanced(e.ring.segmentRingAckedFsn()); err != nil {
			return err
		}
		if err := e.watermark.sync(); err != nil {
			return err
		}
		if active, manifest := e.ring.getActiveSegment(), e.ring.ringManifest(); active != nil && manifest != nil {
			if err := manifest.update(active.segmentBaseSeq(), active.segmentBaseSeq()); err != nil {
				return err
			}
		}
		*barriersDone = true
	}
	var err error
	if e.ring != nil {
		err = qwpRunCleanupPhaseGuarded("segment ring", func() error {
			qwpSfRunCleanupTestHook(qwpSfCleanupTestRingClosePhase)
			return e.ring.segmentRingClose()
		})
	}
	for _, s := range e.looseSegments {
		err = errors.Join(err, qwpRunCleanupPhaseGuarded("construction segment", func() error {
			if hook := qwpSfTestBeforeSegmentCloseHook.Load(); hook != nil {
				(*hook)(s)
			}
			return s.close()
		}))
	}
	err = errors.Join(err, qwpRunCleanupPhaseGuarded("construction manifest", func() error {
		if e.looseManifest != nil {
			qwpSfRunCleanupTestHook(qwpSfCleanupTestManifestClosePhase)
		}
		return e.looseManifest.close()
	}))
	err = errors.Join(err, qwpRunCleanupPhaseGuarded("ack watermark", func() error {
		qwpSfRunCleanupTestHook(qwpSfCleanupTestWatermarkClosePhase)
		return e.watermark.close()
	}))
	err = errors.Join(err, qwpRunCleanupPhaseGuarded("symbol dictionary", func() error {
		qwpSfRunCleanupTestHook(qwpSfCleanupTestSymbolDictClosePhase)
		return e.persistedSymbolDict.close()
	}))
	err = errors.Join(err, e.acquired.close())
	// The manager saved any unfinished release or file-removal work in its
	// entry. It has now stopped, so this worker can finish that work.
	if e.managerEntry != nil {
		err = errors.Join(err, e.manager.closeServiceResidue(e.managerEntry))
	}
	if errors.Is(err, ErrCleanupFailed) || !e.cleanupResourcesReleased() {
		return err
	}
	if drained {
		if !*segmentsGone {
			if unlinkErr := qwpSfUnlinkSegmentsAndSyncDir(e.sfDir); unlinkErr != nil {
				return errors.Join(err, unlinkErr)
			}
			*segmentsGone = true
		}
		if !*manifestGone {
			if unlinkErr := qwpSfRemoveManifestAndSyncDir(e.sfDir); unlinkErr != nil {
				return errors.Join(err, unlinkErr)
			}
			*manifestGone = true
		}
		// Best-effort tidying of the side files, deliberately unchecked and
		// deliberately without a directory barrier of its own. Startup does not
		// depend on either removal: qwpSfAckWatermarkOpenPrepared retires a stale
		// ACK record durably on the next construction, and a residual file is a
		// documented outcome of close. Making a successful deletion the
		// correctness mechanism would move the safety decision onto the one path
		// that cannot report a failure.
		qwpSfAckWatermarkRemoveOrphan(e.sfDir)
		qwpSfSymbolDictRemoveOrphan(e.sfDir)
	}
	return err
}

func (e *qwpSfCursorEngine) cleanupResourcesReleased() bool {
	if !e.acquired.released() {
		return false
	}
	if e.ring != nil && !e.ring.resourcesReleased() {
		return false
	}
	for _, s := range e.looseSegments {
		if !s.resourcesReleased() {
			return false
		}
	}
	if e.looseManifest != nil && e.looseManifest.file != nil {
		return false
	}
	if e.watermark != nil && (e.watermark.buf != nil || e.watermark.file != nil) {
		return false
	}
	if e.persistedSymbolDict != nil && e.persistedSymbolDict.file != nil {
		return false
	}
	return e.managerEntry == nil || e.managerEntry.serviceResidueReleased()
}
