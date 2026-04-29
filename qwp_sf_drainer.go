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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// qwpSfDrainOutcome is the terminal state of a drainer's run.
type qwpSfDrainOutcome int32

const (
	qwpSfDrainOutcomePending qwpSfDrainOutcome = iota
	qwpSfDrainOutcomeLockedByOther
	qwpSfDrainOutcomeSuccess
	qwpSfDrainOutcomeFailed
	qwpSfDrainOutcomeStopped
)

// qwpSfDrainerPollInterval is how often the drainer wakes to
// re-check whether the slot is fully drained.
const qwpSfDrainerPollInterval = 50 * time.Millisecond

// qwpSfDrainerPoolCloseGrace bounds how long the pool's close()
// waits for active drainers to exit cleanly. Mirrors the Java
// 3-second grace.
const qwpSfDrainerPoolCloseGrace = 3 * time.Second

// qwpSfOrphanDrainer empties one orphan slot and exits. Owned by
// qwpSfDrainerPool; one instance per slot.
//
// Lifecycle:
//  1. Open a cursor engine on the slot — recovery picks up every
//     .sfa file already on disk. The engine itself acquires the
//     slot lock; if it's held by someone else we exit silently.
//  2. Open a fresh transport via the supplied factory (separate
//     connection from the foreground sender).
//  3. Run a send loop until ackedFsn catches up to the snapshot of
//     publishedFsn taken at startup.
//  4. Close everything in reverse order; release the lock.
//
// On terminal failure (auth-rejection, reconnect-budget exhaustion,
// recovery error), the drainer drops a .failed sentinel into the
// slot before exiting. Future scans skip the slot until an operator
// clears the sentinel — bounded automatic retry, then human-in-
// the-loop.
type qwpSfOrphanDrainer struct {
	slotPath                  string
	segmentSize               int64
	sfMaxTotalBytes           int64
	clientFactory             qwpSfReconnectFactory
	reconnectMaxDuration      time.Duration
	reconnectInitialBackoff   time.Duration
	reconnectMaxBackoff       time.Duration
	stopRequested             atomic.Bool
	targetFsn                 atomic.Int64 // -1 until startup observes publishedFsn
	ackedFsn                  atomic.Int64 // mirrors engine.ackedFsn for visibility
	outcome                   atomic.Int32
	lastErrorMessage          atomic.Pointer[string]
}

// qwpSfNewOrphanDrainer constructs a drainer for the given slot.
// All knobs are required; pool defaults are not applied here so
// the caller (the drainer pool) can pass through user-configured
// values verbatim.
func qwpSfNewOrphanDrainer(
	slotPath string,
	segmentSize, sfMaxTotalBytes int64,
	clientFactory qwpSfReconnectFactory,
	reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff time.Duration,
) *qwpSfOrphanDrainer {
	d := &qwpSfOrphanDrainer{
		slotPath:                slotPath,
		segmentSize:             segmentSize,
		sfMaxTotalBytes:         sfMaxTotalBytes,
		clientFactory:           clientFactory,
		reconnectMaxDuration:    reconnectMaxDuration,
		reconnectInitialBackoff: reconnectInitialBackoff,
		reconnectMaxBackoff:     reconnectMaxBackoff,
	}
	d.targetFsn.Store(-1)
	d.ackedFsn.Store(-1)
	d.outcome.Store(int32(qwpSfDrainOutcomePending))
	return d
}

// drainerOutcome returns the terminal state of the drainer's run,
// or qwpSfDrainOutcomePending while it's still running.
func (d *qwpSfOrphanDrainer) drainerOutcome() qwpSfDrainOutcome {
	return qwpSfDrainOutcome(d.outcome.Load())
}

// drainerTargetFsn returns the publishedFsn snapshot taken at
// startup, or -1 if the drainer hasn't started yet.
func (d *qwpSfOrphanDrainer) drainerTargetFsn() int64 {
	return d.targetFsn.Load()
}

// drainerAckedFsn returns the latest known ackedFsn for the slot.
func (d *qwpSfOrphanDrainer) drainerAckedFsn() int64 {
	return d.ackedFsn.Load()
}

// drainerRequestStop politely asks the drainer to exit at its next
// poll. Used by the pool's close path; drainers ALSO exit on their
// own when the slot fully drains.
func (d *qwpSfOrphanDrainer) drainerRequestStop() {
	d.stopRequested.Store(true)
}

func (d *qwpSfOrphanDrainer) recordFailure(reason string) {
	d.lastErrorMessage.Store(&reason)
	qwpSfMarkSlotFailed(d.slotPath, reason)
	d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
}

// drainerRun is the drainer goroutine entry point. Runs to
// completion (or terminal failure), then sets outcome and exits.
func (d *qwpSfOrphanDrainer) drainerRun(ctx context.Context) {
	engine, err := qwpSfNewCursorEngine(d.slotPath, d.segmentSize, d.sfMaxTotalBytes, qwpSfEngineDefaultAppendDeadline)
	if err != nil {
		// Lock contention is expected (a sibling drainer or the
		// foreground sender holds it) — exit silently, no .failed.
		if errors.Is(err, qwpSfErrLockBusy) || strings.Contains(err.Error(), "slot already in use") {
			d.outcome.Store(int32(qwpSfDrainOutcomeLockedByOther))
			return
		}
		// Recovery / disk error — surface as failure with sentinel.
		msg := err.Error()
		d.lastErrorMessage.Store(&msg)
		qwpSfMarkSlotFailed(d.slotPath, "engine open: "+msg)
		d.outcome.Store(int32(qwpSfDrainOutcomeFailed))
		return
	}
	defer func() { _ = engine.engineClose() }()

	target := engine.enginePublishedFsn()
	d.targetFsn.Store(target)
	if engine.engineAckedFsn() >= target {
		// Slot is already drained — engineClose will unlink residual
		// .sfa files in its own logic.
		d.outcome.Store(int32(qwpSfDrainOutcomeSuccess))
		return
	}
	transport, err := d.clientFactory(ctx)
	if err != nil {
		msg := err.Error()
		d.recordFailure("initial connect: " + msg)
		return
	}
	loop := qwpSfNewSendLoop(engine, transport, d.clientFactory,
		qwpSfDefaultParkInterval,
		d.reconnectMaxDuration, d.reconnectInitialBackoff, d.reconnectMaxBackoff)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	timer := time.NewTicker(qwpSfDrainerPollInterval)
	defer timer.Stop()
	for {
		acked := engine.engineAckedFsn()
		d.ackedFsn.Store(acked)
		if acked >= target {
			d.outcome.Store(int32(qwpSfDrainOutcomeSuccess))
			return
		}
		if err := loop.sendLoopCheckError(); err != nil {
			d.recordFailure("wire: " + err.Error())
			return
		}
		if d.stopRequested.Load() {
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		select {
		case <-ctx.Done():
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		case <-timer.C:
		}
	}
}

// qwpSfDrainerPool is a bounded thread pool that runs orphan
// drainer tasks. One pool per foreground sender; size capped by
// max_background_drainers.
//
// Each drainer gets its own goroutine, throttled by a buffered
// semaphore channel. Idle pool (no orphans submitted) costs zero
// goroutines. Closing the pool requests every still-running
// drainer to stop and waits up to qwpSfDrainerPoolCloseGrace for
// them to exit cleanly.
type qwpSfDrainerPool struct {
	maxConcurrent int
	sem           chan struct{}
	closed        atomic.Bool
	wg            sync.WaitGroup

	mu     sync.Mutex
	active []*qwpSfOrphanDrainer
}

// qwpSfNewDrainerPool constructs a pool with the given concurrency
// cap. Panics on a non-positive cap.
func qwpSfNewDrainerPool(maxConcurrent int) *qwpSfDrainerPool {
	if maxConcurrent <= 0 {
		panic("qwp/sf: maxConcurrent must be > 0")
	}
	return &qwpSfDrainerPool{
		maxConcurrent: maxConcurrent,
		sem:           make(chan struct{}, maxConcurrent),
	}
}

// drainerPoolSubmit launches the drainer in a managed goroutine.
// Returns an error if the pool has been closed.
//
// Drainers queue when the concurrency cap is reached: the
// goroutine takes a slot on the semaphore and proceeds.
func (p *qwpSfDrainerPool) drainerPoolSubmit(ctx context.Context, d *qwpSfOrphanDrainer) error {
	if p.closed.Load() {
		return errors.New("qwp/sf: drainer pool closed")
	}
	p.mu.Lock()
	p.active = append(p.active, d)
	p.mu.Unlock()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// Wait for a slot. If the pool closes mid-wait, the slot
		// channel never frees up — but ctx.Done unblocks us.
		select {
		case p.sem <- struct{}{}:
		case <-ctx.Done():
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		defer func() { <-p.sem }()
		if p.closed.Load() {
			d.outcome.Store(int32(qwpSfDrainOutcomeStopped))
			return
		}
		d.drainerRun(ctx)
	}()
	return nil
}

// drainerPoolSnapshot returns a copy of the currently-tracked
// drainers (active + finished). Useful for status accessors.
func (p *qwpSfDrainerPool) drainerPoolSnapshot() []*qwpSfOrphanDrainer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*qwpSfOrphanDrainer, len(p.active))
	copy(out, p.active)
	return out
}

// drainerPoolClose stops the pool. Sets closed=true so new submits
// fail; requests stop on every tracked drainer; waits up to
// qwpSfDrainerPoolCloseGrace for drainers to exit, then proceeds.
// Idempotent.
func (p *qwpSfDrainerPool) drainerPoolClose() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.mu.Lock()
	for _, d := range p.active {
		d.drainerRequestStop()
	}
	p.mu.Unlock()
	doneCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(qwpSfDrainerPoolCloseGrace):
	}
}
