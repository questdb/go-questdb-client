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
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Stop the real manager inside spare creation, forcing close to take the
// handoff route. Only the test's own slot is blocked. Cleanup releases the
// worker even if an assertion fails before the test does so explicitly.
func newQwpCleanupBlockedManager(t *testing.T) (*qwpSfCursorEngine, func()) {
	t.Helper()
	dir := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	hook := func(path string) {
		if filepath.Dir(path) == dir && filepath.Base(path) != "sf-initial.sfa" {
			enteredOnce.Do(func() { close(entered) })
			<-release
		}
	}
	oldGrace := qwpSfManagerCloseGrace.load()
	qwpSfManagerCloseGrace.store(10 * time.Millisecond)
	qwpSfTestSegmentCreateHook.Store(&hook)
	var engine *qwpSfCursorEngine
	t.Cleanup(func() {
		unblock()
		qwpSfTestSegmentCreateHook.Store(nil)
		qwpSfManagerCloseGrace.store(oldGrace)
		if engine != nil {
			waitQwpCleanupSignal(t, engine.manager.done, "manager exit during test cleanup")
			_ = engine.engineClose()
		}
	})
	var err error
	engine, err = qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	// Ensure a failed early assertion can still stop the manager.
	t.Cleanup(func() { unblock(); _ = engine.manager.segmentManagerClose() })
	waitQwpCleanupSignal(t, entered, "manager spare creation")
	return engine, unblock
}

func waitQwpCleanupSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func requireQwpCleanupLockHeld(t *testing.T, engine *qwpSfCursorEngine) {
	t.Helper()
	lock, err := qwpSfAcquireSlotLock(engine.sfDir)
	if lock != nil {
		_ = lock.close()
	}
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	require.False(t, engine.engineCloseCompleted())
}

func requireQwpCleanupLockReleased(t *testing.T, engine *qwpSfCursorEngine) {
	t.Helper()
	require.True(t, engine.engineCloseCompleted())
	lock, err := qwpSfAcquireSlotLock(engine.sfDir)
	require.NoError(t, err, "completion must mean another owner can actually acquire the slot")
	require.NoError(t, lock.close())
}

func TestQwpEngineCleanupHandoffRegistrationPanicRetries(t *testing.T) {
	engine, unblock := newQwpCleanupBlockedManager(t)
	var handoffCalls, terminalCalls atomic.Int32
	var stale qwpSfCleanupToken
	hook := func(point qwpSfCleanupTestPoint) {
		switch point {
		case qwpSfCleanupTestBeforeManagerHandoff:
			if handoffCalls.Add(1) == 1 {
				s := engine.cleanup.snapshot()
				stale = qwpSfCleanupToken{phase: s.phase, owner: s.owner, generation: s.generation}
				panic("injected handoff registration panic")
			}
		case qwpSfCleanupTestDuringTerminalCleanup:
			terminalCalls.Add(1)
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestCleanupHook.Store(nil) })

	require.PanicsWithValue(t, "injected handoff registration panic", func() { _ = engine.engineClose() })
	require.Equal(t, int32(1), handoffCalls.Load(), "must reach registration, not an earlier panic guard")
	s := engine.cleanup.snapshot()
	require.Equal(t, qwpSfCleanupOpen, s.phase)
	require.Equal(t, qwpSfCleanupOwnerNone, s.owner)
	require.Greater(t, s.generation, stale.generation)
	requireQwpCleanupLockHeld(t, engine)
	require.FileExists(t, filepath.Join(engine.sfDir, "sf-initial.sfa"))
	require.NotNil(t, engine.ring.getActiveSegment().address())
	require.Zero(t, terminalCalls.Load(), "no cleanup while the manager can still touch files")

	// Retrying before the worker exits must re-prove quiescence and hand off
	// again, not skip directly to terminal cleanup on a stale observation.
	require.NoError(t, engine.engineClose())
	require.Equal(t, int32(2), handoffCalls.Load())
	require.Equal(t, qwpSfCleanupManagerOwned, engine.cleanup.snapshot().phase)
	engine.engineCompleteDeferredClose(stale)
	require.Zero(t, terminalCalls.Load(), "an abandoned handoff callback cannot claim a newer generation")
	requireQwpCleanupLockHeld(t, engine)
	unblock()
	waitQwpCleanupSignal(t, engine.manager.done, "deferred cleanup")
	requireQwpCleanupLockReleased(t, engine)
	require.Equal(t, int32(1), terminalCalls.Load())
}

func TestQwpEngineCleanupBusyHandoffRetries(t *testing.T) {
	engine, unblock := newQwpCleanupBlockedManager(t)
	var otherCalls atomic.Int32
	other := &qwpSfManagerCleanupHandoff{cleanup: func() { otherCalls.Add(1) }}
	require.Equal(t, qwpSfManagerHandoffAccepted, engine.manager.deferOwnedCleanupHandoffUntilWorkerExit(other))

	require.ErrorContains(t, engine.engineClose(), "handoff slot is busy")
	s := engine.cleanup.snapshot()
	require.Equal(t, qwpSfCleanupOpen, s.phase)
	require.Equal(t, qwpSfCleanupOwnerNone, s.owner)
	requireQwpCleanupLockHeld(t, engine)
	require.FileExists(t, filepath.Join(engine.sfDir, "sf-initial.sfa"))
	require.NotNil(t, engine.ring.getActiveSegment().address())

	unblock()
	waitQwpCleanupSignal(t, engine.manager.done, "existing handoff callback")
	require.Equal(t, int32(1), otherCalls.Load(), "busy registration must not replace the existing callback")
	requireQwpCleanupLockHeld(t, engine)
	require.NoError(t, engine.engineRetryCloseIfNeeded())
	requireQwpCleanupLockReleased(t, engine)
}

func TestQwpEngineCleanupDeclinedHandoffFinishesInline(t *testing.T) {
	engine, unblock := newQwpCleanupBlockedManager(t)
	var handoffCalls, terminalCalls atomic.Int32
	hook := func(point qwpSfCleanupTestPoint) {
		switch point {
		case qwpSfCleanupTestBeforeManagerHandoff:
			handoffCalls.Add(1)
			// Close has already timed out, but the worker exits before callback
			// registration. The manager must return Quiescent, not Accepted.
			unblock()
			select {
			case <-engine.manager.done:
			case <-time.After(3 * time.Second):
				panic("manager did not exit before handoff registration")
			}
		case qwpSfCleanupTestDuringTerminalCleanup:
			terminalCalls.Add(1)
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestCleanupHook.Store(nil) })

	require.NoError(t, engine.engineClose())
	require.Equal(t, int32(1), handoffCalls.Load(), "must reach the timed-out handoff path")
	require.Equal(t, int32(1), terminalCalls.Load(), "declined handoff must finish on the caller")
	requireQwpCleanupLockReleased(t, engine)
	require.NoError(t, engine.engineClose())
	require.Equal(t, int32(1), terminalCalls.Load(), "repeated close must not run terminal cleanup twice")
}

func TestQwpEngineCleanupClaimPanicAllowsRetry(t *testing.T) {
	engine, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })
	var claimCalls, terminalCalls atomic.Int32
	var stale qwpSfCleanupToken
	hook := func(point qwpSfCleanupTestPoint) {
		switch point {
		case qwpSfCleanupTestAfterTerminalClaim:
			if claimCalls.Add(1) == 1 {
				s := engine.cleanup.snapshot()
				stale = qwpSfCleanupToken{phase: s.phase, owner: s.owner, generation: s.generation}
				panic("injected panic before terminal cleanup starts")
			}
		case qwpSfCleanupTestDuringTerminalCleanup:
			terminalCalls.Add(1)
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestCleanupHook.Store(nil) })

	require.ErrorContains(t, engine.engineClose(), "injected panic before terminal cleanup starts")
	require.Equal(t, int32(1), claimCalls.Load())
	require.Zero(t, terminalCalls.Load())
	s := engine.cleanup.snapshot()
	require.Equal(t, qwpSfCleanupRetryable, s.phase)
	require.Equal(t, qwpSfCleanupOwnerNone, s.owner)
	require.Greater(t, s.generation, stale.generation)
	requireQwpCleanupLockHeld(t, engine)
	require.FileExists(t, filepath.Join(engine.sfDir, "sf-initial.sfa"))

	require.NoError(t, engine.engineFinishCloseGuarded(stale))
	require.Zero(t, terminalCalls.Load(), "the abandoned claim cannot start cleanup")
	requireQwpCleanupLockHeld(t, engine)
	require.NoError(t, engine.engineRetryCloseIfNeeded())
	require.Equal(t, int32(1), terminalCalls.Load())
	requireQwpCleanupLockReleased(t, engine)
}

func TestQwpEngineCleanupRetryOwnerPanicStartsReplacement(t *testing.T) {
	engine, err := qwpSfNewCursorEngine(t.TempDir(), 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })
	var flockCalls atomic.Int32
	flockHook := func() error {
		if flockCalls.Add(1) == 1 {
			return errors.New("injected first flock release failure")
		}
		return nil
	}
	qwpSfTestBeforeFlockReleaseHook.Store(&flockHook)
	t.Cleanup(func() { qwpSfTestBeforeFlockReleaseHook.Store(nil) })
	require.ErrorContains(t, engine.engineClose(), "injected first flock release failure")
	requireQwpCleanupLockHeld(t, engine)

	replacementEntered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var ownerStarts atomic.Int32
	hook := func(point qwpSfCleanupTestPoint) {
		if point != qwpSfCleanupTestRetryOwnerStarted {
			return
		}
		switch ownerStarts.Add(1) {
		case 1:
			// This is outside engineRunCloseRetryAttempt: the outer owner
			// recovery must clear its marker and launch a new goroutine.
			panic("injected retry owner panic")
		case 2:
			close(replacementEntered)
			<-release
		}
	}
	qwpSfTestCleanupHook.Store(&hook)
	t.Cleanup(func() {
		unblock()
		qwpSfTestCleanupHook.Store(nil)
		waitQwpSfCloseRetryOwner(t, engine)
	})

	engine.engineStartCloseRetryOwner(nil)
	waitQwpCleanupSignal(t, replacementEntered, "replacement retry owner")
	require.Equal(t, int32(2), ownerStarts.Load())
	require.Equal(t, int32(1), flockCalls.Load(), "neither owner has entered a cleanup attempt yet")
	require.True(t, engine.cleanup.snapshot().retryOwnerStarted)
	requireQwpCleanupLockHeld(t, engine)
	engine.engineStartCloseRetryOwner(nil)
	require.Equal(t, int32(2), ownerStarts.Load(), "replacement must exclude another retry owner")

	unblock()
	waitQwpSfCloseRetryOwner(t, engine)
	require.Equal(t, int32(2), ownerStarts.Load())
	require.Equal(t, int32(2), flockCalls.Load())
	requireQwpCleanupLockReleased(t, engine)
}
