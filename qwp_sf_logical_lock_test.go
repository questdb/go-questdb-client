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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the parent-anchored logical slot lock that serialises a
// slot pathname's close -> rename -> recreate transition. They are evidence for
// participants that use this protocol on a filesystem providing the advisory
// locking and rename semantics it relies on; they say nothing about operators
// moving files by hand, older clients that never take this lock, or hosts where
// advisory locks do not work.

// TestQwpSfLogicalLockFencesTheTransitionWindow stops a constructor inside the
// window the directory-local lock cannot cover — after a failed build's cleanup
// released that lock and before the rename — and pins that no other
// participant can take the pathname there.
func TestQwpSfLogicalLockFencesTheTransitionWindow(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	contents := qwpSfTestRefusedSlot(t, slot)

	paused, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(dir string) {
		if filepath.Clean(dir) != filepath.Clean(slot) {
			return
		}
		once.Do(func() {
			close(paused)
			<-release
		})
	}
	qwpSfTestBeforeWholeSlotQuarantineHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeWholeSlotQuarantineHook.Store(nil) })

	type buildResult struct {
		engine *qwpSfCursorEngine
		err    error
	}
	results := make(chan buildResult, 1)
	go func() {
		e, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
		results <- buildResult{engine: e, err: err}
	}()

	select {
	case <-paused:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("the constructor never reached the transition window")
	}

	// Prove this is the intended gap, not merely an early-construction pause:
	// failed-build cleanup has released the directory-local flock, even though
	// the logical pathname lock still fences participating open routes.
	directoryLock, err := qwpSfAcquireSlotLock(slot)
	require.NoError(t, err)
	require.NoError(t, directoryLock.close())

	// A second constructor must not acquire the pathname mid-transition.
	other, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, other)
	require.ErrorIs(t, err, qwpSfErrLockBusy)

	// Neither may a queued drainer, and being fenced out must not be mistaken
	// for a verdict about the slot's bytes.
	drainer := qwpSfNewOrphanDrainer(slot, 4096, qwpSfUnlimitedTotalBytes,
		qwpSfDialFor(srv), nil, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	drainer.drainerRun(context.Background())
	assert.Equal(t, qwpSfDrainOutcomeLockedByOther, drainer.drainerOutcome())
	require.NoFileExists(t, filepath.Join(slot, qwpSfFailedSentinelName))

	close(release)
	built := <-results
	require.NoError(t, built.err)
	require.NotNil(t, built.engine)
	t.Cleanup(func() { _ = built.engine.engineClose() })

	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	require.Equal(t, preserved, built.engine.engineQuarantinedSlotPath())
	qwpSfRequireSameFiles(t, preserved, contents)
	require.NoDirExists(t, filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"1"),
		"the fenced-out participants must not have produced a second copy")
}

// TestQwpSfLogicalLockIsHeldAcrossProcesses pins that the exclusion is a real
// file lock rather than a process-local mutex: the holder is a separate
// process. It is evidence for this platform's advisory locking only.
func TestQwpSfLogicalLockIsHeldAcrossProcesses(t *testing.T) {
	if root := os.Getenv("QWP_LOGICAL_LOCK_CHILD"); root != "" {
		lock, err := qwpSfAcquireLogicalSlotLock(filepath.Join(root, "sender-a"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "held"), []byte("held"), 0o644))
		// Hold it while the parent tries, then leave without releasing: the
		// kernel drops the lock when the process exits.
		time.Sleep(3 * time.Second)
		require.True(t, lock.held())
		os.Exit(0)
	}
	root := t.TempDir()
	cmd := qwpTestSubprocess(t, "TestQwpSfLogicalLockIsHeldAcrossProcesses")
	cmd.Env = append(os.Environ(), "QWP_LOGICAL_LOCK_CHILD="+root)
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(root, "held"))
		return err == nil
	}, qwpTestWaitTimeout, 10*time.Millisecond, "the child never took the lock")

	engine, err := qwpSfNewCursorEngine(filepath.Join(root, "sender-a"), 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.ErrorIs(t, err, qwpSfErrLockBusy)
	assert.NotContains(t, err.Error(), qwpSfSelfHolder(), "the holder is another process")

	require.NoError(t, <-done)
	// Once the holder is gone the pathname is available again.
	engine, err = qwpSfNewCursorEngine(filepath.Join(root, "sender-a"), 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())
}

// TestQwpSfLogicalLockReleaseOutcomes pins the two release classes apart. A
// failure that left the descriptor intact keeps the lock held, so exclusion
// survives and a retry is legitimate. A failure that consumed the descriptor
// says nothing about what the operating system did, so it is terminal: it is
// reported as a cleanup failure and the lock value is retained rather than
// closed again.
func TestQwpSfLogicalLockReleaseOutcomes(t *testing.T) {
	t.Run("retryable", func(t *testing.T) {
		root := t.TempDir()
		lock, err := qwpSfAcquireLogicalSlotLock(filepath.Join(root, "sender-a"))
		require.NoError(t, err)

		fault := errors.New("release refused")
		hook := func(*qwpSfSlotLock) error { return fault }
		qwpSfTestBeforeLogicalLockReleaseHook.Store(&hook)
		releaseErr := qwpSfReleaseLogicalLock(lock)
		qwpSfTestBeforeLogicalLockReleaseHook.Store(nil)

		require.ErrorIs(t, releaseErr, fault)
		assert.NotErrorIs(t, releaseErr, ErrCleanupFailed,
			"a lock that is still held has not lost its release obligation")
		require.True(t, lock.held(), "exclusion must survive a failed release")

		// While it is held, nothing else may take the pathname.
		_, busy := qwpSfAcquireLogicalSlotLock(filepath.Join(root, "sender-a"))
		require.ErrorIs(t, busy, qwpSfErrLockBusy)

		require.NoError(t, qwpSfReleaseLogicalLock(lock))
		assert.False(t, lock.held())
		again, err := qwpSfAcquireLogicalSlotLock(filepath.Join(root, "sender-a"))
		require.NoError(t, err)
		require.NoError(t, qwpSfReleaseLogicalLock(again))
	})

	t.Run("terminal", func(t *testing.T) {
		root := t.TempDir()
		lock, err := qwpSfAcquireLogicalSlotLock(filepath.Join(root, "sender-a"))
		require.NoError(t, err)
		// Model a release operation that consumed the descriptor but still
		// returned an error. It clears the pointer itself, so no code retries a
		// numeric descriptor whose ownership is already unknown.
		fault := errors.New("release consumed descriptor")
		hook := func(lock *qwpSfSlotLock) error {
			closeErr := lock.file.Close()
			lock.file = nil
			return errors.Join(closeErr, fault)
		}
		qwpSfTestBeforeLogicalLockReleaseHook.Store(&hook)
		releaseErr := qwpSfReleaseLogicalLock(lock)
		qwpSfTestBeforeLogicalLockReleaseHook.Store(nil)
		require.Error(t, releaseErr)
		require.ErrorIs(t, releaseErr, fault)
		require.ErrorIs(t, releaseErr, ErrCleanupFailed)
		assert.Contains(t, releaseErr.Error(), "cannot be established")
		assert.False(t, lock.held(), "a consumed descriptor must not be closed again")

		qwpSfRetainedLogicalLocks.mu.Lock()
		retained := len(qwpSfRetainedLogicalLocks.locks)
		qwpSfRetainedLogicalLocks.mu.Unlock()
		assert.Positive(t, retained, "a lock in an unknown state stays reachable")
	})
}

// TestQwpSfEngineWithUnreleasableLogicalLockReturnsNoSender pins that a
// construction which cannot resolve its transition protection does not hand
// back a usable engine, keeps the cause reportable, and leaves the obligation
// with a cleanup owner rather than with the caller.
func TestQwpSfEngineWithUnreleasableLogicalLockReturnsNoSender(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")

	var once sync.Once
	fault := errors.New("logical release refused")
	hook := func(*qwpSfSlotLock) error {
		var err error
		once.Do(func() { err = fault })
		return err
	}
	qwpSfTestBeforeLogicalLockReleaseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeLogicalLockReleaseHook.Store(nil) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine, "an unresolved transition lock must not produce a sender")
	require.ErrorIs(t, err, fault)

	// The owner that took over finishes the release, so the slot becomes
	// usable again without the caller retrying anything itself.
	require.Eventually(t, func() bool {
		next, openErr := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
		if openErr != nil {
			return false
		}
		require.NoError(t, next.engineClose())
		return true
	}, qwpTestWaitTimeout, 20*time.Millisecond)
}

// TestQwpSfFailedConstructionRetainsAStillHeldLogicalLock pins the branch that
// has no built engine to own cleanup. A retryable release failure must still
// have an ordinary cleanup reporter and must keep excluding contenders until
// that owner confirms release.
func TestQwpSfFailedConstructionRetainsAStillHeldLogicalLock(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "quarantined")
	require.NoError(t, os.MkdirAll(filepath.Join(slot, "legacy-copy"), 0o755))

	fault := errors.New("logical release refused after construction failure")
	var allowRelease atomic.Bool
	hook := func(*qwpSfSlotLock) error {
		if !allowRelease.Load() {
			return fault
		}
		return nil
	}
	qwpSfTestBeforeLogicalLockReleaseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeLogicalLockReleaseHook.Store(nil) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.Nil(t, engine)
	require.ErrorIs(t, err, qwpSfErrLegacyQuarantineContainer)
	require.ErrorIs(t, err, fault)
	var buildErr *qwpSfBuildCleanupError
	require.ErrorAs(t, err, &buildErr,
		"the held lock needs a cleanup owner even though no engine was built")

	_, busy := qwpSfAcquireLogicalSlotLock(slot)
	require.ErrorIs(t, busy, qwpSfErrLockBusy,
		"the cleanup owner must retain exclusion until release succeeds")

	allowRelease.Store(true)
	require.Eventually(t, buildErr.closeCompleted, qwpTestWaitTimeout, 10*time.Millisecond)
	again, acquireErr := qwpSfAcquireLogicalSlotLock(slot)
	require.NoError(t, acquireErr)
	require.NoError(t, qwpSfReleaseLogicalLock(again))
}

// TestQwpSfFailedConstructionWithConsumedLogicalDescriptorIsTerminal pins the
// other release outcome after an earlier construction error. The descriptor is
// consumed exactly once by the injected release operation; cleanup must not
// retry it, the original cause remains visible, and no exclusion is claimed
// once the OS has released the lock.
func TestQwpSfFailedConstructionWithConsumedLogicalDescriptorIsTerminal(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "quarantined")
	require.NoError(t, os.MkdirAll(filepath.Join(slot, "legacy-copy"), 0o755))

	fault := errors.New("close consumed the logical descriptor")
	hook := func(lock *qwpSfSlotLock) error {
		require.NotNil(t, lock.file)
		closeErr := lock.file.Close()
		lock.file = nil
		return errors.Join(closeErr, fault)
	}
	qwpSfTestBeforeLogicalLockReleaseHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeLogicalLockReleaseHook.Store(nil) })

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	qwpSfTestBeforeLogicalLockReleaseHook.Store(nil)
	require.Nil(t, engine)
	require.ErrorIs(t, err, qwpSfErrLegacyQuarantineContainer)
	require.ErrorIs(t, err, fault)
	require.ErrorIs(t, err, ErrCleanupFailed)
	var buildErr *qwpSfBuildCleanupError
	assert.False(t, errors.As(err, &buildErr),
		"a consumed descriptor has no retryable release work to transfer")

	again, acquireErr := qwpSfAcquireLogicalSlotLock(slot)
	require.NoError(t, acquireErr,
		"the test observed a consumed descriptor, so continued exclusion must not be claimed")
	require.NoError(t, qwpSfReleaseLogicalLock(again))
}

// TestQwpSfRetiredSlotKeepsReusableLogicalLockFiles pins the safe lock-file
// lifecycle. A stale pair is harmless and reused; unlinking it would permit a
// process that already opened the old inode to race a new inode at the same
// pathname and split transition ownership.
func TestQwpSfRetiredSlotKeepsReusableLogicalLockFiles(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, 0)
	require.NoError(t, err)

	_, lockPath, pidPath, err := qwpSfResolveLogicalLock(slot)
	require.NoError(t, err)
	require.FileExists(t, lockPath)
	require.NoError(t, engine.engineClose())
	require.FileExists(t, lockPath)
	require.FileExists(t, pidPath)

	identity, err := os.Stat(lockPath)
	require.NoError(t, err)
	again, err := qwpSfAcquireLogicalSlotLock(slot)
	require.NoError(t, err)
	require.NoError(t, qwpSfReleaseLogicalLock(again))
	after, err := os.Stat(lockPath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(identity, after), "the next owner must flock the same inode")
}
