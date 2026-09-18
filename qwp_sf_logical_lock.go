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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// qwpSfLogicalLockDirName is the parent-anchored directory that holds one lock
// file per logical slot name. Its paths match the inspected Java client's
// layout (<sf_dir>/.slot-locks/<slot-name>.lock plus a .lock.pid sidecar).
// Matching paths are necessary but not sufficient for safe cross-client
// sharing: every participant must also retain the pathname for the same lock
// lifecycle. This client never unlinks these files.
const qwpSfLogicalLockDirName = ".slot-locks"

// The directory-local .lock lives inside the slot directory, so it travels with
// the inode when a refused slot is renamed aside. That makes it useless for the
// one window this client must serialise: the failed build's cleanup, the
// quarantine rename, and the fresh slot's creation all concern a *pathname*,
// not an inode. A second constructor or a queued orphan drainer can otherwise
// take the original pathname between the cleanup and the rename, and then
// operate on a renamed inode or race the candidate selection.
//
// The logical lock is anchored in the slot's parent, so it stays attached to
// the logical slot name across the whole transition. It is short-lived: a
// constructor releases it once the new engine holds the directory-local lock,
// and a drainer releases it once its engine holds that lock.
//
// Scope: this serialises processes that participate in the protocol, on
// filesystems that provide the advisory locking and rename semantics the
// client relies on. It is not protection against an operator moving files, an
// older client that never takes this lock, or a broken lock implementation.
// The directory-local lock remains the multi-writer guard for an open slot.

// qwpSfResolveLogicalLock returns the logical lock directory and the lock and
// pid paths for slotDir. It fails for a path with no usable parent or name,
// which no configured slot has.
func qwpSfResolveLogicalLock(slotDir string) (dir, lockPath, pidPath string, err error) {
	if slotDir == "" {
		return "", "", "", errors.New("qwp/sf: slotDir must not be empty")
	}
	clean := filepath.Clean(slotDir)
	parent, name := filepath.Split(clean)
	if name == "" || name == "." || name == ".." || parent == "" {
		return "", "", "", fmt.Errorf("qwp/sf: slot path must have a parent and a name [slot=%s]", slotDir)
	}
	dir = filepath.Join(filepath.Clean(parent), qwpSfLogicalLockDirName)
	return dir, filepath.Join(dir, name+qwpSfLockFileName), filepath.Join(dir, name+qwpSfLockPidFileName), nil
}

// qwpSfAcquireLogicalSlotLock takes the transition lock for slotDir's pathname.
// It creates only the shared lock directory and the lock files, never the slot
// itself: a caller must be able to hold the lock while deciding that the slot
// path must not be created at all.
//
// Contention returns qwpSfErrLockBusy, which callers treat like any other
// in-use slot: it is an operational condition, never evidence about the slot's
// bytes, and it must not quarantine anything or write a .failed marker.
func qwpSfAcquireLogicalSlotLock(slotDir string) (*qwpSfSlotLock, error) {
	dir, lockPath, pidPath, err := qwpSfResolveLogicalLock(slotDir)
	if err != nil {
		return nil, err
	}
	// Concurrent senders under one sf_dir race to create this directory;
	// MkdirAll already treats an existing directory as success.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, qwpSfDurabilityError("create logical slot lock directory", dir, err)
	}
	return qwpSfAcquireLockAt(slotDir, lockPath, pidPath)
}

// qwpSfRetainedLogicalLocks keeps a logical lock whose release failed while
// consuming its descriptor. The OS state is then unknown: the lock may or may
// not still be held, and the descriptor cannot be closed again. Keeping the
// value reachable stops a finalizer from reusing it and keeps the failure
// visible; nothing here retries or reclaims it.
var qwpSfRetainedLogicalLocks struct {
	mu    sync.Mutex
	locks []*qwpSfSlotLock
}

// qwpSfTestBeforeLogicalLockReleaseHook injects a failure before the shared
// slot-lock close path is entered. It lets tests fail the parent-anchored lock
// without also failing the directory-local lock released by engine cleanup.
var qwpSfTestBeforeLogicalLockReleaseHook atomic.Pointer[func(*qwpSfSlotLock) error]

// qwpSfReleaseLogicalLock releases lock and classifies the outcome.
//
// A failure that left the descriptor intact keeps the lock held, so the caller
// still owns the exclusion and may retry. A failure that consumed the
// descriptor is terminal: it is joined with ErrCleanupFailed, the lock is
// retained, and no caller may claim either that the lock is still held or that
// the OS released it.
func qwpSfReleaseLogicalLock(lock *qwpSfSlotLock) error {
	if lock == nil {
		return nil
	}
	var err error
	if hook := qwpSfTestBeforeLogicalLockReleaseHook.Load(); hook != nil {
		err = (*hook)(lock)
	}
	if err == nil {
		err = lock.close()
	}
	if err == nil {
		return nil
	}
	if lock.held() {
		// Retryable: this process still owns the flock, so exclusion survives
		// and a later attempt can close it.
		return fmt.Errorf("release logical slot lock [slot=%s]: %w", lock.slotPath(), err)
	}
	qwpSfRetainedLogicalLocks.mu.Lock()
	qwpSfRetainedLogicalLocks.locks = append(qwpSfRetainedLogicalLocks.locks, lock)
	qwpSfRetainedLogicalLocks.mu.Unlock()
	return errors.Join(ErrCleanupFailed,
		fmt.Errorf("qwp/sf: releasing the logical slot lock failed while closing its descriptor, "+
			"so whether the operating system still holds it cannot be established [slot=%s]: %w",
			lock.slotPath(), err))
}

// adoptLogicalLock transfers a still-held logical lock to this engine's cleanup
// owner, reporting whether the engine took it. It refuses a lock whose
// descriptor is already gone, a second lock, and an engine whose cleanup has
// passed its release point — in those cases the caller keeps the obligation.
func (e *qwpSfCursorEngine) adoptLogicalLock(lock *qwpSfSlotLock) bool {
	if e == nil || lock == nil || !lock.held() {
		return false
	}
	e.logicalLockMu.Lock()
	defer e.logicalLockMu.Unlock()
	if e.logicalLockReleased || e.logicalLock != nil {
		return false
	}
	e.logicalLock = lock
	return true
}

// logicalLockHeld reports whether this engine still owns an adopted logical
// lock. A cleanup worker uses it to tell a retryable release failure, where
// exclusion survives, from a finished one.
func (e *qwpSfCursorEngine) logicalLockHeld() bool {
	if e == nil {
		return false
	}
	e.logicalLockMu.Lock()
	defer e.logicalLockMu.Unlock()
	return e.logicalLock.held()
}

// releaseAdoptedLogicalLock releases an adopted logical lock, if any, and
// records that this engine's cleanup reached its release point. A retryable
// failure keeps the lock, so the cleanup worker's next pass tries again; a
// failure that consumed the descriptor is terminal and is reported as such by
// qwpSfReleaseLogicalLock.
func (e *qwpSfCursorEngine) releaseAdoptedLogicalLock() error {
	e.logicalLockMu.Lock()
	lock := e.logicalLock
	if lock == nil {
		e.logicalLockReleased = true
		e.logicalLockMu.Unlock()
		return nil
	}
	e.logicalLockMu.Unlock()
	err := qwpSfReleaseLogicalLock(lock)
	e.logicalLockMu.Lock()
	if !lock.held() {
		e.logicalLock = nil
		e.logicalLockReleased = true
	}
	e.logicalLockMu.Unlock()
	return err
}
