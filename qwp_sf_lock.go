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
	"io"
	"os"
	"path/filepath"
	"strings"
)

// qwpSfLockFileName is the per-slot lock file name. One lock file per
// slot directory; held for the engine's lifetime via flock/LockFileEx.
const qwpSfLockFileName = ".lock"

// qwpSfLockPidFileName is the sibling sidecar that carries the
// holder's PID. The PID lives in a separate file because Windows'
// LockFileEx is a mandatory range lock — while .lock is held, a
// second handle cannot read its bytes, so the holder's PID can't be
// recovered from the lock file itself. POSIX flock is advisory and
// would tolerate co-locating the two, but keeping the layout
// identical across platforms (and matching the Java client) avoids
// platform-specific divergence in tests and tooling.
const qwpSfLockPidFileName = ".lock.pid"

// qwpSfSlotLock is an advisory exclusive lock on a single SF slot
// directory. The holder's PID is written to a sibling .lock.pid
// sidecar at acquisition time. A failed acquisition reads it back so
// the error message can name the offending process — turning a vague
// "slot in use" into actionable diagnostics.
//
// Two senders pointing at the same slot dir is the multi-writer
// footgun the slot model exists to prevent: their FSN sequences would
// interleave on disk and corrupt recovery. Detecting the collision at
// acquisition time and refusing to start is the contract — recoverable,
// no data on disk yet, vs. the alternative of silently scrambling the
// slot.
//
// The lock is released automatically on close() OR when the process
// exits (the kernel cleans up flocks for terminated processes).
//
// Known operational hole: if an external actor unlinks .lock while it
// is held (e.g., an operator running `rm .lock`), a fresh acquirer's
// open(O_CREATE) allocates a new inode and successfully flocks it —
// both processes then believe they own the slot. flock(2), POSIX
// fcntl(F_SETLK), and Linux F_OFD_SETLK are all inode-bound on
// Linux/BSD; no POSIX primitive is path-bound, so this cannot be
// closed client-side. Operators must treat .lock as opaque metadata
// and not delete it while a sender is running against the slot. The
// Java MmapSegment SlotLock has the same property.
type qwpSfSlotLock struct {
	slotDir  string
	lockPath string
	file     *os.File
}

// qwpSfAcquireSlotLock creates slotDir if needed, opens
// `<slotDir>/.lock`, and acquires an exclusive flock on it. On
// contention, reads the existing PID payload from the .lock.pid
// sidecar and returns an error naming the offending process.
func qwpSfAcquireSlotLock(slotDir string) (*qwpSfSlotLock, error) {
	if slotDir == "" {
		return nil, errors.New("qwp/sf: slotDir must not be empty")
	}
	if err := os.MkdirAll(slotDir, 0o755); err != nil {
		return nil, fmt.Errorf("qwp/sf: could not create slot dir %s: %w", slotDir, err)
	}
	lockPath := filepath.Join(slotDir, qwpSfLockFileName)
	pidPath := filepath.Join(slotDir, qwpSfLockPidFileName)
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: could not open slot lock file %s: %w", lockPath, err)
	}
	if err := qwpSfFlockExclusive(f); err != nil {
		holder := qwpSfReadHolder(pidPath)
		_ = f.Close()
		if errors.Is(err, qwpSfErrLockBusy) {
			return nil, fmt.Errorf(
				"qwp/sf: slot already in use by another process [slot=%s, holder=%s]",
				slotDir, holder)
		}
		return nil, err
	}
	qwpSfWritePid(pidPath)
	return &qwpSfSlotLock{
		slotDir:  slotDir,
		lockPath: lockPath,
		file:     f,
	}, nil
}

// qwpSfReadHolder reads the PID payload of an existing .lock.pid
// sidecar. Best-effort — returns "unknown" if the file can't be read
// or the payload is empty. The caller is in the error path; we never
// want a failed PID-read to mask the original lock-busy error.
func qwpSfReadHolder(pidPath string) string {
	f, err := os.Open(pidPath)
	if err != nil {
		return "unknown"
	}
	defer f.Close()
	// 64 bytes is more than enough for "<pid>\n" — clamp so a vandal
	// can't make us read MB of payload on the error path.
	buf := make([]byte, 64)
	n, err := f.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return "unknown"
	}
	if n <= 0 {
		return "unknown"
	}
	return "pid=" + strings.TrimSpace(string(buf[:n]))
}

// qwpSfWritePid writes the current process's PID to the .lock.pid
// sidecar. Diagnostic-only — never block lock acquisition on it; a
// failed write only degrades the contention error message, it does
// not affect correctness of the lock itself.
func qwpSfWritePid(pidPath string) {
	f, err := os.OpenFile(pidPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	payload := fmt.Sprintf("%d\n", os.Getpid())
	_, _ = f.WriteAt([]byte(payload), 0)
}

// slotPath returns the slot directory this lock guards.
func (l *qwpSfSlotLock) slotPath() string {
	return l.slotDir
}

// close releases the lock by closing the underlying file. We do NOT
// remove the file — a stale .lock with the previous PID is harmless
// (the next acquirer can flock it just fine, and overwrites the PID
// on success). Idempotent.
func (l *qwpSfSlotLock) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
