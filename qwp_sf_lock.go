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

// qwpSfSlotLock is an advisory exclusive lock on a single SF slot
// directory. The lock file's payload is the holder's PID, written at
// acquisition time. A failed acquisition reads it back so the error
// message can name the offending process — turning a vague "slot in
// use" into actionable diagnostics.
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
type qwpSfSlotLock struct {
	slotDir  string
	lockPath string
	file     *os.File
}

// qwpSfAcquireSlotLock creates slotDir if needed, opens
// `<slotDir>/.lock`, and acquires an exclusive flock on it. On
// contention, reads the existing PID payload and returns an error
// naming the offending process.
func qwpSfAcquireSlotLock(slotDir string) (*qwpSfSlotLock, error) {
	if slotDir == "" {
		return nil, errors.New("qwp/sf: slotDir must not be empty")
	}
	if err := os.MkdirAll(slotDir, 0o755); err != nil {
		return nil, fmt.Errorf("qwp/sf: could not create slot dir %s: %w", slotDir, err)
	}
	lockPath := filepath.Join(slotDir, qwpSfLockFileName)
	// O_RDWR | O_CREATE — never O_TRUNC; another process's PID
	// payload is read on contention to surface a useful error.
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: could not open slot lock file %s: %w", lockPath, err)
	}
	if err := qwpSfFlockExclusive(f); err != nil {
		holder := qwpSfReadHolder(lockPath)
		_ = f.Close()
		if errors.Is(err, qwpSfErrLockBusy) {
			return nil, fmt.Errorf(
				"qwp/sf: slot already in use by another process [slot=%s, holder=%s]",
				slotDir, holder)
		}
		return nil, err
	}
	if err := qwpSfWritePid(f); err != nil {
		// We hold the lock; releasing on the way out is safe — closing
		// the fd drops the flock per kernel semantics.
		_ = f.Close()
		return nil, err
	}
	return &qwpSfSlotLock{
		slotDir:  slotDir,
		lockPath: lockPath,
		file:     f,
	}, nil
}

// qwpSfReadHolder reads the PID payload of an existing lock file.
// Best-effort — returns "unknown" if the file can't be read or the
// payload is empty. The caller is in the error path; we never want a
// failed PID-read to mask the original lock-busy error.
func qwpSfReadHolder(lockPath string) string {
	f, err := os.Open(lockPath)
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

// qwpSfWritePid truncates the lock file and writes the current
// process's PID followed by a newline.
func qwpSfWritePid(f *os.File) error {
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("qwp/sf: truncate lock file: %w", err)
	}
	pid := os.Getpid()
	payload := fmt.Sprintf("%d\n", pid)
	if _, err := f.WriteAt([]byte(payload), 0); err != nil {
		return fmt.Errorf("qwp/sf: write pid: %w", err)
	}
	return nil
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
