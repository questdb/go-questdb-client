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

//go:build unix

package questdb

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// qwpSfMmapRW maps the first sizeBytes of f read-write into a slice
// backed by the kernel mmap region. The returned slice's length and
// capacity equal sizeBytes; indexing into it reads/writes the file
// directly. Caller must qwpSfMunmap before discarding the slice and
// before closing f to avoid leaking mappings.
func qwpSfMmapRW(f *os.File, sizeBytes int64) ([]byte, error) {
	if sizeBytes <= 0 {
		return nil, fmt.Errorf("qwp/sf: mmap size must be positive: %d", sizeBytes)
	}
	buf, err := unix.Mmap(int(f.Fd()), 0, int(sizeBytes), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: mmap %s: %w", f.Name(), err)
	}
	return buf, nil
}

// qwpSfMunmap unmaps buf. Safe to call with a nil buf (no-op).
func qwpSfMunmap(buf []byte) error {
	if buf == nil {
		return nil
	}
	if err := unix.Munmap(buf); err != nil {
		return fmt.Errorf("qwp/sf: munmap: %w", err)
	}
	return nil
}

// qwpSfMsync flushes [0, length) of buf to disk synchronously. The
// length must not exceed cap(buf). Used for OS-crash durability when
// the user opts in; off the steady-state hot path.
func qwpSfMsync(buf []byte, length int64) error {
	if buf == nil || length <= 0 {
		return nil
	}
	if int(length) > cap(buf) {
		return fmt.Errorf("qwp/sf: msync length %d exceeds buf cap %d", length, cap(buf))
	}
	// Slice with the original capacity preserved so unix.Msync can pass
	// the right address+length pair to the kernel; we don't want to
	// reslice arbitrarily because that would change the start offset.
	if err := unix.Msync(buf[:length], unix.MS_SYNC); err != nil {
		return fmt.Errorf("qwp/sf: msync: %w", err)
	}
	return nil
}

// qwpSfFlockExclusive acquires an exclusive non-blocking lock on f.
// Returns qwpSfErrLockBusy on contention with another process. The
// lock is released when f is closed or the process exits (the kernel
// drops flocks on process termination).
func qwpSfFlockExclusive(f *os.File) error {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return qwpSfErrLockBusy
	}
	return fmt.Errorf("qwp/sf: flock %s: %w", f.Name(), err)
}
