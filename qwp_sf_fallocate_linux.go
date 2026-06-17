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

//go:build linux

package questdb

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// qwpSfReserveNewBlocks reserves real disk blocks for f's range
// [currentSize, currentSize+newBytes) via the fallocate(2) syscall
// with mode 0 — the kernel path glibc's posix_fallocate funnels into
// when the filesystem supports it. Caller-side contract (never shrinks,
// short-circuit, post-truncate) is owned by qwpSfAllocate; this helper
// is single-concern.
//
// Anchoring the reservation at currentSize matches macOS's
// F_PEOFPOSMODE so the two POSIX platforms agree on what gets
// reserved (the newly-extended range only); existing sparse holes in
// [0, currentSize) are not touched.
//
// The errno tolerance list (EOPNOTSUPP / ENOTSUP, EINVAL) matches the
// Java reference's posix_fallocate path: those errnos indicate the
// filesystem cannot reserve, and the spec authorises a sparse
// fallback. All other errnos (notably ENOSPC, EFBIG, EIO) surface as
// errors so the caller doesn't end up mmap'ing a sparse file that
// will SIGBUS on first write past the actually-allocated region.
//
// Unlike Java's posix_fallocate (which has glibc's userspace
// zero-write fallback baked in for kernels missing the fallocate
// syscall), this is the raw syscall — ENOSYS on a pre-2.6.23 kernel
// would surface here. Modern targets are unaffected.
func qwpSfReserveNewBlocks(f *os.File, currentSize, newBytes int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, currentSize, newBytes)
	if err == nil {
		return nil
	}
	// EOPNOTSUPP and ENOTSUP share the same numeric value on Linux,
	// but the unix package exposes both names — accept either symbol
	// to stay robust if that ever changes.
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EINVAL) {
		return nil
	}
	return fmt.Errorf("qwp/sf: fallocate %s offset=%d len=%d: %w",
		f.Name(), currentSize, newBytes, err)
}
