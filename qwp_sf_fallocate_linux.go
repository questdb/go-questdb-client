//go:build linux

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
// Every reservation failure surfaces, including unsupported-operation
// errors. There is no sparse or zero-write fallback: mmap stores into an
// unallocated range can terminate the process if blocks cannot be allocated.
func qwpSfReserveNewBlocks(f *os.File, currentSize, newBytes int64) error {
	err := qwpSfFallocateFn.load()(int(f.Fd()), 0, currentSize, newBytes)
	if err == nil {
		return nil
	}
	return fmt.Errorf("qwp/sf: fallocate %s offset=%d len=%d: %w",
		f.Name(), currentSize, newBytes, err)
}

// Tests inject errors below the reservation policy, at the syscall boundary.
var qwpSfFallocateFn = qwpSfSwappable(unix.Fallocate)
