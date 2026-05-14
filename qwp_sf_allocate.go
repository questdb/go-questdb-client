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
)

// qwpSfAllocate extends f to at least size bytes and reserves real
// disk blocks for the newly-extended range. Mirrors the Java client's
// Files.allocate contract (see java-questdb-client core/src/main/java
// /io/questdb/client/std/Files.java#allocate) so the two implementations
// agree on what an `allocate(fd, size)` call observably does.
//
// Cross-platform contract — identical observable behaviour on Linux,
// macOS, Windows, and the "other unix" stub for any caller that does
// not deliberately produce sparse files:
//
//  1. Never shrinks. Let currentSize be f's current logical size and
//     target = max(size, currentSize). Requests where
//     size <= currentSize short-circuit as a no-op success — f is
//     left exactly as it was, no syscall reaches the kernel.
//  2. Reserves blocks for [currentSize, target). Pre-existing sparse
//     holes inside [0, currentSize) are not retroactively filled
//     (Linux and macOS anchor the reservation at currentSize; Windows'
//     FileAllocationInfo is file-scope and will re-reserve the
//     existing range too, but a caller relying on hole-filling is
//     writing non-portable code).
//  3. Real errors surface as a wrapped error — notably ENOSPC, EFBIG,
//     EIO (POSIX) or ERROR_DISK_FULL (Windows). The caller is
//     responsible for closing the fd and unlinking the partial file.
//  4. Sparse fallback (Linux / macOS only). When the reservation
//     primitive itself reports the filesystem doesn't support it
//     (EOPNOTSUPP / EINVAL on Linux; EOPNOTSUPP / ENOTSUP on macOS),
//     the call still extends the logical size via ftruncate but
//     leaves blocks sparse — the SIGBUS risk re-emerges for that
//     filesystem only. Windows has no equivalent fallback; any
//     failure is fatal.
//
// Implementation split: this function owns the cross-platform
// invariants (fstat, target computation, short-circuit, post-reserve
// ftruncate). The platform-specific qwpSfReserveNewBlocks owns the
// single concern of "reserve real disk blocks for [currentSize,
// currentSize+newBytes)" on its OS.
func qwpSfAllocate(f *os.File, size int64) error {
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("qwp/sf: stat %s: %w", f.Name(), err)
	}
	currentSize := st.Size()
	target := size
	if currentSize > target {
		target = currentSize
	}
	if target == currentSize {
		// Never-shrinks short-circuit: nothing to extend, nothing to
		// reserve. Returning here is what makes the property hold —
		// without it the ftruncate below would shrink files when
		// size < currentSize.
		return nil
	}
	newBytes := target - currentSize
	if err := qwpSfReserveNewBlocks(f, currentSize, newBytes); err != nil {
		return err
	}
	// Unified EOF advancement. On Linux when fallocate succeeded the
	// file is already at target and this is a no-op; on the Linux
	// sparse-fallback path and on macOS / Windows it is the call that
	// grows the file. Never shrinks because target > currentSize by
	// the time we reach here (the short-circuit above covered equal).
	if err := f.Truncate(target); err != nil {
		return fmt.Errorf("qwp/sf: truncate %s to %d bytes: %w", f.Name(), target, err)
	}
	return nil
}
