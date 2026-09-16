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

// qwpSfReserveNewBlocksFn is the indirection qwpSfAllocate calls to
// reserve real disk blocks. In production it points at the
// platform-specific qwpSfReserveNewBlocks; tests swap it to fault-inject
// a reservation failure (e.g. ENOSPC) without having to actually fill a
// filesystem, then restore the original in a t.Cleanup. Mirrors the Java
// client's FilesFacade seam, where ENOSPC at allocate is fault-injected
// through a test facade (see MmapSegment.create's facade overload).
var qwpSfReserveNewBlocksFn = qwpSfSwappable(qwpSfReserveNewBlocks)

// qwpSfAllocate extends f to at least size bytes and reserves real
// disk blocks for the newly-extended range. Unlike Java's Files.allocate,
// unsupported native reservation is an error, not a sparse-file fallback.
// Unix targets without a reservation implementation reject file growth.
//
// Contract:
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
//  3. All reservation errors surface as wrapped errors, including unsupported
//     operations, ENOSPC, EFBIG, EIO (POSIX) or ERROR_DISK_FULL (Windows).
//     The caller is responsible for closing the fd and cleaning up a partial
//     newly-created file. No truncate or zero-write fallback is attempted.
//
// Success relies on the filesystem's reservation guarantees. It does not
// certify existing sparse ranges or prevent faults caused by later storage
// failures, external truncation, or copy-on-write allocation.
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
	if err := qwpSfReserveNewBlocksFn.load()(f, currentSize, newBytes); err != nil {
		return err
	}
	// Unified EOF advancement. On Linux when fallocate succeeded the
	// file is already at target and this is a no-op; on macOS / Windows
	// it is the call that grows the file. Never shrinks because target is greater
	// than currentSize here (the short-circuit above covered equal).
	if err := f.Truncate(target); err != nil {
		return fmt.Errorf("qwp/sf: truncate %s to %d bytes: %w", f.Name(), target, err)
	}
	return nil
}
