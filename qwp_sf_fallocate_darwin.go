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

//go:build darwin

package questdb

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// qwpSfReserveNewBlocks reserves real disk blocks for f's range
// [currentSize, currentSize+newBytes) via fcntl(F_PREALLOCATE), in
// two phases — matching the Java reference's native allocate on macOS:
//
//  1. F_ALLOCATECONTIG | F_ALLOCATEALL: try for a single contiguous
//     extent first. Best for mmap streaming and least fragmentation,
//     but can fail on a fragmented APFS even when free space is
//     plentiful.
//  2. On any failure, retry with just F_ALLOCATEALL (relaxed
//     contiguity, still all-or-nothing). This is the path that
//     surfaces ENOSPC.
//  3. Only when the second attempt fails with ENOTSUP / EOPNOTSUPP do
//     we accept a sparse fallback — those errnos indicate the
//     filesystem doesn't implement F_PREALLOCATE at all (SMB,
//     certain network mounts). Every other failure (notably ENOSPC,
//     EFBIG, EIO) surfaces so the caller doesn't end up mmap'ing a
//     sparse file that will SIGBUS on first write past the
//     actually-allocated region.
//
// F_PEOFPOSMODE positions the allocation immediately after EOF, so
// the caller MUST ensure f's EOF is at currentSize before invoking
// this. qwpSfAllocate guarantees that by fstat'ing first; direct
// callers must do the same. F_PREALLOCATE does NOT advance EOF —
// qwpSfAllocate's ftruncate follow-up handles that.
//
// The currentSize parameter isn't needed by F_PREALLOCATE itself
// (F_PEOFPOSMODE is implicit-from-EOF), but it's kept on the
// signature for cross-platform symmetry and surfaces in error
// messages.
func qwpSfReserveNewBlocks(f *os.File, currentSize, newBytes int64) error {
	fstore := &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  newBytes,
	}
	if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, fstore); err == nil {
		return nil
	}
	// Contiguous allocation failed (typically fragmented APFS). Retry
	// non-contiguous all-or-nothing — this is where ENOSPC surfaces if
	// free space is genuinely insufficient.
	fstore.Flags = unix.F_ALLOCATEALL
	fstore.Bytesalloc = 0
	err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, fstore)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
		return nil
	}
	return fmt.Errorf("qwp/sf: F_PREALLOCATE %s offset=%d len=%d: %w",
		f.Name(), currentSize, newBytes, err)
}
