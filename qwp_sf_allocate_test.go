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
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQwpSfAllocateNeverShrinks pins the cross-platform contract
// documented on qwpSfAllocate: never shrinks, short-circuits on
// size <= currentSize, extends on size > currentSize. Mirrors the
// Java client's testAllocateNeverShrinks (FilesTest) so the two
// implementations stay in lockstep.
func TestQwpSfAllocateNeverShrinks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "allocate-shrink.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	requireSize := func(want int64) {
		t.Helper()
		st, err := f.Stat()
		require.NoError(t, err)
		assert.Equal(t, want, st.Size())
	}

	// Grow to 64 KiB.
	require.NoError(t, qwpSfAllocate(f, 64*1024))
	requireSize(64 * 1024)

	// Smaller request: must not shrink the file.
	require.NoError(t, qwpSfAllocate(f, 4096))
	requireSize(64 * 1024)

	// Equal request: no-op success, size unchanged.
	require.NoError(t, qwpSfAllocate(f, 64*1024))
	requireSize(64 * 1024)

	// Larger request: extends to the new target.
	require.NoError(t, qwpSfAllocate(f, 128*1024))
	requireSize(128 * 1024)
}

// TestQwpSfAllocateZeroOnFreshFile exercises the no-op short-circuit
// on a brand-new (size=0) file — no reservation syscall should reach
// the kernel, the file stays at size 0.
func TestQwpSfAllocateZeroOnFreshFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "allocate-zero.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	require.NoError(t, qwpSfAllocate(f, 0))
	st, err := f.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(0), st.Size())
}

// withInjectedReserveFailure swaps the block-reservation primitive for
// one that always fails with a wrapped ENOSPC, and restores the original
// on cleanup. Lets the durability-layer ENOSPC tests run without having
// to actually fill a filesystem. Tests run sequentially within a package
// so the package-level swap is race-free.
func withInjectedReserveFailure(t *testing.T) {
	t.Helper()
	orig := qwpSfReserveNewBlocksFn
	t.Cleanup(func() { qwpSfReserveNewBlocksFn = orig })
	qwpSfReserveNewBlocksFn = func(_ *os.File, _, _ int64) error {
		return fmt.Errorf("qwp/sf: fallocate fault-injected: %w", syscall.ENOSPC)
	}
}

// TestQwpSfAllocateSurfacesReserveFailure pins item 3 of qwpSfAllocate's
// cross-platform contract: a real reservation failure (ENOSPC, EFBIG,
// EIO) surfaces as an error and the file is NOT extended. There is no
// silent sparse fallback for those errnos — that path is reserved for
// "filesystem cannot reserve" (EOPNOTSUPP/EINVAL), which the platform
// helper absorbs internally. A sparse extension here would defer ENOSPC
// to an mmap-store SIGBUS that tears down the whole process.
func TestQwpSfAllocateSurfacesReserveFailure(t *testing.T) {
	withInjectedReserveFailure(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "enospc.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	err = qwpSfAllocate(f, 64*1024)
	require.Error(t, err, "reserve failure must surface, not silently fall back to sparse")
	assert.ErrorIs(t, err, syscall.ENOSPC)

	// The post-reserve ftruncate that advances EOF is only reached on
	// reservation success, so a failed reservation must leave the file at
	// its pre-call size (0). That is exactly what prevents a
	// logically-sized-but-sparse mapping.
	st, statErr := f.Stat()
	require.NoError(t, statErr)
	assert.Equal(t, int64(0), st.Size(),
		"a failed reservation must not extend the file (no sparse mapping)")
}

// TestQwpSfCreateSegmentRemovesPartialFileOnReserveFailure pins the
// create-path cleanup contract: when pre-allocation fails (ENOSPC),
// qwpSfCreateSegment returns the error AND unlinks the partially-created
// file, so a sustained disk-full burst with the segment manager polling
// does not litter the slot directory with full-size empty .sfa files.
// Mirrors the Java MmapSegment.create() ff.remove() on allocate failure.
func TestQwpSfCreateSegmentRemovesPartialFileOnReserveFailure(t *testing.T) {
	withInjectedReserveFailure(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "sf-initial.sfa")

	seg, err := qwpSfCreateSegment(path, 0, 256*1024)
	require.Error(t, err, "create must fail when pre-allocation fails")
	assert.Nil(t, seg)
	assert.ErrorIs(t, err, syscall.ENOSPC)

	_, statErr := os.Stat(path)
	assert.Truef(t, os.IsNotExist(statErr),
		"the partially-created segment file must be unlinked on pre-allocation "+
			"failure; stat err = %v", statErr)
}
