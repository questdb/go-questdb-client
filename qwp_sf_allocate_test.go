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
	"os"
	"path/filepath"
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
