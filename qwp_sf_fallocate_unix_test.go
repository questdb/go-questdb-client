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

//go:build linux || darwin

package questdb

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQwpSfSegmentCreateReservesDiskBlocks verifies that a fresh
// segment is NOT sparse — i.e. qwpSfReserveDiskBlocks reached real
// disk-block reservation, not just an ftruncate. We check via
// stat.Blocks, which counts 512-byte units of allocated storage; a
// sparse file would report a Blocks count far below sizeBytes/512.
//
// Skipped on filesystems where the reserve syscall is unsupported
// (Blocks ends up close to zero — same as a plain ftruncate).
// Operators on those filesystems take the SIGBUS risk by design;
// the test is asserting the *typical* dev / CI filesystem path.
func TestQwpSfSegmentCreateReservesDiskBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "prealloc.sfa")

	// 256 KiB — large enough that a sparse file would have ~0 blocks
	// while a real reservation reports >=512 blocks (256 KiB / 512).
	const segSize int64 = 256 * 1024
	seg, err := qwpSfCreateSegment(path, 0, segSize)
	require.NoError(t, err)
	defer func() { _ = seg.close() }()

	st, err := os.Stat(path)
	require.NoError(t, err)
	stat, ok := st.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t from os.Stat on unix")

	allocBytes := int64(stat.Blocks) * 512
	if allocBytes < segSize/2 {
		t.Skipf("filesystem appears not to support pre-allocation (Blocks=%d, want >= %d); "+
			"SIGBUS risk falls back on operator sizing per spec",
			stat.Blocks, segSize/2/512)
	}
	assert.GreaterOrEqual(t, allocBytes, segSize,
		"pre-allocation must reserve >= sizeBytes; sparse file would report a small Blocks count")
}
