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

// qwpSfStartWriteback starts writing back the dirty pages of [from, to) in f
// and returns without waiting, using sync_file_range(SYNC_FILE_RANGE_WRITE).
// It commits no metadata and gives no durability on its own: the rotation's
// msync and fsync still do that, and find these pages already written or in
// flight. Pages dirtied through the shared mapping belong to the same page
// cache, so the range covers them.
func qwpSfStartWriteback(f *os.File, _ []byte, from, to int64) error {
	if err := unix.SyncFileRange(int(f.Fd()), from, to-from, unix.SYNC_FILE_RANGE_WRITE); err != nil {
		return fmt.Errorf("qwp/sf: sync_file_range: %w", err)
	}
	return nil
}
