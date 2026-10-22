//go:build unix && !linux

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

// qwpSfStartWriteback schedules the dirty pages of [from, to) of the mapping
// for writing with msync(MS_ASYNC) and returns without waiting. It gives no
// durability on its own: the rotation's msync(MS_SYNC) and fsync still do
// that, and find these pages already written or in flight. msync needs a
// page-aligned start, so the range is widened down to a page boundary.
func qwpSfStartWriteback(_ *os.File, buf []byte, from, to int64) error {
	if buf == nil || to > int64(len(buf)) {
		return fmt.Errorf("qwp/sf: writeback range [%d, %d) exceeds mapping of %d bytes", from, to, len(buf))
	}
	from &^= int64(os.Getpagesize() - 1)
	if err := unix.Msync(buf[from:to], unix.MS_ASYNC); err != nil {
		return fmt.Errorf("qwp/sf: msync(MS_ASYNC): %w", err)
	}
	return nil
}
