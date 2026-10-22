//go:build unix && !linux && !darwin

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

// qwpSfReserveNewBlocks rejects allocation on Unix targets without a native
// reservation implementation here (BSDs, Solaris, AIX, illumos). Extending a
// file without reserving blocks would permit unsafe writes through its mapping.
// Disk-backed SF creation needs a platform-specific reservation implementation;
// memory-backed senders do not use this function.
func qwpSfReserveNewBlocks(f *os.File, currentSize, newBytes int64) error {
	return fmt.Errorf("qwp/sf: block reservation is not implemented on this platform for %s offset=%d len=%d: %w",
		f.Name(), currentSize, newBytes, unix.EOPNOTSUPP)
}
