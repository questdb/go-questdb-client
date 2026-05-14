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

//go:build unix && !linux && !darwin

package questdb

import "os"

// qwpSfReserveNewBlocks is a no-op on unix variants without a
// block-reservation syscall wired into golang.org/x/sys/unix here
// (BSDs, Solaris, AIX, illumos). qwpSfAllocate's ftruncate step still
// extends the file to the new logical size, so the call returns
// success as if the spec's sparse-fallback path were taken — blocks
// remain sparse, SIGBUS risk per sf-client.md §6 applies. Operators
// on these targets must size sf_max_bytes conservatively against
// free space.
//
// Add a platform-specific implementation here if QuestDB Go ever
// supports one of these targets in production.
func qwpSfReserveNewBlocks(f *os.File, currentSize, newBytes int64) error {
	return nil
}
