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
	"os"

	"golang.org/x/sys/unix"
)

// qwpSfFsync flushes f's data and metadata to stable storage with fsync(2) —
// the same call the Java client's native Files.fsync makes (see
// core/src/main/c/share/files.c), so both clients give the same guarantee for
// the segment and manifest formats they share on disk.
//
// os.File.Sync is not that call on darwin: it issues fcntl(F_FULLFSYNC), which
// additionally asks the drive to empty its own write cache. That is two orders
// of magnitude more expensive — 4.6ms against 27µs, measured on an APFS SSD —
// and segment rotation performs two such flushes on the producer's goroutine
// while it holds appendMu, so the difference lands straight on the caller's
// flush latency. How often depends on row width: roughly one flush in a
// hundred for narrow rows, and every flush once a batch fills a segment on its
// own, which a few hundred columns will do.
//
// What the weaker call gives up is a power cut between the fsync and the drive
// destaging its cache. Store-and-forward is a client-side buffer whose promise
// is that a crashed or restarted process finds its unsent frames again, and
// fsync(2) covers that: the bytes are with the kernel and outlive the process.
func qwpSfFsync(f *os.File) error {
	for {
		err := unix.Fsync(int(f.Fd()))
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return &os.PathError{Op: "fsync", Path: f.Name(), Err: err}
		}
		return nil
	}
}
