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
	"bufio"
	"bytes"
	"os"
	"strconv"
)

// qwpBenchStorageWriteBytes returns how many bytes this process has caused to
// be sent to storage, from /proc/self/io: write_bytes minus
// cancelled_write_bytes. The kernel charges a page when it is dirtied and
// credits it back if the page is dropped unwritten, for example when its file
// is deleted first, so the difference is what reaches the disk.
func qwpBenchStorageWriteBytes() (int64, bool) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, false
	}
	var written, cancelled int64
	var sawWritten, sawCancelled bool
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		key, value, ok := bytes.Cut(sc.Bytes(), []byte(": "))
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			continue
		}
		switch string(key) {
		case "write_bytes":
			written, sawWritten = n, true
		case "cancelled_write_bytes":
			cancelled, sawCancelled = n, true
		}
	}
	return written - cancelled, sawWritten && sawCancelled
}
