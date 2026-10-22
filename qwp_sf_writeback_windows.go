//go:build windows

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
	"unsafe"

	"golang.org/x/sys/windows"
)

// qwpSfStartWriteback hands the dirty pages of [from, to) of the mapped view
// to the system for writing with FlushViewOfFile, which does not wait for the
// disk. It gives no durability on its own: the rotation's msync and fsync
// (FlushViewOfFile and FlushFileBuffers) still do that.
func qwpSfStartWriteback(_ *os.File, buf []byte, from, to int64) error {
	if buf == nil || to > int64(len(buf)) {
		return fmt.Errorf("qwp/sf: writeback range [%d, %d) exceeds mapping of %d bytes", from, to, len(buf))
	}
	addr := uintptr(unsafe.Pointer(&buf[from]))
	if err := windows.FlushViewOfFile(addr, uintptr(to-from)); err != nil {
		return fmt.Errorf("qwp/sf: FlushViewOfFile: %w", err)
	}
	return nil
}
