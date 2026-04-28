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

//go:build windows

package questdb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// On Windows, mmap requires a separate file-mapping object handle
// alongside the file handle. We track them in a side map keyed by the
// mmap'd slice's data pointer so the cross-platform helper signatures
// stay aligned with the unix variant.
var (
	qwpSfWindowsMappingMu sync.Mutex
	qwpSfWindowsMappings  = map[uintptr]windows.Handle{}
)

// qwpSfMmapRW maps the first sizeBytes of f read-write. See the unix
// counterpart; this version creates a CreateFileMapping+MapViewOfFile
// pair under the hood and tracks the mapping handle for later cleanup.
func qwpSfMmapRW(f *os.File, sizeBytes int64) ([]byte, error) {
	if sizeBytes <= 0 {
		return nil, fmt.Errorf("qwp/sf: mmap size must be positive: %d", sizeBytes)
	}
	hi := uint32(sizeBytes >> 32)
	lo := uint32(sizeBytes & 0xFFFFFFFF)
	mapHandle, err := windows.CreateFileMapping(
		windows.Handle(f.Fd()), nil, windows.PAGE_READWRITE, hi, lo, nil)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: CreateFileMapping %s: %w", f.Name(), err)
	}
	addr, err := windows.MapViewOfFile(mapHandle, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0, 0, uintptr(sizeBytes))
	if err != nil {
		_ = windows.CloseHandle(mapHandle)
		return nil, fmt.Errorf("qwp/sf: MapViewOfFile %s: %w", f.Name(), err)
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(addr)), sizeBytes)
	qwpSfWindowsMappingMu.Lock()
	qwpSfWindowsMappings[addr] = mapHandle
	qwpSfWindowsMappingMu.Unlock()
	return buf, nil
}

// qwpSfMunmap unmaps buf and closes its associated file mapping.
func qwpSfMunmap(buf []byte) error {
	if buf == nil {
		return nil
	}
	addr := uintptr(unsafe.Pointer(&buf[0]))
	qwpSfWindowsMappingMu.Lock()
	mapHandle, ok := qwpSfWindowsMappings[addr]
	if ok {
		delete(qwpSfWindowsMappings, addr)
	}
	qwpSfWindowsMappingMu.Unlock()
	if err := windows.UnmapViewOfFile(addr); err != nil {
		return fmt.Errorf("qwp/sf: UnmapViewOfFile: %w", err)
	}
	if ok {
		if err := windows.CloseHandle(mapHandle); err != nil {
			return fmt.Errorf("qwp/sf: CloseHandle(mapping): %w", err)
		}
	}
	return nil
}

// qwpSfMsync synchronously flushes [0, length) of buf to disk.
func qwpSfMsync(buf []byte, length int64) error {
	if buf == nil || length <= 0 {
		return nil
	}
	if int(length) > cap(buf) {
		return fmt.Errorf("qwp/sf: msync length %d exceeds buf cap %d", length, cap(buf))
	}
	addr := uintptr(unsafe.Pointer(&buf[0]))
	if err := windows.FlushViewOfFile(addr, uintptr(length)); err != nil {
		return fmt.Errorf("qwp/sf: FlushViewOfFile: %w", err)
	}
	return nil
}

// qwpSfFlockExclusive acquires an exclusive non-blocking lock on f.
// Implemented via LockFileEx with LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY.
// Returns qwpSfErrLockBusy on contention.
func qwpSfFlockExclusive(f *os.File) error {
	const lockBytes uint32 = 1
	var ol windows.Overlapped
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, lockBytes, 0, &ol)
	if err == nil {
		return nil
	}
	// ERROR_LOCK_VIOLATION = 33; ERROR_IO_PENDING = 997 (treated as
	// contention by LOCKFILE_FAIL_IMMEDIATELY).
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
		return qwpSfErrLockBusy
	}
	return fmt.Errorf("qwp/sf: LockFileEx %s: %w", f.Name(), err)
}
