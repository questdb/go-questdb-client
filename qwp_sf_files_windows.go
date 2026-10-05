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
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/windows"
)

// On Windows, mmap requires a separate file-mapping object handle
// alongside the file handle. We track them in a side map keyed by the
// mmap'd slice's data pointer so the cross-platform helper signatures
// stay aligned with the unix variant.
var (
	qwpSfWindowsMappingMu sync.Mutex
	qwpSfWindowsMappings  = map[uintptr]*qwpSfWindowsMapping{}
)

type qwpSfWindowsMapping struct {
	mu     sync.Mutex
	handle windows.Handle
}

// Tests can replace these Windows calls to simulate failures. Failed release
// keeps the still-open handle or mapped memory in the mapping record.
type qwpSfWindowsReleaseOps struct {
	closeHandle func(windows.Handle) error
	unmapView   func(uintptr) error
	mapView     func(windows.Handle) (uintptr, error)
}

var qwpSfTestWindowsReleaseOps atomic.Pointer[qwpSfWindowsReleaseOps]

type qwpSfFailedWindowsMapping struct {
	handle  windows.Handle
	address uintptr
}

var qwpSfFailedWindowsHandles struct {
	sync.Mutex
	mappings []qwpSfFailedWindowsMapping
}

func qwpSfRetainWindowsMapping(handle windows.Handle, address uintptr) {
	qwpSfFailedWindowsHandles.Lock()
	qwpSfFailedWindowsHandles.mappings = append(qwpSfFailedWindowsHandles.mappings, qwpSfFailedWindowsMapping{handle: handle, address: address})
	qwpSfFailedWindowsHandles.Unlock()
}

func qwpSfWindowsCloseMapping(handle windows.Handle) error {
	if ops := qwpSfTestWindowsReleaseOps.Load(); ops != nil && ops.closeHandle != nil {
		return ops.closeHandle(handle)
	}
	return windows.CloseHandle(handle)
}
func qwpSfWindowsUnmapView(addr uintptr) error {
	if ops := qwpSfTestWindowsReleaseOps.Load(); ops != nil && ops.unmapView != nil {
		return ops.unmapView(addr)
	}
	return windows.UnmapViewOfFile(addr)
}

// mmapAddrToPointer converts a uintptr returned by MapViewOfFile
// into an unsafe.Pointer addressing the OS-managed mmap region.
//
// Direct `unsafe.Pointer(uintptr_var)` is flagged by go vet's
// unsafeptr analyzer because it cannot tell whether the integer was
// derived from a Go heap pointer (where the GC may relocate the
// referent and invalidate the address). For an OS-managed mmap
// region the warning is a false positive — the kernel pins the
// pages until UnmapViewOfFile. Loading the address through a stack
// alias (&p is a known-valid Go pointer) defeats the analyzer
// without disabling the check globally.
func mmapAddrToPointer(p uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&p))
}

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
	var addr uintptr
	defer func() {
		if r := recover(); r != nil {
			qwpSfRetainWindowsMapping(mapHandle, addr)
			panic(r)
		}
	}()
	if ops := qwpSfTestWindowsReleaseOps.Load(); ops != nil && ops.mapView != nil {
		addr, err = ops.mapView(mapHandle)
	} else {
		addr, err = windows.MapViewOfFile(mapHandle, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, 0, uintptr(sizeBytes))
	}
	if err != nil {
		cause := fmt.Errorf("qwp/sf: MapViewOfFile %s: %w", f.Name(), err)
		return nil, qwpSfFailedAcquisition(cause, &qwpSfAcquiredResources{
			mappingObjects: []*qwpSfMappingObject{{handle: uintptr(mapHandle)}},
		})
	}
	buf := unsafe.Slice((*byte)(mmapAddrToPointer(addr)), sizeBytes)
	qwpSfWindowsMappingMu.Lock()
	qwpSfWindowsMappings[addr] = &qwpSfWindowsMapping{handle: mapHandle}
	qwpSfWindowsMappingMu.Unlock()
	return buf, nil
}

func qwpSfCloseMappingObject(m *qwpSfMappingObject) error {
	if m == nil || m.handle == 0 {
		return nil
	}
	if err := qwpSfWindowsCloseMapping(windows.Handle(m.handle)); err != nil {
		return err
	}
	m.handle = 0
	return nil
}

// qwpSfMunmap unmaps buf and closes its associated file mapping.
func qwpSfMunmap(buf []byte) error {
	if buf == nil {
		return nil
	}
	addr := uintptr(unsafe.Pointer(&buf[0]))
	qwpSfWindowsMappingMu.Lock()
	mapping := qwpSfWindowsMappings[addr]
	qwpSfWindowsMappingMu.Unlock()
	if mapping == nil {
		return fmt.Errorf("qwp/sf: mapping ownership missing for %x", addr)
	}
	mapping.mu.Lock()
	defer mapping.mu.Unlock()
	// Closing the mapping handle does not unmap the memory. Close the handle
	// first so that, if it fails, buf still points to valid memory. Unmapping
	// first could leave a retry using an address now owned by another mapping.
	if mapping.handle != 0 {
		if err := qwpSfWindowsCloseMapping(mapping.handle); err != nil {
			return fmt.Errorf("qwp/sf: CloseHandle(mapping): %w", err)
		}
		mapping.handle = 0
	}
	if err := qwpSfWindowsUnmapView(addr); err != nil {
		return fmt.Errorf("qwp/sf: UnmapViewOfFile: %w", err)
	}
	qwpSfWindowsMappingMu.Lock()
	// The address may already belong to a new mapping. Remove only our record.
	if qwpSfWindowsMappings[addr] == mapping {
		delete(qwpSfWindowsMappings, addr)
	}
	qwpSfWindowsMappingMu.Unlock()
	return nil
}

// qwpSfMsync flushes the mapped dirty pages of [0, length) to the
// filesystem via FlushViewOfFile.
//
// This is weaker than the unix MS_SYNC counterpart: FlushViewOfFile
// writes the modified pages into the filesystem cache but Windows does
// not guarantee they reach the physical disk until FlushFileBuffers is
// called on the underlying file handle. The OS-crash-durability path that
// would need that guarantee is not wired yet (qwpSfSegment.msync has only
// test callers), so the gap is currently latent rather than live.
//
// TODO(durability): when fsync-grade durability is enabled, follow the
// FlushViewOfFile below with windows.FlushFileBuffers(fileHandle) to match
// MS_SYNC. The file handle is not reachable here — this helper takes only
// the mapped buffer — so plumb qwpSfSegment.file (or track the file handle
// alongside the mapping handle in qwpSfWindowsMappings) into the signature
// at that time.
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

// qwpSfReserveNewBlocks reserves real disk clusters for f up to
// currentSize+newBytes via SetFileInformationByHandle(FileAllocationInfo).
// On NTFS this reserves clusters synchronously and fails with
// ERROR_DISK_FULL when free space is insufficient. Caller-side
// contract (never shrinks, short-circuit, post-truncate) is owned by
// qwpSfAllocate; this helper is single-concern.
//
// FileAllocationInfo is file-scope, not range-based — there is no
// per-range API on NTFS — so the call implicitly re-reserves
// [0, currentSize) as well. Visible only to a caller who deliberately
// created sparse holes inside that range; the qwpSfAllocate doc flags
// hole-filling as non-portable behaviour.
//
// FileAllocationInfo does NOT extend the file's logical size (EOF);
// qwpSfAllocate's f.Truncate follow-up handles that. Windows has no
// equivalent of the Linux / macOS sparse-fallback path — any failure
// here surfaces as an error.
func qwpSfReserveNewBlocks(f *os.File, currentSize, newBytes int64) error {
	target := currentSize + newBytes
	// FILE_ALLOCATION_INFO is a single LARGE_INTEGER. Lay it out via a
	// fixed-size struct so the &info / Sizeof pair matches the
	// kernel's expectation regardless of Go alignment quirks.
	info := struct {
		AllocationSize int64
	}{AllocationSize: target}
	err := windows.SetFileInformationByHandle(
		windows.Handle(f.Fd()),
		windows.FileAllocationInfo,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
	if err != nil {
		return fmt.Errorf("qwp/sf: SetFileInformationByHandle(FileAllocationInfo) %s to %d bytes: %w",
			f.Name(), target, err)
	}
	return nil
}

// qwpSfFlockExclusive acquires an exclusive non-blocking lock on f.
// Implemented via LockFileEx with LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY.
// Returns qwpSfErrLockBusy on contention.
func qwpSfFlockExclusive(f *os.File) error {
	const lockBytes uint32 = 1
	// Stack-allocated OVERLAPPED is safe here because LOCKFILE_FAIL_IMMEDIATELY
	// forces a synchronous return — the kernel never dereferences &ol after
	// LockFileEx returns. Do not remove that flag without switching to a
	// heap-allocated OVERLAPPED with an event handle.
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
