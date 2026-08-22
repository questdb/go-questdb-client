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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	qwpSfManifestFileName        = "sf-manifest.bin"
	qwpSfManifestMagic    uint32 = 0x314d4653

	qwpSfDualRecordVersion   uint32 = 1
	qwpSfDualRecordSize             = 64
	qwpSfDualRecordCRCOffset        = 60
	qwpSfDualRecordSlotSize  int64  = 4096
	qwpSfDualRecordFileSize  int64  = 8192
)

// qwpSfManifestSync is a test seam for manifest durability failures. A nil
// pointer means production qwpSfFsync. Tests publish a temporary hook
// atomically because manifest updates also run on the live manager worker.
var qwpSfManifestSync atomic.Pointer[func(f *os.File) error]

func qwpSfManifestSyncFile(f *os.File) error {
	if hook := qwpSfManifestSync.Load(); hook != nil {
		return (*hook)(f)
	}
	return qwpSfFsync(f)
}

type qwpSfDualRecord struct {
	generation int64
	first      int64
	second     int64
}

func qwpSfEncodeDualRecord(dst []byte, magic uint32, generation, first, second int64) {
	clear(dst[:qwpSfDualRecordSize])
	binary.LittleEndian.PutUint32(dst[0:4], magic)
	binary.LittleEndian.PutUint32(dst[4:8], qwpSfDualRecordVersion)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(generation))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(first))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(second))
	crc := crc32.Checksum(dst[:qwpSfDualRecordCRCOffset], qwpSfCrcTable)
	binary.LittleEndian.PutUint32(dst[qwpSfDualRecordCRCOffset:qwpSfDualRecordSize], crc)
}

func qwpSfDecodeDualRecord(src []byte, magic uint32, validPayload func(qwpSfDualRecord) bool) (qwpSfDualRecord, bool) {
	if len(src) < qwpSfDualRecordSize ||
		binary.LittleEndian.Uint32(src[0:4]) != magic ||
		binary.LittleEndian.Uint32(src[4:8]) != qwpSfDualRecordVersion ||
		binary.LittleEndian.Uint32(src[qwpSfDualRecordCRCOffset:qwpSfDualRecordSize]) != crc32.Checksum(src[:qwpSfDualRecordCRCOffset], qwpSfCrcTable) {
		return qwpSfDualRecord{}, false
	}
	rec := qwpSfDualRecord{
		generation: int64(binary.LittleEndian.Uint64(src[8:16])),
		first:      int64(binary.LittleEndian.Uint64(src[16:24])),
		second:     int64(binary.LittleEndian.Uint64(src[24:32])),
	}
	if rec.generation <= 0 || !validPayload(rec) {
		return qwpSfDualRecord{}, false
	}
	return rec, true
}

func qwpSfSelectDualRecord(slot0 qwpSfDualRecord, valid0 bool, slot1 qwpSfDualRecord, valid1 bool) (qwpSfDualRecord, bool) {
	if !valid0 {
		return slot1, valid1
	}
	if !valid1 || slot0.generation > slot1.generation {
		return slot0, true
	}
	return slot1, true
}

type qwpSfManifest struct {
	mu         sync.Mutex
	file       *os.File
	generation int64
	headBase   int64
	activeBase int64
	closed     bool
	scratch    [qwpSfDualRecordSize]byte
}

func qwpSfManifestCreate(dir string, headBase, activeBase int64) (*qwpSfManifest, error) {
	if headBase < 0 || activeBase < headBase {
		return nil, fmt.Errorf("qwp/sf: invalid manifest boundaries: head=%d active=%d", headBase, activeBase)
	}
	path := filepath.Join(dir, qwpSfManifestFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: create manifest %s: %w", path, err)
	}
	m := &qwpSfManifest{file: f, headBase: -1, activeBase: -1}
	ok := false
	defer func() {
		if !ok {
			_ = m.close()
			_ = os.Remove(path)
		}
	}()
	if err := qwpSfAllocate(f, qwpSfDualRecordFileSize); err != nil {
		return nil, err
	}
	if err := m.update(headBase, activeBase); err != nil {
		return nil, err
	}
	if err := qwpSfSyncSlotDir(dir); err != nil {
		return nil, fmt.Errorf("qwp/sf: fsync manifest directory %s: %w", dir, err)
	}
	ok = true
	return m, nil
}

func qwpSfManifestOpen(dir string) (*qwpSfManifest, error) {
	path := filepath.Join(dir, qwpSfManifestFileName)
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: stat manifest %s: %w", path, err)
	}
	if st.Size() != qwpSfDualRecordFileSize {
		if err := qwpSfQuarantineCreationDebris(path); err != nil {
			return nil, err
		}
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: open manifest %s: %w", path, err)
	}
	var raw [2][qwpSfDualRecordSize]byte
	if _, err := io.ReadFull(io.NewSectionReader(f, 0, qwpSfDualRecordSize), raw[0][:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: read manifest slot 0: %w", err)
	}
	if _, err := io.ReadFull(io.NewSectionReader(f, qwpSfDualRecordSlotSize, qwpSfDualRecordSize), raw[1][:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: read manifest slot 1: %w", err)
	}
	valid := func(rec qwpSfDualRecord) bool { return rec.first >= 0 && rec.second >= rec.first }
	r0, ok0 := qwpSfDecodeDualRecord(raw[0][:], qwpSfManifestMagic, valid)
	r1, ok1 := qwpSfDecodeDualRecord(raw[1][:], qwpSfManifestMagic, valid)
	rec, ok := qwpSfSelectDualRecord(r0, ok0, r1, ok1)
	if !ok {
		_ = f.Close()
		if err := qwpSfQuarantineCreationDebris(path); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return &qwpSfManifest{file: f, generation: rec.generation, headBase: rec.first, activeBase: rec.second}, nil
}

func (m *qwpSfManifest) update(newHead, newActive int64) error {
	if m == nil {
		return errors.New("qwp/sf: manifest is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("qwp/sf: manifest is closed")
	}
	if m.generation > 0 {
		if newHead < m.headBase {
			newHead = m.headBase
		}
		if newActive < m.activeBase {
			newActive = m.activeBase
		}
	}
	if newHead < 0 || newActive < newHead {
		return fmt.Errorf("qwp/sf: invalid manifest boundaries: head=%d active=%d", newHead, newActive)
	}
	if m.generation > 0 && newHead == m.headBase && newActive == m.activeBase {
		return nil
	}
	if m.generation == math.MaxInt64 {
		return errors.New("qwp/sf: manifest generation overflow")
	}
	next := m.generation + 1
	qwpSfEncodeDualRecord(m.scratch[:], qwpSfManifestMagic, next, newHead, newActive)
	offset := (next & 1) * qwpSfDualRecordSlotSize
	if n, err := m.file.WriteAt(m.scratch[:], offset); err != nil || n != len(m.scratch) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("qwp/sf: write manifest generation %d: %w", next, err)
	}
	if err := qwpSfManifestSyncFile(m.file); err != nil {
		return fmt.Errorf("qwp/sf: fsync manifest: %w", err)
	}
	m.generation = next
	m.headBase = newHead
	m.activeBase = newActive
	return nil
}

func (m *qwpSfManifest) close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

func qwpSfManifestRemove(dir string) bool {
	err := os.Remove(filepath.Join(dir, qwpSfManifestFileName))
	return err == nil || errors.Is(err, os.ErrNotExist)
}

// qwpSfManifestQuarantineRename is the rename qwpSfQuarantineCreationDebris
// uses. Production holds os.Rename; tests replace it to reach the failure exit,
// which no real filesystem can be talked into on demand.
var qwpSfManifestQuarantineRename = qwpSfSwappable(os.Rename)

// qwpSfQuarantineCreationDebris sets an unusable manifest aside under a
// .corrupt name. It runs from qwpSfManifestOpen, before recovery has decided
// whether the slot as a whole fails closed, so it must not destroy anything: a
// slot that is about to be preserved whole would otherwise arrive at the
// quarantine directory already missing the boundary record that explains it.
// The target name is probed the same way segment quarantine probes it, so an
// earlier quarantine's evidence survives.
//
// A rename that cannot succeed is neither ignored nor escalated to a delete.
// Deleting is never the way forward: it costs the boundary record, and it
// would not even unblock the slot, because every segment this client writes
// carries the manifest-required flag -- a slot whose manifest is gone fails
// closed rather than falling back to the legacy path.
//
// What the failure is reported as depends on whether the fault says anything
// about the slot. A permanent one -- a read-only mount, a name that cannot be
// formed -- means no later attempt will do better, so it is fail-closed: the
// caller preserves the whole slot under <sf_dir>/quarantined/ and starts a
// fresh one, which is a rename in the PARENT directory and so is not blocked
// by whatever is wrong with this one. Returning a plain error there strands the
// sender for good, because the recovery policy only quarantines on
// qwpSfErrRecoveryFailClosed and every later construction fails identically.
//
// A transient one -- a full disk, an exhausted fd table, an I/O error -- says
// nothing about the bytes, and fail-closed is far too strong for it: a
// foreground sender would move a legacy slot's undelivered rows into
// quarantined/, where nothing scans for them again, and a drainer would write
// the permanent .failed sentinel that disqualifies the slot from every later
// adoption. Those faults are reported as an ordinary error, so the next
// attempt retries once the disk or the fd table recovers.
//
// Either way the cause stays reachable with errors.Is.
func qwpSfQuarantineCreationDebris(path string) error {
	corrupt, err := qwpSfQuarantineTargetPath(path)
	if err != nil {
		return qwpSfManifestQuarantineError("could not choose a quarantine name for invalid "+path, err)
	}
	if err := qwpSfManifestQuarantineRename.load()(path, corrupt); err != nil && !errors.Is(err, os.ErrNotExist) {
		return qwpSfManifestQuarantineError("could not quarantine invalid "+path, err)
	}
	return nil
}

// qwpSfManifestQuarantineError classifies a failure to set the manifest aside.
// Only a fault that no retry can clear justifies condemning the slot.
func qwpSfManifestQuarantineError(what string, cause error) error {
	if qwpSfIsTransientFileFault(cause) {
		return fmt.Errorf("qwp/sf: %s: %w", what, cause)
	}
	return fmt.Errorf("%w: %s: %w", qwpSfErrRecoveryFailClosed, what, cause)
}

// qwpSfIsTransientFileFault reports whether a filesystem error is one a later
// attempt could get past. Anything unrecognised counts as transient: treating
// an unknown fault as proof that the slot is inconsistent is the expensive
// mistake, since that verdict is permanent.
func qwpSfIsTransientFileFault(err error) bool {
	switch {
	case errors.Is(err, syscall.EROFS),
		errors.Is(err, syscall.ENAMETOOLONG),
		errors.Is(err, syscall.EACCES),
		errors.Is(err, syscall.EPERM):
		return false
	}
	return true
}
