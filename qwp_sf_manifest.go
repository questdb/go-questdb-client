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
	if err := qwpSfSyncDir(dir); err != nil {
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
	if err := m.file.Sync(); err != nil {
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

func qwpSfQuarantineCreationDebris(path string) error {
	corrupt := path + ".corrupt"
	_ = os.Remove(corrupt)
	if err := os.Rename(path, corrupt); err == nil {
		return nil
	}
	if err := os.Remove(path); err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	} else {
		return fmt.Errorf("qwp/sf: could not quarantine or remove invalid %s: %w", path, err)
	}
}
