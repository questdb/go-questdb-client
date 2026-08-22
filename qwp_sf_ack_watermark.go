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
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	qwpSfAckWatermarkFileName        = ".ack-watermark"
	qwpSfAckWatermarkMagic    uint32 = 0x31574b41
	qwpSfAckWatermarkFileSize        = qwpSfDualRecordFileSize
)

const qwpSfAckWatermarkInvalid int64 = math.MinInt64

// qwpSfAckWatermarkWriteAt is a test seam for block-forcing write failures on
// an existing correctly-sized watermark.
var qwpSfAckWatermarkWriteAt = qwpSfSwappable(func(f *os.File, p []byte, off int64) (int, error) {
	return f.WriteAt(p, off)
})

// qwpSfAckWatermarkSync is a test seam for close/trim durability-barrier
// failures. A nil pointer means production qwpSfFsync. Tests publish hooks
// atomically because sync also runs on the live manager worker.
var qwpSfAckWatermarkSync atomic.Pointer[func(f *os.File) error]

func qwpSfAckWatermarkSyncFile(f *os.File) error {
	if hook := qwpSfAckWatermarkSync.Load(); hook != nil {
		return (*hook)(f)
	}
	return qwpSfFsync(f)
}

// qwpSfAckWatermark uses the Java-compatible dual-slot record. Stores only
// dirty the mapping; sync is a separate control-point barrier used before trim
// and close-time unlink.
type qwpSfAckWatermark struct {
	mu               sync.Mutex
	file             *os.File
	buf              []byte
	generation       int64
	fsn              int64
	lastPersistedAck int64
	closed           bool
	scratch          [qwpSfDualRecordSize]byte
}

func qwpSfAckWatermarkOpen(slotDir string) *qwpSfAckWatermark {
	w, _ := qwpSfAckWatermarkOpenRequired(slotDir)
	return w
}

func qwpSfAckWatermarkOpenRequired(slotDir string) (*qwpSfAckWatermark, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfAckWatermarkFileName)
	st, err := os.Stat(path)
	existing := err == nil && st.Size() == qwpSfDualRecordFileSize
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("qwp/sf: stat ack watermark %s: %w", path, err)
	}
	flags := os.O_RDWR | os.O_CREATE
	if !existing {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: open ack watermark %s: %w", path, err)
	}
	// image is what goes back through the descriptor to force real blocks under
	// every page that will be mapped: the existing dual-slot records where there
	// are any, zeros for a file this call creates.
	var image [qwpSfAckWatermarkFileSize]byte
	if !existing {
		if err := qwpSfAllocate(f, qwpSfDualRecordFileSize); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else if _, err := io.ReadFull(f, image[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: read existing ack watermark %s: %w", path, err)
	}
	if err := qwpSfAckWatermarkReserveBlocks(f, path, image[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	buf, err := qwpSfMmapRW(f, qwpSfDualRecordFileSize)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	valid := func(rec qwpSfDualRecord) bool { return rec.first >= -1 }
	r0, ok0 := qwpSfDecodeDualRecord(buf[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, valid)
	off1 := int(qwpSfDualRecordSlotSize)
	r1, ok1 := qwpSfDecodeDualRecord(buf[off1:off1+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, valid)
	rec, ok := qwpSfSelectDualRecord(r0, ok0, r1, ok1)
	if existing && !ok {
		_ = qwpSfMunmap(buf)
		_ = f.Close()
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, fmt.Errorf("qwp/sf: reset ack watermark %s: %w", path, err)
		}
		if err := qwpSfAllocate(f, qwpSfDualRecordFileSize); err != nil {
			_ = f.Close()
			return nil, err
		}
		var reset [qwpSfAckWatermarkFileSize]byte
		if err := qwpSfAckWatermarkReserveBlocks(f, path, reset[:]); err != nil {
			_ = f.Close()
			return nil, err
		}
		buf, err = qwpSfMmapRW(f, qwpSfDualRecordFileSize)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	w := &qwpSfAckWatermark{
		file:             f,
		buf:              buf,
		fsn:              qwpSfAckWatermarkInvalid,
		lastPersistedAck: -1,
	}
	if ok {
		w.generation = rec.generation
		w.fsn = rec.first
	}
	return w, nil
}

// qwpSfAckWatermarkReserveBlocks writes image back through the descriptor so
// every page the mapping will touch is backed by a real block. Storing through
// a sparse mapping on a full disk raises SIGBUS, and this file is stored to
// from the segment manager's goroutine, where that would take the process down
// with no error to report.
//
// The write is needed on every path, not only for a file that was already
// there. qwpSfAllocate reports success without reserving anything whenever the
// filesystem has no reservation primitive it can use -- NFS, SMB and overlayfs
// take its sparse fallback, and the generic-unix build has no primitive at all
// -- and it deliberately does nothing for a file that is already the right
// size, which is what an existing (possibly foreign, possibly sparse) one is.
// Writing the image allocates the holes now, so a full disk surfaces as ENOSPC
// from the open instead of as a signal later.
func qwpSfAckWatermarkReserveBlocks(f *os.File, path string, image []byte) error {
	n, err := qwpSfAckWatermarkWriteAt.load()(f, image, 0)
	if err != nil {
		return fmt.Errorf("qwp/sf: reserve blocks for ack watermark %s: %w", path, err)
	}
	if n != len(image) {
		return fmt.Errorf("qwp/sf: reserve blocks for ack watermark %s: wrote %d of %d bytes: %w",
			path, n, len(image), io.ErrShortWrite)
	}
	return nil
}

func qwpSfAckWatermarkRemoveOrphan(slotDir string) {
	if slotDir != "" {
		_ = os.Remove(filepath.Join(slotDir, qwpSfAckWatermarkFileName))
	}
}

func (w *qwpSfAckWatermark) read() int64 {
	if w == nil {
		return qwpSfAckWatermarkInvalid
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return qwpSfAckWatermarkInvalid
	}
	return w.fsn
}

func (w *qwpSfAckWatermark) persistIfAdvanced(fsn int64) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || fsn < -1 || fsn <= w.lastPersistedAck {
		return false
	}
	next := w.generation + 1
	if next <= 0 {
		return false
	}
	qwpSfEncodeDualRecord(w.scratch[:], qwpSfAckWatermarkMagic, next, fsn, 0)
	off := int((next & 1) * qwpSfDualRecordSlotSize)
	copy(w.buf[off:off+qwpSfDualRecordSize], w.scratch[:])
	w.generation = next
	w.fsn = fsn
	w.lastPersistedAck = fsn
	return true
}

func (w *qwpSfAckWatermark) sync() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("qwp/sf: ack watermark is closed")
	}
	if err := qwpSfMsync(w.buf, int64(len(w.buf))); err != nil {
		return fmt.Errorf("qwp/sf: msync ack watermark: %w", err)
	}
	if err := qwpSfAckWatermarkSyncFile(w.file); err != nil {
		return fmt.Errorf("qwp/sf: fsync ack watermark: %w", err)
	}
	return nil
}

func (w *qwpSfAckWatermark) close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	var firstErr error
	if w.buf != nil {
		if err := qwpSfMunmap(w.buf); err != nil {
			firstErr = err
		}
		w.buf = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.file = nil
	}
	return firstErr
}
