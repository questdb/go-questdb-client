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
	"log/slog"
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
	path             string
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
	return qwpSfAckWatermarkOpenRequiredWithLogger(slotDir, nil)
}

func qwpSfAckWatermarkOpenRequiredWithLogger(slotDir string, logger *slog.Logger) (*qwpSfAckWatermark, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfAckWatermarkFileName)
	st, err := os.Stat(path)
	existing := err == nil && st.Size() == qwpSfDualRecordFileSize
	legacy16Byte := err == nil && st.Size() == 16
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, qwpSfDurabilityError("stat ack watermark", path, err)
	}
	flags := os.O_RDWR | os.O_CREATE
	if !existing {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, qwpSfDurabilityError("open ack watermark", path, err)
	}
	if legacy16Byte {
		// A 16-byte watermark uses the legacy single-record format. Resetting
		// it discards its FSN because that format has no checksum or generation.
		// Recovery then uses segment boundaries, which may replay acknowledged
		// rows. The warning is emitted as soon as truncation succeeds because
		// the reset is complete at that point.
		qwpEffectiveLogger(logger).Warn("qwp/sf: reset legacy 16-byte ack watermark; acknowledged rows may replay", "path", path)
	}
	// image is what goes back through the descriptor to force real blocks under
	// every page that will be mapped: the existing dual-slot records where there
	// are any, zeros for a file this call creates.
	var image [qwpSfAckWatermarkFileSize]byte
	if !existing {
		if err := qwpSfAckWatermarkAllocate(f, path); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else if _, err := io.ReadFull(f, image[:]); err != nil {
		_ = f.Close()
		return nil, qwpSfDurabilityError("read existing ack watermark", path, err)
	}
	if err := qwpSfAckWatermarkReserveBlocks(f, path, image[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	buf, err := qwpSfMmapRW(f, qwpSfDualRecordFileSize)
	if err != nil {
		_ = f.Close()
		return nil, qwpSfDurabilityError("map ack watermark", path, err)
	}
	r0, ok0 := qwpSfDecodeDualRecord(buf[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
	off1 := int(qwpSfDualRecordSlotSize)
	r1, ok1 := qwpSfDecodeDualRecord(buf[off1:off1+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
	rec, ok := qwpSfSelectDualRecord(r0, ok0, r1, ok1)
	if existing && !ok {
		_ = qwpSfMunmap(buf)
		_ = f.Close()
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, qwpSfDurabilityError("reset ack watermark", path, err)
		}
		if err := qwpSfAckWatermarkAllocate(f, path); err != nil {
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
			return nil, qwpSfDurabilityError("map reset ack watermark", path, err)
		}
	}
	w := &qwpSfAckWatermark{
		file:             f,
		path:             path,
		buf:              buf,
		fsn:              qwpSfAckWatermarkInvalid,
		lastPersistedAck: -1,
	}
	if ok {
		w.generation = rec.generation
		w.fsn = rec.first
		w.lastPersistedAck = rec.first
	}
	return w, nil
}

// qwpSfAckWatermarkRecordValid is deliberately narrower than the manifest's
// predicate: first is the acknowledged FSN and -1 is the valid empty-prefix
// watermark. The second field is reserved by the Java format and does not
// participate in record validity.
func qwpSfAckWatermarkRecordValid(rec qwpSfDualRecord) bool {
	return rec.first >= -1
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
		return errors.Join(qwpSfErrAckWatermarkUnbacked,
			qwpSfDurabilityError("reserve blocks for ack watermark", path, err))
	}
	if n != len(image) {
		return errors.Join(qwpSfErrAckWatermarkUnbacked,
			qwpSfDurabilityError(
				fmt.Sprintf("reserve blocks for ack watermark: wrote %d of %d bytes", n, len(image)),
				path,
				io.ErrShortWrite,
			))
	}
	return nil
}

// qwpSfErrAckWatermarkUnbacked marks the one failure class the caller may carry
// on without: storage would not put real blocks behind the file, so the mapping
// cannot be stored into safely. The watermark is an optimisation -- it saves a
// recovered slot from re-sending frames a previous session already got acked --
// so a sender or drainer runs without it, at the cost of duplicate rows on
// replay against a table that does not dedupe. Failing instead would stop a
// drainer on a full disk, and draining that slot is what frees the disk.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrAckWatermarkUnbacked = errors.New("qwp/sf: storage will not back the ack watermark")

// qwpSfAckWatermarkAllocate reserves the file's blocks up front, reporting a
// refusal in the same skippable class as the write-back.
func qwpSfAckWatermarkAllocate(f *os.File, path string) error {
	if err := qwpSfAllocate(f, qwpSfDualRecordFileSize); err != nil {
		return errors.Join(qwpSfErrAckWatermarkUnbacked,
			qwpSfDurabilityError("allocate ack watermark", path, err))
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

func (w *qwpSfAckWatermark) persistIfAdvanced(fsn int64) (bool, error) {
	if w == nil {
		return false, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || fsn < -1 || fsn <= w.lastPersistedAck {
		return false, nil
	}
	if w.generation == math.MaxInt64 {
		return false, qwpSfDurabilityError("ack watermark generation overflow", w.path, qwpSfErrGenerationOverflow)
	}
	next := w.generation + 1
	qwpSfEncodeDualRecord(w.scratch[:], qwpSfAckWatermarkMagic, next, fsn, 0)
	off := int((next & 1) * qwpSfDualRecordSlotSize)
	copy(w.buf[off:off+qwpSfDualRecordSize], w.scratch[:])
	w.generation = next
	w.fsn = fsn
	w.lastPersistedAck = fsn
	return true, nil
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
		return qwpSfDurabilityError("msync ack watermark", w.path, err)
	}
	if err := qwpSfAckWatermarkSyncFile(w.file); err != nil {
		return qwpSfDurabilityError("fsync ack watermark", w.path, err)
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
