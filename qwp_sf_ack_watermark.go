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

// qwpSfAckWatermarkSync is a test seam for startup/close/trim
// durability-barrier failures. A nil pointer means production qwpSfFsync.
// Tests publish hooks atomically because sync also runs on the live manager
// worker.
var qwpSfAckWatermarkSync atomic.Pointer[func(f *os.File) error]

// qwpSfAckWatermarkTruncate is a test seam for reset failures. Startup retires
// old ACK evidence by truncating the file in place, so a refused truncation
// has to be injectable without a read-only filesystem.
var qwpSfAckWatermarkTruncate = qwpSfSwappable(func(f *os.File, size int64) error {
	return f.Truncate(size)
})

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
	closeErr         error
	closed           bool
	scratch          [qwpSfDualRecordSize]byte
}

// qwpSfAckWatermarkStartup carries the frame history this slot's recovery
// established. It is what decides whether a record found on disk may be
// trusted at all: an acknowledged FSN only means something relative to the
// frames it claims to cover.
type qwpSfAckWatermarkStartup struct {
	// freshHistory is set when recovery produced no ring. Frame numbering then
	// restarts at 0, so any record on disk describes a previous lifecycle whose
	// frames are gone, and it has to be retired before this session republishes
	// those numbers. A recovered ring holding no frames is NOT fresh: it can
	// carry a nonzero sequence base, and its record is still about its own
	// history.
	freshHistory bool
	// publishedFsn is the recovered ring's highest published FSN. A selected
	// record above it cannot have come from a correctly operating session for
	// this history, so startup retires it rather than ignoring it for one run:
	// ignoring it lets the very same record become plausible again once new
	// frames reach that number.
	//
	// Ignored when freshHistory is set.
	publishedFsn int64
}

// qwpSfAckWatermarkOpenPrepared prepares, then opens, a slot's ack watermark.
//
// Every disk-backed construction runs the same startup checkpoint, under the
// slot lock, before anything can publish a frame number:
//
//  1. Open without truncation and inspect the file through its descriptor.
//  2. Decide preserve or reset from startup. A reset truncates the file to zero
//     bytes, which retires both records together; it never unlinks, recreates,
//     or clears one record slot -- clearing only the newer slot would expose
//     the older one to the next restart.
//  3. Sync the file, then run the slot-directory barrier. Both run
//     unconditionally, even when the file was already empty, invalid or
//     acceptable: an earlier attempt may have truncated the file and died
//     before either barrier completed, and bytes visible in the page cache are
//     no evidence that it did. This unconditional checkpoint IS the retry
//     mechanism, so no persistent marker, lifecycle identifier or in-memory
//     memory of a previous attempt is needed.
//  4. Only after those barriers succeed: allocate, write the image back through
//     the descriptor, and map.
//
// A zero-length file is an intermediate startup state, not a new on-disk
// format; a successful allocation restores the Java-compatible dual-record
// layout. Inspection, truncation and barrier failures fail construction.
// Allocation and write-back failure is the only class a caller may carry on
// without, and only once the checkpoint has succeeded -- see
// qwpSfErrAckWatermarkUnbacked and qwpSfAckWatermarkStorageFallbackAllowed.
//
// This preserves the useful full-disk case where truncating and syncing an
// existing file succeeds even though reserving its blocks does not. It is not
// a promise of startup on failing storage.
func qwpSfAckWatermarkOpenPrepared(slotDir string, startup qwpSfAckWatermarkStartup, logger *slog.Logger) (result *qwpSfAckWatermark, err error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfAckWatermarkFileName)
	// Never O_TRUNC: the decision to discard this file's records belongs to the
	// inspection below, and a truncation that open performed would be neither
	// observable nor durable at that point.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, qwpSfDurabilityError("open ack watermark", path, err)
	}
	held := &qwpSfAckWatermark{file: f, path: path}
	resources := &qwpSfAcquiredResources{watermarks: []*qwpSfAckWatermark{held}}
	defer func() {
		if result != nil {
			return
		}
		if r := recover(); r != nil {
			qwpSfRetainAcquisitionPanic(r, resources)
		}
		err = qwpSfFailedAcquisition(err, resources)
	}()

	// Step 1: inspect. image is also what goes back through the descriptor to
	// force real blocks under every page that will be mapped.
	var image [qwpSfAckWatermarkFileSize]byte
	st, err := f.Stat()
	if err != nil {
		return nil, qwpSfDurabilityError("stat ack watermark", path, err)
	}
	size := st.Size()
	// Only a correctly sized file can hold the dual-record layout. Anything
	// else -- a file this call just created, a zero-length file left by an
	// interrupted reset, a 16-byte legacy watermark, a truncated remnant -- is a
	// reset candidate, and must not be read as though it held records.
	rightSize := size == qwpSfAckWatermarkFileSize
	if rightSize {
		if _, readErr := io.ReadFull(f, image[:]); readErr != nil {
			// A short read of a correctly sized file is a storage failure, not
			// evidence that its records are unusable. Failing keeps the bytes
			// for a later attempt instead of discarding them on a bad syscall.
			return nil, qwpSfDurabilityError("read existing ack watermark", path, readErr)
		}
	}
	// Selection follows the existing format, checksum and generation rules, and
	// the history comparison applies to the selected record only -- an older
	// record slot being above the tip does not disqualify the newer one that
	// selection accepted.
	rec, ok := qwpSfSelectAckWatermarkRecord(image[:], rightSize)
	reset := true
	switch {
	case startup.freshHistory:
	case !rightSize:
	case !ok:
	case rec.first > startup.publishedFsn:
	default:
		reset = false
	}

	// Step 2: reset in place. Truncation retires both record slots at once.
	if reset {
		if truncErr := qwpSfAckWatermarkTruncate.load()(f, 0); truncErr != nil {
			return nil, qwpSfDurabilityError("reset ack watermark", path, truncErr)
		}
		if size == 16 {
			// A 16-byte watermark uses the legacy single-record format.
			// Resetting it discards its FSN because that format has no checksum
			// or generation. Recovery then uses segment boundaries, which may
			// replay acknowledged rows. The warning is emitted as soon as
			// truncation succeeds because the reset is complete at that point.
			qwpEffectiveLogger(logger).Warn("qwp/sf: reset legacy 16-byte ack watermark; acknowledged rows may replay", "path", path)
		}
	}

	// Step 3: the unconditional startup checkpoint. Neither barrier may be
	// skipped because the visible file already looks safe.
	if syncErr := qwpSfAckWatermarkSyncFile(f); syncErr != nil {
		return nil, qwpSfDurabilityError("sync prepared ack watermark", path, syncErr)
	}
	if dirErr := qwpSfSyncSlotDir(slotDir); dirErr != nil {
		return nil, qwpSfDurabilityError("sync slot directory for prepared ack watermark", slotDir, dirErr)
	}

	// Step 4: allocate, reserve, map. Initialization writes back the inspected
	// image for a preserved file and zeros for a reset one; it never
	// reintroduces a discarded record or revisits record selection.
	if err := qwpSfAckWatermarkAllocate(f, path); err != nil {
		return nil, err
	}
	writeBack := image[:]
	if reset {
		var zeros [qwpSfAckWatermarkFileSize]byte
		writeBack = zeros[:]
	}
	if err := qwpSfAckWatermarkReserveBlocks(f, path, writeBack); err != nil {
		return nil, err
	}
	buf, err := qwpSfMmapRW(f, qwpSfDualRecordFileSize)
	held.buf = buf
	if err != nil {
		return nil, qwpSfDurabilityError("map ack watermark", path, err)
	}
	// A reset object starts out exactly like a new empty watermark, generation
	// included, so a genuine lower ACK can still be persisted afterwards:
	// persistIfAdvanced refuses decreases and must not be asked to lower a
	// record. The next restart then selects that new record, not the retired one.
	w := &qwpSfAckWatermark{
		file:             f,
		path:             path,
		buf:              buf,
		fsn:              qwpSfAckWatermarkInvalid,
		lastPersistedAck: -1,
	}
	if !reset {
		w.generation = rec.generation
		w.fsn = rec.first
		w.lastPersistedAck = rec.first
	}
	return w, nil
}

// qwpSfSelectAckWatermarkRecord selects the live record from a watermark image
// under the existing dual-record rules. present is false for a file that cannot
// hold the layout at all, so its bytes are never decoded.
func qwpSfSelectAckWatermarkRecord(image []byte, present bool) (qwpSfDualRecord, bool) {
	off1 := int(qwpSfDualRecordSlotSize)
	if !present || len(image) < off1+qwpSfDualRecordSize {
		return qwpSfDualRecord{}, false
	}
	r0, ok0 := qwpSfDecodeDualRecord(image[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
	r1, ok1 := qwpSfDecodeDualRecord(image[off1:off1+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
	return qwpSfSelectDualRecord(r0, ok0, r1, ok1)
}

// qwpSfAckWatermarkStorageFallbackAllowed reports whether err authorises
// running without a mapped watermark.
//
// The sentinel alone is not authorisation. A joined error can carry both the
// storage refusal and a failure to release what the attempt acquired, and
// ownership of retained resources outranks an optimisation: the engine then
// fails construction and keeps its cleanup obligation. Inspection, truncation
// and barrier failures never reach here, because they do not carry the
// sentinel -- so preparation is known to have completed before a caller may
// continue with a nil watermark.
func qwpSfAckWatermarkStorageFallbackAllowed(err error) bool {
	if err == nil || !errors.Is(err, qwpSfErrAckWatermarkUnbacked) {
		return false
	}
	var retained *qwpSfAcquisitionError
	if errors.As(err, &retained) {
		return false
	}
	return !errors.Is(err, ErrCleanupFailed) && !errors.Is(err, ErrSfCleanupPending)
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
// It licenses nothing about the file on disk. A nil in-memory watermark is not
// evidence that the file is harmless: the completed startup checkpoint is. Only
// allocation and write-back join this sentinel, so it cannot mask an
// inspection, truncation or barrier failure.
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

// qwpSfAckWatermarkRemoveOrphan is best-effort close-time tidying for a fully
// drained slot. Startup correctness must never depend on it: a residual file is
// permitted, and qwpSfAckWatermarkOpenPrepared is what makes a stale record
// safe on the next construction.
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
	w.closed = true
	if w.buf != nil {
		if err := qwpSfReleaseMapping(w.buf); err != nil {
			return errors.Join(w.closeErr, err)
		}
		w.buf = nil
	}
	if w.file != nil {
		w.closeErr = errors.Join(w.closeErr, qwpSfCloseFile(w.file))
		w.file = nil
	}
	return w.closeErr
}
