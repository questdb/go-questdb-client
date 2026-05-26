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
	"math"
	"os"
	"sync/atomic"
	"time"
)

// qwpSf* constants describe the on-disk store-and-forward segment
// format. The layout matches the Java client (`MmapSegment.java`)
// exactly so segments are interchangeable with the Java client when
// sharing an SF group root.
//
// On-disk layout — header and frame format:
//
//	[u32 magic 'SF01'] [u8 ver=1] [u8 flags=0] [u16 reserved=0]
//	[u64 baseSeq]      [u64 createdMicros]                       24-byte header
//	frame, frame, ...                                            each frame:
//	                                                              [u32 crc32c]
//	                                                              [u32 payloadLen]
//	                                                              [payloadLen bytes]
//	crc32c covers (payloadLen, payload).
const (
	qwpSfFileMagic       uint32 = 0x31304653 // 'SF01' little-endian on disk
	qwpSfFrameHeaderSize int64  = 8          // u32 crc + u32 payloadLen
	qwpSfHeaderSize      int64  = 24         // total file header
	qwpSfSegmentVersion  byte   = 1
)

// qwpSfCrcTable is the CRC32C (Castagnoli) polynomial table shared
// across SF segment writers and readers. Allocated once.
var qwpSfCrcTable = crc32.MakeTable(crc32.Castagnoli)

// qwpSfErrLockBusy is returned by qwpSfFlockExclusive when another
// process already holds the lock. Matches Java's "sf slot already in
// use" error path; callers map it to a more informative message after
// reading the holder PID payload.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrLockBusy = errors.New("qwp/sf: lock busy")

// qwpSfErrSegmentFull is returned by qwpSfSegment.tryAppend when
// the requested frame won't fit in the segment's remaining capacity.
// The caller (the ring) is expected to rotate to a fresh segment and
// retry; if the payload still doesn't fit, the ring returns
// qwpSfPayloadTooLarge to its caller.
//
//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
var qwpSfErrSegmentFull = errors.New("qwp/sf: segment full")

// qwpSfSegment is one mmap-backed (or in-memory) SF segment. The
// producer thread (single user goroutine) appends frames into the
// mapping; the I/O thread (single consumer goroutine) reads up to
// publishedOffset() for wire send. No locks; the cursor pair
// (appendCursor / publishedCursor) is the only cross-thread
// coordination, and publishedCursor is the publish barrier — the
// consumer MUST NOT read any byte at offset >= publishedOffset().
//
// The mapping is sized at construction and never grows. When tryAppend
// returns qwpSfErrSegmentFull the caller must rotate to a fresh
// segment. Closing the segment unmaps and closes the file; data
// already written is durable under the page cache (and recoverable
// across process restarts) — call msync for OS-crash durability.
type qwpSfSegment struct {
	path         string
	sizeBytes    int64
	memoryBacked bool

	// file is nil for memory-backed segments. For file-backed segments
	// it is held for the segment's lifetime so munmap can run before
	// close. POSIX guarantees the mapping persists after close, but
	// holding the handle keeps the contract uniform with Windows.
	file *os.File

	// buf is the mmap'd or malloc'd backing store; len(buf) == sizeBytes.
	buf []byte

	// appendCursor is written only by the producer — it's the
	// reservation cursor. Plain int64; the producer is single-threaded
	// against this segment.
	appendCursor int64

	// baseSeq is provisional at create time, finalized by rebaseSeq()
	// at rotation time. Mutable to support the segment manager's
	// hot-spare design — spares are pre-created before the producer
	// knows what baseSeq the new active will need. Plain field;
	// rebaseSeq() is called on the producer thread before any
	// cross-thread reader can observe the new identity.
	baseSeq int64

	// frameCount: number of frames successfully appended. Single
	// writer (the producer thread in tryAppend); read cross-thread by
	// the I/O thread via the ring's findSegmentContaining and lastSeq
	// computations on the active segment. Atomic for cross-thread
	// visibility.
	frameCount atomic.Int64

	// publishedCursor: written by producer, read by consumer (I/O
	// thread). Atomic because the consumer must see writes in
	// publication order — once the producer bumps publishedCursor,
	// every byte before it is fully written.
	publishedCursor atomic.Int64

	// tornTailBytes is the byte count between the last valid frame and
	// the file end that look like an attempted-but-invalid frame write
	// (non-zero bytes at the bail-out position). Zero for fresh
	// segments and for cleanly partially-filled segments (uninitialised
	// tail). Set only by qwpSfOpenSegment; visible to recovery callers
	// for diagnostics. Final after construction.
	tornTailBytes int64
}

// qwpSfCreateSegment creates a fresh segment file at path,
// pre-allocating exactly sizeBytes and mmapping it RW. The 24-byte
// header is written in-place; the cursor lands at qwpSfHeaderSize.
// Returns an error on any I/O failure (openCleanRW, disk full, mmap
// rejected).
//
// Pre-allocation goes through qwpSfAllocate, which owns the
// cross-platform "extend + reserve real disk blocks + never shrinks"
// contract (see qwp_sf_allocate.go). For this call path the file is
// freshly O_TRUNC'd so currentSize == 0 and qwpSfAllocate reserves
// blocks for [0, sizeBytes) and advances EOF to sizeBytes in one
// step. Without the reservation a later store into the mmap'd region
// after the filesystem fills up would deliver SIGBUS (POSIX) /
// STATUS_IN_PAGE_ERROR (Windows), tearing down the process —
// sf-client.md §6 marks block reservation a core invariant of the
// create path.
func qwpSfCreateSegment(path string, baseSeq, sizeBytes int64) (*qwpSfSegment, error) {
	if sizeBytes < qwpSfHeaderSize+qwpSfFrameHeaderSize+1 {
		return nil, fmt.Errorf("qwp/sf: sizeBytes too small for header + one minimal frame: %d", sizeBytes)
	}
	// O_TRUNC discards any prior content at the same path — segment
	// files are write-once-then-fixed, so reusing a stale file is
	// always an error in the recovery code path; here, on a fresh
	// create, truncation is the documented behavior. The post-open
	// EOF is 0, which is the precondition qwpSfAllocate's macOS
	// reservation (F_PEOFPOSMODE — allocates the requested length
	// immediately beyond EOF) needs in order to cover [0, sizeBytes).
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: openCleanRW %s: %w", path, err)
	}
	if err := qwpSfAllocate(f, sizeBytes); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	buf, err := qwpSfMmapRW(f, sizeBytes)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	s := &qwpSfSegment{
		path:         path,
		sizeBytes:    sizeBytes,
		memoryBacked: false,
		file:         f,
		buf:          buf,
		appendCursor: qwpSfHeaderSize,
		baseSeq:      baseSeq,
	}
	s.publishedCursor.Store(qwpSfHeaderSize)
	s.writeHeader(baseSeq)
	return s, nil
}

// qwpSfCreateInMemorySegment creates a memory-backed segment with the
// same on-the-wire layout as qwpSfCreateSegment but without any file.
// Used by the non-SF async ingest path (memory mode) — same cursor
// architecture, no disk involvement; the slice is freed when the
// segment is closed and goes out of scope (the GC reclaims it).
func qwpSfCreateInMemorySegment(baseSeq, sizeBytes int64) (*qwpSfSegment, error) {
	if sizeBytes < qwpSfHeaderSize+qwpSfFrameHeaderSize+1 {
		return nil, fmt.Errorf("qwp/sf: sizeBytes too small for header + one minimal frame: %d", sizeBytes)
	}
	buf := make([]byte, sizeBytes)
	s := &qwpSfSegment{
		path:         "",
		sizeBytes:    sizeBytes,
		memoryBacked: true,
		file:         nil,
		buf:          buf,
		appendCursor: qwpSfHeaderSize,
		baseSeq:      baseSeq,
	}
	s.publishedCursor.Store(qwpSfHeaderSize)
	s.writeHeader(baseSeq)
	return s, nil
}

// qwpSfOpenSegment opens an existing segment file for recovery. mmaps
// it RW, validates the header magic / version, then scans frames
// forward verifying each CRC. The first bad CRC (or a frame whose
// declared length runs past the file end) is treated as a torn tail;
// both cursors are positioned at the start of that frame. Returns the
// segment ready for further appends.
//
// If recovery observes a torn tail (bytes at the bail-out position
// are non-zero, indicating an attempted-but-failed frame write), the
// byte count is exposed via tornTailBytes() so operators can detect
// silent truncation from corruption or partial writes.
func qwpSfOpenSegment(path string) (*qwpSfSegment, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: stat %s: %w", path, err)
	}
	fileSize := st.Size()
	if fileSize < qwpSfHeaderSize {
		return nil, fmt.Errorf("qwp/sf: file shorter than header: %s size=%d", path, fileSize)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("qwp/sf: openRW %s: %w", path, err)
	}
	buf, err := qwpSfMmapRW(f, fileSize)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != qwpSfFileMagic {
		_ = qwpSfMunmap(buf)
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: bad magic in %s: 0x%x", path, magic)
	}
	version := buf[4]
	if version != qwpSfSegmentVersion {
		_ = qwpSfMunmap(buf)
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: unsupported version in %s: %d", path, version)
	}
	baseSeq := int64(binary.LittleEndian.Uint64(buf[8:16]))
	// FSNs are non-negative by construction. A negative baseSeq on disk
	// means bit-rot or a hand-edited file — refuse so qwpSfOpenRing's
	// per-file skip handles it like any other unreadable .sfa rather
	// than feeding the bad value into the unsigned-comparison sort and
	// contiguity check (which would place the segment last and trip the
	// FSN-gap error, taking the whole recovery down).
	if baseSeq < 0 {
		_ = qwpSfMunmap(buf)
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: bad baseSeq in %s: %d", path, baseSeq)
	}
	lastGood := qwpSfScanFrames(buf, fileSize)
	count := qwpSfCountFrames(buf, lastGood)
	tornTail := qwpSfDetectTornTail(buf, lastGood, fileSize)
	s := &qwpSfSegment{
		path:          path,
		sizeBytes:     fileSize,
		memoryBacked:  false,
		file:          f,
		buf:           buf,
		appendCursor:  lastGood,
		baseSeq:       baseSeq,
		tornTailBytes: tornTail,
	}
	s.publishedCursor.Store(lastGood)
	s.frameCount.Store(count)
	return s, nil
}

// writeHeader populates the 24-byte file header at offset 0.
// Producer-only; called from constructors and rebaseSeq.
func (s *qwpSfSegment) writeHeader(baseSeq int64) {
	binary.LittleEndian.PutUint32(s.buf[0:4], qwpSfFileMagic)
	s.buf[4] = qwpSfSegmentVersion
	s.buf[5] = 0 // flags
	binary.LittleEndian.PutUint16(s.buf[6:8], 0) // reserved
	binary.LittleEndian.PutUint64(s.buf[8:16], uint64(baseSeq))
	binary.LittleEndian.PutUint64(s.buf[16:24], uint64(time.Now().UnixMicro()))
}

// address returns a slice view of the underlying mapped buffer. The
// returned slice's length == sizeBytes; reads past publishedOffset()
// are not safe (the producer may be mid-write).
func (s *qwpSfSegment) address() []byte {
	return s.buf
}

// segmentBaseSeq returns the segment's current baseSeq. Called
// cross-thread by the I/O loop; safe because baseSeq is set at
// construction or rebaseSeq() (producer thread) before the segment
// becomes visible to readers — and is never further mutated.
func (s *qwpSfSegment) segmentBaseSeq() int64 {
	return s.baseSeq
}

// capacityRemaining returns bytes available for further appends,
// accounting for the per-frame 8-byte envelope a future tryAppend
// would also write. This is payload bytes the caller can still fit,
// NOT raw remaining-mapping bytes.
func (s *qwpSfSegment) capacityRemaining() int64 {
	left := s.sizeBytes - s.appendCursor - qwpSfFrameHeaderSize
	if left < 0 {
		return 0
	}
	return left
}

// isFull reports whether tryAppend would refuse any non-empty frame.
func (s *qwpSfSegment) isFull() bool {
	return s.capacityRemaining() <= 0
}

// publishedOffset returns the bytes safely written and visible to the
// consumer. Reading any byte at offset >= publishedOffset() from the
// mapping is undefined — the producer may be mid-write.
func (s *qwpSfSegment) publishedOffset() int64 {
	return s.publishedCursor.Load()
}

// segmentFrameCount returns the number of frames written since
// create (or recovered by openExisting). Used by the ring to compute
// lastSeq = baseSeq + frameCount - 1 for ACK / trim decisions.
func (s *qwpSfSegment) segmentFrameCount() int64 {
	return s.frameCount.Load()
}

// rebaseSeq re-stamps the segment's baseSeq, both in memory and in
// the on-disk header at offset 8. Used by the ring at rotation time
// to pin the segment's identity once the active's frame count is
// final (the segment manager pre-creates spares with a provisional
// baseSeq that may be stale by rotation time). Returns an error if
// any frames have already been appended — a rebase after first append
// would corrupt the FSN sequence.
func (s *qwpSfSegment) rebaseSeq(newBaseSeq int64) error {
	if s.frameCount.Load() > 0 {
		return fmt.Errorf("qwp/sf: cannot rebase: segment has %d frame(s) already appended",
			s.frameCount.Load())
	}
	s.baseSeq = newBaseSeq
	binary.LittleEndian.PutUint64(s.buf[8:16], uint64(newBaseSeq))
	return nil
}

// tryAppend appends one frame: writes [crc32c | u32 payloadLen | payload]
// starting at the current append cursor, then advances both cursors
// (publishedCursor last via atomic store, so the consumer never sees
// a partial frame). Returns the offset of the appended frame on
// success, or qwpSfErrSegmentFull if the remaining capacity cannot
// fit qwpSfFrameHeaderSize + payloadLen.
//
// This is the producer thread's hot path. No syscall, no allocation;
// just a CRC pass and a copy into the mapped region.
func (s *qwpSfSegment) tryAppend(payload []byte) (int64, error) {
	payloadLen := int64(len(payload))
	if payloadLen < 0 {
		return 0, fmt.Errorf("qwp/sf: negative payloadLen: %d", payloadLen)
	}
	// The on-disk length is a u32 read back as int32 by the recovery
	// scanner (qwpSfScanFrames), so any value with bit 31 set would
	// round-trip as negative and be rejected as a torn tail. Bracket
	// the writer to the reader's tolerance so a too-large frame fails
	// here instead of corrupting the segment.
	if payloadLen > math.MaxInt32 {
		return 0, fmt.Errorf("qwp/sf: payloadLen exceeds int32: %d", payloadLen)
	}
	total := qwpSfFrameHeaderSize + payloadLen
	offset := s.appendCursor
	if offset+total > s.sizeBytes {
		return 0, qwpSfErrSegmentFull
	}
	// Frame layout: [u32 crc][u32 payloadLen][payload].
	// Length goes first so the CRC pass can include it without
	// recomputing offsets.
	binary.LittleEndian.PutUint32(s.buf[offset+4:offset+8], uint32(payloadLen))
	if payloadLen > 0 {
		copy(s.buf[offset+qwpSfFrameHeaderSize:offset+total], payload)
	}
	// CRC32C over (payloadLen, payload). Recovery scans validate each
	// frame by recomputing this CRC over the on-disk bytes.
	crc := crc32.Update(0, qwpSfCrcTable, s.buf[offset+4:offset+8])
	if payloadLen > 0 {
		crc = crc32.Update(crc, qwpSfCrcTable, s.buf[offset+qwpSfFrameHeaderSize:offset+total])
	}
	binary.LittleEndian.PutUint32(s.buf[offset:offset+4], crc)
	s.appendCursor = offset + total
	s.frameCount.Add(1)
	// Publish last. Until this atomic store retires, the consumer
	// cannot see any of the bytes we just wrote.
	s.publishedCursor.Store(s.appendCursor)
	return offset, nil
}

// msync synchronously flushes dirty pages of [HEADER_SIZE,
// publishedOffset()) to disk via msync(MS_SYNC). Off the hot path —
// call only when the user has opted into OS-crash durability. No-op
// for memory-backed segments.
func (s *qwpSfSegment) msync() error {
	if s.memoryBacked {
		return nil
	}
	pub := s.publishedCursor.Load()
	if pub > qwpSfHeaderSize {
		return qwpSfMsync(s.buf, pub)
	}
	return nil
}

// close unmaps the buffer and closes the underlying file. Safe to
// call on a segment that has been partially constructed (e.g. after
// a failed mmap during qwpSfOpenSegment); fields that were never
// initialised are nil and we skip them.
func (s *qwpSfSegment) close() error {
	var firstErr error
	if !s.memoryBacked && s.buf != nil {
		if err := qwpSfMunmap(s.buf); err != nil {
			firstErr = err
		}
	}
	s.buf = nil
	if s.file != nil {
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}
	return firstErr
}

// segmentPath returns the file path the segment was created from /
// opened against. Empty for memory-backed segments.
func (s *qwpSfSegment) segmentPath() string {
	return s.path
}

// segmentSize returns the configured segment size in bytes — the
// total allocation, not the published portion.
func (s *qwpSfSegment) segmentSize() int64 {
	return s.sizeBytes
}

// segmentTornTailBytes returns the byte count between the last valid
// frame and the file end that look like an attempted-but-invalid
// frame write — set by qwpSfOpenSegment when recovery observes
// non-zero bytes past the bail-out point. Zero for fresh segments,
// memory-backed segments, and cleanly partially-filled recovered
// segments. Operators / tests can read this to tell silent
// truncation (corruption) from a normal partial fill (no incident).
func (s *qwpSfSegment) segmentTornTailBytes() int64 {
	return s.tornTailBytes
}

// qwpSfScanFrames is a forward scan that returns the offset just past
// the last frame whose CRC verifies. A torn-tail frame (declared
// length runs past EOF, or CRC mismatch) leaves both cursors at the
// start of that frame; the next tryAppend will overwrite it. The
// scan only reads from the mapping — no syscalls.
func qwpSfScanFrames(buf []byte, fileSize int64) int64 {
	pos := qwpSfHeaderSize
	for pos+qwpSfFrameHeaderSize <= fileSize {
		crcRead := binary.LittleEndian.Uint32(buf[pos : pos+4])
		payloadLen := int64(int32(binary.LittleEndian.Uint32(buf[pos+4 : pos+8])))
		// Defensive: a corrupt length field could be enormous or
		// negative, both of which would otherwise overrun the mapping.
		if payloadLen < 0 || pos+qwpSfFrameHeaderSize+payloadLen > fileSize {
			return pos
		}
		crcCalc := crc32.Update(0, qwpSfCrcTable, buf[pos+4:pos+8])
		if payloadLen > 0 {
			crcCalc = crc32.Update(crcCalc, qwpSfCrcTable, buf[pos+qwpSfFrameHeaderSize:pos+qwpSfFrameHeaderSize+payloadLen])
		}
		if crcCalc != crcRead {
			return pos
		}
		pos += qwpSfFrameHeaderSize + payloadLen
	}
	return pos
}

// qwpSfDetectTornTail distinguishes "torn tail" (writer attempted a
// write past the last valid frame and failed — partial write,
// mid-stream corruption, bit rot) from clean unwritten space
// (manager-allocated segment with zero-filled tail). Returns the byte
// count from lastGood to fileSize when the bytes at the bail-out
// frame header are non-zero, else 0.
//
// Heuristic but robust for the common cases: qwpSfCreateSegment
// truncates the file to size, leaving the tail zero-filled; the
// writer only writes non-zero bytes via tryAppend, which writes the
// CRC and length fields together. So a non-zero byte at the
// failed-frame position implies an attempted write — exactly the
// case operators want flagged.
func qwpSfDetectTornTail(buf []byte, lastGood, fileSize int64) int64 {
	if lastGood >= fileSize {
		return 0
	}
	probe := qwpSfFrameHeaderSize
	if fileSize-lastGood < probe {
		probe = fileSize - lastGood
	}
	for i := int64(0); i < probe; i++ {
		if buf[lastGood+i] != 0 {
			return fileSize - lastGood
		}
	}
	return 0
}

// qwpSfCountFrames counts frames in [HEADER_SIZE, lastGood). Walks
// the framing in lockstep with qwpSfScanFrames (which already
// validated CRCs); so this is just length-driven traversal, no CRC
// re-check.
func qwpSfCountFrames(buf []byte, lastGood int64) int64 {
	pos := qwpSfHeaderSize
	count := int64(0)
	for pos < lastGood {
		payloadLen := int64(int32(binary.LittleEndian.Uint32(buf[pos+4 : pos+8])))
		pos += qwpSfFrameHeaderSize + payloadLen
		count++
	}
	return count
}
