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
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// qwpSfAckWatermark is the persisted high-water mark for the
// durably-acknowledged FSN. It lives at `<slot>/.ack-watermark`
// alongside the segment files and the slot lock, and is read at
// engine startup to seed ackedFsn — eliminating the segment-granular
// re-replay of partially-acked sealed segments across process
// restarts and across orphan adoption by a different client.
//
// The on-disk format is normative and interchangeable with the Java
// client's AckWatermark.java (sf-client.md §5.4, §19): 16 bytes,
// little-endian.
//
//	offset 0:  u32 magic = 0x31574B41 ('AKW1', stamped on first write)
//	offset 4:  u32 reserved (zero)
//	offset 8:  i64 fsn (cumulative durable-ack high-water mark)
//
// Durable acks are cumulative ("everything <= N is durable"), so a
// single monotonic FSN suffices; no per-frame bitmap is needed.
//
// Why the file is OPTIONAL but format normative: a missing or
// bad-magic file makes read() report qwpSfAckWatermarkInvalid and
// recovery falls back to the bare lowestBase-1 seed (no regression).
// A drainer adopting a slot another client populated MUST honour an
// existing watermark; ignoring it re-replays already-durable frames,
// producing row-level duplicates against a still-alive server.
//
// No CRC and no fsync: the watermark is a best-effort replay
// optimisation, not a crash-durable record. Real fsync durability is
// the deferred sf_durability=flush|append follow-up; segment files
// carry their own CRC32C and torn-tail recovery, the watermark does
// not. Recovery's clamp + bound catch only the gross corruption modes:
//   - stale-low / INVALID: max(lowestBase-1, watermark) picks the
//     segment-derived seed, costing only extra re-replay.
//   - stale-high past publishedFsn: the seed > publishedFsn check
//     rejects it and falls back to the segment seed.
// Neither catches a torn 8-byte FSN store that lands in range — true
// acked FSN < w' <= publishedFsn — from a partially persisted aligned
// store surviving a power loss: frames in (acked, w'] then look acked
// and never replay, a silent loss. That residual window is the cost
// of running without fsync/CRC and closes only when sf_durability
// gains real flush semantics.
//
// Concurrency: single-writer after construction (the segment-manager
// goroutine, via persistIfAdvanced). read() runs once at engine
// startup before the manager observes the entry. The mutex guards
// every access against close() so a manager tick that races a slow
// engine shutdown can never store into an unmapped region — Go can't
// lean on the JVM single-thread argument the Java reference uses, and
// an unguarded store-after-munmap is a hard SIGSEGV (and a -race
// failure). The lock is uncontended in the steady state and is off
// the producer hot path entirely (manager-tick cadence), so it does
// not affect BenchmarkQwpSenderSteadyState.
type qwpSfAckWatermark struct {
	mu     sync.Mutex
	file   *os.File
	buf    []byte
	closed bool

	// magicWritten flips once — at open() if a prior session already
	// stamped the magic, or on the first store that observes it unset.
	// After it flips, stores degenerate to a single 8-byte FSN put.
	magicWritten bool

	// lastPersistedAck is the highest FSN written so far this session.
	// Gates persistIfAdvanced so a steady ackedFsn doesn't dirty the
	// mapped page every manager tick. -1 until the first store.
	lastPersistedAck int64
}

// qwpSfAckWatermark on-disk constants. The magic and offsets are
// normative — they MUST match the Java client so a slot written by
// one client is honoured by a drainer from the other.
const (
	qwpSfAckWatermarkFileName       = ".ack-watermark"
	qwpSfAckWatermarkFileSize int64 = 16
	// qwpSfAckWatermarkMagic is 'AKW1' little-endian. A different
	// value at offset 0 means "no usable watermark" (freshly
	// zero-filled file, or corruption) and read() reports INVALID.
	qwpSfAckWatermarkMagic       uint32 = 0x31574B41
	qwpSfAckWatermarkMagicOffset int64  = 0
	qwpSfAckWatermarkFsnOffset   int64  = 8
)

// qwpSfAckWatermarkInvalid is the sentinel read() returns when the
// file has never been written (magic unset because the OS zero-filled
// a freshly created file) or is otherwise unusable. Recovery treats
// it as "no watermark" and seeds from the segment-derived value only.
// math.MinInt64 so max(watermark, lowestBase-1) always picks the
// segment seed in that case.
const qwpSfAckWatermarkInvalid int64 = math.MinInt64

// qwpSfAckWatermarkOpen opens (creating if absent) the watermark file
// in slotDir and maps its 16 bytes for the engine's lifetime. Returns
// nil on any setup failure (empty dir, open/allocate/mmap error) — the
// caller falls back to the no-watermark behaviour, no error escapes
// (the watermark is an optimisation, never a correctness dependency).
//
// An existing, correctly-sized file is opened read-write WITHOUT
// truncation so the previous session's (or another client's) FSN
// survives — defeating which is the whole point of the feature.
// A missing or wrong-sized file is (re)created at FILE_SIZE with zero
// magic, so the first read() reports INVALID until the first store.
func qwpSfAckWatermarkOpen(slotDir string) *qwpSfAckWatermark {
	if slotDir == "" {
		return nil
	}
	path := filepath.Join(slotDir, qwpSfAckWatermarkFileName)
	st, statErr := os.Stat(path)
	var (
		f   *os.File
		err error
	)
	if statErr == nil && st.Size() == qwpSfAckWatermarkFileSize {
		// Preserve the existing watermark bytes, but force a real disk
		// block under the mapping. A foreign watermark may be sparse
		// (truncated to 16 bytes but never block-allocated, or copied
		// sparse); mmap'ing it and later storing through it from the
		// manager goroutine would SIGBUS on a full disk when the page
		// fault cannot back the hole. qwpSfAllocate would no-op here (it
		// reserves only the newly-extended range, and the file is
		// already full size), so instead read the bytes and write them
		// straight back: the write allocates the block (the whole file
		// fits one block) and surfaces ENOSPC here, at open, where it
		// degrades to the no-watermark fallback — rather than faulting
		// the manager. A non-sparse foreign file just rewrites in place.
		f, err = os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil
		}
		var preserved [qwpSfAckWatermarkFileSize]byte
		if _, err := io.ReadFull(f, preserved[:]); err != nil {
			_ = f.Close()
			return nil
		}
		if _, err := f.WriteAt(preserved[:], 0); err != nil {
			_ = f.Close()
			return nil
		}
	} else {
		// Missing / wrong size: start clean and reserve a real disk
		// block via the same allocate contract the segment create path
		// uses, so a later store into the mapped region can't SIGBUS on
		// a sparse hole.
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return nil
		}
		if allocErr := qwpSfAllocate(f, qwpSfAckWatermarkFileSize); allocErr != nil {
			_ = f.Close()
			return nil
		}
	}
	buf, mmapErr := qwpSfMmapRW(f, qwpSfAckWatermarkFileSize)
	if mmapErr != nil {
		_ = f.Close()
		return nil
	}
	magic := binary.LittleEndian.Uint32(buf[qwpSfAckWatermarkMagicOffset : qwpSfAckWatermarkMagicOffset+4])
	return &qwpSfAckWatermark{
		file:             f,
		buf:              buf,
		magicWritten:     magic == qwpSfAckWatermarkMagic,
		lastPersistedAck: -1,
	}
}

// qwpSfAckWatermarkRemoveOrphan best-effort removes a stale watermark
// file. Used by the engine when no segments are recovered (a fresh
// disk slot, or after a clean fully-drained shutdown) — a watermark
// with no segments behind it refers to a lifecycle now gone and would
// only confuse the next session's seed. No-op for memory mode.
func qwpSfAckWatermarkRemoveOrphan(slotDir string) {
	if slotDir == "" {
		return
	}
	_ = os.Remove(filepath.Join(slotDir, qwpSfAckWatermarkFileName))
}

// read returns the persisted FSN, or qwpSfAckWatermarkInvalid if the
// file has never been written (magic field zero) or has been closed.
// Called once at engine startup before the manager observes the entry.
func (w *qwpSfAckWatermark) read() int64 {
	if w == nil {
		return qwpSfAckWatermarkInvalid
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return qwpSfAckWatermarkInvalid
	}
	magic := binary.LittleEndian.Uint32(w.buf[qwpSfAckWatermarkMagicOffset : qwpSfAckWatermarkMagicOffset+4])
	if magic != qwpSfAckWatermarkMagic {
		// Freshly created (all zeros) or corrupt — fall back to the
		// segment-derived seed.
		return qwpSfAckWatermarkInvalid
	}
	return int64(binary.LittleEndian.Uint64(w.buf[qwpSfAckWatermarkFsnOffset : qwpSfAckWatermarkFsnOffset+8]))
}

// storeLocked writes fsn into the mapped region. Caller MUST hold
// w.mu and have checked !w.closed. FSN is stored before the magic so
// that, within a live page cache, a reader which observes the magic
// (stamped second, in program order) also observes a valid FSN — this
// keeps a clean restart from honouring magic=AKW1 over a still-zero
// FSN. No memory fence is needed: the same goroutine performs both
// stores and read() runs only at startup. The ordering is an
// intra-process / page-cache property; it does not survive a torn
// power-loss write, whose residual silent-loss window the no-CRC /
// no-fsync note above covers.
func (w *qwpSfAckWatermark) storeLocked(fsn int64) {
	binary.LittleEndian.PutUint64(w.buf[qwpSfAckWatermarkFsnOffset:qwpSfAckWatermarkFsnOffset+8], uint64(fsn))
	if !w.magicWritten {
		binary.LittleEndian.PutUint32(w.buf[qwpSfAckWatermarkMagicOffset:qwpSfAckWatermarkMagicOffset+4], qwpSfAckWatermarkMagic)
		w.magicWritten = true
	}
}

// persistIfAdvanced stores fsn iff it advanced past the last value
// persisted this session, returning true if it wrote. The gate keeps
// the dirty-page footprint minimal under steady-state load with no
// new acks arriving. No-op after close. This is the segment manager's
// entry point, called once per maintenance tick BEFORE trim so the
// on-disk ordering recovery's max() clamp relies on holds across a
// crash in either order (sf-client.md §5.4).
func (w *qwpSfAckWatermark) persistIfAdvanced(fsn int64) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || fsn <= w.lastPersistedAck {
		return false
	}
	w.storeLocked(fsn)
	w.lastPersistedAck = fsn
	return true
}

// close unmaps the region and closes the fd. Idempotent and
// safe to call concurrently with a manager-tick persistIfAdvanced:
// the mutex serialises them, and a store that loses the race observes
// closed==true and returns without touching the (now unmapped) buffer.
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
