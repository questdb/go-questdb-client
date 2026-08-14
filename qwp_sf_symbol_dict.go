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
	"os"
	"path/filepath"
	"sync"
)

// errQwpSfSymbolDictUnusable marks content proven unusable for recovery (a
// short/oversized file or bad magic/version), as distinct from a transient
// stat/open/read/truncate failure. Recovery may fall back to the surviving
// frames for the former but must leave a potentially valid file intact and
// retry the latter.
var errQwpSfSymbolDictUnusable = errors.New("qwp/sf: unusable symbol dictionary")

// qwpSfSymbolDict is the append-only, per-slot persistence of the global
// symbol dictionary a store-and-forward sender ships with delta encoding. It
// lives at `<slot>/.symbol-dict` alongside the segment files, the slot lock
// and `.ack-watermark`.
//
// Delta-encoded SF frames are NOT self-sufficient: a frame carries only the
// symbols it introduces, so recovering (process restart) or draining (orphan
// adoption) a slot must re-register the whole dictionary on the fresh server
// before those frames replay. This file is that dictionary. Unlike the
// ack-watermark — a discardable optimisation guarded by a max() clamp — its
// trusted prefix is load-bearing whenever surviving frames do not themselves
// prove the missing ids. Recovery folds both sources and fails before replay if
// neither establishes contiguous coverage.
//
// On-disk layout (little-endian):
//
//	offset 0: u32 magic = 'SYD1'
//	offset 4: u8  version = 1
//	offset 5: 3 bytes reserved (zero)
//	offset 8: chunks, each
//	          [entryCount: varint][entryBytes: varint][entries][crc32c: u32]
//	          entries = [len: varint][utf8] repeated entryCount times;
//	          crc32c covers both header varints and the entry region
//
// Symbol id i is the i-th entry (ids are dense from 0), so no id is stored.
//
// Durability: the producer appends the symbols a frame introduces BEFORE that
// frame is published to the ring, but does NOT fsync — matching the rest of
// store-and-forward (page-cache, not disk, durable). This ordering suffices for
// a process crash (the page cache survives, so the dictionary stays a superset
// of every recoverable frame's references). It does NOT survive a host/power
// crash that tears the dictionary relative to its frames. Each append is one
// Java-compatible CRC-32C chunk: recovery trusts only the checksum-valid prefix
// and then folds surviving frames to rebuild and heal any provable suffix. A
// remaining gap is rejected before connection (with a send-loop guard as
// defense in depth), turning a detectable tear into "resend required" instead
// of silently shifting the dense id-to-symbol mapping.
//
// Single-writer (the producer goroutine). loaded is read once at open to seed
// recovery/orphan-drain; the engine owns close. The mutex serialises append
// against close so a Close racing an in-flight flush cannot write a closed fd.
type qwpSfSymbolDict struct {
	mu           sync.Mutex
	file         *os.File
	appendOffset int64
	count        int
	closed       bool
	scratch      []byte
	// loaded holds the entries recovered at open, in id order; nil for a
	// freshly created file. Consumed once to seed the producer's global
	// dictionary and the send loop's catch-up mirror.
	loaded []string
}

const (
	qwpSfSymbolDictFileName          = ".symbol-dict"
	qwpSfSymbolDictMagic      uint32 = 0x31445953 // 'SYD1' little-endian
	qwpSfSymbolDictHeaderSize int64  = 8
	qwpSfSymbolDictVersion    byte   = 1
	qwpSfSymbolDictCRCSize           = 4
	// qwpSfSymbolDictMaxEntryLen bounds one symbol's decoded length so a torn
	// or corrupt length prefix cannot drive a runaway allocation. Symbols are
	// short; this ceiling is generous.
	qwpSfSymbolDictMaxEntryLen = 1 << 20
	// qwpSfSymbolDictMaxFileSize bounds the whole-file read buffer at open;
	// the parsed entry count is bounded separately by
	// qwpSfSymbolDictMaxRecoveredEntries (empty-string entries amplify ~16x
	// in string headers, so the byte cap alone cannot bound the allocation).
	qwpSfSymbolDictMaxFileSize = 1 << 30
	// qwpSfSymbolDictMaxRecoveredEntries caps recovery parsing so a crafted
	// CRC-valid file cannot drive an unbounded []string allocation on a
	// recovery/drainer goroutine. Far above the admission cap: an over-cap
	// slot from an older client must keep every positional id.
	qwpSfSymbolDictMaxRecoveredEntries = 4 * qwpMaxSymbolDictionarySize
)

// qwpSfSymbolDictOpen opens (creating if absent) the dictionary file in
// slotDir. An existing file's complete entries are loaded into memory; a
// missing/invalid file is (re)created with a fresh header. Returns nil on any
// unrecoverable I/O failure — the caller then falls back to full self-
// sufficient frames for the slot, so a broken side-file degrades gracefully.
func qwpSfSymbolDictOpen(slotDir string) *qwpSfSymbolDict {
	if slotDir == "" {
		return nil
	}
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	if st, err := os.Stat(path); err == nil && st.Size() >= qwpSfSymbolDictHeaderSize {
		if d := qwpSfSymbolDictOpenExisting(path, st.Size()); d != nil {
			return d
		}
		// A header/parse failure on an existing file means it cannot be
		// trusted for delta replay; start clean.
	}
	return qwpSfSymbolDictOpenFresh(path)
}

// qwpSfSymbolDictOpenClean starts a fresh slot with an empty side-file. Unlike
// qwpSfSymbolDictOpen, it never inherits entries left by a previous lifecycle:
// an existing file must be successfully truncated or construction fails. If
// the file was provably absent and cannot be created, nil is a safe degraded
// result because there is no stale id mapping for a later recovery to trust.
func qwpSfSymbolDictOpenClean(slotDir string) (*qwpSfSymbolDict, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("qwp/sf: inspect fresh symbol dictionary %s: %w", path, statErr)
	}
	d := qwpSfSymbolDictOpenFresh(path)
	if d != nil {
		return d, nil
	}
	if existed {
		return nil, fmt.Errorf("qwp/sf: existing symbol dictionary %s could not be truncated for a fresh slot", path)
	}
	return nil, nil
}

// qwpSfSymbolDictOpenRecovered opens the dictionary for a slot recovered from
// disk. Unlike qwpSfSymbolDictOpen it NEVER recreates: a recovered slot's
// segments hold delta frames that reference the dictionary's ids by position,
// so silently truncating a corrupt or version-mismatched file would restart the
// id space and re-register the wrong id→name map. The engine subsequently
// folds surviving frames over any trusted prefix and decides whether replay is
// provably safe. It returns:
//
//   - (dict, nil) when the header is valid; only its CRC-proven chunk prefix is
//     loaded and any torn/corrupt suffix is truncated,
//   - (nil, nil) when the file is absent or its content is provably unusable
//     (short, bad magic/version, or over the defensive read bound); the original
//     file is preserved and frame analysis determines recoverability,
//   - (nil, err) for transient stat/open/read failures or when an untrusted tail
//     cannot be truncated; the caller propagates the recovery failure.
func qwpSfSymbolDictOpenRecovered(slotDir string) (*qwpSfSymbolDict, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if st.Size() < qwpSfSymbolDictHeaderSize {
		// Proven content damage, not an I/O outage. Leave the stub intact and
		// let the recovered-frame fold decide whether the slot is still
		// self-sufficient.
		return nil, nil
	}
	d, openErr := qwpSfSymbolDictOpenExistingDetailed(path, st.Size())
	if errors.Is(openErr, errQwpSfSymbolDictUnusable) {
		return nil, nil
	}
	if openErr != nil {
		return nil, fmt.Errorf("qwp/sf: recover symbol dictionary %s: %w", path, openErr)
	}
	return d, nil
}

// qwpSfSymbolDictRemoveOrphan best-effort removes a stale dictionary file at
// fully-drained close (nothing references it any more; fresh slots instead
// truncate in place via qwpSfSymbolDictOpenClean). No-op for memory mode.
func qwpSfSymbolDictRemoveOrphan(slotDir string) {
	if slotDir == "" {
		return
	}
	_ = os.Remove(filepath.Join(slotDir, qwpSfSymbolDictFileName))
}

// qwpSfSymbolDictOpenExisting is the error-agnostic form for callers that
// treat every failure the same way (recreate fresh).
func qwpSfSymbolDictOpenExisting(path string, fileLen int64) *qwpSfSymbolDict {
	d, _ := qwpSfSymbolDictOpenExistingDetailed(path, fileLen)
	return d
}

// qwpSfSymbolDictOpenExistingDetailed opens an existing side-file, loading its
// CRC-proven chunk prefix and truncating any untrusted tail. Failures split
// into errQwpSfSymbolDictUnusable (proven content damage — recovery may fall
// back to the frame fold) and transient I/O errors (retryable; propagated).
func qwpSfSymbolDictOpenExistingDetailed(path string, fileLen int64) (*qwpSfSymbolDict, error) {
	if fileLen > qwpSfSymbolDictMaxFileSize {
		return nil, errQwpSfSymbolDictUnusable
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, fileLen)
	if _, err := io.ReadFull(f, buf); err != nil {
		_ = f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(buf[:4]) != qwpSfSymbolDictMagic || buf[4] != qwpSfSymbolDictVersion {
		_ = f.Close()
		return nil, errQwpSfSymbolDictUnusable
	}
	loaded, pos, chunks := qwpSfParseChunkedSymbolDict(buf)
	if len(buf) > int(qwpSfSymbolDictHeaderSize) && chunks == 0 {
		// The pre-checksum Go prototype used the same version byte and a flat
		// entry stream, so it cannot be distinguished safely from a corrupted
		// first Java chunk. Do not accept structural plausibility as integrity:
		// preserve the file and let the frame fold prove recovery from scratch.
		_ = f.Close()
		return nil, errQwpSfSymbolDictUnusable
	}
	// A later append may be shorter than the torn tail. Merely writing at pos
	// would then leave the tail's suffix in place, and a subsequent recovery
	// could parse that suffix as one or more real entries (shifting every id
	// above it). Drop all untrusted bytes before exposing the append handle.
	if int64(pos) < fileLen {
		if err := f.Truncate(int64(pos)); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return &qwpSfSymbolDict{
		file:         f,
		appendOffset: int64(pos),
		count:        len(loaded),
		loaded:       loaded,
	}, nil
}

// qwpSfParseChunkedSymbolDict validates the Java-compatible per-append chunk
// stream and returns only its CRC-proven prefix. pos is the first untrusted byte
// (or len(buf)); chunks is the number of complete chunks accepted.
func qwpSfParseChunkedSymbolDict(buf []byte) (loaded []string, pos, chunks int) {
	pos = int(qwpSfSymbolDictHeaderSize)
	for pos < len(buf) {
		chunkStart := pos
		entryCount, n, err := qwpReadVarint(buf[pos:])
		// Not capped at the one-million admission limit — an existing over-cap
		// entry must retain its positional id. Bounded only by
		// qwpSfSymbolDictMaxRecoveredEntries (crafted-file allocation guard).
		if err != nil || entryCount == 0 ||
			entryCount > uint64(qwpSfSymbolDictMaxRecoveredEntries-len(loaded)) {
			break
		}
		pos += n
		entryBytes, n, err := qwpReadVarint(buf[pos:])
		if err != nil || entryBytes == 0 {
			pos = chunkStart
			break
		}
		pos += n
		if entryBytes > uint64(len(buf)-pos) {
			pos = chunkStart
			break
		}
		entriesEnd := pos + int(entryBytes)
		if entriesEnd > len(buf)-qwpSfSymbolDictCRCSize {
			pos = chunkStart
			break
		}
		storedCRC := binary.LittleEndian.Uint32(buf[entriesEnd : entriesEnd+qwpSfSymbolDictCRCSize])
		if crc32.Checksum(buf[chunkStart:entriesEnd], qwpSfCrcTable) != storedCRC {
			pos = chunkStart
			break
		}

		before := len(loaded)
		p := pos
		valid := true
		for i := uint64(0); i < entryCount; i++ {
			entryLen, adv, err := qwpReadVarint(buf[p:entriesEnd])
			if err != nil || entryLen > qwpSfSymbolDictMaxEntryLen {
				valid = false
				break
			}
			p += adv
			if entryLen > uint64(entriesEnd-p) {
				valid = false
				break
			}
			loaded = append(loaded, string(buf[p:p+int(entryLen)]))
			p += int(entryLen)
		}
		if !valid || p != entriesEnd {
			loaded = loaded[:before]
			pos = chunkStart
			break
		}
		pos = entriesEnd + qwpSfSymbolDictCRCSize
		chunks++
	}
	return loaded, pos, chunks
}

func qwpSfSymbolDictOpenFresh(path string) *qwpSfSymbolDict {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil
	}
	var hdr [qwpSfSymbolDictHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], qwpSfSymbolDictMagic)
	hdr[4] = qwpSfSymbolDictVersion
	if _, err := f.WriteAt(hdr[:], 0); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil
	}
	return &qwpSfSymbolDict{file: f, appendOffset: qwpSfSymbolDictHeaderSize}
}

// appendSymbols durably extends the dictionary with names in ascending-id
// order, in one write, before the referencing frame is published. Not fsync'd
// (see the type doc). A no-op after close; a short/failed write returns the
// error so the caller can withhold the frame rather than persist a dictionary
// the frame outlives.
func (d *qwpSfSymbolDict) appendSymbols(names []string) error {
	if d == nil || len(names) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	if len(names) > qwpMaxSymbolDictionarySize-d.count {
		return fmt.Errorf("qwp/sf: symbol dictionary exceeds maximum size %d", qwpMaxSymbolDictionarySize)
	}
	d.scratch = d.scratch[:0]
	for _, name := range names {
		if len(name) > qwpSfSymbolDictMaxEntryLen {
			return fmt.Errorf("qwp/sf: symbol dictionary entry length %d exceeds limit %d",
				len(name), qwpSfSymbolDictMaxEntryLen)
		}
		var vb [qwpMaxVarintLen]byte
		n := qwpPutVarint(vb[:], uint64(len(name)))
		d.scratch = append(d.scratch, vb[:n]...)
		d.scratch = append(d.scratch, name...)
	}
	entriesLen := len(d.scratch)
	var hdr [2 * qwpMaxVarintLen]byte
	hdrLen := qwpPutVarint(hdr[:], uint64(len(names)))
	hdrLen += qwpPutVarint(hdr[hdrLen:], uint64(entriesLen))
	// Prepend the two varints in-place and reserve the trailing CRC. copy
	// has memmove semantics, so the overlapping shift is safe.
	d.scratch = append(d.scratch, make([]byte, hdrLen+qwpSfSymbolDictCRCSize)...)
	copy(d.scratch[hdrLen:hdrLen+entriesLen], d.scratch[:entriesLen])
	copy(d.scratch[:hdrLen], hdr[:hdrLen])
	bodyEnd := hdrLen + entriesLen
	binary.LittleEndian.PutUint32(
		d.scratch[bodyEnd:bodyEnd+qwpSfSymbolDictCRCSize],
		crc32.Checksum(d.scratch[:bodyEnd], qwpSfCrcTable),
	)
	written, err := d.file.WriteAt(d.scratch, d.appendOffset)
	if err != nil {
		return err
	}
	if written != len(d.scratch) {
		return io.ErrShortWrite
	}
	d.appendOffset += int64(len(d.scratch))
	d.count += len(names)
	return nil
}

// loadedSymbols returns the entries recovered at open, in id order (entry i is
// symbol id i). Empty when nothing was recovered. Open-time snapshot only:
// later appendSymbols advance size() but never extend this list.
func (d *qwpSfSymbolDict) loadedSymbols() []string {
	if d == nil {
		return nil
	}
	return d.loaded
}

// size is the number of symbols the dictionary holds (highest id + 1).
func (d *qwpSfSymbolDict) size() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

func (d *qwpSfSymbolDict) close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	// Drop the recovered-entry copy at teardown. The send-loop mirror and the
	// producer's global dictionary both read it during construction and leave it
	// in place (each is a consumer; neither can tell it is the last), so the copy
	// lives until the dict is closed — the slot's whole lifetime for a running
	// sender, the drain for an orphan drainer.
	d.loaded = nil
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		return err
	}
	return nil
}
