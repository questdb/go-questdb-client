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

// qwpSfSymbolDict is the append-only, per-slot persistence of the global
// symbol dictionary a store-and-forward sender ships with delta encoding. It
// lives at `<slot>/.symbol-dict` alongside the segment files, the slot lock
// and `.ack-watermark`.
//
// Delta-encoded SF frames are NOT self-sufficient: a frame carries only the
// symbols it introduces, so recovering (process restart) or draining (orphan
// adoption) a slot must re-register the whole dictionary on the fresh server
// before those frames replay. This file is that dictionary. Unlike the
// ack-watermark — a discardable optimisation guarded by a max() clamp — it is
// load-bearing: a surviving frame that references an id missing from it is
// unrecoverable. Proven absence/corruption degrades to full self-sufficient
// frames, while ambiguous legacy bytes and operational I/O failures abort
// startup without destroying the file.
//
// On-disk layout (little-endian), byte-compatible with the Java client:
//
//	offset 0: u32 magic = 'SYD1'
//	offset 4: u8  version = 1
//	offset 5: 3 bytes reserved (zero)
//	offset 8: chunks, each
//	          [entryCount: uvarint][entryBytes: uvarint][entries][crc32c: u32]
//	          where entries is entryCount repetitions of [len: uvarint][utf8]
//	          occupying exactly entryBytes bytes, and CRC-32C covers both
//	          header varints plus the entries region.
//
// One appendSymbols call writes one chunk. Symbol id i is the i-th entry across
// all chunks (ids are dense from 0), so no id is stored.
//
// Durability: the producer appends the symbols a frame introduces BEFORE that
// frame is published to the ring, but does NOT fsync — matching the rest of
// store-and-forward (page-cache, not disk, durable). This ordering suffices for
// a process crash (the page cache survives, so the dictionary stays a superset
// of every recoverable frame's references). It does NOT survive a host/power
// crash that tears the dictionary relative to its frames — per-chunk CRC-32C
// stops recovery at the first untrusted chunk, and the send loop's guard fails
// loudly if a surviving frame references beyond that trusted prefix. A torn
// trailing chunk is physically truncated before the next append.
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
	// Java rejects a persisted-dictionary varint after shift exceeds 35,
	// allowing at most six bytes including the terminating byte.
	qwpSfSymbolDictMaxVarintLen = 6
	// qwpSfSymbolDictMaxEntryLen bounds one symbol's decoded length so a torn
	// or corrupt length prefix cannot drive a runaway allocation. Symbols are
	// short; this ceiling is generous.
	qwpSfSymbolDictMaxEntryLen = 1 << 20
	// qwpSfSymbolDictMaxFileSize bounds the whole-file read at open so a torn
	// or foreign file cannot drive a multi-GB allocation on a background
	// recovery/drainer goroutine. Even pathological high-cardinality symbol use
	// stays far below this.
	qwpSfSymbolDictMaxFileSize = 1 << 30
	// Bound the initial []string allocation independently of the byte-region
	// limit. The final length is separately bounded by
	// qwpMaxSymbolDictionarySize; this smaller initial capacity avoids eagerly
	// reserving the full bounded maximum for a malformed chunk.
	qwpSfSymbolDictMaxPreallocEntries = 1 << 16
)

var (
	// qwpSfErrSymbolDictAmbiguousFormat is operational, not a recovery
	// fail-closed verdict: the bytes may be healthy legacy data or a torn first
	// chunk and must not be auto-quarantined or recreated.
	//lint:ignore ST1012 prefix kept for grouping with other qwpSf* errors
	qwpSfErrSymbolDictAmbiguousFormat = errors.New("qwp/sf: ambiguous .symbol-dict format")
	qwpSfSymbolDictCRCTable           = crc32.MakeTable(crc32.Castagnoli)
	qwpSfSymbolDictStat               = os.Stat
	qwpSfSymbolDictTruncate           = func(f *os.File, size int64) error { return f.Truncate(size) }
	qwpSfSymbolDictWriteAt            = func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p, off) }
)

// qwpSfSymbolDictOpen opens (creating if absent) the dictionary file in
// slotDir. An existing chunked file's valid prefix is loaded; an ordinary
// invalid file is recreated. A legacy flat file is never recreated because it
// may be the only id-to-name map for unacked delta frames.
func qwpSfSymbolDictOpen(slotDir string) (*qwpSfSymbolDict, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	st, statErr := qwpSfSymbolDictStat(path)
	if statErr == nil {
		if st.Size() >= qwpSfSymbolDictHeaderSize {
			d, openErr := qwpSfSymbolDictOpenExisting(path, st.Size())
			if openErr != nil {
				return nil, openErr
			}
			if d != nil {
				return d, nil
			}
			// A header/parse failure on an existing file means it cannot be
			// trusted for delta replay; start clean.
		}
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("qwp/sf: could not stat symbol dictionary %s: %w", path, statErr)
	}
	return qwpSfSymbolDictOpenFresh(path), nil
}

// qwpSfSymbolDictOpenRecovered opens the dictionary for a slot recovered from
// disk. Unlike qwpSfSymbolDictOpen it NEVER recreates: a recovered slot's
// segments hold delta frames that reference the dictionary's ids by position,
// so silently truncating a corrupt or version-mismatched file would restart the
// id space and re-register the wrong id→name map. It returns:
//
//   - (dict, nil) when the file exists and parses,
//   - (nil, nil)  when the file is absent — the caller falls back to full
//     self-sufficient frames for the recovered segments,
//   - (nil, err)  when the file exists but is corrupt/unreadable/wrong-version,
//     which the caller propagates as a fatal recovery error (a sanctioned
//     terminal: quarantine rather than replay against a mismatched dictionary).
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
		return nil, fmt.Errorf("qwp/sf: recovered symbol dictionary %s is truncated (%d bytes)", path, st.Size())
	}
	d, err := qwpSfSymbolDictOpenExisting(path, st.Size())
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("qwp/sf: recovered symbol dictionary %s is corrupt or unreadable", path)
	}
	return d, nil
}

// qwpSfSymbolDictRemoveOrphan best-effort removes a stale dictionary file.
// Used at fresh-start (a dict with no segments behind it is meaningless) and at
// fully-drained close (nothing references it any more). No-op for memory mode.
func qwpSfSymbolDictRemoveOrphan(slotDir string) {
	if slotDir == "" {
		return
	}
	_ = os.Remove(filepath.Join(slotDir, qwpSfSymbolDictFileName))
}

func qwpSfSymbolDictOpenExisting(path string, fileLen int64) (*qwpSfSymbolDict, error) {
	if fileLen > qwpSfSymbolDictMaxFileSize {
		return nil, nil
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
		return nil, nil
	}

	loaded, validLen, validChunks, entryLimitExceeded := qwpSfSymbolDictScanChunks(buf)
	if entryLimitExceeded {
		_ = f.Close()
		return nil, fmt.Errorf("qwp/sf: symbol dictionary %s exceeds the %d-entry limit", path, qwpMaxSymbolDictionarySize)
	}
	if validChunks == 0 && fileLen > qwpSfSymbolDictHeaderSize {
		_ = f.Close()
		if qwpSfSymbolDictLegacyEntryCount(buf[qwpSfSymbolDictHeaderSize:]) > 0 {
			return nil, fmt.Errorf("%w: body is ambiguous between the legacy flat format and a torn first chunk; if written by an older client, drain this slot with go-questdb-client <= v4.x; otherwise restore the file, or delete %s to fall back to full-dict frames (safe only if the slot holds no unacked delta frames), or remove the slot after draining",
				qwpSfErrSymbolDictAmbiguousFormat, path)
		}
		return nil, nil
	}
	if validLen < len(buf) {
		if err := qwpSfSymbolDictTruncate(f, int64(validLen)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("qwp/sf: could not drop torn/stale symbol dictionary tail %s: %w", path, err)
		}
	}
	return &qwpSfSymbolDict{
		file:         f,
		appendOffset: int64(validLen),
		count:        len(loaded),
		loaded:       loaded,
	}, nil
}

// qwpSfSymbolDictScanChunks returns the symbols in the CRC-proven prefix, its
// physical end offset, the number of valid chunks, and whether a checksum-valid
// chunk would exceed the protocol entry ceiling. The first malformed, torn,
// inconsistent, or checksum-failing chunk ends the trusted prefix.
func qwpSfSymbolDictScanChunks(buf []byte) (loaded []string, validLen int, validChunks int, entryLimitExceeded bool) {
	validLen = int(qwpSfSymbolDictHeaderSize)
	pos := validLen
	for pos < len(buf) {
		chunkStart := pos
		entryCount, next, ok := qwpSfSymbolDictReadVarint(buf, pos, len(buf))
		if !ok {
			break
		}
		entryBytes, entriesStart, ok := qwpSfSymbolDictReadVarint(buf, next, len(buf))
		if !ok || entryCount == 0 || entryBytes == 0 {
			break
		}
		// Every entry consumes at least its one-byte length varint. Reject an
		// internally impossible count as a malformed tail before classifying a
		// checksum-valid but genuinely oversized dictionary.
		if entryCount > entryBytes || entryCount > uint64(^uint(0)>>1)-uint64(len(loaded)) || entryBytes > uint64(len(buf)-entriesStart) {
			break
		}
		chunkEnd := entriesStart + int(entryBytes)
		if chunkEnd > len(buf)-qwpSfSymbolDictCRCSize {
			break
		}
		storedCRC := binary.LittleEndian.Uint32(buf[chunkEnd : chunkEnd+qwpSfSymbolDictCRCSize])
		if crc32.Checksum(buf[chunkStart:chunkEnd], qwpSfSymbolDictCRCTable) != storedCRC {
			break
		}
		if entryCount > uint64(qwpMaxSymbolDictionarySize-len(loaded)) {
			return loaded, validLen, validChunks, true
		}
		entries, ok := qwpSfSymbolDictParseEntries(buf[entriesStart:chunkEnd], entryCount)
		if !ok {
			break
		}
		loaded = append(loaded, entries...)
		pos = chunkEnd + qwpSfSymbolDictCRCSize
		validLen = pos
		validChunks++
	}
	return loaded, validLen, validChunks, false
}

func qwpSfSymbolDictParseEntries(region []byte, expected uint64) ([]string, bool) {
	if expected > qwpMaxSymbolDictionarySize {
		return nil, false
	}
	// Every entry occupies at least its one-byte length varint.
	if expected > uint64(len(region)) {
		return nil, false
	}
	prealloc := expected
	if prealloc > qwpSfSymbolDictMaxPreallocEntries {
		prealloc = qwpSfSymbolDictMaxPreallocEntries
	}
	entries := make([]string, 0, int(prealloc))
	pos := 0
	for uint64(len(entries)) < expected {
		entryLen, next, ok := qwpSfSymbolDictReadVarint(region, pos, len(region))
		if !ok || entryLen > qwpSfSymbolDictMaxEntryLen || entryLen > uint64(len(region)-next) {
			return nil, false
		}
		end := next + int(entryLen)
		entries = append(entries, string(region[next:end]))
		pos = end
	}
	return entries, pos == len(region)
}

// qwpSfSymbolDictReadVarint decodes canonical unsigned LEB128 within
// [pos, limit). Persisted dictionary varints are deliberately stricter than
// general QWP wire varints to match the Java file format's six-byte ceiling.
func qwpSfSymbolDictReadVarint(buf []byte, pos, limit int) (uint64, int, bool) {
	var value uint64
	for i := 0; i < qwpSfSymbolDictMaxVarintLen && pos+i < limit; i++ {
		b := buf[pos+i]
		value |= uint64(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			if i > 0 && b == 0 {
				return 0, 0, false
			}
			return value, pos + i + 1, true
		}
	}
	return 0, 0, false
}

// qwpSfSymbolDictLegacyEntryCount probes the pre-upgrade flat body without
// trusting it. Any complete legacy entry makes the version-1 file ambiguous:
// it may be healthy legacy data or a torn first chunk, so callers fail closed
// and preserve the bytes for explicit migration.
func qwpSfSymbolDictLegacyEntryCount(body []byte) int {
	count := 0
	pos := 0
	for pos < len(body) {
		entryLen, n, err := qwpReadVarint(body[pos:])
		if err != nil {
			break
		}
		start := pos + n
		if entryLen > qwpSfSymbolDictMaxEntryLen || entryLen > uint64(len(body)-start) {
			break
		}
		count++
		pos = start + int(entryLen)
	}
	return count
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
	d.scratch = d.scratch[:0]
	var vb [binary.MaxVarintLen64]byte
	entriesLen := 0
	for _, name := range names {
		if len(name) > qwpSfSymbolDictMaxEntryLen {
			return fmt.Errorf("qwp/sf: symbol dictionary entry exceeds %d bytes", qwpSfSymbolDictMaxEntryLen)
		}
		wireLen := binary.PutUvarint(vb[:], uint64(len(name))) + len(name)
		entriesLen += wireLen
	}
	// Reuse the existing scratch capacity on the flush path. The capacity hint
	// covers both header varints and the CRC; the encoded bytes themselves use
	// canonical varints.
	if cap(d.scratch) < entriesLen+2*binary.MaxVarintLen64+qwpSfSymbolDictCRCSize {
		d.scratch = make([]byte, 0, entriesLen+2*binary.MaxVarintLen64+qwpSfSymbolDictCRCSize)
	}
	n := binary.PutUvarint(vb[:], uint64(len(names)))
	d.scratch = append(d.scratch, vb[:n]...)
	n = binary.PutUvarint(vb[:], uint64(entriesLen))
	d.scratch = append(d.scratch, vb[:n]...)
	for _, name := range names {
		n = binary.PutUvarint(vb[:], uint64(len(name)))
		d.scratch = append(d.scratch, vb[:n]...)
		d.scratch = append(d.scratch, name...)
	}
	crc := crc32.Checksum(d.scratch, qwpSfSymbolDictCRCTable)
	d.scratch = binary.LittleEndian.AppendUint32(d.scratch, crc)
	written, err := qwpSfSymbolDictWriteAt(d.file, d.scratch, d.appendOffset)
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
// symbol id i). Empty when nothing was recovered.
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
