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

// errQwpSfSymbolDictUnusable says the file's content cannot be used: it is too
// short or too long, or its magic or version is wrong. That is a different
// situation from a stat/open/read/truncate error, which may well go away on a
// later attempt. Recovery falls back to the surviving frames for unusable
// content, but leaves a possibly good file alone and retries an I/O error.
var errQwpSfSymbolDictUnusable = errors.New("qwp/sf: unusable symbol dictionary")

// qwpSfSymbolDict is the append-only, per-slot persistence of the global
// symbol dictionary a store-and-forward sender ships with delta encoding. It
// lives at `<slot>/.symbol-dict` alongside the segment files, the slot lock
// and `.ack-watermark`.
//
// Delta-encoded SF frames are NOT self-sufficient: a frame carries only the
// symbols it introduces, so recovering (process restart) or draining (orphan
// adoption) a slot must re-register the whole dictionary on the fresh server
// before those frames replay. This file is that dictionary. The ack-watermark
// next to it is only an optimisation and can be thrown away, but this file
// cannot: whenever the surviving frames do not spell out an id themselves, its
// trusted entries are the only remaining source. Recovery reads both, and if
// together they still leave a hole, it fails before any replay starts.
//
// On-disk layout (little-endian), byte-compatible with the Java client:
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
// crash that leaves the dictionary out of step with its frames. Each append is
// one CRC-32C chunk, in the same format the Java client writes: recovery keeps
// only the run of chunks whose checksums match, then reads the surviving frames
// to rebuild whatever ids came after that and writes them back. If a hole is
// still left, recovery fails before connecting (and the send loop checks again
// before each frame goes out), so a detectable tear turns into "resend
// required" rather than quietly shifting every id to a different symbol.
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
	// qwpSfSymbolDictMaxFileSize limits the buffer open() reads the file into.
	// The number of entries needs its own limit, qwpSfSymbolDictMaxRecoveredEntries:
	// one empty-string entry is a single byte on disk but about 16 bytes of
	// string header in memory, so a byte limit alone says little about how much
	// gets allocated.
	qwpSfSymbolDictMaxFileSize = 1 << 30
	// qwpSfSymbolDictMaxRecoveredEntries limits how many entries recovery will
	// parse, so a hand-crafted file with valid checksums cannot make a recovery
	// or drainer goroutine allocate an arbitrarily large []string. Set well
	// above qwpMaxSymbolDictionarySize, because a slot written by an older
	// client may hold more than that and every id still has to keep its place.
	qwpSfSymbolDictMaxRecoveredEntries = 4 * qwpMaxSymbolDictionarySize
	// qwpSfSymbolDictMaxPreallocEntries bounds the initial []string
	// allocation independently of the byte-region limit, so a malformed chunk
	// header cannot make the parser reserve the full recovered-entry ceiling
	// up front. The slice still grows to whatever the chunk really holds.
	qwpSfSymbolDictMaxPreallocEntries = 1 << 16
)

// Filesystem calls the dictionary makes, indirected so tests can inject the
// failures a real disk produces rarely: a stat outage, a refused truncate, and
// a short write.
var (
	qwpSfSymbolDictStat     = qwpSfSwappable(os.Stat)
	qwpSfSymbolDictTruncate = qwpSfSwappable(func(f *os.File, size int64) error { return f.Truncate(size) })
	qwpSfSymbolDictWriteAt  = qwpSfSwappable(func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p, off) })
)

// qwpSfSymbolDictOpen opens (creating if absent) the dictionary file in
// slotDir. An existing file's trusted chunk prefix is loaded into memory; a
// missing or untrustworthy file is (re)created with a fresh header. A nil dict
// with no error means the file could not be created, and the caller falls back
// to full self-sufficient frames for the slot. A stat failure is reported as an
// error: without knowing whether a file is there, recreating it could destroy
// the only id-to-name map a surviving delta frame has.
func qwpSfSymbolDictOpen(slotDir string) (*qwpSfSymbolDict, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	st, statErr := qwpSfSymbolDictStat.load()(path)
	if statErr == nil {
		if st.Size() >= qwpSfSymbolDictHeaderSize {
			if d := qwpSfSymbolDictOpenExisting(path, st.Size()); d != nil {
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

// qwpSfSymbolDictOpenClean starts a fresh slot with an empty side-file. It
// never takes over entries a previous run left behind: if a file is already
// there it must be truncated, and construction fails if that does not work.
// When there was no file to begin with and one cannot be created, returning nil
// is safe — the slot then runs on full self-sufficient frames, and there are no
// stale entries on disk for a later recovery to believe.
func qwpSfSymbolDictOpenClean(slotDir string) (*qwpSfSymbolDict, error) {
	if slotDir == "" {
		return nil, nil
	}
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	_, statErr := qwpSfSymbolDictStat.load()(path)
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
// so quietly truncating a corrupt or version-mismatched file would restart the
// ids at 0 and give them the wrong names. The engine then reads the surviving
// frames on top of whatever this returns and decides whether replay is safe.
// It returns:
//
//   - (dict, nil) when the header is valid. Only the run of chunks whose
//     checksums match is loaded, and anything after it is truncated.
//   - (nil, nil) when the file is absent, or its content cannot be used (too
//     short, bad magic or version, or larger than the read limit). The file is
//     left as it is, and the frame scan decides whether the slot is recoverable.
//   - (nil, err) when stat/open/read fails, or the untrusted tail cannot be
//     truncated. The caller turns that into a recovery failure.
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
		// The content is damaged; this is not an I/O outage that might clear
		// up. Leave the stub on disk and let the frame scan decide whether the
		// slot can still be recovered.
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

// qwpSfSymbolDictRemoveOrphan removes a stale dictionary file, best-effort, at
// a fully-drained close when nothing refers to it any more. A fresh slot does
// not come through here; it truncates the file in place via
// qwpSfSymbolDictOpenClean. No-op for memory mode.
func qwpSfSymbolDictRemoveOrphan(slotDir string) {
	if slotDir == "" {
		return
	}
	_ = os.Remove(filepath.Join(slotDir, qwpSfSymbolDictFileName))
}

// qwpSfSymbolDictOpenExisting drops the error, for callers that react the same
// way to every failure: recreate the file from scratch.
func qwpSfSymbolDictOpenExisting(path string, fileLen int64) *qwpSfSymbolDict {
	d, _ := qwpSfSymbolDictOpenExistingDetailed(path, fileLen)
	return d
}

// qwpSfSymbolDictOpenExistingDetailed opens an existing side-file, loads the
// run of chunks whose checksums match, and truncates whatever follows. Failures
// come in two kinds: errQwpSfSymbolDictUnusable for damaged content, which lets
// recovery fall back to reading the surviving frames, and plain I/O errors,
// which may succeed on a later attempt and are returned as they are.
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
		// An earlier Go prototype wrote a flat entry stream with no checksums
		// under this same version byte, and there is no reliable way to tell
		// such a file apart from one whose first chunk is corrupt. Content that
		// merely parses is not content that can be trusted, so keep the file
		// and let the frame scan rebuild the dictionary from nothing.
		_ = f.Close()
		return nil, errQwpSfSymbolDictUnusable
	}
	// Cut the file back to the last trusted byte before handing out a handle
	// that can append. The next append may be shorter than the tail being
	// replaced, and writing at pos alone would leave the rest of that tail on
	// disk, where a later recovery could read it as real entries and shift
	// every id after it.
	if int64(pos) < fileLen {
		if err := qwpSfSymbolDictTruncate.load()(f, int64(pos)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("qwp/sf: could not drop torn/stale symbol dictionary tail %s: %w", path, err)
		}
	}
	return &qwpSfSymbolDict{
		file:         f,
		appendOffset: int64(pos),
		count:        len(loaded),
		loaded:       loaded,
	}, nil
}

// qwpSfParseChunkedSymbolDict reads the one-chunk-per-append stream, in the
// format the Java client also writes, and returns the entries from the leading
// chunks whose checksums match. pos is the first byte that could not be
// trusted (or len(buf)); chunks is how many complete chunks were accepted. The
// first malformed, torn, inconsistent, or checksum-failing chunk ends the
// trusted prefix.
func qwpSfParseChunkedSymbolDict(buf []byte) (loaded []string, pos, chunks int) {
	pos = int(qwpSfSymbolDictHeaderSize)
	for pos < len(buf) {
		chunkStart := pos
		entryCount, next, ok := qwpSfSymbolDictReadVarint(buf, pos, len(buf))
		// Deliberately not held to qwpMaxSymbolDictionarySize: a file that
		// already holds more entries than that must keep every one of them at
		// its own position. The only limit here is
		// qwpSfSymbolDictMaxRecoveredEntries, which caps the allocation a
		// hand-crafted file can ask for.
		if !ok || entryCount == 0 ||
			entryCount > uint64(qwpSfSymbolDictMaxRecoveredEntries-len(loaded)) {
			break
		}
		entryBytes, entriesStart, ok := qwpSfSymbolDictReadVarint(buf, next, len(buf))
		if !ok || entryBytes == 0 {
			break
		}
		// Every entry consumes at least its one-byte length varint, so a count
		// above the region size is internally impossible.
		if entryCount > entryBytes || entryBytes > uint64(len(buf)-entriesStart) {
			break
		}
		entriesEnd := entriesStart + int(entryBytes)
		if entriesEnd > len(buf)-qwpSfSymbolDictCRCSize {
			break
		}
		storedCRC := binary.LittleEndian.Uint32(buf[entriesEnd : entriesEnd+qwpSfSymbolDictCRCSize])
		if crc32.Checksum(buf[chunkStart:entriesEnd], qwpSfCrcTable) != storedCRC {
			break
		}
		entries, ok := qwpSfSymbolDictParseEntries(buf[entriesStart:entriesEnd], entryCount)
		if !ok {
			break
		}
		loaded = append(loaded, entries...)
		pos = entriesEnd + qwpSfSymbolDictCRCSize
		chunks++
	}
	return loaded, pos, chunks
}

// qwpSfSymbolDictParseEntries decodes exactly expected entries from a
// checksum-proven region, which it must consume completely. The CRC says the
// bytes are the ones that were written; these checks say they describe the
// entries the chunk header claims.
func qwpSfSymbolDictParseEntries(region []byte, expected uint64) ([]string, bool) {
	if expected > qwpSfSymbolDictMaxRecoveredEntries {
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
	var vb [qwpMaxVarintLen]byte
	entriesLen := 0
	for _, name := range names {
		if len(name) > qwpSfSymbolDictMaxEntryLen {
			return fmt.Errorf("qwp/sf: symbol dictionary entry length %d exceeds limit %d",
				len(name), qwpSfSymbolDictMaxEntryLen)
		}
		entriesLen += qwpPutVarint(vb[:], uint64(len(name))) + len(name)
	}
	// Reuse the scratch capacity the steady-state flush path has already grown.
	// The hint covers both chunk-header varints and the CRC; the bytes written
	// are canonical varints, which are usually shorter.
	if cap(d.scratch) < entriesLen+2*qwpMaxVarintLen+qwpSfSymbolDictCRCSize {
		d.scratch = make([]byte, 0, entriesLen+2*qwpMaxVarintLen+qwpSfSymbolDictCRCSize)
	}
	n := qwpPutVarint(vb[:], uint64(len(names)))
	d.scratch = append(d.scratch, vb[:n]...)
	n = qwpPutVarint(vb[:], uint64(entriesLen))
	d.scratch = append(d.scratch, vb[:n]...)
	for _, name := range names {
		n = qwpPutVarint(vb[:], uint64(len(name)))
		d.scratch = append(d.scratch, vb[:n]...)
		d.scratch = append(d.scratch, name...)
	}
	d.scratch = binary.LittleEndian.AppendUint32(
		d.scratch, crc32.Checksum(d.scratch, qwpSfCrcTable))
	written, err := qwpSfSymbolDictWriteAt.load()(d.file, d.scratch, d.appendOffset)
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
// symbol id i). Empty when nothing was recovered. It reflects what was on disk
// at open and nothing else: a later appendSymbols raises size() but leaves this
// list alone.
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
