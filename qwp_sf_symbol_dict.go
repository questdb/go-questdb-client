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
// Durability: every disk-backed engine holds an open dictionary containing
// every id its frames use. Engine construction writes the dictionary (keeping,
// healing, or rebuilding it from the surviving frames) and fsyncs the whole
// file before the slot is registered with the segment manager, so everything
// it holds at that point is durable before any frame of the session exists.
// After that, the producer appends the symbols a frame introduces BEFORE that
// frame is published to the ring, without an fsync, and the segment manager
// fsyncs the file (syncAppended) before a trim deletes frames. An id is
// therefore always recoverable from the surviving frame that introduced it or
// from a durable copy in this file, within qwpSfFsync's guarantee.
//
// Each append is one CRC-32C chunk, in the same format the Java client writes:
// recovery keeps only the run of chunks whose checksums match, then reads the
// surviving frames to rebuild whatever ids came after that and writes them
// back. If a hole is still left, recovery fails before connecting (and the
// send loop checks again before each frame goes out), so a detectable tear
// turns into "resend required" rather than quietly shifting every id to a
// different symbol.
//
// Single-writer (the producer goroutine). loaded is read once at open to seed
// recovery/orphan-drain; the engine owns close. The mutex serialises append
// against close so a Close racing an in-flight flush cannot write a closed fd.
type qwpSfSymbolDict struct {
	mu           sync.Mutex
	file         *os.File
	path         string
	appendOffset int64
	// untrustedEnd is where the file ends when an open left bytes after the
	// last chunk whose checksum matched. It is no greater than appendOffset
	// when there is no such tail.
	untrustedEnd int64
	count        int
	// unsynced records content that syncAppended has not made durable yet.
	// Every open sets it, so construction always fsyncs what it found.
	unsynced bool
	closeErr error
	closed   bool
	scratch  []byte
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
	// qwpSfSymbolDictMaxFileSize is the largest dictionary the writer may
	// create and recovery may read into memory.
	qwpSfSymbolDictMaxFileSize = 1 << 30
	// qwpSfSymbolDictEntryOverhead is the most bytes one entry can add to the
	// file beyond its name: its length varint (up to six bytes), plus the
	// overhead of a chunk holding only that entry (two varints of up to six
	// bytes each and a four-byte CRC). See qwpSfSymbolDictBound.
	qwpSfSymbolDictEntryOverhead = 22
	// qwpSfSymbolDictMaxPreallocEntries bounds the initial []string
	// allocation independently of the byte-region limit, so a malformed chunk
	// header cannot make the parser reserve the full recovered-entry ceiling
	// up front. The slice still grows to whatever the chunk really holds.
	qwpSfSymbolDictMaxPreallocEntries = 1 << 16
)

// qwpSfSymbolDictLimitSet holds the limits every store-and-forward dictionary
// obeys. The producer refuses a new symbol value that would break one, and
// recovery refuses a slot whose dictionary breaks one, so a slot this client
// writes always passes its own recovery.
//
//   - maxEntries caps the ids. It also caps how many entries recovery parses,
//     so a hand-crafted file with valid checksums cannot make a recovery or
//     drainer goroutine allocate an arbitrarily large []string. One
//     empty-string entry is a single byte on disk but about 16 bytes of string
//     header in memory, so the byte limit alone says little about allocation.
//   - maxEntryLen caps one name.
//   - maxFileBytes caps qwpSfSymbolDictBound of the whole dictionary.
type qwpSfSymbolDictLimitSet struct {
	maxEntries   int
	maxEntryLen  int
	maxFileBytes int64
}

// qwpSfSymbolDictLimits holds the limits in force. Tests lower them.
var qwpSfSymbolDictLimits = qwpSfSwappable(qwpSfSymbolDictLimitSet{
	maxEntries:   qwpMaxSymbolDictionarySize,
	maxEntryLen:  qwpSfSymbolDictMaxEntryLen,
	maxFileBytes: qwpSfSymbolDictMaxFileSize,
})

// qwpSfSymbolDictRewriteBatch bounds the chunks appendSymbolsBatched writes
// when construction heals or rebuilds a dictionary: at most entries names and
// at most bytes of names per chunk, whichever is reached first. It keeps
// startup memory small and the chunks close to the one-per-flush chunks the
// writer normally makes. Tests lower it.
var qwpSfSymbolDictRewriteBatch = qwpSfSwappable(struct{ entries, bytes int }{
	entries: 1 << 16,
	bytes:   16 << 20,
})

// qwpSfSymbolDictEntryBound is the most bytes the entry name can add to the
// dictionary file, however the entries are split into chunks.
func qwpSfSymbolDictEntryBound(name string) int64 {
	return int64(len(name)) + qwpSfSymbolDictEntryOverhead
}

// qwpSfSymbolDictBound is never below the size of a dictionary file holding
// symbols, however they are split into chunks. The producer keeps the same
// total incrementally and recovery computes it here, so both apply the byte
// limit to the same number.
func qwpSfSymbolDictBound(symbols []string) int64 {
	total := qwpSfSymbolDictHeaderSize
	for _, name := range symbols {
		total += qwpSfSymbolDictEntryBound(name)
	}
	return total
}

// qwpSfCheckRecoveredDictLimits refuses a recovered dictionary that breaks a
// limit in qwpSfSymbolDictLimits. No Go writer produces such a dictionary,
// because the producer refuses the symbol value first; released Java clients
// have no per-name or 1 GiB limit and can. The refusal is a fail-closed
// verdict: the slot is preserved, not retried.
func qwpSfCheckRecoveredDictLimits(symbols []string) error {
	limits := qwpSfSymbolDictLimits.load()
	if len(symbols) > limits.maxEntries {
		return qwpSfFailClosed("recovered symbol dictionary holds %d ids, more than the limit %d",
			len(symbols), limits.maxEntries)
	}
	for id, name := range symbols {
		if len(name) > limits.maxEntryLen {
			return qwpSfFailClosed("recovered symbol id %d is %d bytes long, more than the limit %d",
				id, len(name), limits.maxEntryLen)
		}
	}
	if bound := qwpSfSymbolDictBound(symbols); bound > limits.maxFileBytes {
		return qwpSfFailClosed("recovered symbol dictionary needs up to %d bytes, more than the limit %d",
			bound, limits.maxFileBytes)
	}
	return nil
}

// Filesystem calls the dictionary makes, indirected so tests can inject the
// failures a real disk produces rarely: a stat outage, a refused create or
// truncate, and a short write.
var (
	qwpSfSymbolDictStat     = qwpSfSwappable(os.Stat)
	qwpSfSymbolDictCreate   = qwpSfSwappable(os.OpenFile)
	qwpSfSymbolDictTruncate = qwpSfSwappable(func(f *os.File, size int64) error { return f.Truncate(size) })
	qwpSfSymbolDictWriteAt  = qwpSfSwappable(func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p, off) })
)

// qwpSfSymbolDictOpenClean opens the slot's side-file with no entries,
// truncating a file a previous run left behind or creating one. It never takes
// over existing entries. Any failure is an ErrSfDurability error: a
// disk-backed engine cannot run without its dictionary. The caller writes the
// entries it needs and calls makeDurable before registering the slot.
func qwpSfSymbolDictOpenClean(slotDir string) (*qwpSfSymbolDict, error) {
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	_, statErr := qwpSfSymbolDictStat.load()(path)
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, qwpSfDurabilityError("inspect symbol dictionary", path, statErr)
	}
	return qwpSfSymbolDictOpenFresh(path)
}

// qwpSfSymbolDictOpenRecovered opens the dictionary for a slot recovered from
// disk that still holds frames. It never recreates the file itself: the
// engine first reads the surviving frames on top of whatever this returns and
// decides whether the slot is recoverable, and only an accepted slot may
// change on disk. It returns:
//
//   - (dict, nil) when the header is valid. Only the run of chunks whose
//     checksums match is loaded. Anything after it stays on disk until the
//     engine accepts the slot and calls dropUntrustedTail, so a slot recovery
//     refuses is preserved with those bytes.
//   - (nil, nil) when the file is absent, or its content cannot be used (too
//     short, bad magic or version, or larger than the read limit). The file is
//     left as it is, and the frame scan decides whether the slot is
//     recoverable. An accepted slot then gets a dictionary rebuilt from the
//     frames.
//   - (nil, err) when stat/open/read fails. The caller fails construction
//     with that retriable error, so a transient fault never leads to a
//     rebuild.
func qwpSfSymbolDictOpenRecovered(slotDir string) (*qwpSfSymbolDict, error) {
	path := filepath.Join(slotDir, qwpSfSymbolDictFileName)
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, qwpSfDurabilityError("stat recovered symbol dictionary", path, err)
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
		return nil, qwpSfDurabilityError("recover symbol dictionary", path, openErr)
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

// qwpSfSymbolDictOpenExistingDetailed opens an existing side-file and loads the
// run of chunks whose checksums match, leaving whatever follows on disk until
// dropUntrustedTail or the first append removes it. Failures come in two kinds:
// errQwpSfSymbolDictUnusable for damaged content, which lets recovery fall back
// to reading the surviving frames, and plain I/O errors, which may succeed on a
// later attempt and are returned as they are.
func qwpSfSymbolDictOpenExistingDetailed(path string, fileLen int64) (result *qwpSfSymbolDict, err error) {
	if fileLen > qwpSfSymbolDictLimits.load().maxFileBytes {
		return nil, errQwpSfSymbolDictUnusable
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	held := &qwpSfSegment{file: f, path: path}
	resources := &qwpSfAcquiredResources{segments: []*qwpSfSegment{held}}
	defer func() {
		if result != nil {
			return
		}
		if r := recover(); r != nil {
			qwpSfRetainAcquisitionPanic(r, resources)
		}
		err = qwpSfFailedAcquisition(err, resources)
	}()
	buf := make([]byte, fileLen)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(buf[:4]) != qwpSfSymbolDictMagic || buf[4] != qwpSfSymbolDictVersion {
		return nil, errQwpSfSymbolDictUnusable
	}
	loaded, pos, chunks := qwpSfParseChunkedSymbolDict(buf)
	if len(buf) > int(qwpSfSymbolDictHeaderSize) && chunks == 0 {
		// An earlier Go prototype wrote a flat entry stream with no checksums
		// under this same version byte, and there is no reliable way to tell
		// such a file apart from one whose first chunk is corrupt. Content that
		// merely parses is not content that can be trusted, so keep the file
		// and let the frame scan rebuild the dictionary from nothing.
		return nil, errQwpSfSymbolDictUnusable
	}
	// The bytes after pos stay on disk for now. Recovery may still refuse the
	// slot, and a preserved copy must then keep them; dropUntrustedTail cuts
	// them once the slot is accepted, and appendSymbols cuts them before its
	// first write in any case.
	// unsynced starts true: nothing proves the file this open found is
	// durable, and construction must fsync it before registering the slot.
	return &qwpSfSymbolDict{
		file:         f,
		path:         path,
		appendOffset: int64(pos),
		untrustedEnd: fileLen,
		count:        len(loaded),
		unsynced:     true,
		loaded:       loaded,
	}, nil
}

// dropUntrustedTail cuts the file back to the last chunk whose checksum
// matched, if the open left anything after it. Recovery calls it once it has
// accepted the slot.
func (d *qwpSfSymbolDict) dropUntrustedTail() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	return d.dropUntrustedTailLocked()
}

// dropUntrustedTailLocked must run before anything is appended. An append may
// be shorter than the tail it replaces, and writing at appendOffset alone would
// leave the rest of that tail on disk, where a later recovery could read it as
// real entries and shift every id after it.
func (d *qwpSfSymbolDict) dropUntrustedTailLocked() error {
	if d.untrustedEnd <= d.appendOffset {
		return nil
	}
	if err := qwpSfSymbolDictTruncate.load()(d.file, d.appendOffset); err != nil {
		return qwpSfDurabilityError("drop torn or stale symbol dictionary tail", d.path, err)
	}
	d.untrustedEnd = d.appendOffset
	d.unsynced = true
	return nil
}

// qwpSfParseChunkedSymbolDict reads the one-chunk-per-append stream, in the
// format the Java client also writes, and returns the entries from the leading
// chunks whose checksums match. pos is the first byte that could not be
// trusted (or len(buf)); chunks is how many complete chunks were accepted. The
// first malformed, torn, inconsistent, or checksum-failing chunk ends the
// trusted prefix.
func qwpSfParseChunkedSymbolDict(buf []byte) (loaded []string, pos, chunks int) {
	maxEntries := qwpSfSymbolDictLimits.load().maxEntries
	pos = int(qwpSfSymbolDictHeaderSize)
	for pos < len(buf) {
		chunkStart := pos
		entryCount, next, ok := qwpSfSymbolDictReadVarint(buf, pos, len(buf))
		// A chunk that would take the dictionary past the id limit ends the
		// trusted prefix. Recovery then refuses the slot if a surviving frame
		// needs those ids, and cuts them with the untrusted tail otherwise.
		if !ok || entryCount == 0 ||
			entryCount > uint64(maxEntries-len(loaded)) {
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
	limits := qwpSfSymbolDictLimits.load()
	if expected > uint64(limits.maxEntries) {
		return nil, false
	}
	maxEntryLen := limits.maxEntryLen
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
		if !ok || entryLen > uint64(maxEntryLen) || entryLen > uint64(len(region)-next) {
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

func qwpSfSymbolDictOpenFresh(path string) (result *qwpSfSymbolDict, err error) {
	f, err := qwpSfSymbolDictCreate.load()(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, qwpSfDurabilityError("create fresh symbol dictionary", path, err)
	}
	held := &qwpSfSegment{file: f, path: path}
	resources := &qwpSfAcquiredResources{segments: []*qwpSfSegment{held}}
	defer func() {
		if result != nil {
			return
		}
		if r := recover(); r != nil {
			qwpSfRetainAcquisitionPanic(r, resources)
		}
		err = qwpSfFailedAcquisition(err, resources)
		if resources.released() && !errors.Is(err, ErrCleanupFailed) {
			_ = os.Remove(path)
		}
	}()
	var hdr [qwpSfSymbolDictHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], qwpSfSymbolDictMagic)
	hdr[4] = qwpSfSymbolDictVersion
	written, err := f.WriteAt(hdr[:], 0)
	if err == nil && written != len(hdr) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return nil, qwpSfDurabilityError("write fresh symbol dictionary header", path, err)
	}
	// unsynced starts true so construction fsyncs the truncation or creation:
	// a crash must not bring back a dictionary this open replaced.
	return &qwpSfSymbolDict{file: f, path: path, appendOffset: qwpSfSymbolDictHeaderSize, unsynced: true}, nil
}

// appendSymbols extends the dictionary with names in ascending-id order, as one
// chunk in one write, before the referencing frame is published. Not fsync'd
// (see the type doc). A no-op after close. A short or failed write returns an
// ErrSfDurability error so the caller can withhold the frame, and marks the
// whole attempted range as untrusted tail: the next append truncates it first,
// so a retry never leaves partly stored bytes ahead of its chunk.
func (d *qwpSfSymbolDict) appendSymbols(names []string) error {
	if d == nil || len(names) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	if err := d.dropUntrustedTailLocked(); err != nil {
		return err
	}
	limits := qwpSfSymbolDictLimits.load()
	if len(names) > limits.maxEntries-d.count {
		return fmt.Errorf("qwp/sf: symbol dictionary exceeds maximum size %d", limits.maxEntries)
	}
	d.scratch = d.scratch[:0]
	var vb [qwpMaxVarintLen]byte
	var entriesLen int64
	for _, name := range names {
		if len(name) > limits.maxEntryLen {
			return fmt.Errorf("qwp/sf: symbol dictionary entry length %d exceeds limit %d",
				len(name), limits.maxEntryLen)
		}
		entriesLen += int64(qwpPutVarint(vb[:], uint64(len(name))) + len(name))
	}
	countLen := qwpPutVarint(vb[:], uint64(len(names)))
	entriesLenLen := qwpPutVarint(vb[:], uint64(entriesLen))
	chunkLen := entriesLen + int64(countLen+entriesLenLen+qwpSfSymbolDictCRCSize)
	if chunkLen > limits.maxFileBytes-d.appendOffset {
		return fmt.Errorf("qwp/sf: symbol dictionary file exceeds maximum size %d", limits.maxFileBytes)
	}
	// Reuse the scratch capacity the steady-state flush path has already grown.
	// The exact hint covers both chunk-header varints and the CRC.
	if cap(d.scratch) < int(chunkLen) {
		d.scratch = make([]byte, 0, int(chunkLen))
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
	if err == nil && written != len(d.scratch) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// written may not count bytes the device partly stored, so the whole
		// attempted range becomes untrusted tail.
		d.untrustedEnd = max(d.untrustedEnd, d.appendOffset+int64(len(d.scratch)))
		return qwpSfDurabilityError("append symbol dictionary", d.path, err)
	}
	d.appendOffset += int64(len(d.scratch))
	d.count += len(names)
	d.unsynced = true
	return nil
}

// qwpSfSymbolDictLimitInconsistency labels an appendSymbols error that is not a
// storage fault, which can only be one of its limit refusals. The producer
// refuses a symbol value, and recovery refuses a slot, against the same limits
// before anything reaches appendSymbols, so such a refusal means those checks
// and the writer disagree: an internal bug, not a storage fault and not
// evidence about the slot. Storage faults pass through unchanged.
func qwpSfSymbolDictLimitInconsistency(err error) error {
	if err == nil || errors.Is(err, ErrSfDurability) {
		return err
	}
	return fmt.Errorf("qwp/sf: internal inconsistency: the symbol dictionary writer refused entries that passed the limit checks: %w", err)
}

// appendSymbolsBatched appends names as a series of chunks bounded by
// qwpSfSymbolDictRewriteBatch. Construction uses it to heal or rebuild a
// dictionary, which can hold up to the full dictionary limit at once.
func (d *qwpSfSymbolDict) appendSymbolsBatched(names []string) error {
	batch := qwpSfSymbolDictRewriteBatch.load()
	for len(names) > 0 {
		n, bytes := 0, 0
		for n < len(names) && n < batch.entries {
			if n > 0 && bytes+len(names[n]) > batch.bytes {
				break
			}
			bytes += len(names[n])
			n++
		}
		if err := d.appendSymbols(names[:n]); err != nil {
			return err
		}
		names = names[n:]
	}
	return nil
}

// makeDurable fsyncs the dictionary and then the slot directory that names
// it. Construction calls it on every disk-backed path before registering the
// slot with the segment manager, so everything the dictionary holds is durable
// before any frame of the session exists. That includes ids an earlier session
// persisted for a frame it never published: no frame carries them, and the
// producer's first frame starts its delta above them.
//
// The directory barrier runs even when this session did not create the file.
// A file another writer created, such as the Java client, which never syncs
// the directory, may have a name that an OS crash would still lose, and a lost
// dictionary after a trim leaves the surviving frames with a gap.
func (d *qwpSfSymbolDict) makeDurable(slotDir string) error {
	if err := d.syncAppended(); err != nil {
		return err
	}
	if err := qwpSfSyncSlotDir(slotDir); err != nil {
		return qwpSfDurabilityError("sync slot directory after symbol dictionary", slotDir, err)
	}
	return nil
}

// qwpSfSymbolDictSync is the fsync syncAppended issues. Tests swap it to fail.
var qwpSfSymbolDictSync = qwpSfSwappable(qwpSfFsync)

// syncAppended makes every chunk appended so far durable, within qwpSfFsync's
// guarantee, and does nothing when nothing was appended since the last call.
// The segment manager calls it before a trim deletes frames: those frames may
// be the last copy, besides this file, of symbol ids that later
// unacknowledged frames refer to by number. It runs on the manager goroutine;
// the mutex orders it against the producer's appends and against close.
func (d *qwpSfSymbolDict) syncAppended() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.file == nil || !d.unsynced {
		return nil
	}
	if err := qwpSfSymbolDictSync.load()(d.file); err != nil {
		return qwpSfDurabilityError("fsync symbol dictionary", d.path, err)
	}
	d.unsynced = false
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
		return d.closeErr
	}
	d.closed = true
	// Drop the recovered-entry copy at teardown. The send-loop mirror and the
	// producer's global dictionary both read it during construction and leave it
	// in place (each is a consumer; neither can tell it is the last), so the copy
	// lives until the dict is closed — the slot's whole lifetime for a running
	// sender, the drain for an orphan drainer.
	d.loaded = nil
	if d.file != nil {
		d.closeErr = qwpSfCloseFile(d.file)
		d.file = nil
		return d.closeErr
	}
	return nil
}
