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
	"fmt"
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
// unrecoverable, so it degrades to full self-sufficient frames when it cannot
// open (open returns nil) rather than risking a gap.
//
// On-disk layout (little-endian):
//
//	offset 0: u32 magic = 'SYD1'
//	offset 4: u8  version = 1
//	offset 5: 3 bytes reserved (zero)
//	offset 8: entries, each [len: varint][utf8 bytes], ascending global-id order
//
// Symbol id i is the i-th entry (ids are dense from 0), so no id is stored.
//
// Durability: the producer appends the symbols a frame introduces BEFORE that
// frame is published to the ring, but does NOT fsync — matching the rest of
// store-and-forward (page-cache, not disk, durable). This ordering suffices for
// a process crash (the page cache survives, so the dictionary stays a superset
// of every recoverable frame's references). It does NOT survive a host/power
// crash that tears the dictionary relative to its frames — that case is caught
// at replay by the send loop's guard, which fails loudly (resend required)
// rather than transmitting a gapped frame. A torn trailing entry from a crash
// mid-append is self-healing: open stops at the first incomplete entry and the
// next append overwrites it.
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
	// qwpSfSymbolDictMaxEntryLen bounds one symbol's decoded length so a torn
	// or corrupt length prefix cannot drive a runaway allocation. Symbols are
	// short; this ceiling is generous.
	qwpSfSymbolDictMaxEntryLen = 1 << 20
	// qwpSfSymbolDictMaxFileSize bounds the whole-file read at open so a torn
	// or foreign file cannot drive a multi-GB allocation on a background
	// recovery/drainer goroutine. Even pathological high-cardinality symbol use
	// stays far below this.
	qwpSfSymbolDictMaxFileSize = 1 << 30
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
	d := qwpSfSymbolDictOpenExisting(path, st.Size())
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

func qwpSfSymbolDictOpenExisting(path string, fileLen int64) *qwpSfSymbolDict {
	if fileLen > qwpSfSymbolDictMaxFileSize {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil
	}
	buf := make([]byte, fileLen)
	if _, err := io.ReadFull(f, buf); err != nil {
		_ = f.Close()
		return nil
	}
	if binary.LittleEndian.Uint32(buf[:4]) != qwpSfSymbolDictMagic || buf[4] != qwpSfSymbolDictVersion {
		_ = f.Close()
		return nil
	}
	// Parse complete entries after the header, stopping at the first torn or
	// oversized trailing entry so a crash mid-append self-heals.
	pos := int(qwpSfSymbolDictHeaderSize)
	var loaded []string
	for pos < len(buf) {
		n, adv, verr := qwpReadVarint(buf[pos:])
		if verr != nil {
			break
		}
		start := pos + adv
		if n > qwpSfSymbolDictMaxEntryLen || start+int(n) > len(buf) {
			break
		}
		loaded = append(loaded, string(buf[start:start+int(n)]))
		pos = start + int(n)
	}
	return &qwpSfSymbolDict{
		file:         f,
		appendOffset: int64(pos),
		count:        len(loaded),
		loaded:       loaded,
	}
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
	for _, name := range names {
		var vb [qwpMaxVarintLen]byte
		n := qwpPutVarint(vb[:], uint64(len(name)))
		d.scratch = append(d.scratch, vb[:n]...)
		d.scratch = append(d.scratch, name...)
	}
	if _, err := d.file.WriteAt(d.scratch, d.appendOffset); err != nil {
		return err
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

// releaseLoaded drops the recovered-entry copy once both the producer's global
// dictionary and the send loop's catch-up mirror have been seeded from it, so a
// large recovered dictionary is not retained for the slot's whole lifetime.
func (d *qwpSfSymbolDict) releaseLoaded() {
	if d == nil {
		return
	}
	d.loaded = nil
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
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		return err
	}
	return nil
}
