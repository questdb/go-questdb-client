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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQwpSfSymbolDictAppendPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Equal(t, 0, d.size())
	require.NoError(t, d.appendSymbols([]string{"AAPL", "GOOG", "MSFT"}))
	require.Equal(t, 3, d.size())
	require.NoError(t, d.close())

	re, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, re)
	require.Equal(t, 3, re.size())
	require.Equal(t, []string{"AAPL", "GOOG", "MSFT"}, re.loadedSymbols())
	// Appending after recovery continues from the recovered tip.
	require.NoError(t, re.appendSymbols([]string{"TSLA"}))
	require.Equal(t, 4, re.size())
	require.NoError(t, re.close())

	third, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, third)
	require.Equal(t, 4, third.size())
	require.Equal(t, "TSLA", third.loadedSymbols()[3])
	require.NoError(t, third.close())
}

func TestQwpSfSymbolDictAppendSymbolsZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("zero-alloc invariant does not hold under -race")
	}
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	// A dictionary that could not be created comes back as (nil, nil), and
	// every method on it is a nil-safe no-op. Measuring that would report zero
	// allocations for a run that appended nothing.
	require.NotNil(t, d)
	defer func() { _ = d.close() }()

	names := []string{"new-symbol"}
	require.NoError(t, d.appendSymbols(names)) // warm scratch and syscall paths
	allocs := testing.AllocsPerRun(100, func() {
		if err := d.appendSymbols(names); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs, "appendSymbols allocated on its warmed flush path")
}

func TestQwpSfSymbolDictUsesJavaCompatibleChecksummedChunks(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.NoError(t, d.appendSymbols([]string{"AAPL", "東京"}))
	require.NoError(t, d.close())

	buf, err := os.ReadFile(filepath.Join(dir, qwpSfSymbolDictFileName))
	require.NoError(t, err)
	pos := int(qwpSfSymbolDictHeaderSize)
	chunkStart := pos
	count, n, err := qwpReadVarint(buf[pos:])
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	pos += n
	entryBytes, n, err := qwpReadVarint(buf[pos:])
	require.NoError(t, err)
	pos += n
	entriesEnd := pos + int(entryBytes)
	require.LessOrEqual(t, entriesEnd+qwpSfSymbolDictCRCSize, len(buf))
	stored := binary.LittleEndian.Uint32(buf[entriesEnd : entriesEnd+qwpSfSymbolDictCRCSize])
	require.Equal(t, crc32.Checksum(buf[chunkStart:entriesEnd], qwpSfCrcTable), stored)
	require.Equal(t, entriesEnd+qwpSfSymbolDictCRCSize, len(buf),
		"one append must produce exactly one chunk, in the Java client's format")
}

func TestQwpSfSymbolDictCRCRejectsCorruptChunkAndKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NoError(t, d.appendSymbols([]string{"first"}))
	require.NoError(t, d.appendSymbols([]string{"second"}))
	require.NoError(t, d.close())

	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	buf, err := os.ReadFile(path)
	require.NoError(t, err)
	firstEnd := qwpTestSymbolDictChunkEnd(t, buf, int(qwpSfSymbolDictHeaderSize))
	secondEntries := qwpTestSymbolDictEntriesStart(t, buf, firstEnd)
	// Step past the entry-length varint and change one UTF-8 byte, leaving the
	// stored checksum as it was. A reader that only checked lengths would
	// accept this without noticing.
	_, adv, err := qwpReadVarint(buf[secondEntries:])
	require.NoError(t, err)
	buf[secondEntries+adv] ^= 0x20
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	re, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, re)
	require.Equal(t, []string{"first"}, re.loadedSymbols())
	require.NoError(t, re.close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(firstEnd), info.Size(), "corrupt suffix must be truncated")
}

func TestQwpSfSymbolDictRecoveredLegacyFormatIsUntrustedAndPreserved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	buf := make([]byte, qwpSfSymbolDictHeaderSize)
	binary.LittleEndian.PutUint32(buf[:4], qwpSfSymbolDictMagic)
	buf[4] = qwpSfSymbolDictVersion
	var vb [qwpMaxVarintLen]byte
	for _, symbol := range []string{"old-a", "old-b"} {
		buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(len(symbol)))]...)
		buf = append(buf, symbol...)
	}
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d,
		"a body with no checksums looks just like a corrupt first chunk, so it must not supply ids")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, buf, got, "recovery must keep the file so it can be inspected or read by an older client")
}

// TestQwpSfSymbolDictRecoveredEntryCountBounded pins the limit that keeps a
// hand-crafted file from making recovery allocate too much. A chunk of
// empty-string entries passes its checksum and costs one byte each on disk, but
// about 16 bytes each as Go strings, so recovery must reject an entry count
// over the limit instead of parsing it.
func TestQwpSfSymbolDictRecoveredEntryCountBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	buf := make([]byte, qwpSfSymbolDictHeaderSize)
	binary.LittleEndian.PutUint32(buf[:4], qwpSfSymbolDictMagic)
	buf[4] = qwpSfSymbolDictVersion
	count := qwpSfSymbolDictMaxRecoveredEntries + 1
	var vb [qwpMaxVarintLen]byte
	chunk := append([]byte(nil), vb[:qwpPutVarint(vb[:], uint64(count))]...)
	chunk = append(chunk, vb[:qwpPutVarint(vb[:], uint64(count))]...)
	chunk = append(chunk, make([]byte, count)...) // one 0x00 varint per empty entry
	entriesEnd := len(chunk)
	var crcb [qwpSfSymbolDictCRCSize]byte
	binary.LittleEndian.PutUint32(crcb[:], crc32.Checksum(chunk[:entriesEnd], qwpSfCrcTable))
	chunk = append(chunk, crcb[:]...)
	require.NoError(t, os.WriteFile(path, append(buf, chunk...), 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d, "an entry count over the limit must be refused, not allocated")
}

func TestQwpSfSymbolDictOpenRecoveredAbsentReturnsNil(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d, "absent dictionary on a recovered slot degrades to full-dict fallback")
}

func TestQwpSfSymbolDictOpenCleanDoesNotInheritExistingIDs(t *testing.T) {
	dir := t.TempDir()
	old, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, old)
	require.NoError(t, old.appendSymbols([]string{"stale-a", "stale-b"}))
	require.NoError(t, old.close())

	clean, err := qwpSfSymbolDictOpenClean(dir)
	require.NoError(t, err)
	require.NotNil(t, clean)
	require.Zero(t, clean.size())
	require.Empty(t, clean.loadedSymbols())
	require.NoError(t, clean.appendSymbols([]string{"new-a"}))
	require.NoError(t, clean.close())

	reopened, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.NotNil(t, reopened)
	require.Equal(t, []string{"new-a"}, reopened.loadedSymbols())
	require.NoError(t, reopened.close())
}

func TestQwpSfSymbolDictOpenRecoveredValid(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NoError(t, d.appendSymbols([]string{"AAPL", "GOOG"}))
	require.NoError(t, d.close())

	re, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.NotNil(t, re)
	require.Equal(t, []string{"AAPL", "GOOG"}, re.loadedSymbols())
	require.NoError(t, re.close())
}

// TestQwpSfSymbolDictOpenRecoveredCorruptFallsBack pins what happens to
// damaged content, matching the Java client: this recovery goes without delta
// encoding and the file is left on disk so it can be inspected. Whether the
// slot can still be rebuilt is decided separately, by the engine's scan of the
// surviving frames.
func TestQwpSfSymbolDictOpenRecoveredCorruptFallsBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	garbage := []byte{9, 9, 9, 9, 9, 9, 9, 9, 42}
	require.NoError(t, os.WriteFile(path, garbage, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d)

	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, garbage, got, "corrupt recovered dictionary must not be truncated")
}

// TestQwpSfSymbolDictVersionMismatch pins that an unknown version byte is
// treated like bad magic. On recovery the file is left alone and the slot runs
// on full dictionaries; on a fresh open it is recreated, since no segment can
// be referring to its ids.
func TestQwpSfSymbolDictVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	hdr := make([]byte, qwpSfSymbolDictHeaderSize+2)
	binary.LittleEndian.PutUint32(hdr[:4], qwpSfSymbolDictMagic)
	hdr[4] = qwpSfSymbolDictVersion + 1
	hdr[8], hdr[9] = 1, 'x'
	require.NoError(t, os.WriteFile(path, hdr, 0o644))

	recovered, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, recovered)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, hdr, got, "recovery must leave a file with an unknown version alone")

	d, openErr := qwpSfSymbolDictOpen(dir)
	require.NoError(t, openErr)
	require.NotNil(t, d)
	require.Equal(t, 0, d.size(), "wrong version recreated empty on a fresh open")
	require.NoError(t, d.close())
}

// TestQwpSfSymbolDictOversizedFileRejected pins that a file past the read
// ceiling is refused before it can drive a multi-GB allocation. A sparse
// truncate keeps the test cheap.
func TestQwpSfSymbolDictOversizedFileRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	var hdr [qwpSfSymbolDictHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:4], qwpSfSymbolDictMagic)
	hdr[4] = qwpSfSymbolDictVersion
	_, err = f.WriteAt(hdr[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(qwpSfSymbolDictMaxFileSize+1))
	require.NoError(t, f.Close())

	recovered, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, recovered, "a file over the size limit must be refused before it is read")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(qwpSfSymbolDictMaxFileSize+1), info.Size(),
		"recovery must not truncate an oversized file it refused to read")
}

func TestQwpSfSymbolDictBadMagicRecreatedEmpty(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfSymbolDictFileName),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0o644))

	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Equal(t, 0, d.size(), "bad-magic file recreated empty")
	require.NoError(t, d.appendSymbols([]string{"X"}))
	require.Equal(t, 1, d.size())
	require.NoError(t, d.close())
}

func TestQwpSfSymbolDictEmptySymbolRoundTrips(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NoError(t, d.appendSymbols([]string{"", "nonempty"}))
	require.NoError(t, d.close())

	re, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.Equal(t, 2, re.size())
	require.Equal(t, []string{"", "nonempty"}, re.loadedSymbols())
	require.NoError(t, re.close())
}

func TestQwpSfSymbolDictRemoveOrphanDeletesFile(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NoError(t, d.appendSymbols([]string{"A"}))
	require.NoError(t, d.close())

	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	_, err = os.Stat(path)
	require.NoError(t, err)
	qwpSfSymbolDictRemoveOrphan(dir)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestQwpSfSymbolDictTornTrailingChunkSelfHeals(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NoError(t, d.appendSymbols([]string{"one", "two"}))
	require.NoError(t, d.close())

	cleanInfo, err := os.Stat(filepath.Join(dir, qwpSfSymbolDictFileName))
	require.NoError(t, err)

	// Append a torn trailing record, chosen so that its tail turns into a valid
	// one-byte entry if a later, shorter append overwrites only the leading 5:
	// [5, 1, 'G'] with an empty symbol [0] written over the 5 becomes
	// [0, 1, 'G']. Unless reopen truncates first, the next recovery reads a
	// symbol "G" that was never written.
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{5, 1, 'G'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	re, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.Equal(t, 2, re.size(), "torn tail ignored")
	require.Equal(t, []string{"one", "two"}, re.loadedSymbols())
	trimmedInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, cleanInfo.Size(), trimmedInfo.Size(), "reopen must truncate the torn tail")
	// An empty symbol is shorter than the torn record, so it leaves the old
	// bytes behind unless reopen truncated first.
	require.NoError(t, re.appendSymbols([]string{""}))
	require.NoError(t, re.close())

	re2, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.Equal(t, 3, re2.size())
	require.Equal(t, []string{"one", "two", ""}, re2.loadedSymbols())
	require.NoError(t, re2.close())
}

func TestQwpSfSymbolDictJavaGoldenOneSymbol(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	// Java encoding of ["x"]: [count=1][entryBytes=2][len=1]['x']
	// followed by little-endian CRC-32C 0x32ae27c7.
	golden := append(qwpSfTestSymbolDictHeader(),
		0x01, 0x02, 0x01, 0x78, 0xc7, 0x27, 0xae, 0x32)
	require.NoError(t, os.WriteFile(path, golden, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, d.loadedSymbols())
	require.NoError(t, d.close())
}

func TestQwpSfSymbolDictGoWriterMatchesJavaGolden(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NoError(t, d.appendSymbols([]string{"x"}))
	require.NoError(t, d.appendSymbols([]string{"", "多字节"}))
	require.NoError(t, d.close())

	// Java encoding of the same two-append sequence. This pins both chunk
	// boundaries as well as empty and multibyte UTF-8 symbols.
	want := append(qwpSfTestSymbolDictHeader(),
		0x01, 0x02, 0x01, 0x78, 0xc7, 0x27, 0xae, 0x32,
		0x02, 0x0b,
		0x00,
		0x09, 0xe5, 0xa4, 0x9a, 0xe5, 0xad, 0x97, 0xe8, 0x8a, 0x82,
		0xfd, 0x6c, 0x05, 0x62)
	got, err := os.ReadFile(filepath.Join(dir, qwpSfSymbolDictFileName))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestQwpSfSymbolDictWriterUsesCanonicalBoundaryVarints(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	names := make([]string, 128)
	for i := range names {
		names[i] = "x"
	}
	require.NoError(t, d.appendSymbols(names))
	require.NoError(t, d.close())

	raw, err := os.ReadFile(filepath.Join(dir, qwpSfSymbolDictFileName))
	require.NoError(t, err)
	// count=128 -> 0x80 0x01; 128 entries of [len=1]['x'] occupy 256
	// bytes -> 0x80 0x02.
	require.Equal(t, []byte{0x80, 0x01, 0x80, 0x02}, raw[qwpSfSymbolDictHeaderSize:qwpSfSymbolDictHeaderSize+4])

	reopened, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Len(t, reopened.loadedSymbols(), 128)
	require.NoError(t, reopened.close())
}

func TestQwpSfSymbolDictHeaderOnlyIsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	require.NoError(t, os.WriteFile(path, qwpSfTestSymbolDictHeader(), 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Empty(t, d.loadedSymbols())
	require.Zero(t, d.size())
	require.NoError(t, d.close())
}

func TestQwpSfSymbolDictBadTrailingChunkTruncatesToTrustedPrefix(t *testing.T) {
	valid := qwpSfTestSymbolDictChunk("ok")
	crcFlip := qwpSfTestSymbolDictChunk("bad")
	crcFlip[len(crcFlip)-1] ^= 0xff
	tests := []struct {
		name string
		tail []byte
	}{
		{name: "torn-entry-count-varint", tail: []byte{0x80}},
		{name: "overlong-entry-count-varint", tail: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}},
		{name: "noncanonical-entry-count-varint", tail: qwpSfTestChecksummedChunk([]byte{0x81, 0x00, 0x01, 0x00})},
		{name: "torn-entry-bytes-varint", tail: []byte{0x01, 0x80}},
		{name: "overlong-entry-bytes-varint", tail: []byte{0x01, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}},
		{name: "torn-chunk", tail: []byte{0x01, 0x02, 0x01, 'x'}},
		{name: "crc-flip", tail: crcFlip},
		{name: "zero-count", tail: qwpSfTestChecksummedChunk([]byte{0x00, 0x01, 0x00})},
		{name: "zero-entry-bytes", tail: qwpSfTestChecksummedChunk([]byte{0x01, 0x00})},
		{name: "entry-count-overflow", tail: qwpSfTestChecksummedChunk([]byte{0x80, 0x80, 0x80, 0x80, 0x10, 0x01, 0x00})},
		{name: "entries-underrun", tail: qwpSfTestChecksummedChunk([]byte{0x02, 0x02, 0x01, 'a'})},
		{name: "entries-overrun", tail: qwpSfTestChecksummedChunk([]byte{0x01, 0x04, 0x01, 'a', 0x01, 'b'})},
		{name: "overlong-entry-length-varint", tail: qwpSfTestChecksummedChunk([]byte{0x01, 0x07, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, qwpSfSymbolDictFileName)
			prefix := append(qwpSfTestSymbolDictHeader(), valid...)
			contents := append(append([]byte(nil), prefix...), tc.tail...)
			require.NoError(t, os.WriteFile(path, contents, 0o644))

			d, err := qwpSfSymbolDictOpenRecovered(dir)
			require.NoError(t, err)
			require.Equal(t, []string{"ok"}, d.loadedSymbols())
			require.NoError(t, d.close())
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, prefix, got, "untrusted tail must be physically truncated")
		})
	}
}

func TestQwpSfSymbolDictZeroValidChunksKeepsCorruptDisposition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	contents := append(qwpSfTestSymbolDictHeader(), 0x80) // torn first entryCount; not a flat entry
	require.NoError(t, os.WriteFile(path, contents, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d, "a corrupt first chunk supplies no ids")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, contents, got, "recovery must preserve a corrupt first chunk")

	d, err = qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Zero(t, d.size())
	require.NoError(t, d.close())
	got, readErr = os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, qwpSfTestSymbolDictHeader(), got, "fresh open keeps recreate-on-corrupt behavior")
}

func TestQwpSfSymbolDictFreshOpenPropagatesStatFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	legacy := append(qwpSfTestSymbolDictHeader(), 0x01, 'x')
	require.NoError(t, os.WriteFile(path, legacy, 0o644))

	originalStat := qwpSfSymbolDictStat.load()
	qwpSfSymbolDictStat.store(func(string) (os.FileInfo, error) { return nil, errors.New("injected stat failure") })
	t.Cleanup(func() { qwpSfSymbolDictStat.store(originalStat) })

	d, err := qwpSfSymbolDictOpen(dir)
	require.Nil(t, d)
	require.ErrorContains(t, err, "could not stat symbol dictionary")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, legacy, got, "a stat failure must not fall through to O_TRUNC")
}

func TestQwpSfSymbolDictShortWriteRetryDoesNotAdvance(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	initialOffset := d.appendOffset

	originalWriteAt := qwpSfSymbolDictWriteAt.load()
	shortWrite := true
	qwpSfSymbolDictWriteAt.store(func(f *os.File, p []byte, off int64) (int, error) {
		if shortWrite {
			shortWrite = false
			return originalWriteAt(f, p[:len(p)-1], off)
		}
		return originalWriteAt(f, p, off)
	})
	t.Cleanup(func() { qwpSfSymbolDictWriteAt.store(originalWriteAt) })

	err = d.appendSymbols([]string{"AAPL", "MSFT"})
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Zero(t, d.size(), "short write must not advance the symbol count")
	require.Equal(t, initialOffset, d.appendOffset, "short write must not advance the append offset")

	require.NoError(t, d.appendSymbols([]string{"AAPL", "MSFT"}), "retry must overwrite at the same offset")
	require.Equal(t, 2, d.size())
	require.NoError(t, d.close())

	reopened, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"AAPL", "MSFT"}, reopened.loadedSymbols())
	require.NoError(t, reopened.close())
}

func TestQwpSfSymbolDictTruncateFailureIsOperational(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	prefix := append(qwpSfTestSymbolDictHeader(), qwpSfTestSymbolDictChunk("ok")...)
	contents := append(append([]byte(nil), prefix...), 0x80)
	require.NoError(t, os.WriteFile(path, contents, 0o644))

	originalTruncate := qwpSfSymbolDictTruncate.load()
	qwpSfSymbolDictTruncate.store(func(*os.File, int64) error { return errors.New("injected truncate failure") })
	t.Cleanup(func() { qwpSfSymbolDictTruncate.store(originalTruncate) })

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.Nil(t, d)
	require.ErrorContains(t, err, "could not drop torn/stale")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, contents, got, "failed truncation must preserve the file for retry")
}

func qwpSfTestSymbolDictHeader() []byte {
	header := make([]byte, qwpSfSymbolDictHeaderSize)
	binary.LittleEndian.PutUint32(header[:4], qwpSfSymbolDictMagic)
	header[4] = qwpSfSymbolDictVersion
	return header
}

func qwpSfTestSymbolDictChunk(entries ...string) []byte {
	entryRegion := make([]byte, 0)
	for _, entry := range entries {
		entryRegion = binary.AppendUvarint(entryRegion, uint64(len(entry)))
		entryRegion = append(entryRegion, entry...)
	}
	body := binary.AppendUvarint(nil, uint64(len(entries)))
	body = binary.AppendUvarint(body, uint64(len(entryRegion)))
	body = append(body, entryRegion...)
	return qwpSfTestChecksummedChunk(body)
}

func qwpSfTestChecksummedChunk(body []byte) []byte {
	chunk := append([]byte(nil), body...)
	return binary.LittleEndian.AppendUint32(chunk, crc32.Checksum(body, qwpSfCrcTable))
}

func qwpTestSymbolDictEntriesStart(t *testing.T, buf []byte, chunkStart int) int {
	t.Helper()
	_, n, err := qwpReadVarint(buf[chunkStart:])
	require.NoError(t, err)
	pos := chunkStart + n
	_, n, err = qwpReadVarint(buf[pos:])
	require.NoError(t, err)
	return pos + n
}

func qwpTestSymbolDictChunkEnd(t *testing.T, buf []byte, chunkStart int) int {
	t.Helper()
	_, n, err := qwpReadVarint(buf[chunkStart:])
	require.NoError(t, err)
	pos := chunkStart + n
	entryBytes, n, err := qwpReadVarint(buf[pos:])
	require.NoError(t, err)
	return pos + n + int(entryBytes) + qwpSfSymbolDictCRCSize
}
