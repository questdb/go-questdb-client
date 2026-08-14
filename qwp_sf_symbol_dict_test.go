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
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestQwpSfSymbolDictOpenRecoveredAbsentReturnsNil(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d, "absent dictionary on a recovered slot degrades to full-dict fallback")
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

// TestQwpSfSymbolDictOpenRecoveredCorruptFailsLoud pins that a recovered slot's
// corrupt dictionary is a hard error and the file is preserved, never
// truncated — recreating it would restart the id space the surviving segments
// reference by position and silently corrupt replayed data.
func TestQwpSfSymbolDictOpenRecoveredCorruptFailsLoud(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	garbage := []byte{9, 9, 9, 9, 9, 9, 9, 9, 42}
	require.NoError(t, os.WriteFile(path, garbage, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.Error(t, err)
	require.Nil(t, d)

	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, garbage, got, "corrupt recovered dictionary must not be truncated")
}

// TestQwpSfSymbolDictVersionMismatch pins that an unknown version byte is
// treated like bad magic: recreated on a fresh open, but a hard error on a
// recovered slot.
func TestQwpSfSymbolDictVersionMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	hdr := make([]byte, qwpSfSymbolDictHeaderSize+2)
	binary.LittleEndian.PutUint32(hdr[:4], qwpSfSymbolDictMagic)
	hdr[4] = qwpSfSymbolDictVersion + 1
	hdr[8], hdr[9] = 1, 'x'
	require.NoError(t, os.WriteFile(path, hdr, 0o644))

	_, err := qwpSfSymbolDictOpenRecovered(dir)
	require.Error(t, err, "wrong version on a recovered slot must fail loud")

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

	_, err = qwpSfSymbolDictOpenRecovered(dir)
	require.Error(t, err, "oversized dictionary must be rejected before the read")
}

func TestQwpSfSymbolDictEntryCountCeiling(t *testing.T) {
	const maxEntries = qwpMaxSymbolDictionarySize

	// Exactly the protocol ceiling remains valid, including the densest legal
	// encoding: one zero-length symbol per byte.
	entries, ok := qwpSfSymbolDictParseEntries(make([]byte, maxEntries), maxEntries)
	require.True(t, ok)
	require.Len(t, entries, maxEntries)

	// One entry beyond the ceiling must be rejected before growing a []string
	// to the attacker-controlled count.
	entries, ok = qwpSfSymbolDictParseEntries(make([]byte, maxEntries+1), maxEntries+1)
	require.False(t, ok)
	require.Nil(t, entries)

	// The ceiling is cumulative across chunks. Because both chunks have valid
	// CRCs, recovery must fail closed and preserve the file rather than treating
	// the second chunk as a torn tail and truncating it.
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	contents := qwpSfTestSymbolDictHeader()
	contents = append(contents, qwpSfTestEmptySymbolChunk(maxEntries/2)...)
	contents = append(contents, qwpSfTestEmptySymbolChunk(maxEntries-maxEntries/2+1)...)
	require.NoError(t, os.WriteFile(path, contents, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.Nil(t, d)
	require.ErrorContains(t, err, "exceeds the 1000000-entry limit")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, contents, got, "checksum-valid oversized dictionary must remain byte-identical")
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

	// Append a torn trailing chunk: count=1, entryBytes=2, entry="x", but
	// omit the CRC, mimicking a crash mid-append.
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{1, 2, 1, 'x'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	re, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.Equal(t, 2, re.size(), "torn tail ignored")
	require.Equal(t, []string{"one", "two"}, re.loadedSymbols())
	// The next append overwrites the torn tail, keeping the file consistent.
	require.NoError(t, re.appendSymbols([]string{"three"}))
	require.NoError(t, re.close())

	re2, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.Equal(t, 3, re2.size())
	require.Equal(t, []string{"one", "two", "three"}, re2.loadedSymbols())
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
	require.Nil(t, d)
	require.Error(t, err)
	require.NotErrorIs(t, err, qwpSfErrSymbolDictAmbiguousFormat)
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

func TestQwpSfSymbolDictTornFirstChunkReportsAmbiguousFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	chunk := qwpSfTestSymbolDictChunk("x")
	contents := append(qwpSfTestSymbolDictHeader(), chunk[:len(chunk)-qwpSfSymbolDictCRCSize]...)
	require.NoError(t, os.WriteFile(path, contents, 0o644))

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.Nil(t, d)
	require.ErrorIs(t, err, qwpSfErrSymbolDictAmbiguousFormat)
	require.ErrorContains(t, err, "ambiguous between the legacy flat format and a torn first chunk")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, contents, got, "ambiguous first chunk must remain byte-identical")
}

func TestQwpSfSymbolDictFreshOpenPropagatesStatFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	legacy := append(qwpSfTestSymbolDictHeader(), 0x01, 'x')
	require.NoError(t, os.WriteFile(path, legacy, 0o644))

	originalStat := qwpSfSymbolDictStat
	qwpSfSymbolDictStat = func(string) (os.FileInfo, error) { return nil, errors.New("injected stat failure") }
	t.Cleanup(func() { qwpSfSymbolDictStat = originalStat })

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

	originalWriteAt := qwpSfSymbolDictWriteAt
	shortWrite := true
	qwpSfSymbolDictWriteAt = func(f *os.File, p []byte, off int64) (int, error) {
		if shortWrite {
			shortWrite = false
			return originalWriteAt(f, p[:len(p)-1], off)
		}
		return originalWriteAt(f, p, off)
	}
	t.Cleanup(func() { qwpSfSymbolDictWriteAt = originalWriteAt })

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

	originalTruncate := qwpSfSymbolDictTruncate
	qwpSfSymbolDictTruncate = func(*os.File, int64) error { return errors.New("injected truncate failure") }
	t.Cleanup(func() { qwpSfSymbolDictTruncate = originalTruncate })

	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.Nil(t, d)
	require.ErrorContains(t, err, "could not drop torn/stale")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, contents, got, "failed truncation must preserve the file for retry")
}

func TestQwpSfSymbolDictLegacyFlatFormatFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	legacy := append(qwpSfTestSymbolDictHeader(), 0x01, 'x', 0x00)
	require.NoError(t, os.WriteFile(path, legacy, 0o644))

	for _, open := range []struct {
		name string
		fn   func(string) (*qwpSfSymbolDict, error)
	}{
		{name: "recovered", fn: qwpSfSymbolDictOpenRecovered},
		{name: "fresh-open", fn: qwpSfSymbolDictOpen},
	} {
		t.Run(open.name, func(t *testing.T) {
			d, err := open.fn(dir)
			require.Nil(t, d)
			require.ErrorIs(t, err, qwpSfErrSymbolDictAmbiguousFormat)
			require.ErrorContains(t, err, "ambiguous between the legacy flat format and a torn first chunk")
			require.ErrorContains(t, err, "safe only if the slot holds no unacked delta frames")
			got, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, legacy, got, "legacy dictionary must remain byte-identical")
		})
	}
}

func TestQwpSfSymbolDictLegacyEngineOpenDoesNotQuarantine(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"x"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	legacy := append(qwpSfTestSymbolDictHeader(), 0x01, 'x')
	require.NoError(t, os.WriteFile(path, legacy, 0o644))

	recovered, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.Nil(t, recovered)
	require.ErrorIs(t, err, qwpSfErrSymbolDictAmbiguousFormat)
	_, statErr := os.Stat(filepath.Join(filepath.Dir(dir), "quarantined"))
	require.True(t, os.IsNotExist(statErr), "operational legacy error must not auto-quarantine the slot")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, legacy, got)
}

func TestQwpSfSymbolDictLegacyDrainerMarksFailedWithRemediation(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"x"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	legacy := append(qwpSfTestSymbolDictHeader(), 0x01, 'x')
	require.NoError(t, os.WriteFile(path, legacy, 0o644))

	drainer := qwpSfNewOrphanDrainer(dir, 4096, qwpSfUnlimitedTotalBytes, nil, nil, 0, 0, 0)
	drainer.drainerRun(context.Background())
	require.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
	failed, err := os.ReadFile(filepath.Join(dir, qwpSfFailedSentinelName))
	require.NoError(t, err)
	require.Contains(t, string(failed), "ambiguous between the legacy flat format and a torn first chunk")
	require.Contains(t, string(failed), "safe only if the slot holds no unacked delta frames")
	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, legacy, got)
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

func qwpSfTestEmptySymbolChunk(count int) []byte {
	body := binary.AppendUvarint(nil, uint64(count))
	body = binary.AppendUvarint(body, uint64(count))
	body = append(body, make([]byte, count)...)
	return qwpSfTestChecksummedChunk(body)
}

func qwpSfTestChecksummedChunk(body []byte) []byte {
	chunk := append([]byte(nil), body...)
	return binary.LittleEndian.AppendUint32(chunk, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
}
