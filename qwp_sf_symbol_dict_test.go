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
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQwpSfSymbolDictAppendPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	d := qwpSfSymbolDictOpen(dir)
	require.NotNil(t, d)
	require.Equal(t, 0, d.size())
	require.NoError(t, d.appendSymbols([]string{"AAPL", "GOOG", "MSFT"}))
	require.Equal(t, 3, d.size())
	require.NoError(t, d.close())

	re := qwpSfSymbolDictOpen(dir)
	require.NotNil(t, re)
	require.Equal(t, 3, re.size())
	require.Equal(t, []string{"AAPL", "GOOG", "MSFT"}, re.loadedSymbols())
	// Appending after recovery continues from the recovered tip.
	require.NoError(t, re.appendSymbols([]string{"TSLA"}))
	require.Equal(t, 4, re.size())
	require.NoError(t, re.close())

	third := qwpSfSymbolDictOpen(dir)
	require.NotNil(t, third)
	require.Equal(t, 4, third.size())
	require.Equal(t, "TSLA", third.loadedSymbols()[3])
	require.NoError(t, third.close())
}

func TestQwpSfSymbolDictUsesJavaCompatibleChecksummedChunks(t *testing.T) {
	dir := t.TempDir()
	d := qwpSfSymbolDictOpen(dir)
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
		"one append must produce exactly one Java-format chunk")
}

func TestQwpSfSymbolDictCRCRejectsCorruptChunkAndKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	d := qwpSfSymbolDictOpen(dir)
	require.NoError(t, d.appendSymbols([]string{"first"}))
	require.NoError(t, d.appendSymbols([]string{"second"}))
	require.NoError(t, d.close())

	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	buf, err := os.ReadFile(path)
	require.NoError(t, err)
	firstEnd := qwpTestSymbolDictChunkEnd(t, buf, int(qwpSfSymbolDictHeaderSize))
	secondEntries := qwpTestSymbolDictEntriesStart(t, buf, firstEnd)
	// Skip the entry-length varint and corrupt one UTF-8 byte without updating
	// the stored checksum. Length-only recovery would silently trust it.
	_, adv, err := qwpReadVarint(buf[secondEntries:])
	require.NoError(t, err)
	buf[secondEntries+adv] ^= 0x20
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	re := qwpSfSymbolDictOpen(dir)
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
		"a checksum-free body is indistinguishable from a corrupt first chunk and must not seed ids")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, buf, got, "recovery must preserve untrusted content for inspection or an older client")
}

// TestQwpSfSymbolDictRecoveredEntryCountBounded pins the crafted-file
// allocation guard: a CRC-valid chunk of empty-string entries amplifies ~16x
// into string headers, so recovery must refuse an over-bound entry count
// rather than parse it.
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
	require.Nil(t, d, "over-bound entry count must fall back, not allocate")
}

func TestQwpSfSymbolDictOpenRecoveredAbsentReturnsNil(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d, "absent dictionary on a recovered slot degrades to full-dict fallback")
}

func TestQwpSfSymbolDictOpenCleanDoesNotInheritExistingIDs(t *testing.T) {
	dir := t.TempDir()
	old := qwpSfSymbolDictOpen(dir)
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
	d := qwpSfSymbolDictOpen(dir)
	require.NoError(t, d.appendSymbols([]string{"AAPL", "GOOG"}))
	require.NoError(t, d.close())

	re, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.NotNil(t, re)
	require.Equal(t, []string{"AAPL", "GOOG"}, re.loadedSymbols())
	require.NoError(t, re.close())
}

// TestQwpSfSymbolDictOpenRecoveredCorruptFallsBack pins the Java-compatible
// disposition: proven bad content disables delta for this recovery while the
// file stays intact for forensics. The engine's frame fold separately decides
// whether the surviving slot can be rebuilt safely.
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
// treated like bad magic: left intact with full-dict fallback on recovery, but
// recreated on a fresh open where no segment can reference its ids.
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
	require.Equal(t, hdr, got, "recovery must leave the unknown-version file intact")

	d := qwpSfSymbolDictOpen(dir)
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
	require.Nil(t, recovered, "oversized content must fall back before the read")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(qwpSfSymbolDictMaxFileSize+1), info.Size(),
		"recovery must not truncate an oversized forensic artifact")
}

func TestQwpSfSymbolDictBadMagicRecreatedEmpty(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfSymbolDictFileName),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 0o644))

	d := qwpSfSymbolDictOpen(dir)
	require.NotNil(t, d)
	require.Equal(t, 0, d.size(), "bad-magic file recreated empty")
	require.NoError(t, d.appendSymbols([]string{"X"}))
	require.Equal(t, 1, d.size())
	require.NoError(t, d.close())
}

func TestQwpSfSymbolDictEmptySymbolRoundTrips(t *testing.T) {
	dir := t.TempDir()
	d := qwpSfSymbolDictOpen(dir)
	require.NoError(t, d.appendSymbols([]string{"", "nonempty"}))
	require.NoError(t, d.close())

	re := qwpSfSymbolDictOpen(dir)
	require.Equal(t, 2, re.size())
	require.Equal(t, []string{"", "nonempty"}, re.loadedSymbols())
	require.NoError(t, re.close())
}

func TestQwpSfSymbolDictRemoveOrphanDeletesFile(t *testing.T) {
	dir := t.TempDir()
	d := qwpSfSymbolDictOpen(dir)
	require.NoError(t, d.appendSymbols([]string{"A"}))
	require.NoError(t, d.close())

	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	_, err := os.Stat(path)
	require.NoError(t, err)
	qwpSfSymbolDictRemoveOrphan(dir)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestQwpSfSymbolDictTornTrailingEntrySelfHeals(t *testing.T) {
	dir := t.TempDir()
	d := qwpSfSymbolDictOpen(dir)
	require.NoError(t, d.appendSymbols([]string{"one", "two"}))
	require.NoError(t, d.close())

	cleanInfo, err := os.Stat(filepath.Join(dir, qwpSfSymbolDictFileName))
	require.NoError(t, err)

	// Append a torn trailing record. Its suffix deliberately forms a valid
	// one-byte entry if a later, shorter append only overwrites the leading 5:
	// [5, 1, 'G'] -> append empty [0] -> [0, 1, 'G']. Without truncating at
	// reopen, the following recovery invents a ghost symbol "G".
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{5, 1, 'G'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	re := qwpSfSymbolDictOpen(dir)
	require.Equal(t, 2, re.size(), "torn tail ignored")
	require.Equal(t, []string{"one", "two"}, re.loadedSymbols())
	trimmedInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, cleanInfo.Size(), trimmedInfo.Size(), "reopen must truncate the torn tail")
	// An empty symbol is shorter than the torn record and exposes stale residue
	// unless reopen truncated first.
	require.NoError(t, re.appendSymbols([]string{""}))
	require.NoError(t, re.close())

	re2 := qwpSfSymbolDictOpen(dir)
	require.Equal(t, 3, re2.size())
	require.Equal(t, []string{"one", "two", ""}, re2.loadedSymbols())
	require.NoError(t, re2.close())
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
