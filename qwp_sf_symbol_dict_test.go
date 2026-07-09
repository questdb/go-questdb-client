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

func TestQwpSfSymbolDictOpenRecoveredAbsentReturnsNil(t *testing.T) {
	dir := t.TempDir()
	d, err := qwpSfSymbolDictOpenRecovered(dir)
	require.NoError(t, err)
	require.Nil(t, d, "absent dictionary on a recovered slot degrades to full-dict fallback")
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

	_, err = qwpSfSymbolDictOpenRecovered(dir)
	require.Error(t, err, "oversized dictionary must be rejected before the read")
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

	// Append a torn trailing record: a length prefix of 5 followed by only
	// 2 bytes, mimicking a crash mid-append.
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{5, 'x', 'y'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	re := qwpSfSymbolDictOpen(dir)
	require.Equal(t, 2, re.size(), "torn tail ignored")
	require.Equal(t, []string{"one", "two"}, re.loadedSymbols())
	// The next append overwrites the torn tail, keeping the file consistent.
	require.NoError(t, re.appendSymbols([]string{"three"}))
	require.NoError(t, re.close())

	re2 := qwpSfSymbolDictOpen(dir)
	require.Equal(t, 3, re2.size())
	require.Equal(t, []string{"one", "two", "three"}, re2.loadedSymbols())
	require.NoError(t, re2.close())
}
