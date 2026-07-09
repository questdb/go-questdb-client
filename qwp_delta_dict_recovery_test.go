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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTestDeltaFrame builds a minimal table-less QWP frame carrying a symbol
// delta section, for exercising the send loop's torn-dictionary guard.
func buildTestDeltaFrame(deltaStart int, syms []string) []byte {
	buf := make([]byte, qwpHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], qwpMagic)
	buf[4] = qwpVersion
	buf[qwpHeaderOffsetFlags] = qwpFlagDeltaSymbolDict
	var vb [qwpMaxVarintLen]byte
	buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(deltaStart))]...)
	buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(len(syms)))]...)
	for _, sym := range syms {
		buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(len(sym)))]...)
		buf = append(buf, sym...)
	}
	binary.LittleEndian.PutUint32(buf[qwpHeaderOffsetPayloadLen:qwpHeaderOffsetPayloadLen+4],
		uint32(len(buf)-qwpHeaderSize))
	return buf
}

// TestQwpPersistNewSymbolsNoDuplicateOnRetry pins C1b: a persist that succeeds
// while the following engine append fails leaves maxSentSymbolId behind the
// persisted count, and the retry must not re-append the same symbols. Entry
// position is the symbol id, so a duplicate would misalign every later id on
// recovery.
func TestQwpPersistNewSymbolsNoDuplicateOnRetry(t *testing.T) {
	dir := t.TempDir()
	d := qwpSfSymbolDictOpen(dir)
	require.NotNil(t, d)

	s := &qwpLineSender{
		persistedSymbolDict: d,
		globalSymbolList:    []string{"AAPL", "GOOG", "MSFT"},
		batchMaxSymbolId:    2,
		maxSentSymbolId:     -1,
	}

	require.NoError(t, s.persistNewSymbols())
	require.Equal(t, 3, d.size())

	// The engine append failed, so maxSentSymbolId stays behind. The retry must
	// be a no-op, not a re-append.
	require.NoError(t, s.persistNewSymbols())
	require.Equal(t, 3, d.size(), "retry must not duplicate persisted symbols")
	require.NoError(t, d.close())

	re := qwpSfSymbolDictOpen(dir)
	require.Equal(t, []string{"AAPL", "GOOG", "MSFT"}, re.loadedSymbols())
	require.NoError(t, re.close())
}

// TestQwpEngineRecoveryMissingDictDisablesDelta pins C1a/C1d: a slot recovered
// with published frames but no side-file falls back to full self-sufficient
// frames (delta disabled) rather than enabling delta over an empty dictionary
// and aliasing the recovered ids.
func TestQwpEngineRecoveryMissingDictDisablesDelta(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	// Append (but never ack) a frame so the segments survive as a recovery.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"AAPL"}))
	require.NoError(t, err)
	require.GreaterOrEqual(t, engine.enginePublishedFsn(), int64(0))
	require.NoError(t, engine.engineClose())

	// Simulate a pre-PR / lost side-file: delete it, keep the segments.
	require.NoError(t, os.Remove(filepath.Join(dir, qwpSfSymbolDictFileName)))

	re, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = re.engineClose() }()
	require.True(t, re.engineWasRecoveredFromDisk())
	require.False(t, re.engineDeltaDictEnabled(),
		"recovered frames with no dictionary must fall back to full-dict mode")
}

// TestQwpEngineRecoveryCorruptDictFailsLoud pins C1c: a recovered slot with a
// corrupt side-file fails the engine open (a sanctioned terminal) rather than
// truncating the dictionary and restarting the id space.
func TestQwpEngineRecoveryCorruptDictFailsLoud(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"AAPL"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	// Corrupt the side-file's magic, keeping the segments intact.
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	require.NoError(t, os.WriteFile(path, []byte{9, 9, 9, 9, 9, 9, 9, 9, 1, 'x'}, 0o644))

	re, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.Error(t, err, "corrupt dictionary on a recovered slot must fail the open")
	require.Nil(t, re)

	got, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte{9, 9, 9, 9, 9, 9, 9, 9, 1, 'x'}, got, "corrupt dictionary must be preserved")
}

// TestQwpSendLoopTornDictGuardGap pins the torn-dictionary guard fires when a
// frame's delta range starts past the mirror tip (a gap).
func TestQwpSendLoopTornDictGuardGap(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	gotCh := make(chan *SenderError, 4)
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		select {
		case gotCh <- e:
		default:
		}
	}, qwpSfMinErrorInboxCapacity)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// The mirror is empty (count 0); a frame starting at id 2 leaves a gap.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(2, []string{"X"}))
	require.NoError(t, err)

	requireTornDictTerminal(t, loop)

	// The terminal is dispatched to the error handler and counted, like every
	// other terminal — not silently latched.
	select {
	case e := <-gotCh:
		assert.Equal(t, CategoryProtocolViolation, e.Category)
	case <-time.After(3 * time.Second):
		t.Fatal("torn-dict terminal was not dispatched to the error handler")
	}
	assert.GreaterOrEqual(t, loop.sendLoopTotalServerErrors(), int64(1))
}

// TestQwpSendLoopTornDictGuardPartialOverlap pins the guard fires on a partial
// overlap — a frame that starts inside the mirror but extends past its tip —
// which the exact-tip accumulate would otherwise silently drop.
func TestQwpSendLoopTornDictGuardPartialOverlap(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// First frame extends the mirror to count 2.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"A", "B"}))
	require.NoError(t, err)
	// Second frame starts at 1 (inside) but runs to 4 (past the tip): torn.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(1, []string{"C", "D", "E"}))
	require.NoError(t, err)

	requireTornDictTerminal(t, loop)
}

func requireTornDictTerminal(t *testing.T, loop *qwpSfSendLoop) {
	t.Helper()
	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 5*time.Second, time.Millisecond, "torn-dict guard never fired")
	var se *SenderError
	require.True(t, errors.As(loop.sendLoopCheckError(), &se))
	assert.Equal(t, CategoryProtocolViolation, se.Category)
	assert.Equal(t, PolicyTerminal, se.AppliedPolicy)
	assert.Contains(t, se.ServerMessage, "resend required")
}
