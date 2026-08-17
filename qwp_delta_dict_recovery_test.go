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
		deltaDictEnabled:    true,
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

func TestQwpPersistFailureDisablesProducerDeltaForRetry(t *testing.T) {
	d := qwpSfSymbolDictOpen(t.TempDir())
	require.NotNil(t, d)
	// Close the underlying file but keep the dictionary object in use, so the
	// next write fails the way a disk problem mid-run would.
	require.NoError(t, d.file.Close())
	s := &qwpLineSender{
		persistedSymbolDict: d,
		globalSymbolList:    []string{"AAPL"},
		batchMaxSymbolId:    0,
		maxSentSymbolId:     -1,
		deltaDictEnabled:    true,
	}

	err := s.persistNewSymbols()
	require.ErrorContains(t, err, "switched to full-dictionary mode")
	require.False(t, s.deltaDictEnabled)
	require.Equal(t, 0, d.size(), "failed write must not advance the durable id count")
	// The caller still holds the rows. Its retry must skip the broken
	// side-file and encode a self-sufficient frame instead of failing again.
	require.NoError(t, s.persistNewSymbols())
	_ = d.close()
}

// TestQwpSplitPersistFailureRetainsBatchForFullDictRetry pins what happens
// when writing the symbols to the side-file fails on the per-table split path.
// The write fails on the split's first frame, the flush reports the switch to
// full-dictionary mode, and the WHOLE batch stays with the caller. The retry
// splits again, this time with self-sufficient frames, and every row reaches
// the server with no missing ids in its dictionary.
func TestQwpSplitPersistFailureRetainsBatchForFullDictRetry(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true})
	defer srv.Close()

	slot := filepath.Join(t.TempDir(), "slot0")
	engine, err := qwpSfNewCursorEngine(slot, 1<<16, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()
	s, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 5*time.Second)
	require.NoError(t, err)
	defer func() { _ = s.Close(context.Background()) }()
	require.True(t, s.deltaDictEnabled, "SF with an open side-file must delta-encode")
	require.NotNil(t, s.persistedSymbolDict)

	ctx := context.Background()
	require.NoError(t, s.Table("t1").Symbol("sym", "AAA").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Table("t2").Symbol("sym", "BBB").Int64Column("v", 2).AtNow(ctx))
	// The combined two-table frame overruns the cap so the flush takes
	// enqueueCursorSplit; each single-table frame fits on its own.
	s.serverMaxBatchSize.Store(60)
	// Close the side-file's underlying file but keep the dictionary object in
	// use, so writing its symbols fails before any frame is appended.
	require.NoError(t, s.persistedSymbolDict.file.Close())

	err = s.Flush(ctx)
	require.ErrorContains(t, err, "switched to full-dictionary mode")
	require.False(t, s.deltaDictEnabled)
	require.Equal(t, 2, s.pendingRowCount, "the failed split must retain the whole batch")

	require.NoError(t, s.Flush(ctx))
	require.Equal(t, 0, s.pendingRowCount)
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 3*time.Second, 5*time.Millisecond, "retried split frames did not drain")

	conn1 := srv.recordedFrames()[1]
	require.Len(t, conn1, 2, "retry must re-split into one frame per table")
	require.Equal(t, []string{"AAA", "BBB"}, reconstructConnDict(conn1),
		"full-dict retry frames must rebuild a gap-free dictionary")
}

// TestQwpEngineRecoveryMissingDictDisablesProducerDeltaButReplaysContiguousFrames
// pins that what the producer may write and what the send loop must track are
// two separate things. A missing side-file stops NEW delta frames, but the send
// loop still mirrors a recovered run of delta frames that starts at id 0 and
// has no holes. If the mirror followed the producer's mode instead, it would
// stay empty and the second frame would be rejected as torn.
func TestQwpEngineRecoveryMissingDictDisablesProducerDeltaButReplaysContiguousFrames(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	// Append (but never ack) two delta frames that between them cover ids 0
	// and 1 with no hole, so the segments are left behind for recovery.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"AAPL"}))
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(1, []string{"MSFT"}))
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
		"new producer frames must fall back to full-dict mode")
	require.Equal(t, []string{"AAPL", "MSFT"}, re.engineRecoveredSymbols(),
		"surviving frames must rebuild the producer dictionary in id order")
	producer := &qwpLineSender{
		globalSymbols:       make(map[string]int32),
		maxSentSymbolId:     -1,
		batchMaxSymbolId:    -1,
		tableBuffers:        make(map[string]*qwpTableBuffer),
		deltaDictEnabled:    true, // wireDeltaDict overwrites this
		persistedSymbolDict: nil,
	}
	producer.wireDeltaDict(re)
	require.Equal(t, int32(0), producer.globalSymbols["AAPL"])
	require.Equal(t, int32(1), producer.globalSymbols["MSFT"])
	require.Equal(t, 1, producer.maxSentSymbolId,
		"new ids must carry on above the recovered ones, not restart at 0")

	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(re, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	require.Eventually(t, func() bool {
		return re.engineAckedFsn() >= re.enginePublishedFsn()
	}, 5*time.Second, time.Millisecond, "contiguous recovered deltas did not drain")
	require.NoError(t, loop.sendLoopCheckError())
	// Wait for the loop to stop before reading fields its I/O goroutine owns.
	require.NoError(t, loop.sendLoopClose())
	require.Equal(t, 2, loop.sentDictCount)
	require.True(t, loop.hasReplayDictionaryDependency,
		"a recovered slot with no side-file must still send the dictionary on reconnect")
}

func TestQwpEngineRecoveryHealsChecksummedDictFromSurvivingFrames(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	pd := engine.enginePersistedSymbolDict()
	require.NotNil(t, pd)
	require.NoError(t, pd.appendSymbols([]string{"AAPL"}))
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"AAPL"}))
	require.NoError(t, err)
	require.NoError(t, pd.appendSymbols([]string{"MSFT"}))
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(1, []string{"MSFT"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	// Corrupt the second chunk's contents so its checksum no longer matches.
	// Open then keeps only the first chunk, and recovery reads id 1 back out of
	// frame 1 and writes it to the side-file before replay.
	path := filepath.Join(dir, qwpSfSymbolDictFileName)
	buf, err := os.ReadFile(path)
	require.NoError(t, err)
	firstEnd := qwpTestSymbolDictChunkEnd(t, buf, int(qwpSfSymbolDictHeaderSize))
	secondEntries := qwpTestSymbolDictEntriesStart(t, buf, firstEnd)
	_, adv, err := qwpReadVarint(buf[secondEntries:])
	require.NoError(t, err)
	buf[secondEntries+adv] ^= 0x20
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	recovered, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	require.Equal(t, []string{"AAPL", "MSFT"}, recovered.engineRecoveredSymbols())
	require.True(t, recovered.engineDeltaDictEnabled())
	require.Equal(t, 2, recovered.enginePersistedSymbolDict().size(),
		"the ids recovered from frames must be written back before those frames are trimmed")
	require.NoError(t, recovered.engineClose())

	reopened := qwpSfSymbolDictOpen(dir)
	require.NotNil(t, reopened)
	require.Equal(t, []string{"AAPL", "MSFT"}, reopened.loadedSymbols())
	require.NoError(t, reopened.close())
}

// TestQwpAnalyzeRecoveredDictAckedGapReset pins both outcomes of a hole in the
// recovered dictionary. A hole in a frame that is already acked is cleared by a
// later frame that carries the dictionary from id 0. The same hole in a frame
// still waiting to be sent is fatal, because that frame goes out first and
// nothing later can undo the order.
func TestQwpAnalyzeRecoveredDictAckedGapReset(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	// fsn 0 leaves a hole: it starts at id 5 with nothing known yet. fsn 1
	// starts over from id 0, and fsn 2 continues straight on from it.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(5, []string{"X"}))
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"A"}))
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(1, []string{"B"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	ring, err := qwpSfOpenRing(dir, 4096)
	require.NoError(t, err)
	defer func() { _ = ring.segmentRingClose() }()

	// With the holed frame acked, the restart at fsn 1 covers everything that
	// still has to be sent.
	analysis, err := qwpSfAnalyzeRecoveredDict(ring, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"A", "B"}, analysis.symbols)
	require.Equal(t, 1, analysis.maxReplayDeltaStart)

	// Same frames, none acked: the holed frame is replayed first, so the
	// restart that follows it comes too late.
	_, err = qwpSfAnalyzeRecoveredDict(ring, -1, nil)
	require.ErrorContains(t, err, "resend required")
}

func TestQwpEngineRecoveryMissingDictRejectsUnackedGap(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(2, []string{"X"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())
	require.NoError(t, os.Remove(filepath.Join(dir, qwpSfSymbolDictFileName)))

	recovered, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.ErrorContains(t, err, "resend required")
	require.Nil(t, recovered)
}

// TestQwpEngineRecoveryCorruptDictFallsBackToSurvivingFrames pins the two
// halves of how a damaged side-file is handled: the bad content stays on disk
// so it can be inspected, and the slot still opens as long as the surviving
// frames rebuild the dictionary on their own.
func TestQwpEngineRecoveryCorruptDictFallsBackToSurvivingFrames(t *testing.T) {
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
	require.NoError(t, err)
	require.NotNil(t, re)
	require.False(t, re.engineDeltaDictEnabled())
	require.Equal(t, []string{"AAPL"}, re.engineRecoveredSymbols())
	require.NoError(t, re.engineClose())

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

// TestQwpSendLoopPartialOverlapExtendsMirror pins that a frame repeating ids
// the mirror already holds is sent, and that only the ids beyond them are added
// to the mirror. Rejecting such a frame as torn, or ignoring it in
// accumulateSentDict, would leave the next connection without those extra ids.
func TestQwpSendLoopPartialOverlapExtendsMirror(t *testing.T) {
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
	// The second frame covers ids 1 through 3. Id 1 repeats what the mirror
	// already has; ids 2 and 3 are new.
	_, err = engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(1, []string{"B", "C", "D"}))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 5*time.Second, time.Millisecond, "partial-overlap frame did not drain")
	require.NoError(t, loop.sendLoopCheckError())
	require.NoError(t, loop.sendLoopClose())
	require.Equal(t, 4, loop.sentDictCount)
	catchUp := loop.buildCatchUpFrame(0, loop.sentDictCount, loop.sentDictBytes)
	require.Equal(t, []string{"A", "B", "C", "D"},
		reconstructConnDict([]string{string(catchUp)}))
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
