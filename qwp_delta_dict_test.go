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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// reconstructConnDict rebuilds a connection's server-side symbol dictionary
// from the frames it received, mirroring the server: each frame's delta
// section extends or overwrites the dict, null-padding to reach deltaStart. A
// missing catch-up would show up as an empty-string gap.
func reconstructConnDict(frames []string) []string {
	var dict []string
	for _, f := range frames {
		start, count, symbols, ok := qwpParseDeltaDict([]byte(f))
		if !ok {
			continue
		}
		for len(dict) < start {
			dict = append(dict, "")
		}
		p := 0
		for i := 0; i < count; i++ {
			el, n, err := qwpReadVarint(symbols[p:])
			if err != nil {
				break
			}
			name := string(symbols[p+n : p+n+int(el)])
			p += n + int(el)
			idx := start + i
			for len(dict) <= idx {
				dict = append(dict, "")
			}
			dict[idx] = name
		}
	}
	return dict
}

func connSawTableLessFrame(frames []string) bool {
	for _, f := range frames {
		msg := []byte(f)
		if len(msg) >= qwpHeaderSize &&
			binary.LittleEndian.Uint16(msg[qwpHeaderOffsetTableCount:qwpHeaderOffsetTableCount+2]) == 0 {
			return true
		}
	}
	return false
}

// TestQwpDeltaDictReconnectCatchUpRebuildsDictionary drops connection 1 after
// its first frame; the memory-mode sender reconnects onto a fresh (empty-dict)
// server, where a naive delta replay would leave a gap. The catch-up frame
// re-registers the whole dictionary first, so connection 2's rebuilt dict is
// complete and gap-free.
func TestQwpDeltaDictReconnectCatchUpRebuildsDictionary(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true, closeAfterFrames: 1})
	defer srv.Close()

	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()
	require.True(t, s.deltaDictEnabled, "memory mode must delta-encode")

	ctx := context.Background()
	require.NoError(t, s.Table("t").Symbol("sym", "AAPL").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))

	require.NoError(t, s.Table("t").Symbol("sym", "BETA").Int64Column("v", 2).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))

	var conn2 []string
	require.Eventually(t, func() bool {
		conn2 = reconstructConnDict(srv.recordedFrames()[2])
		return len(conn2) == 2 && conn2[0] == "AAPL" && conn2[1] == "BETA"
	}, 3*time.Second, 5*time.Millisecond, "conn2 dict = %v", conn2)

	require.Equal(t, []string{"AAPL", "BETA"}, conn2)
	require.True(t, connSawTableLessFrame(srv.recordedFrames()[2]),
		"connection 2 must see a table-less catch-up frame")

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 3*time.Second, 5*time.Millisecond)
}

// TestQwpDeltaDictNoCatchUpWhenNothingSent verifies a fresh connection with an
// empty mirror keeps the plain 1:1 baseline (no catch-up frame).
func TestQwpDeltaDictNoCatchUpWhenNothingSent(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true, closeAfterFrames: 1})
	defer srv.Close()

	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	ctx := context.Background()
	// First frame carries no prior state, so connection 1 sees no table-less
	// catch-up frame ahead of it.
	require.NoError(t, s.Table("t").Symbol("sym", "AAPL").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.NoError(t, s.Table("t").Symbol("sym", "BETA").Int64Column("v", 2).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 3*time.Second, 5*time.Millisecond)

	require.False(t, connSawTableLessFrame(srv.recordedFrames()[1]),
		"connection 1 (empty mirror) must not emit a catch-up frame")
}

// TestQwpDeltaDictSplitPathStaysDeltaAcrossReconnect covers the per-table split
// path (enqueueCursorSplit) with symbol columns — the case no other test
// exercised. When one over-cap batch splits into a frame per table, the symbol
// dictionary must stay delta-encoded: the first split frame ships only the
// batch's new ids (above the already-sent watermark, never the whole
// dictionary) and advances the watermark, so every later split frame carries an
// empty delta instead of re-shipping the batch. A full-dict frame here would be
// skipped by the send-loop mirror and gap the reconnect catch-up, so the test
// also drops the connection after the split and checks the catch-up rebuilds the
// whole dictionary — including the split-path ids.
func TestQwpDeltaDictSplitPathStaysDeltaAcrossReconnect(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true, closeAfterFrames: 3})
	defer srv.Close()

	s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()
	require.True(t, s.deltaDictEnabled, "memory mode must delta-encode")

	ctx := context.Background()

	// Send one symbol first so the dictionary watermark sits past id 0 when the
	// split batch runs — that is what makes "ships only the new ids" observable.
	require.NoError(t, s.Table("t0").Symbol("sym", "OLD").Int64Column("v", 0).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 3*time.Second, 5*time.Millisecond)

	// A two-table batch forced over a small cap so enqueueCursorSplit re-encodes
	// each table as its own frame. The combined frame is 70 bytes; each
	// single-table frame is well under 60, so both fit once split.
	s.serverMaxBatchSize.Store(60)
	require.NoError(t, s.Table("t1").Symbol("sym", "AAA").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Table("t2").Symbol("sym", "BBB").Int64Column("v", 2).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))

	// conn1 receives [OLD, split-t1, split-t2] before the drop at frame 3.
	require.Eventually(t, func() bool {
		return len(srv.recordedFrames()[1]) >= 3
	}, 3*time.Second, 5*time.Millisecond, "split did not produce two per-table frames")
	conn1 := srv.recordedFrames()[1]
	require.Len(t, conn1, 3)

	// The first split frame ships only the batch's new ids, starting above the
	// OLD watermark (start=1) — never the whole dictionary, which would start at 0.
	start1, count1, _, ok1 := qwpParseDeltaDict([]byte(conn1[1]))
	require.True(t, ok1)
	require.Equal(t, 1, start1, "split frame must not re-ship already-sent ids")
	require.Equal(t, 2, count1, "the first split frame ships the batch's two new ids")

	// The second split frame carries an empty delta: the baseline advanced
	// per-frame, so it references the ids the first frame already registered
	// rather than re-shipping the batch delta.
	_, count2, _, ok2 := qwpParseDeltaDict([]byte(conn1[2]))
	require.True(t, ok2)
	require.Equal(t, 0, count2, "later split frames must not re-ship the batch delta")

	require.Equal(t, []string{"OLD", "AAA", "BBB"}, reconstructConnDict(conn1))

	// The drop after the split forces a reconnect onto a fresh (empty-dict)
	// server; a follow-up flush drives it. The catch-up must re-register the
	// whole dictionary, including the split-path ids, or the replayed delta
	// frames would dangle a symbol id on the fresh server.
	require.NoError(t, s.Table("t3").Symbol("sym", "CCC").Int64Column("v", 3).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))

	var conn2 []string
	require.Eventually(t, func() bool {
		conn2 = reconstructConnDict(srv.recordedFrames()[2])
		return len(conn2) >= 3 &&
			conn2[0] == "OLD" && conn2[1] == "AAA" && conn2[2] == "BBB"
	}, 3*time.Second, 5*time.Millisecond, "conn2 dict = %v", conn2)

	require.True(t, connSawTableLessFrame(srv.recordedFrames()[2]),
		"connection 2 must re-register the dictionary with a catch-up frame")
}

// TestQwpDeltaDictSeedFromPersisted checks the recovery seeding: the producer
// resumes its global dictionary and delta baseline at the recovered tip, and
// the send loop rebuilds a catch-up frame carrying the same symbols in id
// order.
func TestQwpDeltaDictSeedFromPersisted(t *testing.T) {
	dir := t.TempDir()
	seed, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, seed)
	require.NoError(t, seed.appendSymbols([]string{"a", "b", "c"}))
	require.NoError(t, seed.close())

	// Reopen: recovery loads the entries via openExisting (a fresh dict has no
	// loaded set), mirroring the engine's recovery path.
	pd, err := qwpSfSymbolDictOpen(dir)
	require.NoError(t, err)
	require.NotNil(t, pd)
	defer pd.close()
	require.Equal(t, []string{"a", "b", "c"}, pd.loadedSymbols())

	// Fill both users of the recovered dictionary from the same id-ordered
	// list, in the order the cursor sender builds them: send-loop mirror first,
	// then producer. Neither one may modify the list they share.
	recovered := pd.loadedSymbols()
	var l qwpSfSendLoop
	l.seedSentDictFromSymbols(recovered)
	require.Equal(t, 3, l.sentDictCount)
	require.Equal(t, []string{"a", "b", "c"}, pd.loadedSymbols(),
		"the mirror seed must not drop the recovered entries before the producer seeds")

	frame := l.buildCatchUpFrame(0, l.sentDictCount, l.sentDictBytes)
	require.Equal(t, []string{"a", "b", "c"}, reconstructConnDict([]string{string(frame)}))

	s := &qwpLineSender{
		globalSymbols:       map[string]int32{},
		maxSentSymbolId:     -1,
		persistedSymbolDict: pd,
	}
	s.seedSymbolDictFromRecovered(recovered)
	require.Equal(t, []string{"a", "b", "c"}, s.globalSymbolList)
	require.Equal(t, int32(0), s.globalSymbols["a"])
	require.Equal(t, int32(2), s.globalSymbols["c"])
	require.Equal(t, 2, s.maxSentSymbolId, "baseline resumes at the recovered tip")
}

// TestQwpDeltaDictSfPersistsSymbols verifies the SF write-ahead persistence:
// new symbols land in the slot's .symbol-dict before their frame is published,
// so a recovered slot can rebuild the dictionary.
func TestQwpDeltaDictSfPersistsSymbols(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	slot := filepath.Join(t.TempDir(), "slot0")
	engine, err := qwpSfNewCursorEngine(slot, 1<<16, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	s, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 5*time.Second)
	require.NoError(t, err)

	require.True(t, s.deltaDictEnabled, "SF with an open side-file must delta-encode")
	require.NotNil(t, s.persistedSymbolDict)

	ctx := context.Background()
	require.NoError(t, s.Table("t").Symbol("sym", "AAPL").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Table("t").Symbol("sym", "MSFT").Int64Column("v", 2).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, 3*time.Second, 5*time.Millisecond)

	// Read the side-file through a second handle (before Close fully drains and
	// removes it): the new symbols were persisted in id order.
	check, err := qwpSfSymbolDictOpen(slot)
	require.NoError(t, err)
	require.NotNil(t, check)
	require.Equal(t, []string{"AAPL", "MSFT"}, check.loadedSymbols())
	require.NoError(t, check.close())

	require.NoError(t, s.Close(ctx))
}

// TestQwpDeltaDictTornDictGuardFires drives trySendOne over a frame whose
// delta starts above the recovered dictionary (a host-crash tear) and asserts
// the guard returns a terminal PROTOCOL_VIOLATION before the frame goes out.
func TestQwpDeltaDictTornDictGuardFires(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	engine.engineSetSendLoopWakeup(func() {})
	defer engine.engineClose()

	// A delta frame claiming to start at id 2 — a gap above a recovered
	// dictionary that only holds id 0.
	frame := make([]byte, qwpHeaderSize)
	binary.LittleEndian.PutUint32(frame[0:4], qwpMagic)
	frame[4] = qwpVersion
	frame[qwpHeaderOffsetFlags] = qwpFlagDeltaSymbolDict
	var vb [qwpMaxVarintLen]byte
	frame = append(frame, vb[:qwpPutVarint(vb[:], 2)]...) // deltaStart = 2
	frame = append(frame, vb[:qwpPutVarint(vb[:], 0)]...) // deltaCount = 0
	binary.LittleEndian.PutUint32(frame[qwpHeaderOffsetPayloadLen:qwpHeaderOffsetPayloadLen+4],
		uint32(len(frame)-qwpHeaderSize))

	_, err = engine.engineAppendBlocking(context.Background(), frame)
	require.NoError(t, err)

	l := &qwpSfSendLoop{engine: engine, sentDictCount: 1}
	l.transport.Store(&qwpTransport{})

	sent, sendErr := l.trySendOne(context.Background())
	require.False(t, sent)
	var se *SenderError
	require.ErrorAs(t, sendErr, &se)
	require.Equal(t, CategoryProtocolViolation, se.Category)
	require.Equal(t, PolicyTerminal, se.AppliedPolicy)
	require.Contains(t, se.ServerMessage, "resend required")
}

func TestQwpDeltaDictTornDictSE(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer engine.engineClose()

	l := &qwpSfSendLoop{engine: engine}
	se := l.qwpSfBuildTornDictSE(5, 3)
	require.Equal(t, CategoryProtocolViolation, se.Category)
	require.Equal(t, PolicyTerminal, se.AppliedPolicy)
	require.Contains(t, se.ServerMessage, "resend required")
}

// TestQwpDeltaDictMirrorAccumulatesPartialOverlap covers a shape replay is
// allowed to produce: a frame that repeats ids the mirror already holds and
// carries on past them. The mirror must skip the repeated ones and keep the new
// ones for the next reconnect; ignoring the whole frame would leave the next
// connection short.
func TestQwpDeltaDictMirrorAccumulatesPartialOverlap(t *testing.T) {
	var l qwpSfSendLoop
	l.accumulateSentDict(buildTestDeltaFrame(0, []string{"a", "b"}))
	require.Equal(t, 2, l.sentDictCount)

	// id 1 repeats the "b" the mirror already holds; ids 2 and 3 are new.
	l.accumulateSentDict(buildTestDeltaFrame(1, []string{"b", "c", "d"}))
	require.Equal(t, 4, l.sentDictCount)

	frame := l.buildCatchUpFrame(0, l.sentDictCount, l.sentDictBytes)
	require.Equal(t, []string{"a", "b", "c", "d"},
		reconstructConnDict([]string{string(frame)}))
}

func TestQwpDeltaDictParseHelpers(t *testing.T) {
	var buf []byte
	hdr := make([]byte, qwpHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], qwpMagic)
	hdr[4] = qwpVersion
	hdr[qwpHeaderOffsetFlags] = qwpFlagDeltaSymbolDict
	buf = append(buf, hdr...)
	var vb [qwpMaxVarintLen]byte
	buf = append(buf, vb[:qwpPutVarint(vb[:], 2)]...) // deltaStart
	buf = append(buf, vb[:qwpPutVarint(vb[:], 2)]...) // deltaCount
	for _, sym := range []string{"hi", "yo"} {
		buf = append(buf, vb[:qwpPutVarint(vb[:], uint64(len(sym)))]...)
		buf = append(buf, sym...)
	}
	binary.LittleEndian.PutUint32(buf[qwpHeaderOffsetPayloadLen:qwpHeaderOffsetPayloadLen+4],
		uint32(len(buf)-qwpHeaderSize))

	rStart, rCount, rOk := qwpFrameDeltaRange(buf)
	require.True(t, rOk)
	require.Equal(t, 2, rStart)
	require.Equal(t, 2, rCount)
	start, count, symbols, ok := qwpParseDeltaDict(buf)
	require.True(t, ok)
	require.Equal(t, 2, start)
	require.Equal(t, 2, count)
	require.Equal(t, []byte{2, 'h', 'i', 2, 'y', 'o'}, symbols)

	nodelta := append([]byte(nil), buf...)
	nodelta[qwpHeaderOffsetFlags] = 0
	_, _, rOk = qwpFrameDeltaRange(nodelta)
	require.False(t, rOk)
	_, _, _, ok = qwpParseDeltaDict(nodelta)
	require.False(t, ok)

	_, _, rOk = qwpFrameDeltaRange([]byte{1, 2, 3})
	require.False(t, rOk)
}
