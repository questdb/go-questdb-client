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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The tests in this file pin the invariant that a disk-backed engine always
// holds an open symbol dictionary containing every id its frames use, made
// durable before the slot is registered with the segment manager.

// qwpTestWriteDeltaSlot writes a recovered-slot fixture: one 4096-byte segment
// per element of segments, holding those frame payloads, with frame numbers
// counted from 0 and a manifest covering every segment. It returns the segment
// paths in order.
func qwpTestWriteDeltaSlot(t *testing.T, dir string, segments ...[][]byte) []string {
	t.Helper()
	var (
		segs  []*qwpSfSegment
		paths []string
		base  int64
	)
	for i, frames := range segments {
		name := "sf-initial.sfa"
		if i > 0 {
			name = fmt.Sprintf("sf-%04d.sfa", i)
		}
		payloads := make([]string, len(frames))
		for j, frame := range frames {
			payloads[j] = string(frame)
		}
		segs = append(segs, createRecoverySegment(t, dir, name, base, payloads...))
		paths = append(paths, filepath.Join(dir, name))
		base += int64(len(frames))
	}
	createRecoveryManifest(t, dir, segs[0].segmentBaseSeq(), segs[len(segs)-1].segmentBaseSeq(), segs...)
	closeRecoverySegments(t, segs...)
	return paths
}

// qwpTestWriteSymbolDict writes a dictionary file holding one checksummed
// chunk per element of chunks.
func qwpTestWriteSymbolDict(t *testing.T, dir string, chunks ...[]string) {
	t.Helper()
	buf := qwpSfTestSymbolDictHeader()
	for _, chunk := range chunks {
		buf = append(buf, qwpSfTestSymbolDictChunk(chunk...)...)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfSymbolDictFileName), buf, 0o644))
}

// qwpTestReadSymbolDict parses the dictionary file in dir and requires that
// every byte of it belongs to a checksummed chunk.
func qwpTestReadSymbolDict(t *testing.T, dir string) (symbols []string, chunks int) {
	t.Helper()
	buf, err := os.ReadFile(filepath.Join(dir, qwpSfSymbolDictFileName))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(buf), int(qwpSfSymbolDictHeaderSize))
	symbols, pos, chunks := qwpSfParseChunkedSymbolDict(buf)
	require.Equal(t, len(buf), pos, "the dictionary must hold no bytes after its last chunk")
	return symbols, chunks
}

// qwpTestReadFiles returns the contents of the named files in dir.
func qwpTestReadFiles(t *testing.T, dir string, names ...string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte, len(names))
	for _, name := range names {
		buf, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		out[name] = buf
	}
	return out
}

// qwpTestSlotFileNames lists the segment, manifest and dictionary files of a
// fixture written by qwpTestWriteDeltaSlot.
func qwpTestSlotFileNames(segmentPaths []string) []string {
	names := []string{qwpSfManifestFileName, qwpSfSymbolDictFileName}
	for _, path := range segmentPaths {
		names = append(names, filepath.Base(path))
	}
	return names
}

// qwpTestSwapSymbolDictLimits installs limits for the rest of the test.
func qwpTestSwapSymbolDictLimits(t *testing.T, limits qwpSfSymbolDictLimitSet) {
	t.Helper()
	original := qwpSfSymbolDictLimits.load()
	qwpSfSymbolDictLimits.store(limits)
	t.Cleanup(func() { qwpSfSymbolDictLimits.store(original) })
}

// qwpTestBarrierLog records, in order, the dictionary fsyncs, the directory
// barriers on one slot, and the engine's registration with its manager.
type qwpTestBarrierLog struct {
	mu     sync.Mutex
	events []string
}

func (l *qwpTestBarrierLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *qwpTestBarrierLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

func qwpTestRecordBarriers(t *testing.T, slotDir string) *qwpTestBarrierLog {
	t.Helper()
	log := &qwpTestBarrierLog{}
	previousSync := qwpSfSymbolDictSync.load()
	qwpSfSymbolDictSync.store(func(f *os.File) error {
		log.add("dict-sync")
		return previousSync(f)
	})
	dirHook := func(dir string) error {
		if dir == slotDir {
			log.add("dir-sync")
		}
		return nil
	}
	qwpSfTestDirSyncHook.Store(&dirHook)
	registerHook := func() error {
		log.add("register")
		return nil
	}
	qwpSfTestBeforeEngineRegisterHook.Store(&registerHook)
	t.Cleanup(func() {
		qwpSfSymbolDictSync.store(previousSync)
		qwpSfTestDirSyncHook.Store(nil)
		qwpSfTestBeforeEngineRegisterHook.Store(nil)
	})
	return log
}

// qwpTestRequireBefore fails unless the first occurrence of first comes before
// the first occurrence of second.
func qwpTestRequireBefore(t *testing.T, events []string, first, second string) {
	t.Helper()
	i := slices.Index(events, first)
	j := slices.Index(events, second)
	require.GreaterOrEqual(t, i, 0, "%s did not happen: %v", first, events)
	require.GreaterOrEqual(t, j, 0, "%s did not happen: %v", second, events)
	require.Less(t, i, j, "%s must come before %s: %v", first, second, events)
}

// qwpTestRequireDirSyncAfterDictSync fails unless a directory barrier on the
// slot follows the first dictionary fsync.
func qwpTestRequireDirSyncAfterDictSync(t *testing.T, events []string) {
	t.Helper()
	i := slices.Index(events, "dict-sync")
	require.GreaterOrEqual(t, i, 0, "the dictionary was not synced: %v", events)
	require.Contains(t, events[i+1:], "dir-sync",
		"a directory barrier must follow the dictionary sync: %v", events)
}

func qwpTestOpenEngine(t *testing.T, dir string) *qwpSfCursorEngine {
	t.Helper()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	return engine
}

func qwpTestRequireNotQuarantined(t *testing.T, engine *qwpSfCursorEngine) {
	t.Helper()
	require.Empty(t, engine.engineQuarantinedSlotPath(), "the slot must recover, not be preserved aside")
}

// qwpTestAckAndAwaitTrim acknowledges through fsn and waits until the segment
// file at path is gone.
func qwpTestAckAndAwaitTrim(t *testing.T, engine *qwpSfCursorEngine, fsn int64, path string) {
	t.Helper()
	engine.engineAcknowledge(fsn)
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return errors.Is(err, os.ErrNotExist)
	}, qwpTestWaitTimeout, time.Millisecond, "the acknowledged segment was not trimmed")
}

// Test 1. The dictionary is lost while the slot still holds the frames that
// introduced its ids. The next session rebuilds it from those frames and makes
// it durable before anything can trim them, so a later session still finds
// the names after the introducing frame is gone.
func TestQwpSfLostDictionarySurvivesTrimAndRestart(t *testing.T) {
	for _, loss := range []string{"truncated", "removed"} {
		for _, reader := range []string{"sender", "drainer"} {
			t.Run(loss+"/"+reader, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "slot")
				require.NoError(t, os.Mkdir(dir, 0o755))
				segments := qwpTestWriteDeltaSlot(t, dir,
					[][]byte{buildTestDeltaFrame(0, []string{"AAPL"})},
					[][]byte{buildTestDeltaFrame(1, []string{"MSFT"})},
				)
				dictPath := filepath.Join(dir, qwpSfSymbolDictFileName)
				qwpTestWriteSymbolDict(t, dir, []string{"AAPL"}, []string{"MSFT"})
				if loss == "truncated" {
					require.NoError(t, os.Truncate(dictPath, 0))
				} else {
					require.NoError(t, os.Remove(dictPath))
				}

				second := qwpTestOpenEngine(t, dir)
				qwpTestRequireNotQuarantined(t, second)
				qwpTestRequireEngineDict(t, second)
				require.Equal(t, []string{"AAPL", "MSFT"}, second.engineRecoveredSymbols())
				qwpTestAckAndAwaitTrim(t, second, 0, segments[0])
				require.NoError(t, second.engineClose())

				if reader == "sender" {
					third := qwpTestOpenEngine(t, dir)
					defer func() { require.NoError(t, third.engineClose()) }()
					qwpTestRequireNotQuarantined(t, third)
					require.Equal(t, []string{"AAPL", "MSFT"}, third.engineRecoveredSymbols(),
						"the trimmed frame's id must keep its name")
					return
				}
				srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true})
				defer srv.Close()
				drainer := qwpSfNewOrphanDrainer(dir, 4096, qwpSfUnlimitedTotalBytes,
					qwpSfDialFor(srv), nil, 200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
				defer cancel()
				drainer.drainerRun(ctx)
				require.Equal(t, qwpSfDrainOutcomeSuccess, drainer.drainerOutcome(), drainer.drainerLastError())
				require.NoFileExists(t, filepath.Join(dir, qwpSfFailedSentinelName))
				require.Equal(t, []string{"AAPL", "MSFT"}, reconstructConnDict(srv.recordedFrames()[1]),
					"the drainer must register the trimmed frame's id under its name")
			})
		}
	}
}

// Test 2. A heal that cannot write fails construction with a retriable error
// and changes no segment or manifest. The partial chunk it leaves is untrusted
// tail, and a retry once the fault clears recovers the slot.
func TestQwpSfHealFailureFailsConstructionRetriably(t *testing.T) {
	dir := t.TempDir()
	segments := qwpTestWriteDeltaSlot(t, dir,
		[][]byte{buildTestDeltaFrame(0, []string{"AAPL"})},
		[][]byte{buildTestDeltaFrame(1, []string{"MSFT"})},
	)
	qwpTestWriteSymbolDict(t, dir, []string{"AAPL"})
	names := []string{qwpSfManifestFileName}
	for _, path := range segments {
		names = append(names, filepath.Base(path))
	}
	before := qwpTestReadFiles(t, dir, names...)

	restore := qwpTestFailSymbolDictWrites(t)
	_, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.ErrorIs(t, err, ErrSfDurability)
	require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.Equal(t, before, qwpTestReadFiles(t, dir, names...), "a failed heal must not change the frames")
	quarantined, globErr := filepath.Glob(dir + ".unreplayable-*")
	require.NoError(t, globErr)
	require.Empty(t, quarantined, "a storage fault is not a reason to preserve the slot aside")

	restore()
	engine := qwpTestOpenEngine(t, dir)
	defer func() { require.NoError(t, engine.engineClose()) }()
	qwpTestRequireNotQuarantined(t, engine)
	require.Equal(t, []string{"AAPL", "MSFT"}, engine.engineRecoveredSymbols())
	symbols, _ := qwpTestReadSymbolDict(t, dir)
	require.Equal(t, []string{"AAPL", "MSFT"}, symbols)
}

// Test 3. A dictionary in the flat format of an earlier prototype is unusable
// and is rewritten in chunked form from the frames. The slot then survives two
// more restarts with partial trims in between.
func TestQwpSfLegacyFlatDictionaryIsRewrittenAndSurvivesTrims(t *testing.T) {
	dir := t.TempDir()
	segments := qwpTestWriteDeltaSlot(t, dir,
		[][]byte{buildTestDeltaFrame(0, []string{"AAPL"})},
		[][]byte{buildTestDeltaFrame(1, []string{"MSFT"})},
		[][]byte{buildTestDeltaFrame(2, []string{"IBM"})},
	)
	flat := qwpSfTestSymbolDictHeader()
	for _, name := range []string{"AAPL", "MSFT", "IBM"} {
		flat = append(flat, byte(len(name)))
		flat = append(flat, name...)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfSymbolDictFileName), flat, 0o644))
	all := []string{"AAPL", "MSFT", "IBM"}

	second := qwpTestOpenEngine(t, dir)
	qwpTestRequireNotQuarantined(t, second)
	qwpTestRequireEngineDict(t, second)
	symbols, chunks := qwpTestReadSymbolDict(t, dir)
	require.Equal(t, all, symbols)
	require.Positive(t, chunks)
	qwpTestAckAndAwaitTrim(t, second, 0, segments[0])
	require.NoError(t, second.engineClose())

	third := qwpTestOpenEngine(t, dir)
	qwpTestRequireNotQuarantined(t, third)
	require.Equal(t, all, third.engineRecoveredSymbols())
	qwpTestAckAndAwaitTrim(t, third, 1, segments[1])
	require.NoError(t, third.engineClose())

	fourth := qwpTestOpenEngine(t, dir)
	defer func() { require.NoError(t, fourth.engineClose()) }()
	qwpTestRequireNotQuarantined(t, fourth)
	require.Equal(t, all, fourth.engineRecoveredSymbols())
}

// Test 4. An earlier session persisted an id for a frame it never published.
// No frame carries that id, and the producer's first frame starts its delta
// above it, so construction must make the inherited dictionary durable before
// the slot is registered.
func TestQwpSfInheritedDictionaryIsSyncedBeforeRegistration(t *testing.T) {
	dir := t.TempDir()
	qwpTestWriteDeltaSlot(t, dir, [][]byte{buildTestDeltaFrame(0, []string{"AAPL"})})
	qwpTestWriteSymbolDict(t, dir, []string{"AAPL"}, []string{"MSFT"})
	log := qwpTestRecordBarriers(t, dir)

	engine := qwpTestOpenEngine(t, dir)
	defer func() { require.NoError(t, engine.engineClose()) }()
	qwpTestRequireNotQuarantined(t, engine)
	require.Equal(t, []string{"AAPL", "MSFT"}, engine.engineRecoveredSymbols())
	events := log.snapshot()
	qwpTestRequireBefore(t, events, "dict-sync", "register")
	// The name of a file another writer created may never have been made
	// durable, so the inherited dictionary gets a directory barrier too.
	qwpTestRequireDirSyncAfterDictSync(t, events)
	qwpTestRequireBefore(t, events[slices.Index(events, "dict-sync"):], "dir-sync", "register")
	producer := &qwpLineSender{globalSymbols: make(map[string]int32), maxSentSymbolId: -1, batchMaxSymbolId: -1}
	producer.wireDeltaDict(engine)
	require.Equal(t, 1, producer.maxSentSymbolId, "the next frame's delta starts above the inherited id")
	require.Equal(t, qwpSfSymbolDictBound(engine.engineRecoveredSymbols())-qwpSfSymbolDictHeaderSize,
		producer.symbolDictBound, "the producer's byte total must count the recovered symbols")
}

// Test 5. A fresh slot truncates a dictionary a previous lifecycle left
// behind, and fsyncs the truncation before the directory barrier that makes
// the fresh slot durable.
func TestQwpSfFreshSlotSyncsTheTruncatedDictionary(t *testing.T) {
	dir := t.TempDir()
	qwpTestWriteSymbolDict(t, dir, []string{"stale"})
	log := qwpTestRecordBarriers(t, dir)

	engine := qwpTestOpenEngine(t, dir)
	defer func() { require.NoError(t, engine.engineClose()) }()
	qwpTestRequireEngineDict(t, engine)
	events := log.snapshot()
	qwpTestRequireDirSyncAfterDictSync(t, events)
	qwpTestRequireBefore(t, events, "dict-sync", "register")
	symbols, _ := qwpTestReadSymbolDict(t, dir)
	require.Empty(t, symbols)
}

// Test 6. A recovered slot whose frames introduce no symbols still gets a
// dictionary, created and made durable, so the sender delta-encodes.
func TestQwpSfRecoveredFramesWithoutSymbolsGetADictionary(t *testing.T) {
	dir := t.TempDir()
	qwpTestWriteDeltaSlot(t, dir, [][]byte{[]byte("row-a"), []byte("row-b")})
	log := qwpTestRecordBarriers(t, dir)

	engine := qwpTestOpenEngine(t, dir)
	defer func() { require.NoError(t, engine.engineClose()) }()
	qwpTestRequireNotQuarantined(t, engine)
	qwpTestRequireEngineDict(t, engine)
	require.Zero(t, engine.enginePersistedSymbolDict().size())
	require.Empty(t, engine.engineRecoveredSymbols())
	events := log.snapshot()
	qwpTestRequireDirSyncAfterDictSync(t, events)
	qwpTestRequireBefore(t, events, "dict-sync", "register")
	symbols, _ := qwpTestReadSymbolDict(t, dir)
	require.Empty(t, symbols)
}

// Test 8. A store-and-forward sender refuses a new symbol value that would take
// its dictionary past the byte limit, measured by qwpSfSymbolDictBound. Rows
// using registered values stay valid. A memory-backed sender has no file and
// no byte limit.
func TestQwpSfSenderRefusesSymbolPastTheDictionaryByteLimit(t *testing.T) {
	limit := qwpSfSymbolDictBound([]string{"AAA", "BBB"})
	qwpTestSwapSymbolDictLimits(t, qwpSfSymbolDictLimitSet{
		maxEntries:   qwpMaxSymbolDictionarySize,
		maxEntryLen:  qwpSfSymbolDictMaxEntryLen,
		maxFileBytes: limit,
	})
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	ctx := context.Background()

	s, _ := qwpTestStartSfSender(t, srv, filepath.Join(t.TempDir(), "slot"), 1<<16)
	require.NoError(t, s.Table("t").Symbol("sym", "AAA").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Table("t").Symbol("sym", "BBB").Int64Column("v", 2).AtNow(ctx))
	err := s.Table("t").Symbol("sym", "CCC").Int64Column("v", 3).AtNow(ctx)
	require.ErrorContains(t, err, "caps its symbol dictionary file")
	require.NotContains(t, s.globalSymbols, "CCC")
	require.NoError(t, s.Table("t").Symbol("sym", "AAA").Int64Column("v", 4).AtNow(ctx),
		"rows using registered values stay valid")
	require.NoError(t, s.Flush(ctx))
	require.Equal(t, limit-qwpSfSymbolDictHeaderSize, s.symbolDictBound)

	memory, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()
	for _, value := range []string{"AAA", "BBB", "CCC"} {
		require.NoError(t, memory.Table("t").Symbol("sym", value).Int64Column("v", 1).AtNow(ctx),
			"a memory-backed sender has no byte limit")
	}
}

// The writer's own limit checks stay as a second line of defence. Reaching one
// means the producer's checks let through a value they should have refused, so
// the error says so and is neither retriable nor a verdict about the slot.
func TestQwpSfDictionaryWriterLimitIsReportedAsInconsistency(t *testing.T) {
	d, err := qwpSfSymbolDictOpen(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, d.close()) }()
	s := &qwpLineSender{
		persistedSymbolDict: d,
		globalSymbolList:    []string{"AAPL", "GOOG"},
		batchMaxSymbolId:    1,
		maxSentSymbolId:     -1,
	}
	defaults := qwpSfSymbolDictLimits.load()
	qwpTestSwapSymbolDictLimits(t, qwpSfSymbolDictLimitSet{
		maxEntries: 1, maxEntryLen: defaults.maxEntryLen, maxFileBytes: defaults.maxFileBytes,
	})

	err = s.persistNewSymbols()
	require.ErrorContains(t, err, "internal inconsistency")
	require.NotErrorIs(t, err, ErrSfDurability)
	require.NotErrorIs(t, err, qwpSfErrRecoveryFailClosed)
	require.Zero(t, d.size())
}

// Test 9. A recovered dictionary beyond the writer's limits is a fail-closed
// verdict, decided while the slot is exactly as it was found: the dictionary's
// untrusted tail, which holds the chunks past the id limit, is still there.
func TestQwpSfOverLimitRecoveredSlotIsRefusedUntouched(t *testing.T) {
	defaults := qwpSfSymbolDictLimits.load()
	cases := []struct {
		name    string
		limits  qwpSfSymbolDictLimitSet
		dict    [][]string
		frame   []byte
		message string
	}{
		{
			name:    "count",
			limits:  qwpSfSymbolDictLimitSet{maxEntries: 2, maxEntryLen: defaults.maxEntryLen, maxFileBytes: defaults.maxFileBytes},
			dict:    [][]string{{"a"}, {"b"}, {"c"}},
			frame:   buildTestDeltaFrame(0, []string{"a", "b", "c"}),
			message: "holds 3 ids, more than the limit 2",
		},
		{
			name:    "bytes",
			limits:  qwpSfSymbolDictLimitSet{maxEntries: defaults.maxEntries, maxEntryLen: defaults.maxEntryLen, maxFileBytes: qwpSfSymbolDictBound([]string{"a", "b", "c"}) - 1},
			dict:    [][]string{{"a"}, {"b"}, {"c"}},
			frame:   buildTestDeltaFrame(0, []string{"a", "b", "c"}),
			message: "needs up to",
		},
		{
			name:    "name",
			limits:  qwpSfSymbolDictLimitSet{maxEntries: defaults.maxEntries, maxEntryLen: 3, maxFileBytes: defaults.maxFileBytes},
			dict:    [][]string{{"a"}},
			frame:   buildTestDeltaFrame(0, []string{"a", "toolong"}),
			message: "is 7 bytes long, more than the limit 3",
		},
		{
			name:    "reference_only",
			limits:  qwpSfSymbolDictLimitSet{maxEntries: 2, maxEntryLen: defaults.maxEntryLen, maxFileBytes: defaults.maxFileBytes},
			dict:    [][]string{{"a"}, {"b"}, {"c"}},
			frame:   buildTestDeltaFrame(3, []string{"d"}),
			message: "recovered symbol dictionary is incomplete",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qwpTestSwapSymbolDictLimits(t, tc.limits)
			dir := filepath.Join(t.TempDir(), "slot")
			require.NoError(t, os.Mkdir(dir, 0o755))
			segments := qwpTestWriteDeltaSlot(t, dir, [][]byte{tc.frame})
			qwpTestWriteSymbolDict(t, dir, tc.dict...)
			names := qwpTestSlotFileNames(segments)
			before := qwpTestReadFiles(t, dir, names...)

			// The drainer's engine reports the verdict without moving the slot.
			_, err := qwpSfNewCursorEngineForDrainer(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.ErrorIs(t, err, qwpSfErrRecoveryFailClosed)
			require.NotErrorIs(t, err, ErrSfDurability)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, before, qwpTestReadFiles(t, dir, names...))

			// A drainer marks it failed.
			drainer := qwpSfNewOrphanDrainer(dir, 4096, qwpSfUnlimitedTotalBytes,
				nil, nil, 200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
			drainer.drainerRun(context.Background())
			require.Equal(t, qwpSfDrainOutcomeFailed, drainer.drainerOutcome())
			require.FileExists(t, filepath.Join(dir, qwpSfFailedSentinelName))
			require.NoError(t, os.Remove(filepath.Join(dir, qwpSfFailedSentinelName)))

			// A foreground sender preserves a byte-identical copy aside.
			engine := qwpTestOpenEngine(t, dir)
			defer func() { require.NoError(t, engine.engineClose()) }()
			preserved := engine.engineQuarantinedSlotPath()
			require.NotEmpty(t, preserved)
			require.Equal(t, before, qwpTestReadFiles(t, preserved, names...))
		})
	}

	t.Run("unused", func(t *testing.T) {
		qwpTestSwapSymbolDictLimits(t, qwpSfSymbolDictLimitSet{maxEntries: 2, maxEntryLen: defaults.maxEntryLen, maxFileBytes: defaults.maxFileBytes})
		dir := t.TempDir()
		qwpTestWriteDeltaSlot(t, dir, [][]byte{buildTestDeltaFrame(0, []string{"a"})})
		qwpTestWriteSymbolDict(t, dir, []string{"a"}, []string{"b"}, []string{"c"})

		engine := qwpTestOpenEngine(t, dir)
		defer func() { require.NoError(t, engine.engineClose()) }()
		qwpTestRequireNotQuarantined(t, engine)
		require.Equal(t, []string{"a", "b"}, engine.engineRecoveredSymbols())
		symbols, _ := qwpTestReadSymbolDict(t, dir)
		require.Equal(t, []string{"a", "b"}, symbols, "the entries past the limit are cut with the untrusted tail")
	})
}

// Test 10. Every disk-backed engine hands its dictionary to the segment
// manager, which fsyncs it before a trim. A drainer appends nothing after
// construction, so its trims find nothing left to sync.
func TestQwpSfEngineRegistersItsDictionaryWithTheManager(t *testing.T) {
	requireWired := func(t *testing.T, engine *qwpSfCursorEngine) {
		t.Helper()
		qwpTestRequireEngineDict(t, engine)
		require.Same(t, engine.enginePersistedSymbolDict(), engine.managerEntry.symbolDict)
	}
	t.Run("fresh", func(t *testing.T) {
		engine := qwpTestOpenEngine(t, t.TempDir())
		defer func() { require.NoError(t, engine.engineClose()) }()
		requireWired(t, engine)
	})
	t.Run("recovered", func(t *testing.T) {
		dir := t.TempDir()
		qwpTestWriteDeltaSlot(t, dir, [][]byte{buildTestDeltaFrame(0, []string{"AAPL"})})
		engine := qwpTestOpenEngine(t, dir)
		defer func() { require.NoError(t, engine.engineClose()) }()
		requireWired(t, engine)
	})
	t.Run("drainer", func(t *testing.T) {
		dir := t.TempDir()
		qwpTestWriteDeltaSlot(t, dir, [][]byte{buildTestDeltaFrame(0, []string{"AAPL"})})
		engine, err := qwpSfNewCursorEngineForDrainer(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		defer func() { require.NoError(t, engine.engineClose()) }()
		requireWired(t, engine)
	})
}

// Test 11. A disk-backed engine whose dictionary cannot be created fails
// construction with a retriable error rather than run without one.
func TestQwpSfDictionaryCreateFailureFailsConstruction(t *testing.T) {
	setups := map[string]func(t *testing.T, dir string){
		"fresh": func(*testing.T, string) {},
		"empty_ring": func(t *testing.T, dir string) {
			seg := createRecoverySegment(t, dir, "sf-active.sfa", 5)
			createRecoveryManifest(t, dir, 5, 5, seg)
			closeRecoverySegments(t, seg)
		},
		"frames_without_dictionary": func(t *testing.T, dir string) {
			qwpTestWriteDeltaSlot(t, dir, [][]byte{buildTestDeltaFrame(0, []string{"AAPL"})})
		},
	}
	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			setup(t, dir)
			original := qwpSfSymbolDictCreate.load()
			qwpSfSymbolDictCreate.store(func(path string, flag int, perm os.FileMode) (*os.File, error) {
				if filepath.Base(path) == qwpSfSymbolDictFileName {
					return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOSPC}
				}
				return original(path, flag, perm)
			})
			t.Cleanup(func() { qwpSfSymbolDictCreate.store(original) })

			_, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.ErrorIs(t, err, ErrSfDurability)
			require.ErrorIs(t, err, syscall.ENOSPC)
		})
	}
}

// Test 12. Construction writes a rebuilt dictionary in bounded chunks, and the
// next recovery reads every entry back in order.
func TestQwpSfRebuiltDictionaryIsWrittenInBatches(t *testing.T) {
	for _, tc := range []struct {
		name    string
		batch   struct{ entries, bytes int }
		symbols []string
		chunks  int
	}{
		{name: "entries", batch: struct{ entries, bytes int }{entries: 2, bytes: 1 << 20}, symbols: []string{"a", "b", "c", "d", "e"}, chunks: 3},
		{name: "bytes", batch: struct{ entries, bytes int }{entries: 1 << 16, bytes: 3}, symbols: []string{"aa", "bb", "cc"}, chunks: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := qwpSfSymbolDictRewriteBatch.load()
			qwpSfSymbolDictRewriteBatch.store(tc.batch)
			t.Cleanup(func() { qwpSfSymbolDictRewriteBatch.store(original) })
			dir := t.TempDir()
			qwpTestWriteDeltaSlot(t, dir, [][]byte{buildTestDeltaFrame(0, tc.symbols)})

			engine := qwpTestOpenEngine(t, dir)
			require.NoError(t, engine.engineClose())
			symbols, chunks := qwpTestReadSymbolDict(t, dir)
			require.Equal(t, tc.symbols, symbols)
			require.Equal(t, tc.chunks, chunks)

			reopened := qwpTestOpenEngine(t, dir)
			defer func() { require.NoError(t, reopened.engineClose()) }()
			qwpTestRequireNotQuarantined(t, reopened)
			require.Equal(t, tc.symbols, reopened.engineRecoveredSymbols())
		})
	}
}

// Test 13. A recovered slot with no frames starts a fresh dictionary even when
// a usable one is on disk: no frame needs the old ids. Neither the producer nor
// the send loop keeps them, so new ids start at 0, matching the truncated file.
func TestQwpSfEmptyRecoveredRingStartsAFreshDictionary(t *testing.T) {
	dir := t.TempDir()
	seg := createRecoverySegment(t, dir, "sf-active.sfa", 5)
	createRecoveryManifest(t, dir, 5, 5, seg)
	closeRecoverySegments(t, seg)
	qwpTestWriteSymbolDict(t, dir, []string{"old0"}, []string{"old1"})
	log := qwpTestRecordBarriers(t, dir)

	engine := qwpTestOpenEngine(t, dir)
	qwpTestRequireEngineDict(t, engine)
	require.Zero(t, engine.enginePersistedSymbolDict().size())
	require.Empty(t, engine.engineRecoveredSymbols())
	qwpTestRequireBefore(t, log.snapshot(), "dict-sync", "register")
	symbols, _ := qwpTestReadSymbolDict(t, dir)
	require.Empty(t, symbols)

	producer := &qwpLineSender{globalSymbols: make(map[string]int32), maxSentSymbolId: -1, batchMaxSymbolId: -1}
	producer.wireDeltaDict(engine)
	require.Equal(t, -1, producer.maxSentSymbolId)
	require.Empty(t, producer.globalSymbolList)
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	loop := qwpSfNewSendLoop(engine, nil, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	require.Zero(t, loop.sentDictCount, "the send loop must not keep the old symbols")
	require.NoError(t, loop.sendLoopClose())

	require.NoError(t, engine.enginePersistedSymbolDict().appendSymbols([]string{"new0"}))
	_, err := engine.engineAppendBlocking(context.Background(), buildTestDeltaFrame(0, []string{"new0"}))
	require.NoError(t, err)
	require.NoError(t, engine.engineClose())

	reopened := qwpTestOpenEngine(t, dir)
	defer func() { require.NoError(t, reopened.engineClose()) }()
	qwpTestRequireNotQuarantined(t, reopened)
	require.Equal(t, []string{"new0"}, reopened.engineRecoveredSymbols())
}

// A slot left by the Java client's full-dictionary fallback holds frames that
// each carry the dictionary from id 0, possibly with no usable side-file. A Go
// sender adopting it rebuilds the side-file from those frames and carries on
// with delta frames.
func TestQwpSfSelfSufficientSlotIsAdoptedInDeltaMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "slot")
	require.NoError(t, os.Mkdir(dir, 0o755))
	qwpTestWriteDeltaSlot(t, dir, [][]byte{
		buildTestDeltaFrame(0, []string{"a"}),
		buildTestDeltaFrame(0, []string{"a", "b"}),
	})
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{recordFrames: true})
	defer srv.Close()

	s, engine := qwpTestStartSfSender(t, srv, dir, 4096)
	qwpTestRequireNotQuarantined(t, engine)
	qwpTestRequireEngineDict(t, engine)
	symbols, _ := qwpTestReadSymbolDict(t, dir)
	require.Equal(t, []string{"a", "b"}, symbols)
	require.Equal(t, 1, s.maxSentSymbolId)

	ctx := context.Background()
	require.NoError(t, s.Table("t").Symbol("sym", "c").Int64Column("v", 1).AtNow(ctx))
	require.NoError(t, s.Flush(ctx))
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= engine.enginePublishedFsn()
	}, qwpTestWaitTimeout, time.Millisecond)
	frames := srv.recordedFrames()[1]
	starts := qwpTestDeltaStarts(frames)
	require.Equal(t, 2, starts[len(starts)-1], "the new frame must be a delta above the adopted ids")
	require.Equal(t, []string{"a", "b", "c"}, reconstructConnDict(frames))
}
