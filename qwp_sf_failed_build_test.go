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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// qwpSfTestFullyAckedSlot writes frames 0..2 into slot and records all three
// as acknowledged in .ack-watermark. With conflict, frame 0 names symbol id 0
// ZZZZ while the side-file says AAPL, so a later recovery refuses the slot.
func qwpSfTestFullyAckedSlot(t *testing.T, slot string, conflict bool) {
	t.Helper()
	ctx := context.Background()
	e, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	require.NoError(t, e.enginePersistedSymbolDict().appendSymbols([]string{"AAPL"}))
	frameSymbol := "AAPL"
	if conflict {
		frameSymbol = "ZZZZ"
	}
	_, err = e.engineAppendBlocking(ctx, buildTestDeltaFrame(0, []string{frameSymbol}))
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = e.engineAppendBlocking(ctx, make([]byte, 16))
		require.NoError(t, err)
	}
	// Leave FSN 2 unacknowledged in this engine so its close keeps the files,
	// then record every frame as acknowledged, as a later session would have.
	e.engineAcknowledge(1)
	require.NoError(t, e.engineClose())
	waitQwpSfEngineCleanup(t, e)
	w, err := qwpSfAckWatermarkOpenPrepared(slot, qwpSfAckWatermarkStartup{publishedFsn: 2}, nil)
	require.NoError(t, err)
	_, err = w.persistIfAdvanced(2)
	require.NoError(t, err)
	require.NoError(t, w.sync())
	require.NoError(t, w.close())
}

// qwpSfTestFirstSpare is the empty spare the first session's manager minted.
// It holds no frames, so recovery deletes it as a stray before anything else
// happens; the tests below do not expect it to survive.
const qwpSfTestFirstSpare = "sf-0000000000000000.sfa"

// qwpSfTestSlotFiles snapshots every regular file in slot except the
// directory-local lock pair, which each open rewrites, and the empty spare.
func qwpSfTestSlotFiles(t *testing.T, slot string) map[string][]byte {
	t.Helper()
	contents := map[string][]byte{}
	entries, err := os.ReadDir(slot)
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || strings.HasPrefix(name, ".lock") || name == qwpSfTestFirstSpare {
			continue
		}
		b, readErr := os.ReadFile(filepath.Join(slot, name))
		require.NoError(t, readErr)
		contents[name] = b
	}
	return contents
}

// A refused slot is preserved byte for byte even when every frame in it was
// acknowledged, including the symbol dictionary's unverified tail: the file
// whose disagreement with the frames is the reason for the refusal.
func TestQwpSfQuarantineOfFullyAckedSlotKeepsEveryByte(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sender-a")
	qwpSfTestFullyAckedSlot(t, slot, true)
	// A torn chunk after the trusted ones, as a crash mid-append leaves.
	dict, err := os.OpenFile(filepath.Join(slot, qwpSfSymbolDictFileName), os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = dict.Write([]byte{0x01, 0x05, 'M', 'S'})
	require.NoError(t, err)
	require.NoError(t, dict.Close())
	contents := qwpSfTestSlotFiles(t, slot)
	require.Contains(t, contents, "sf-initial.sfa")

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, engine.engineClose())
		waitQwpSfEngineCleanup(t, engine)
	}()
	preserved := filepath.Join(root, "sender-a"+qwpSfQuarantineSlotInfix+"0")
	require.Equal(t, preserved, engine.engineQuarantinedSlotPath())
	qwpSfRequireSameFiles(t, preserved, contents)
}

// A build that fails for a local reason on a fully acknowledged slot leaves
// the slot as it was, and the retry continues its frame sequence rather than
// starting a fresh one at FSN 0.
func TestQwpSfFailedBuildOnFullyAckedSlotKeepsIt(t *testing.T) {
	slot := filepath.Join(t.TempDir(), "s")
	qwpSfTestFullyAckedSlot(t, slot, false)
	contents := qwpSfTestSlotFiles(t, slot)

	injected := errors.New("injected registration fault")
	hook := func() error {
		return qwpSfDurabilityError("scan quarantined bytes during manager registration", slot, injected)
	}
	qwpSfTestBeforeEngineRegisterHook.Store(&hook)
	t.Cleanup(func() { qwpSfTestBeforeEngineRegisterHook.Store(nil) })
	_, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	qwpSfTestBeforeEngineRegisterHook.Store(nil)
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, ErrSfDurability)
	if held := qwpSfPendingCleanupEngine(err); held != nil {
		waitQwpSfEngineCleanup(t, held)
	}
	require.Contains(t, contents, "sf-initial.sfa")
	qwpSfRequireSameFiles(t, slot, contents)

	engine, err := qwpSfNewCursorEngine(slot, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	require.True(t, engine.engineWasRecoveredFromDisk())
	require.Equal(t, int64(2), engine.enginePublishedFsn(), "the retry continues the slot's sequence")
	require.NoError(t, engine.engineClose())
	waitQwpSfEngineCleanup(t, engine)
}
