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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the startup checkpoint that keeps an old ACK record from
// acknowledging new frames that reuse its numbers. They exercise the startup
// decision (preserve versus reset), the unconditional file and directory
// barriers, and the retry behaviour after a partly completed reset. Process
// restart is exercised by reconstruction and, for the fresh-slot case, by a
// subprocess that exits without cleanup. Neither is evidence about host crash
// or power loss: the barriers only carry the platform guarantees documented for
// qwpSfFsync and qwpSfSyncSlotDir, and the Windows directory barrier is a
// no-op.

const qwpSfStartupSegmentSize int64 = 4096

// qwpSfStartupProbe records the startup file operations in order and can fail
// any one of them exactly once, so a test can pin both the required ordering
// and the retry that follows a failure.
//
// The hooks are package-global and the watermark's sync hook is also used by
// the live manager and by cleanup, so these tests must not run in parallel and
// must arm a fault immediately before the construction it belongs to.
type qwpSfStartupProbe struct {
	mu   sync.Mutex
	slot string
	// steps records the startup operations this probe observed, in order.
	steps []string
	// armedDirFault gates directory-barrier observation on a preceding
	// watermark file sync: unrelated slot barriers (recovery, fresh-slot
	// creation, cleanup unlinks) go through the same helper and must not be
	// mistaken for the watermark's barrier.
	armedDirFault bool

	truncateFault error
	fileSyncFault error
	dirSyncFault  error
	writeFault    error
}

func installQwpSfStartupProbe(t *testing.T, slot string) *qwpSfStartupProbe {
	t.Helper()
	p := &qwpSfStartupProbe{slot: slot}

	originalTruncate := qwpSfAckWatermarkTruncate.load()
	qwpSfAckWatermarkTruncate.store(func(f *os.File, size int64) error {
		p.record("truncate")
		if err := p.take(&p.truncateFault); err != nil {
			return err
		}
		return originalTruncate(f, size)
	})
	t.Cleanup(func() { qwpSfAckWatermarkTruncate.store(originalTruncate) })

	fileSync := func(f *os.File) error {
		p.record("file-barrier")
		p.armDirFault()
		if err := p.take(&p.fileSyncFault); err != nil {
			return err
		}
		return qwpSfFsync(f)
	}
	qwpSfAckWatermarkSync.Store(&fileSync)
	t.Cleanup(func() { qwpSfAckWatermarkSync.Store(nil) })

	dirSync := func(dir string) error {
		if dir != p.slot || !p.disarmDirFault() {
			return nil
		}
		p.record("dir-barrier")
		return p.take(&p.dirSyncFault)
	}
	qwpSfTestDirSyncHook.Store(&dirSync)
	t.Cleanup(func() { qwpSfTestDirSyncHook.Store(nil) })

	originalWrite := qwpSfAckWatermarkWriteAt.load()
	qwpSfAckWatermarkWriteAt.store(func(f *os.File, b []byte, off int64) (int, error) {
		p.record("reserve-blocks")
		if err := p.take(&p.writeFault); err != nil {
			return 0, err
		}
		return originalWrite(f, b, off)
	})
	t.Cleanup(func() { qwpSfAckWatermarkWriteAt.store(originalWrite) })
	return p
}

func (p *qwpSfStartupProbe) record(step string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.steps = append(p.steps, step)
}

func (p *qwpSfStartupProbe) take(slot *error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := *slot
	*slot = nil
	return err
}

func (p *qwpSfStartupProbe) armDirFault() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armedDirFault = true
}

func (p *qwpSfStartupProbe) disarmDirFault() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	armed := p.armedDirFault
	p.armedDirFault = false
	return armed
}

// reset clears the recorded steps so an assertion covers one construction.
func (p *qwpSfStartupProbe) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.steps = nil
	p.armedDirFault = false
}

func (p *qwpSfStartupProbe) failTruncate(err error) { p.setFault(&p.truncateFault, err) }
func (p *qwpSfStartupProbe) failFileSync(err error) { p.setFault(&p.fileSyncFault, err) }
func (p *qwpSfStartupProbe) failDirSync(err error)  { p.setFault(&p.dirSyncFault, err) }
func (p *qwpSfStartupProbe) failWriteBack(err error) {
	p.setFault(&p.writeFault, err)
}

func (p *qwpSfStartupProbe) setFault(slot *error, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*slot = err
}

func (p *qwpSfStartupProbe) recorded() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.steps...)
}

// requireCheckpointPrecedesAllocation pins the ordering the plan requires: the
// file barrier and the slot-directory barrier both complete before any block
// reservation, on every construction, whether or not a reset was needed.
// Looking safe in the page cache is not proof that an earlier attempt's barrier
// completed, so neither barrier may be skipped.
func (p *qwpSfStartupProbe) requireCheckpointPrecedesAllocation(t *testing.T, wantTruncate bool) {
	t.Helper()
	steps := p.recorded()
	file, dir, write, trunc := -1, -1, -1, -1
	for i, step := range steps {
		switch step {
		case "truncate":
			if trunc < 0 {
				trunc = i
			}
		case "file-barrier":
			if file < 0 {
				file = i
			}
		case "dir-barrier":
			if dir < 0 {
				dir = i
			}
		case "reserve-blocks":
			if write < 0 {
				write = i
			}
		}
	}
	require.GreaterOrEqual(t, file, 0, "startup must sync the watermark file: %v", steps)
	require.GreaterOrEqual(t, dir, 0, "startup must run the slot-directory barrier: %v", steps)
	require.Less(t, file, dir, "the file barrier precedes the directory barrier: %v", steps)
	if write >= 0 {
		require.Less(t, dir, write, "both barriers precede allocation/write-back: %v", steps)
	}
	if wantTruncate {
		require.GreaterOrEqual(t, trunc, 0, "this case must reset the record: %v", steps)
		require.Less(t, trunc, file, "the reset precedes its barriers: %v", steps)
	} else {
		require.Equal(t, -1, trunc, "an acceptable record must not be truncated: %v", steps)
	}
}

// writeAckWatermarkImage stamps chosen records into both slots of the
// Java-compatible layout. A zero generation leaves that slot zero-filled, which
// the existing rules reject.
func writeAckWatermarkImage(t *testing.T, slotDir string, slot0Gen, slot0Fsn, slot1Gen, slot1Fsn int64) {
	t.Helper()
	image := make([]byte, qwpSfAckWatermarkFileSize)
	if slot0Gen != 0 {
		qwpSfEncodeDualRecord(image[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, slot0Gen, slot0Fsn, 0)
	}
	if slot1Gen != 0 {
		off := int(qwpSfDualRecordSlotSize)
		qwpSfEncodeDualRecord(image[off:off+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, slot1Gen, slot1Fsn, 0)
	}
	require.NoError(t, os.WriteFile(filepath.Join(slotDir, qwpSfAckWatermarkFileName), image, 0o644))
}

func ackWatermarkSlotRecords(t *testing.T, slotDir string) (qwpSfDualRecord, bool, qwpSfDualRecord, bool) {
	t.Helper()
	b := readAckWatermarkFileBytes(t, slotDir)
	require.Len(t, b, int(qwpSfAckWatermarkFileSize),
		"a prepared watermark must be restored to the dual-record size")
	r0, ok0 := qwpSfDecodeDualRecord(b[:qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
	off := int(qwpSfDualRecordSlotSize)
	r1, ok1 := qwpSfDecodeDualRecord(b[off:off+qwpSfDualRecordSize], qwpSfAckWatermarkMagic, qwpSfAckWatermarkRecordValid)
	return r0, ok0, r1, ok1
}

// requireBothAckRecordsRetired asserts the reset removed both record slots
// together. Clearing only the selected one would leave the alternate record for
// the next restart to select.
func requireBothAckRecordsRetired(t *testing.T, slotDir string) {
	t.Helper()
	_, ok0, _, ok1 := ackWatermarkSlotRecords(t, slotDir)
	require.False(t, ok0, "record slot 0 must be retired")
	require.False(t, ok1, "record slot 1 must be retired")
}

func requireSelectedAckRecord(t *testing.T, slotDir string, wantFsn int64) {
	t.Helper()
	b := readAckWatermarkFileBytes(t, slotDir)
	rec, ok := qwpSfSelectAckWatermarkRecord(b, int64(len(b)) == qwpSfAckWatermarkFileSize)
	require.True(t, ok, "a usable record must remain on disk")
	require.Equal(t, wantFsn, rec.first)
}

func openStartupEngine(t *testing.T, dir string) *qwpSfCursorEngine {
	t.Helper()
	e, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
	require.NoError(t, err)
	require.NotNil(t, e)
	return e
}

// reopenStartupEngineAfterFailure retries construction while the failed
// attempt's cleanup still owns the slot. A failed construction keeps ownership
// until its worker releases the lock, and a retry must wait for that rather
// than reopening the slot underneath the previous owner.
func reopenStartupEngineAfterFailure(t *testing.T, dir string) *qwpSfCursorEngine {
	t.Helper()
	deadline := time.Now().Add(qwpTestWaitTimeout)
	for {
		e, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		if err == nil {
			require.NotNil(t, e)
			return e
		}
		if !errors.Is(err, qwpSfErrLockBusy) || time.Now().After(deadline) {
			require.NoError(t, err, "retry after cleanup released the slot must succeed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func appendStartupFrames(t *testing.T, e *qwpSfCursorEngine, from, count int) []string {
	t.Helper()
	payloads := make([]string, 0, count)
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("frame-%02d", from+i)
		_, err := e.engineAppendBlocking(context.Background(), []byte(payload))
		require.NoError(t, err)
		payloads = append(payloads, payload)
	}
	return payloads
}

// collectReplayFrames returns the payloads a fresh send loop would replay:
// every frame above the recovered ACK position, located the same way the loop
// locates them. An unreachable FSN fails the test rather than being skipped.
func collectReplayFrames(t *testing.T, e *qwpSfCursorEngine) []string {
	t.Helper()
	var out []string
	for fsn := e.engineAckedFsn() + 1; fsn <= e.enginePublishedFsn(); fsn++ {
		seg := e.engineFindSegmentContaining(fsn)
		require.NotNil(t, seg, "FSN %d must remain locatable for replay", fsn)
		buf := seg.address()
		offset := qwpSfHeaderSize
		for at := seg.segmentBaseSeq(); at < fsn; at++ {
			offset += qwpSfFrameHeaderSize + int64(int32(binary.LittleEndian.Uint32(buf[offset+4:offset+8])))
		}
		payloadLen := int64(int32(binary.LittleEndian.Uint32(buf[offset+4 : offset+8])))
		out = append(out, string(buf[offset+qwpSfFrameHeaderSize:offset+qwpSfFrameHeaderSize+payloadLen]))
	}
	return out
}

func requireNoQuarantine(t *testing.T, slotDir string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(filepath.Dir(slotDir), "quarantined"))
	require.True(t, os.IsNotExist(err),
		"an environmental storage fault must not quarantine the slot")
}

// seedStartupSlot writes count frames into a fresh slot and closes it without
// ACKs, so the files and their FSN range survive for the next construction.
func seedStartupSlot(t *testing.T, dir string, count int) []string {
	t.Helper()
	e := openStartupEngine(t, dir)
	payloads := appendStartupFrames(t, e, 0, count)
	require.Equal(t, int64(count-1), e.enginePublishedFsn())
	require.NoError(t, e.engineClose())
	return payloads
}

// TestQwpSfFreshSlotRetiresPreviousLifecycleAckRecord covers plan case A: a
// slot with no segments whose previous, fully drained lifecycle left a valid
// ACK record behind. New frame numbering restarts at 0, so trusting that record
// would acknowledge frames it never covered. Startup used to ignore the error
// from unlinking the file; correctness must now come from a durable reset, with
// the file left in place.
func TestQwpSfFreshSlotRetiresPreviousLifecycleAckRecord(t *testing.T) {
	for _, foreground := range []bool{false, true} {
		name := "drainer"
		if foreground {
			name = "foreground"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, qwpSfAckWatermarkFileName)
			writeForeignAckWatermark(t, dir, 9)
			identity, err := os.Stat(path)
			require.NoError(t, err)

			probe := installQwpSfStartupProbe(t, dir)
			var e *qwpSfCursorEngine
			if foreground {
				e, err = qwpSfNewCursorEngine(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
				require.NoError(t, err)
			} else {
				e = openStartupEngine(t, dir)
			}
			probe.requireCheckpointPrecedesAllocation(t, true)
			assert.Equal(t, int64(-1), e.engineAckedFsn(),
				"a fresh slot starts with no acknowledged frames")
			assert.Equal(t, qwpSfAckWatermarkInvalid, e.watermark.read())

			current, statErr := os.Stat(path)
			require.NoError(t, statErr)
			assert.True(t, os.SameFile(identity, current),
				"the reset truncates in place; startup must not depend on deleting or recreating the file")
			requireBothAckRecordsRetired(t, dir)

			// Publish through and beyond the old value with no ACKs at all.
			payloads := appendStartupFrames(t, e, 0, 12)
			require.Equal(t, int64(11), e.enginePublishedFsn())
			require.NoError(t, e.engineClose())

			restarted := openStartupEngine(t, dir)
			defer func() { require.NoError(t, restarted.engineClose()) }()
			assert.Equal(t, int64(11), restarted.enginePublishedFsn())
			assert.Equal(t, int64(-1), restarted.engineAckedFsn(),
				"the retired record must not acknowledge the reused frame numbers")
			assert.Equal(t, payloads, collectReplayFrames(t, restarted),
				"every unacknowledged frame remains replayable")
		})
	}
}

// TestQwpSfFreshSlotRetiresAckRecordAcrossProcessExit runs the same fresh-slot
// case in a child that exits without cleanup, so no orderly close can mask an
// initialization mistake by syncing files on the way out. This is evidence for
// the exercised process-restart sequence only.
func TestQwpSfFreshSlotRetiresAckRecordAcrossProcessExit(t *testing.T) {
	if dir := os.Getenv("QWP_WATERMARK_FRESH_CHILD"); dir != "" {
		writeForeignAckWatermark(t, dir, 9)
		e, err := qwpSfNewCursorEngine(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		require.NoError(t, err)
		require.Equal(t, int64(-1), e.engineAckedFsn())
		appendStartupFrames(t, e, 0, 12)
		require.Equal(t, int64(11), e.enginePublishedFsn())
		// Leave without closing: no flush, no barriers, no unlink.
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := qwpTestSubprocess(t, "TestQwpSfFreshSlotRetiresAckRecordAcrossProcessExit")
	cmd.Env = append(os.Environ(), "QWP_WATERMARK_FRESH_CHILD="+dir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)

	recovered := openStartupEngine(t, dir)
	defer func() { require.NoError(t, recovered.engineClose()) }()
	assert.Equal(t, int64(11), recovered.enginePublishedFsn())
	assert.Equal(t, int64(-1), recovered.engineAckedFsn(),
		"an abrupt exit must not leave the old record able to acknowledge the new frames")
	assert.Len(t, collectReplayFrames(t, recovered), 12)
}

// TestQwpSfDamagedActiveTailKeepsOldAckOffReplacementFrames covers plan case C.
// Recovery discards the unreadable active tail, which can leave a genuine ACK
// record above the surviving frames. The vacated numbers are then republished,
// so that record has to be retired rather than ignored for one run. The
// deliberate valid-prefix/zeroed-suffix policy is unchanged: the discarded
// suffix is not preserved, and discarding ACK evidence can replay surviving
// frames that were already acknowledged.
func TestQwpSfDamagedActiveTailKeepsOldAckOffReplacementFrames(t *testing.T) {
	for _, prefixFrames := range []int{0, 1} {
		t.Run(fmt.Sprintf("prefix-%d", prefixFrames), func(t *testing.T) {
			dir := t.TempDir()
			const activeBase int64 = 2
			sealed := createRecoverySegment(t, dir, "sf-initial.sfa", 0, "sealed-00", "sealed-01")
			payloads := []string{"damaged", "intact-but-unreachable"}
			if prefixFrames > 0 {
				payloads = append([]string{"valid-prefix"}, payloads...)
			}
			active := createRecoverySegment(t, dir, "sf-active.sfa", activeBase, payloads...)
			tail := qwpSfHeaderSize
			if prefixFrames > 0 {
				tail += qwpSfFrameHeaderSize + int64(len(payloads[0]))
			}
			active.buf[tail] ^= 0xff // Damage this frame; the next one stays intact.
			createRecoveryManifest(t, dir, 0, activeBase, sealed, active)
			closeRecoverySegments(t, sealed, active)

			// A genuine acknowledgement of FSN 0..3, recorded while the frames
			// discarded below still existed.
			writeForeignAckWatermark(t, dir, 3)

			probe := installQwpSfStartupProbe(t, dir)
			e := openStartupEngine(t, dir)
			probe.requireCheckpointPrecedesAllocation(t, true)
			surviving := activeBase + int64(prefixFrames) - 1
			require.Equal(t, surviving, e.enginePublishedFsn(),
				"recovery keeps the valid prefix and discards the rest of the tail")
			requireBothAckRecordsRetired(t, dir)
			assert.Equal(t, int64(-1), e.engineAckedFsn(),
				"with its ACK evidence discarded, replay falls back to the surviving segments")

			// Republish the vacated numbers.
			replacements := appendStartupFrames(t, e, 100, 4)
			require.Equal(t, surviving+4, e.enginePublishedFsn())
			require.NoError(t, e.engineClose())

			restarted := openStartupEngine(t, dir)
			defer func() { require.NoError(t, restarted.engineClose()) }()
			assert.Equal(t, int64(-1), restarted.engineAckedFsn(),
				"the old ACK must never acknowledge the newly appended frames")
			replay := collectReplayFrames(t, restarted)
			require.Equal(t, int(surviving+4)+1, len(replay))
			assert.Equal(t, replacements, replay[len(replay)-4:],
				"the replacement frames replay")
			assert.NotContains(t, replay, "intact-but-unreachable",
				"the discarded suffix stays discarded")
			if prefixFrames > 0 {
				assert.Contains(t, replay, "valid-prefix")
			}
		})
	}
}

// TestQwpSfAckWatermarkFallbackCannotBypassValidation covers plan case D. The
// old open path could fail allocation/write-back before it ever compared the
// record against the recovered frames, so the fallback ran with the stale
// record still on disk. Returning a nil watermark is not success: the file has
// to be safe first.
func TestQwpSfAckWatermarkFallbackCannotBypassValidation(t *testing.T) {
	t.Run("above-tip-record-is-retired-before-fallback", func(t *testing.T) {
		dir := t.TempDir()
		seeded := seedStartupSlot(t, dir, 4)
		writeForeignAckWatermark(t, dir, 9)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failWriteBack(syscall.ENOSPC)
		e := openStartupEngine(t, dir)
		probe.requireCheckpointPrecedesAllocation(t, true)
		require.Nil(t, e.watermark, "the write-back fault must take the unmapped fallback")
		requireBothAckRecordsRetired(t, dir)
		requireNoQuarantine(t, dir)
		assert.Equal(t, int64(-1), e.engineAckedFsn())

		appended := appendStartupFrames(t, e, 4, 6)
		require.Equal(t, int64(9), e.enginePublishedFsn())
		require.NoError(t, e.engineClose())

		restarted := openStartupEngine(t, dir)
		defer func() { require.NoError(t, restarted.engineClose()) }()
		assert.Equal(t, int64(-1), restarted.engineAckedFsn(),
			"a fallback construction must not leave an above-tip record to be believed later")
		assert.Equal(t, append(append([]string(nil), seeded...), appended...),
			collectReplayFrames(t, restarted))
	})

	t.Run("acceptable-record-survives-fallback", func(t *testing.T) {
		dir := t.TempDir()
		seedStartupSlot(t, dir, 4)
		writeForeignAckWatermark(t, dir, 2)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failWriteBack(syscall.ENOSPC)
		e := openStartupEngine(t, dir)
		// No reset is needed, yet both barriers still run before allocation.
		probe.requireCheckpointPrecedesAllocation(t, false)
		require.Nil(t, e.watermark)
		requireSelectedAckRecord(t, dir, 2)
		assert.Equal(t, int64(-1), e.engineAckedFsn(),
			"without a mapped watermark the seed comes from the surviving segments")
		require.NoError(t, e.engineClose())

		restarted := openStartupEngine(t, dir)
		defer func() { require.NoError(t, restarted.engineClose()) }()
		assert.Equal(t, int64(2), restarted.engineAckedFsn(),
			"a record acceptable for the unchanged history must still be honoured")
	})

	t.Run("fresh-slot-with-no-old-evidence", func(t *testing.T) {
		dir := t.TempDir()
		probe := installQwpSfStartupProbe(t, dir)
		probe.failWriteBack(syscall.ENOSPC)
		e := openStartupEngine(t, dir)
		defer func() { require.NoError(t, e.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, true)
		require.Nil(t, e.watermark, "safe fallback remains available with nothing to invalidate")
		_, err := e.engineAppendBlocking(context.Background(), []byte("frame"))
		require.NoError(t, err)
	})
}

// TestQwpSfAckWatermarkStartupCheckpointRetries covers plan case E. The
// unconditional checkpoint is the retry mechanism: an attempt that truncated
// the file but could not make that durable leaves a file that merely looks
// safe, and the next attempt has to repeat both barriers before it may allocate
// or map.
func TestQwpSfAckWatermarkStartupCheckpointRetries(t *testing.T) {
	t.Run("reset-retires-both-record-slots", func(t *testing.T) {
		dir := t.TempDir()
		seedStartupSlot(t, dir, 4)
		// The newest record is above the recovered tip; an older, in-range
		// record sits in the other slot. Clearing only the newer one would
		// expose FSN 2 to the next restart.
		writeAckWatermarkImage(t, dir, 2, 9, 1, 2)

		probe := installQwpSfStartupProbe(t, dir)
		e := openStartupEngine(t, dir)
		defer func() { require.NoError(t, e.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, true)
		requireBothAckRecordsRetired(t, dir)
		assert.Equal(t, int64(-1), e.engineAckedFsn(),
			"the alternate slot's record must not survive the reset")
	})

	t.Run("newer-in-range-record-wins-over-older-above-tip", func(t *testing.T) {
		dir := t.TempDir()
		seedStartupSlot(t, dir, 4)
		// Selection is by generation: the newest record is in range, so it is
		// preserved even though the older slot holds an impossible value.
		writeAckWatermarkImage(t, dir, 2, 2, 1, 99)

		probe := installQwpSfStartupProbe(t, dir)
		e := openStartupEngine(t, dir)
		defer func() { require.NoError(t, e.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, false)
		assert.Equal(t, int64(2), e.engineAckedFsn())
		requireSelectedAckRecord(t, dir, 2)
	})

	t.Run("truncation-failure-prevents-construction", func(t *testing.T) {
		dir := t.TempDir()
		seedStartupSlot(t, dir, 4)
		writeAckWatermarkImage(t, dir, 2, 9, 1, 2)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failTruncate(syscall.EIO)
		e, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		require.Error(t, err)
		require.Nil(t, e)
		require.ErrorIs(t, err, ErrSfDurability)
		require.ErrorIs(t, err, syscall.EIO)
		require.NotErrorIs(t, err, qwpSfErrAckWatermarkUnbacked,
			"a refused reset is not the allocation class the fallback may take")
		require.NotContains(t, probe.recorded(), "reserve-blocks",
			"a failed reset must stop before allocation and mapping")
		requireNoQuarantine(t, dir)
		// The bytes are untouched, so the next attempt still has the evidence.
		requireSelectedAckRecord(t, dir, 9)

		probe.reset()
		retried := reopenStartupEngineAfterFailure(t, dir)
		defer func() { require.NoError(t, retried.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, true)
		requireBothAckRecordsRetired(t, dir)
		assert.Equal(t, int64(-1), retried.engineAckedFsn())
	})

	t.Run("file-barrier-failure-after-truncation-is-retried", func(t *testing.T) {
		dir := t.TempDir()
		seeded := seedStartupSlot(t, dir, 4)
		writeForeignAckWatermark(t, dir, 9)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failFileSync(syscall.EIO)
		_, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		require.ErrorIs(t, err, ErrSfDurability)
		require.ErrorIs(t, err, syscall.EIO)
		require.NotContains(t, probe.recorded(), "reserve-blocks")
		// Truncation became visible; its durability did not.
		st, statErr := os.Stat(filepath.Join(dir, qwpSfAckWatermarkFileName))
		require.NoError(t, statErr)
		require.Zero(t, st.Size(), "a zero-length file is the intermediate startup state")

		probe.reset()
		retried := reopenStartupEngineAfterFailure(t, dir)
		// The empty file still gets both barriers: page-cache contents are no
		// evidence that the previous attempt's barrier completed.
		probe.requireCheckpointPrecedesAllocation(t, true)
		requireBothAckRecordsRetired(t, dir)
		assert.Equal(t, int64(-1), retried.engineAckedFsn())

		appended := appendStartupFrames(t, retried, 4, 6)
		require.Equal(t, int64(9), retried.enginePublishedFsn())
		require.NoError(t, retried.engineClose())

		restarted := openStartupEngine(t, dir)
		defer func() { require.NoError(t, restarted.engineClose()) }()
		assert.Equal(t, int64(-1), restarted.engineAckedFsn())
		assert.Equal(t, append(append([]string(nil), seeded...), appended...),
			collectReplayFrames(t, restarted))
	})

	t.Run("directory-barrier-failure-prevents-construction-when-record-is-acceptable", func(t *testing.T) {
		dir := t.TempDir()
		seedStartupSlot(t, dir, 4)
		writeForeignAckWatermark(t, dir, 2)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failDirSync(syscall.EIO)
		_, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		require.ErrorIs(t, err, ErrSfDurability)
		require.ErrorIs(t, err, syscall.EIO,
			"a barrier failure prevents construction even when nothing needed resetting")
		require.NotContains(t, probe.recorded(), "truncate")
		require.NotContains(t, probe.recorded(), "reserve-blocks")
		requireNoQuarantine(t, dir)

		probe.reset()
		retried := reopenStartupEngineAfterFailure(t, dir)
		defer func() { require.NoError(t, retried.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, false)
		assert.Equal(t, int64(2), retried.engineAckedFsn(),
			"the acceptable record survives the failed attempt")
	})

	t.Run("already-empty-and-invalid-files-take-the-same-barriers", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			write func(t *testing.T, dir string)
		}{
			{
				name: "already-empty",
				write: func(t *testing.T, dir string) {
					require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfAckWatermarkFileName), nil, 0o644))
				},
			},
			{
				name: "invalid-records",
				write: func(t *testing.T, dir string) {
					image := make([]byte, qwpSfAckWatermarkFileSize)
					binary.LittleEndian.PutUint32(image[0:4], 0xDEADBEEF)
					require.NoError(t, os.WriteFile(filepath.Join(dir, qwpSfAckWatermarkFileName), image, 0o644))
				},
			},
			{
				name: "missing",
				write: func(t *testing.T, dir string) {
					require.NoError(t, os.Remove(filepath.Join(dir, qwpSfAckWatermarkFileName)))
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				seedStartupSlot(t, dir, 4)
				tc.write(t, dir)

				probe := installQwpSfStartupProbe(t, dir)
				e := openStartupEngine(t, dir)
				defer func() { require.NoError(t, e.engineClose()) }()
				probe.requireCheckpointPrecedesAllocation(t, true)
				assert.Equal(t, int64(-1), e.engineAckedFsn())
				assert.Equal(t, int64(3), e.enginePublishedFsn())

				// A barrier failure must prevent construction here too, even
				// though no above-tip value is visible.
				require.NoError(t, e.engineClose())
				probe.reset()
				probe.failDirSync(syscall.EIO)
				_, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
				require.ErrorIs(t, err, ErrSfDurability)
				require.ErrorIs(t, err, syscall.EIO)
			})
		}
	})

	t.Run("write-back-fallback-after-a-previous-failed-preparation", func(t *testing.T) {
		dir := t.TempDir()
		seedStartupSlot(t, dir, 4)
		writeForeignAckWatermark(t, dir, 9)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failFileSync(syscall.EIO)
		_, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		require.ErrorIs(t, err, syscall.EIO)

		probe.reset()
		probe.failWriteBack(syscall.ENOSPC)
		retried := reopenStartupEngineAfterFailure(t, dir)
		defer func() { require.NoError(t, retried.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, true)
		require.Nil(t, retried.watermark,
			"allocation failure after a completed checkpoint may still fall back")
		requireBothAckRecordsRetired(t, dir)
		assert.Equal(t, int64(-1), retried.engineAckedFsn())
	})
}

// TestQwpSfAckWatermarkResetPreservesTrimmedSegmentSeed covers plan case F.
// Resetting the watermark file must not erase what the surviving segment
// boundaries establish: acknowledged lower segments have actually been removed,
// so replay starts at the lowest surviving frame, not at FSN 0.
func TestQwpSfAckWatermarkResetPreservesTrimmedSegmentSeed(t *testing.T) {
	// head 5 with two frames, active 7 with one: FSN 0..4 were trimmed after
	// being acknowledged, so they are gone from disk.
	build := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		sealed := createRecoverySegment(t, dir, "sf-0000000000000005.sfa", 5, "kept-05", "kept-06")
		active := createRecoverySegment(t, dir, "sf-0000000000000007.sfa", 7, "kept-07")
		createRecoveryManifest(t, dir, 5, 7, sealed, active)
		closeRecoverySegments(t, sealed, active)
		return dir
	}

	t.Run("above-tip-record-resets-to-the-segment-derived-seed", func(t *testing.T) {
		dir := build(t)
		writeForeignAckWatermark(t, dir, 99)

		probe := installQwpSfStartupProbe(t, dir)
		e := openStartupEngine(t, dir)
		probe.requireCheckpointPrecedesAllocation(t, true)
		requireBothAckRecordsRetired(t, dir)
		require.Equal(t, int64(7), e.enginePublishedFsn())
		assert.Equal(t, int64(4), e.engineAckedFsn(),
			"the seed stays lowestSurvivingBase-1; a reset must not reach back to -1")
		assert.Equal(t, []string{"kept-05", "kept-06", "kept-07"}, collectReplayFrames(t, e),
			"replay starts at the first surviving unacknowledged frame")

		appended := appendStartupFrames(t, e, 8, 3)
		require.NoError(t, e.engineClose())

		restarted := openStartupEngine(t, dir)
		defer func() { require.NoError(t, restarted.engineClose()) }()
		assert.Equal(t, int64(4), restarted.engineAckedFsn())
		assert.Equal(t, append([]string{"kept-05", "kept-06", "kept-07"}, appended...),
			collectReplayFrames(t, restarted),
			"replay must neither skip the first surviving frame nor seek removed segments")
	})

	t.Run("seed-survives-a-failed-reset-and-retry", func(t *testing.T) {
		dir := build(t)
		writeForeignAckWatermark(t, dir, 99)

		probe := installQwpSfStartupProbe(t, dir)
		probe.failTruncate(syscall.EIO)
		_, err := qwpSfNewCursorEngineForDrainer(dir, qwpSfStartupSegmentSize, qwpSfUnlimitedTotalBytes, qwpTestAppendTimeout)
		require.ErrorIs(t, err, syscall.EIO)

		probe.reset()
		retried := reopenStartupEngineAfterFailure(t, dir)
		defer func() { require.NoError(t, retried.engineClose()) }()
		probe.requireCheckpointPrecedesAllocation(t, true)
		assert.Equal(t, int64(4), retried.engineAckedFsn())
	})

	t.Run("in-range-record-is-preserved", func(t *testing.T) {
		dir := build(t)
		writeForeignAckWatermark(t, dir, 6)

		e := openStartupEngine(t, dir)
		defer func() { require.NoError(t, e.engineClose()) }()
		assert.Equal(t, int64(6), e.engineAckedFsn(),
			"a valid watermark inside the surviving range still refines the seed")
		requireSelectedAckRecord(t, dir, 6)
		assert.Equal(t, []string{"kept-07"}, collectReplayFrames(t, e))
	})
}

// TestQwpSfEmptyRecoveredRingIsNotAFreshHistory pins the distinction the
// startup decision rests on: a recovered ring holding no frames still carries
// its own sequence base, so its record is about its own history and its tip is
// base-1. Treating it as fresh would discard a legitimate record and restart
// numbering at 0.
func TestQwpSfEmptyRecoveredRingIsNotAFreshHistory(t *testing.T) {
	// A fully trimmed chain: one empty segment at base 8, committed as both
	// head and active. publishedFsn is therefore 7.
	build := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		active := reopenEmptySegment(t, dir, "sf-0000000000000008.sfa", 8)
		createRecoveryManifest(t, dir, 8, 8, active)
		closeRecoverySegments(t, active)
		return dir
	}

	for _, tc := range []struct {
		name        string
		record      int64
		wantReset   bool
		wantAcked   int64
		wantPreserv int64
	}{
		{name: "record-at-the-tip-is-preserved", record: 7, wantAcked: 7, wantPreserv: 7},
		{name: "record-below-the-head-is-preserved", record: 3, wantAcked: 7, wantPreserv: 3},
		{name: "record-above-the-tip-is-reset", record: 8, wantReset: true, wantAcked: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := build(t)
			writeForeignAckWatermark(t, dir, tc.record)

			probe := installQwpSfStartupProbe(t, dir)
			e := openStartupEngine(t, dir)
			defer func() { require.NoError(t, e.engineClose()) }()
			probe.requireCheckpointPrecedesAllocation(t, tc.wantReset)
			require.True(t, e.engineWasRecoveredFromDisk(),
				"an empty recovered ring is still a recovered history")
			assert.Equal(t, int64(7), e.enginePublishedFsn())
			assert.Equal(t, tc.wantAcked, e.engineAckedFsn())
			if tc.wantReset {
				requireBothAckRecordsRetired(t, dir)
			} else {
				requireSelectedAckRecord(t, dir, tc.wantPreserv)
			}

			// Numbering continues from the recovered base, not from 0.
			fsn, err := e.engineAppendBlocking(context.Background(), []byte("after-trim"))
			require.NoError(t, err)
			assert.Equal(t, int64(8), fsn)
		})
	}
}

// TestQwpSfAckWatermarkAcceptsGenuineLowerAckAfterReset pins that a reset does
// not latch the discarded value: the object starts like a new empty watermark,
// so the manager can persist a genuine, lower ACK afterwards and a restart
// selects that new record instead of the retired one.
func TestQwpSfAckWatermarkAcceptsGenuineLowerAckAfterReset(t *testing.T) {
	dir := t.TempDir()
	seedStartupSlot(t, dir, 4)
	writeForeignAckWatermark(t, dir, 9)

	e := openStartupEngine(t, dir)
	require.NotNil(t, e.watermark)
	require.Equal(t, int64(-1), e.engineAckedFsn())
	e.engineAcknowledge(1)
	require.Eventually(t, func() bool {
		b := readAckWatermarkFileBytes(t, dir)
		rec, ok := qwpSfSelectAckWatermarkRecord(b, int64(len(b)) == qwpSfAckWatermarkFileSize)
		return ok && rec.first == 1
	}, qwpTestWaitTimeout, 5*time.Millisecond,
		"a genuine lower ACK must be persistable after the old high value was retired")
	require.NoError(t, e.engineClose())

	restarted := openStartupEngine(t, dir)
	defer func() { require.NoError(t, restarted.engineClose()) }()
	assert.Equal(t, int64(1), restarted.engineAckedFsn(),
		"the restart must select the new record, not an old one from the alternate slot")
}

// TestQwpSfAckWatermarkStorageFallbackAllowedRequiresReleasedResources pins the
// authorisation rule directly: the sentinel alone does not license running
// without a mapped watermark, because a joined error can also carry a failure
// to release what the attempt acquired.
func TestQwpSfAckWatermarkStorageFallbackAllowedRequiresReleasedResources(t *testing.T) {
	unbacked := errors.Join(qwpSfErrAckWatermarkUnbacked,
		qwpSfDurabilityError("reserve blocks for ack watermark", "/slot/.ack-watermark", syscall.ENOSPC))
	assert.True(t, qwpSfAckWatermarkStorageFallbackAllowed(unbacked))

	assert.False(t, qwpSfAckWatermarkStorageFallbackAllowed(nil))
	assert.False(t, qwpSfAckWatermarkStorageFallbackAllowed(
		qwpSfDurabilityError("sync prepared ack watermark", "/slot/.ack-watermark", syscall.EIO)),
		"a barrier failure is not the allocation class")
	assert.False(t, qwpSfAckWatermarkStorageFallbackAllowed(
		errors.Join(unbacked, ErrCleanupFailed)),
		"an internal cleanup failure outranks the optimisation")
	assert.False(t, qwpSfAckWatermarkStorageFallbackAllowed(&qwpSfAcquisitionError{
		original:  unbacked,
		cause:     errors.Join(ErrSfDurability, syscall.EIO),
		resources: &qwpSfAcquiredResources{},
	}), "retained resources must fail construction instead")
}
