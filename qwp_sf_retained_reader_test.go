/******************************************************************************
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
 *****************************************************************************/

package questdb

import (
	"context"
	"encoding/binary"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQwpSfRetainedReaderOwnership holds a real mapped frame through a caller
// timeout, without a server or a hung filesystem. The substitute I/O worker
// participates in the loop's join and ignores cancellation until released or
// its subprocess exits. Run on Windows as well as Unix; cross-compilation is
// not an execution result.
func TestQwpSfRetainedReaderOwnership(t *testing.T) {
	if mode := os.Getenv("QWP_RETAINED_READER_CHILD"); mode != "" {
		qwpTestRetainedReaderChild(t, mode, os.Getenv("QWP_RETAINED_READER_DIR"))
		return
	}

	for _, mode := range []string{"unacked-exit", "acked-exit", "unacked-late", "acked-late"} {
		t.Run(mode, func(t *testing.T) {
			// Only the parent removes this directory, after the child exits.
			// Never force-unmap or repair a retained engine to clean up a test.
			dir := t.TempDir()
			cmd := qwpTestSubprocess(t, "TestQwpSfRetainedReaderOwnership")
			cmd.Env = append(os.Environ(), "QWP_RETAINED_READER_CHILD="+mode, "QWP_RETAINED_READER_DIR="+dir)
			out, childErr := cmd.CombinedOutput()
			t.Logf("%s", out)
			if childErr != nil {
				t.Errorf("retained-reader child failed: %v", childErr)
			}

			// Even a reader retained until process exit must leave the slot
			// reopenable afterwards, with its unacknowledged frame recoverable.
			engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err, "slot recovery after child exit")
			defer func() { require.NoError(t, engine.engineClose()) }()
			if mode == "unacked-exit" || mode == "unacked-late" {
				require.Equal(t, int64(0), engine.enginePublishedFsn(), "unacknowledged frame must survive")
				require.Equal(t, int64(-1), engine.engineAckedFsn(), "shutdown must not invent an ACK")
			}
		})
	}
}

func qwpTestRetainedReaderChild(t *testing.T, mode, dir string) {
	t.Helper()
	acked := mode == "acked-exit" || mode == "acked-late"
	late := mode == "acked-late" || mode == "unacked-late"
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	factory := func(context.Context, int) (*qwpTransport, error) { return nil, context.Canceled }
	loop := qwpSfNewSendLoop(engine, nil, factory, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond)
	sender, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	require.NoError(t, sender.Table("held_reader").Int64Column("value", 42).AtNow(context.Background()))
	fsn, err := sender.FlushAndGetSequence(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(0), fsn)
	seg := engine.engineActiveSegment()
	require.NotNil(t, seg)
	require.False(t, seg.memoryBacked)
	path := seg.segmentPath()
	length := int64(binary.LittleEndian.Uint32(seg.buf[qwpSfHeaderSize+4 : qwpSfHeaderSize+8]))
	frame := seg.buf[qwpSfHeaderSize+qwpSfFrameHeaderSize : qwpSfHeaderSize+qwpSfFrameHeaderSize+length]
	want := string(frame)

	read := make(chan struct{})
	result := make(chan string, 1)
	exit := make(chan struct{})
	loop.wg.Add(1)
	go func() {
		defer loop.wg.Done()
		defer close(loop.done)
		<-read
		result <- string(frame) // A real dereference after Close's join expires.
		<-exit
		runtime.KeepAlive(frame)
	}()
	if acked {
		// The active segment cannot be trimmed by normal manager service.
		// This ACK exercises final drained-file deletion, not normal trim.
		engine.engineAcknowledge(fsn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.Logf("Close result: %v", sender.Close(ctx))

	close(read)
	select {
	case got := <-result:
		require.Equal(t, want, got, "mapping must stay readable while the reader is alive")
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("retained reader did not respond")
	}
	if engine.engineCloseCompleted() || sender.SlotLockReleased() {
		t.Error("cleanup reported complete or slot released while a mapped reader is still alive")
	}
	lock, lockErr := qwpSfAcquireSlotLock(dir)
	if lockErr == nil {
		require.NoError(t, lock.close())
		t.Error("a new owner acquired the slot while the old mapped reader is still alive")
	} else {
		require.ErrorIs(t, lockErr, qwpSfErrLockBusy)
	}
	_, statErr := os.Stat(path)
	t.Logf("mapped segment after Close: %v", statErr)
	if statErr != nil {
		t.Errorf("reader-accessible segment must remain present: %v", statErr)
	}
	if !late {
		// The subprocess exits with the reader alive. No production repair
		// hook or test-only forced release is needed, including on Windows.
		return
	}
	close(exit)
	waitQwpCleanupSignal(t, loop.done, "late reader exit")
	require.Eventually(t, engine.engineCloseCompleted, qwpTestWaitTimeout, time.Millisecond,
		"the existing owner must finish after reader exit, without another Close")
	require.True(t, seg.buf == nil, "late reader exit must permit unmapping, not permanent abandonment")
	require.True(t, sender.SlotLockReleased())
}
