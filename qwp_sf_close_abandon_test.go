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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQwpSfSegmentCloseLeakMappingKeepsBuf pins that closeInternal(true) leaves
// a disk-backed segment's mmap mapped and its buf reference intact, so a
// goroutine wedged mid-dereference keeps a valid address instead of faulting.
func TestQwpSfSegmentCloseLeakMappingKeepsBuf(t *testing.T) {
	dir := t.TempDir()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, "s.sfa"), 0, 4096)
	require.NoError(t, err)
	require.False(t, seg.memoryBacked)
	require.NotNil(t, seg.address())

	require.NoError(t, seg.closeInternal(true))
	require.NotNil(t, seg.buf, "leaked mapping keeps buf")
	_ = seg.address()[0] // must not fault

	// Release the deliberately-leaked mapping so the test does not leak it.
	require.NoError(t, qwpSfMunmap(seg.buf))
}

// TestQwpSfSegmentCloseUnmaps pins the normal close still unmaps and nils buf.
func TestQwpSfSegmentCloseUnmaps(t *testing.T) {
	dir := t.TempDir()
	seg, err := qwpSfCreateSegment(filepath.Join(dir, "s.sfa"), 0, 4096)
	require.NoError(t, err)
	require.NoError(t, seg.close())
	require.Nil(t, seg.buf)
}

// TestQwpEngineCloseLeakSegmentsKeepsMappings pins that engineCloseLeakSegments
// tears the engine down (fds, watermark, slot lock) but leaves the segment
// mmaps valid for a still-live wedged send-loop goroutine.
func TestQwpEngineCloseLeakSegmentsKeepsMappings(t *testing.T) {
	dir := t.TempDir()
	engine, err := qwpSfNewCursorEngine(dir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)
	seg := engine.engineActiveSegment()
	require.NotNil(t, seg)
	require.False(t, seg.memoryBacked)

	require.NoError(t, engine.engineCloseLeakSegments())
	require.NotNil(t, seg.buf, "leaked mapping stays valid after engine close")
	_ = seg.address()[0] // must not fault
	require.NoError(t, qwpSfMunmap(seg.buf))
}

// TestQwpEngineSurfacesManagerWorkerPanic pins that a latched segment-manager
// worker panic is surfaced to producers as a terminal via engineTerminalError,
// rather than leaving them to stall in backpressure forever with no signal.
func TestQwpEngineSurfacesManagerWorkerPanic(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	require.NoError(t, engine.engineTerminalError(), "healthy manager: no terminal")

	detail := "boom\nstack"
	engine.manager.workerPanic.Store(&detail)

	got := engine.engineTerminalError()
	require.Error(t, got)
	require.Contains(t, got.Error(), "segment manager worker stopped")
}

// TestQwpSendLoopCloseAbandonSignalsAndReleasesTransport pins that a send loop
// whose I/O goroutine never joins abandons after the grace, flags itself so the
// engine teardown leaks the mappings, and still releases the WebSocket.
func TestQwpSendLoopCloseAbandonSignalsAndReleasesTransport(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	// Simulate a wedged I/O goroutine: a wg count that is never matched by a
	// Done, so the join never completes and sendLoopClose must abandon.
	loop.wg.Add(1)

	old := qwpSfSendLoopCloseGrace
	qwpSfSendLoopCloseGrace = 20 * time.Millisecond
	defer func() { qwpSfSendLoopCloseGrace = old }()

	require.NoError(t, loop.sendLoopClose())
	require.True(t, loop.sendLoopAbandoned(), "wedged join must abandon")
	require.Nil(t, loop.transport.Load(), "transport released on abandon")

	loop.wg.Done() // let the internal join goroutine finish
}
