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
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// qwpSfTestServerOpts shapes the fake QWP server's behavior across
// the various reconnect / failure scenarios.
type qwpSfTestServerOpts struct {
	// closeAfterFrames > 0 → close the connection after receiving N
	// total frames (across reconnects). Used to exercise reconnect.
	closeAfterFrames int
	// rejectStatus, when non-zero, causes the server to respond
	// with an error ACK carrying the given status. Used to exercise
	// terminal-server-error.
	rejectStatus QwpStatusCode
	// upgradeStatus, when non-zero, causes the server to respond
	// with that HTTP status code on the WebSocket upgrade request,
	// rejecting the connection. Used to exercise auth-terminal.
	upgradeStatus int
	// silentDropAfterFrames > 0 → on EVERY connection, read N frames
	// then close the WebSocket without sending any ACK. Models a
	// server that accepts the upgrade but doesn't speak our wire
	// protocol (version/config mismatch). This is what
	// TestQwpSfSendLoopProtocolMismatchIsTerminal exercises.
	silentDropAfterFrames int
	// silentDropUntilConn, when > 0, scopes silentDropAfterFrames to
	// connections with myConnID < silentDropUntilConn; connections at
	// or beyond that id ACK normally. Models a *transient* ACK-less
	// drop (a server restart or LB RST in the first-frame→first-ACK
	// window) on the first connection(s), after which a healthy
	// server resumes ACKing — the case the never-ACKed terminal
	// heuristic must NOT mistake for an incompatible build.
	silentDropUntilConn int
	// silentAcks → read frames forever and never write any ACK
	// back. Connection stays alive so the send loop does not go
	// terminal; the producer's Close drain-wait is what surfaces
	// the missing ACKs. Used by close-drain-timeout tests.
	silentAcks bool
	// rejectFirstNFrames > 0, in combination with rejectStatus,
	// causes only the first N frames on the very first connection to
	// receive an error ACK; everything after gets OK. Used to test
	// DROP-and-continue semantics where the loop must keep draining
	// past the rejected span.
	rejectFirstNFrames int
	// rejectFromConn > 0, in combination with rejectStatus, causes
	// only connections with myConnID >= rejectFromConn to issue
	// rejection ACKs. Connections below that threshold ACK OK
	// normally. Used to model "server transient close → reconnect
	// succeeds → next batch hits a rejection".
	rejectFromConn int
	// recordFrames → capture every frame's payload bytes, keyed by
	// the connection that received it, into qwpSfTestServer. Lets a
	// test reconstruct exactly which rows reached the server on each
	// connection so it can assert gap-free, correctly-anchored replay
	// after a mid-flush drop. Off by default so the other suites pay
	// nothing for the bookkeeping.
	recordFrames bool
	// unsolicitedRejectAtConnect, when non-zero, makes the server
	// emit a single error ACK (sequence 0) immediately on connection
	// accept, BEFORE reading any frame from the client. Models a
	// server that rejects the connection (auth halt, server-side
	// circuit breaker, transient validation failure on
	// reconnect) right after the WS upgrade — exercises the
	// receiver's pre-send rejection guard.
	unsolicitedRejectAtConnect QwpStatusCode
	// forgedAckAtConnect, when non-nil, is written verbatim to the
	// client as a single WebSocket binary message immediately on
	// connect — before and without reading any frame — after which the
	// handler falls through to its normal read loop (which blocks,
	// since tests using this don't run the sender). Lets a test inject
	// an early / forged ACK whose sequence names a frame the client has
	// not finished sending, exercising the receiver's highestFullySent
	// clamp. Build it with buildAckOK / buildAckError.
	forgedAckAtConnect []byte
}

// qwpSfTestServer is a fake QWP server for send-loop tests. It
// counts received frames across all connections (so tests can
// observe replays after reconnect).
type qwpSfTestServer struct {
	*httptest.Server
	totalFramesReceived atomic.Int64
	connCount           atomic.Int64
	// kill is closed by tests that want to actively tear down every
	// in-flight WS connection. httptest.Server.Close (and even
	// CloseClientConnections) do not force-close hijacked
	// connections, so handlers select on this channel to exit.
	kill chan struct{}
	// framesMu guards framesByConn. One handler goroutine runs per
	// connection; in the reconnect tests only one is live at a time,
	// but the lock keeps the recorder correct under the shared-handler
	// pattern regardless. Populated only when opts.recordFrames is set.
	framesMu     sync.Mutex
	framesByConn map[int64][]string
}

// recordedFrames returns a deep copy of the per-connection payload
// log, keyed by the 1-based connection id (s.connCount order). Only
// non-empty when the server was built with recordFrames:true.
func (s *qwpSfTestServer) recordedFrames() map[int64][]string {
	s.framesMu.Lock()
	defer s.framesMu.Unlock()
	out := make(map[int64][]string, len(s.framesByConn))
	for connID, payloads := range s.framesByConn {
		cp := make([]string, len(payloads))
		copy(cp, payloads)
		out[connID] = cp
	}
	return out
}

func newQwpSfTestServer(t *testing.T, opts qwpSfTestServerOpts) *qwpSfTestServer {
	t.Helper()
	s := &qwpSfTestServer{kill: make(chan struct{})}
	s.Server = httptest.NewServer(qwpSfTestServerHandler(t, s, opts))
	return s
}

// newQwpSfTestServerOnListener builds a test server bound to the
// given pre-existing listener (rather than letting httptest pick a
// free port). Used by tests that need to reserve the port BEFORE
// creating the server — e.g. the async-initial-connect path where
// the producer must dial first and wait for the server to arrive on
// a known address.
//
// Takes ownership of the listener; the server's Close also closes
// the underlying listener.
func newQwpSfTestServerOnListener(t *testing.T, listener net.Listener) *qwpSfTestServer {
	t.Helper()
	s := &qwpSfTestServer{kill: make(chan struct{})}
	s.Server = httptest.NewUnstartedServer(qwpSfTestServerHandler(t, s, qwpSfTestServerOpts{}))
	_ = s.Server.Listener.Close()
	s.Server.Listener = listener
	s.Server.Start()
	return s
}

// qwpSfTestServerHandler returns the WebSocket handler used by the
// fake QWP test server, configured by `opts` and reporting stats on
// `s`. Extracted from newQwpSfTestServer so the same handler can be
// wired onto a pre-existing listener via newQwpSfTestServerOnListener.
func qwpSfTestServerHandler(t *testing.T, s *qwpSfTestServer, opts qwpSfTestServerOpts) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.upgradeStatus != 0 {
			w.WriteHeader(opts.upgradeStatus)
			return
		}
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("websocket accept error: %v", err)
			return
		}
		defer conn.CloseNow()
		// killWatcher: if the test fires s.kill, drop this WS.
		// httptest.Server.Close/CloseClientConnections do not force-
		// close hijacked WebSocket conns, so we need our own signal.
		killCtx, cancelKill := context.WithCancel(context.Background())
		defer cancelKill()
		go func() {
			select {
			case <-s.kill:
				_ = conn.CloseNow()
			case <-killCtx.Done():
			}
		}()
		myConnID := s.connCount.Add(1)
		var localSeq int64
		var localFramesReceived int
		if opts.unsolicitedRejectAtConnect != 0 {
			// Send a single rejection ACK with sequence 0 BEFORE the
			// client has had a chance to send anything. The receiver
			// must observe highestSent < 0 and route through the
			// pre-send rejection guard (no engineAcknowledge advance).
			_ = conn.Write(context.Background(), websocket.MessageBinary,
				buildAckError(opts.unsolicitedRejectAtConnect, 0, "pre-send-reject"))
		}
		if opts.forgedAckAtConnect != nil {
			// Inject a caller-built early / forged ACK before reading
			// any frame, then fall through to the read loop below (which
			// blocks until the client tears the connection down).
			_ = conn.Write(context.Background(), websocket.MessageBinary,
				opts.forgedAckAtConnect)
		}
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			s.totalFramesReceived.Add(1)
			localFramesReceived++
			if opts.recordFrames {
				// Record BEFORE the closeAfterFrames drop below: a
				// frame the server read but never ACKed (its ACK lost
				// to the drop) still "reached the server" — that is
				// exactly the persisted-but-unacked row the real
				// server dedups when replay re-sends it.
				s.framesMu.Lock()
				if s.framesByConn == nil {
					s.framesByConn = make(map[int64][]string)
				}
				s.framesByConn[myConnID] = append(s.framesByConn[myConnID], string(data))
				s.framesMu.Unlock()
			}
			// closeAfterFrames triggers ONLY on the first connection:
			// we accept N frames and then drop. Subsequent reconnects
			// behave normally so the loop can drain.
			if opts.closeAfterFrames > 0 &&
				myConnID == 1 &&
				localFramesReceived >= opts.closeAfterFrames {
				return
			}
			// silentDropAfterFrames applies to EVERY connection: read N
			// frames then close without ACKing. Models a server that
			// accepts the upgrade but doesn't understand our wire
			// protocol — reconnects would just hammer it. When
			// silentDropUntilConn is set the drop is scoped to the
			// first (silentDropUntilConn-1) connections, so later
			// reconnects ACK normally — a transient drop, not an
			// incompatible build.
			silentDropActive := opts.silentDropAfterFrames > 0
			if silentDropActive && opts.silentDropUntilConn > 0 {
				silentDropActive = myConnID < int64(opts.silentDropUntilConn)
			}
			if silentDropActive &&
				localFramesReceived >= opts.silentDropAfterFrames {
				return
			}
			if opts.silentAcks {
				continue
			}
			if opts.rejectStatus != 0 {
				// Default behavior with no gating: reject every frame.
				rejectThisFrame := true
				// rejectFirstNFrames gates rejection to the first N
				// frames of conn 1 (and silently passes on conn 2+).
				if opts.rejectFirstNFrames > 0 {
					if myConnID == 1 {
						rejectThisFrame = localFramesReceived <= opts.rejectFirstNFrames
					} else {
						rejectThisFrame = false
					}
				}
				// rejectFromConn additively re-enables rejection on
				// conn N+. Combined with rejectFirstNFrames, this models
				// "reject some on conn 1, reject all on conn ≥ N".
				if opts.rejectFromConn > 0 {
					if myConnID >= int64(opts.rejectFromConn) {
						rejectThisFrame = true
					} else if opts.rejectFirstNFrames == 0 {
						rejectThisFrame = false
					}
				}
				if rejectThisFrame {
					_ = conn.Write(context.Background(), websocket.MessageBinary,
						buildAckError(opts.rejectStatus, localSeq, "rejected"))
					localSeq++
					continue
				}
			}
			_ = conn.Write(context.Background(), websocket.MessageBinary,
				buildAckOK(localSeq))
			localSeq++
		}
	})
}

// qwpSfDialFor builds a transport connected to the given
// httptest server. Used as the qwpSfReconnectFactory for tests.
// The idx parameter is accepted for signature symmetry with
// multi-host factories and ignored — tests use a single host.
func qwpSfDialFor(server *qwpSfTestServer) qwpSfReconnectFactory {
	return func(ctx context.Context, _ int) (*qwpTransport, error) {
		var t qwpTransport
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		if err := t.connect(ctx, wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

// qwpSfDurableDialFor is qwpSfDialFor with request_durable_ack set, so connect()
// rejects a server that does not advertise durable-ack.
func qwpSfDurableDialFor(server *qwpSfTestServer) qwpSfReconnectFactory {
	return func(ctx context.Context, _ int) (*qwpTransport, error) {
		var t qwpTransport
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		if err := t.connect(ctx, wsURL, qwpTransportOpts{endpointPath: qwpWritePath, requestDurableAck: true}); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

// qwpSfDialAt builds a transport connected to a fixed httptest URL.
func qwpSfDialAt(url string) qwpSfReconnectFactory {
	return func(ctx context.Context, _ int) (*qwpTransport, error) {
		var t qwpTransport
		wsURL := "ws" + strings.TrimPrefix(url, "http")
		if err := t.connect(ctx, wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

func TestQwpSfSendLoopHappyPath(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Append 10 frames.
	for i := 0; i < 10; i++ {
		_, err := engine.engineAppendBlocking(context.Background(), []byte(fmt.Sprintf("frame-%d", i)))
		require.NoError(t, err)
	}

	// Wait until ackedFsn catches up.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 9
	}, 2*time.Second, 1*time.Millisecond, "loop did not drain")
	assert.Equal(t, int64(10), srv.totalFramesReceived.Load())
	assert.Equal(t, int64(10), loop.sendLoopTotalFramesSent())
	assert.Equal(t, int64(10), loop.sendLoopTotalAcks())
	assert.Equal(t, int64(0), loop.sendLoopTotalReconnects())
	assert.NoError(t, loop.sendLoopCheckError())
}

// positionCursorAt walks frame headers on the unrecovered I/O
// goroutine. A corrupt-but-positive payloadLen must be rejected with
// an error (which both callers route through recordFatal) rather than
// overrunning offset and panicking the next slice index — that panic
// would crash the whole process and bypass the typed-error path.
func TestQwpSfPositionCursorAtRejectsCorruptPayloadLen(t *testing.T) {
	unusedFactory := func(context.Context, int) (*qwpTransport, error) {
		return nil, errors.New("factory not used in this test")
	}

	// Build an engine with a few real frames so a segment exists with
	// baseSeq 0 and FSNs 0..2, then corrupt the first frame's
	// payloadLen field in place and walk past it.
	newCorruptLoop := func(t *testing.T, corruptBytes [4]byte) *qwpSfSendLoop {
		t.Helper()
		engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.engineClose() })

		for i := 0; i < 3; i++ {
			_, err := engine.engineAppendBlocking(context.Background(), []byte("payl"))
			require.NoError(t, err)
		}
		seg := engine.engineFindSegmentContaining(0)
		require.NotNil(t, seg)

		// payloadLen of the first frame lives at
		// [qwpSfHeaderSize+4 : qwpSfHeaderSize+8].
		addr := seg.address()
		plOff := qwpSfHeaderSize + 4
		copy(addr[plOff:plOff+4], corruptBytes[:])

		return qwpSfNewSendLoop(engine, nil, unusedFactory,
			time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	}

	t.Run("corrupt-but-positive payloadLen", func(t *testing.T) {
		// 0x7FFFFFFF little-endian: positive int32, ~2 GiB stride.
		loop := newCorruptLoop(t, [4]byte{0xFF, 0xFF, 0xFF, 0x7F})
		// targetFsn=2 forces a multi-frame walk; pre-fix this panicked
		// on the second iteration's out-of-bounds header read.
		err := loop.positionCursorAt(2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "corrupt segment")
	})

	t.Run("negative payloadLen", func(t *testing.T) {
		// 0xFFFFFFFF little-endian: int32(-1).
		loop := newCorruptLoop(t, [4]byte{0xFF, 0xFF, 0xFF, 0xFF})
		err := loop.positionCursorAt(2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "corrupt segment")
	})

	t.Run("valid walk is not a false positive", func(t *testing.T) {
		engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.engineClose() })

		for i := 0; i < 3; i++ {
			_, err := engine.engineAppendBlocking(context.Background(), []byte("payl"))
			require.NoError(t, err)
		}
		loop := qwpSfNewSendLoop(engine, nil, unusedFactory,
			time.Millisecond, time.Second, time.Millisecond, time.Millisecond)

		require.NoError(t, loop.positionCursorAt(2))
		// Two 4-byte-payload frames walked: HEADER + 2*(8+4).
		assert.Equal(t, qwpSfHeaderSize+2*(qwpSfFrameHeaderSize+4), loop.sendOffset)
	})
}

// TestQwpSfPositionCursorAtReconnectRace is a regression guard for the
// reconnect-under-load row-loss bug (Java PR #40). On reconnect,
// swapClient pins fsnAtZero to targetFsn = ackedFsn+1 and resets
// nextWireSeq to 0, then calls positionCursorAt(targetFsn). For wireSeq=0
// to map back to targetFsn on the new connection, the cursor MUST land on
// the byte offset where targetFsn's frame begins.
//
// The producer runs concurrently with the I/O goroutine: positionCursorAt's
// first findSegmentContaining can miss targetFsn, and the buggy fallback
// then read the active segment's *post-publish* tip and parked one frame
// PAST targetFsn — silently dropping targetFsn and misnumbering every
// later frame by one, which the server trimmed on its next cumulative ACK
// while close() still reported clean delivery.
//
// We can't pin the exact interleaving, so we hammer positionCursorAt
// against a live producer and assert the invariant that must always hold
// post-fix: after positionCursorAt(targetFsn) the cursor sits exactly at
// targetFsn's frame offset, never past it. targetFsn is always at most one
// past publishedFsn (just like the reconnect anchor), so its offset is
// fixed whether the frame is already published, published mid-call, or not
// yet published. Pre-fix this trips whenever a publish lands inside the
// lookup→snapshot window; best run under -race.
func TestQwpSfPositionCursorAtReconnectRace(t *testing.T) {
	const (
		frames     = 4000
		payloadLen = 4
	)
	payload := []byte("payl") // payloadLen bytes
	stride := int64(qwpSfFrameHeaderSize + payloadLen)
	// One segment large enough to hold every frame, so baseSeq stays 0 and
	// no rotation perturbs the offset arithmetic.
	segSize := qwpSfHeaderSize + int64(frames)*stride + 1024

	engine, err := qwpSfNewCursorEngine("", segSize, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })

	unusedFactory := func(context.Context, int) (*qwpTransport, error) {
		return nil, errors.New("factory not used in this test")
	}
	loop := qwpSfNewSendLoop(engine, nil, unusedFactory,
		time.Millisecond, time.Second, time.Millisecond, time.Millisecond)

	// Frame N begins at this offset; the segment never rotates so it is
	// stable for the whole run.
	expectedOffset := func(fsn int64) int64 { return qwpSfHeaderSize + fsn*stride }

	// Stop + drain the producer before the engine is torn down. t.Cleanup
	// runs LIFO, so this (registered after the engine-close cleanup above)
	// runs first: on a require failure the test goroutine unwinds via
	// Goexit, and this guarantees the producer is no longer appending when
	// engineClose runs — otherwise it would nil-deref on the closed segment
	// and mask the real assertion message with a panic.
	var prodErr atomic.Value // holds error
	stop := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		for i := 0; i < frames; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := engine.engineAppendBlocking(context.Background(), payload); err != nil {
				prodErr.Store(err)
				return
			}
		}
	}()

positioning:
	for {
		select {
		case <-done:
			break positioning
		default:
		}
		// At most one past what's published right now — either already
		// published, published during the call (the race window), or the
		// very next frame. This mirrors the reconnect anchor
		// targetFsn = ackedFsn+1.
		targetFsn := engine.enginePublishedFsn() + 1
		if targetFsn >= int64(frames) {
			continue
		}
		require.NoError(t, loop.positionCursorAt(targetFsn))
		require.Equalf(t, expectedOffset(targetFsn), loop.sendOffset,
			"positionCursorAt(%d) parked %d stride(s) past the frame — a reconnect here would drop it",
			targetFsn, (loop.sendOffset-expectedOffset(targetFsn))/stride)
	}
	if e := prodErr.Load(); e != nil {
		t.Fatalf("producer failed: %v", e.(error))
	}

	// Producer done: every frame is published. A deterministic position on
	// the last frame must land exactly on it, in the original baseSeq-0
	// segment (no rotation happened).
	require.NoError(t, loop.positionCursorAt(int64(frames-1)))
	require.Equal(t, expectedOffset(int64(frames-1)), loop.sendOffset)
	require.Equal(t, int64(0), loop.sendingSegment.segmentBaseSeq(),
		"single segment expected; a rotation would invalidate the offset math above")
}

func TestQwpSfSendLoopReconnectAfterServerClose(t *testing.T) {
	// Run over both engine backings so disk-backed reconnect+replay —
	// otherwise exercised only by the jar-gated fuzz workflow — is
	// covered here too. "" selects a memory-backed engine; a TempDir
	// selects disk-backed segments under that slot directory.
	t.Run("memory", func(t *testing.T) { testQwpSfSendLoopReconnectAfterServerClose(t, "") })
	t.Run("disk", func(t *testing.T) { testQwpSfSendLoopReconnectAfterServerClose(t, t.TempDir()) })
}

func testQwpSfSendLoopReconnectAfterServerClose(t *testing.T, sfDir string) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 5})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine(sfDir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Warm-up frame: process one ACK deterministically before the
	// burst so the run() silent-drop guard gates on lifetime
	// totalAcks > 0 and treats the upcoming mid-burst drop as
	// transient (reconnect) rather than as "server doesn't speak our
	// protocol" (terminal halt). Without this, when closeAfterFrames
	// fires, conn.CloseNow runs with the client's still-unread frames
	// in the server's TCP RX buffer — Linux turns that close into a
	// RST, which discards the 4 ACKs the server wrote before the
	// trigger from the OS receive buffer on the client side. The
	// receiver loop never sees them, lifetime totalAcks stays at 0,
	// and run() latches the wrong terminal classification.
	_, err = engine.engineAppendBlocking(context.Background(), []byte("warm-up"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond, "warm-up frame should ACK before the burst")

	for i := 0; i < 10; i++ {
		_, err := engine.engineAppendBlocking(context.Background(), []byte(fmt.Sprintf("f-%d", i)))
		require.NoError(t, err)
	}
	// All 11 frames (warm-up + 10 burst) should eventually be ACKed
	// despite the server dropping conn 1 after reading 5 (warm-up +
	// first 4 burst). closeAfterFrames is gated on myConnID == 1 so
	// the reconnect lands on a fresh handler instance that ACKs
	// every frame cleanly; the remaining burst frames hit the server
	// on conn 2.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 10
	}, 5*time.Second, 1*time.Millisecond, "loop did not drain after reconnect")
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1))
	// fsnAtZero should have advanced past 0 after the swap.
	assert.Greater(t, loop.sendLoopFsnAtZero(), int64(0))
}

// TestQwpSfSendLoopProgressMonotonicAcrossReconnect pins that the ack-progress
// stream stays strictly increasing across a mid-stream connection drop + replay.
// lastProgressFsn is deliberately not reset on reconnect, so a replayed/re-acked
// frame can never re-emit an FSN already reported.
func TestQwpSfSendLoopProgressMonotonicAcrossReconnect(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 5})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)

	var mu sync.Mutex
	var progress []int64
	regressedTo := int64(-1)
	loop.sendLoopSetProgressHandler(func(fsn int64) {
		mu.Lock()
		defer mu.Unlock()
		if len(progress) > 0 && fsn <= progress[len(progress)-1] {
			regressedTo = fsn
		}
		progress = append(progress, fsn)
	}, 64)

	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Warm-up ACK first so the mid-burst drop is classified as transient (see
	// testQwpSfSendLoopReconnectAfterServerClose for the RST rationale).
	_, err = engine.engineAppendBlocking(context.Background(), []byte("warm-up"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return loop.sendLoopTotalAcks() >= 1 },
		time.Second, time.Millisecond, "warm-up frame should ACK before the burst")

	for i := 0; i < 10; i++ {
		_, err := engine.engineAppendBlocking(context.Background(), []byte(fmt.Sprintf("f-%d", i)))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return engine.engineAckedFsn() >= 10 },
		5*time.Second, time.Millisecond, "loop did not drain after reconnect")
	require.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(progress) > 0 && progress[len(progress)-1] == 10
	}, 5*time.Second, time.Millisecond, "progress stream did not reach fsn 10")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, int64(-1), regressedTo, "progress regressed across reconnect")
}

// TestQwpSfSendLoopReplayIsGapFree pins the single most important
// correctness property of the cursor/SF architecture: after a
// mid-flush connection drop, the union of frames the server receives
// across all connections covers EVERY appended row with no gap, and
// the post-reconnect replay is FSN-contiguous, anchored exactly at
// the client's fsnAtZero (= engineAckedFsn()+1 at swap time).
//
// This is at-least-once on the wire by design — qwp-cursor-durability
// §"Stated assumptions": "Replay-after-reconnect produces
// duplicates", and the real server dedups by messageSequence; the
// recovery+dedup contract is explicitly out of this repo's scope. So
// the test deliberately *expects* duplicates and asserts none of the
// things server-side dedup handles. It fails only on a replay GAP
// (permanent data loss) or a MISALIGNED anchor (the client stamping a
// messageSequence the server's dedup can't key on) — the two failure
// modes dedup cannot paper over, and the two that are this client's
// job to guarantee.
//
// Why the scenario has teeth: closeAfterFrames:5 over (warm-up + 10
// burst) appends means the server reads warm-up + f-0..f-3 on conn 1
// and never sees f-4..f-9 on conn 1 at all. The ONLY path by which
// f-4..f-9 ever reach the server is the post-reconnect replay, so a
// cursor-repositioning bug that skips any of them is permanent loss
// that neither the global frame counter nor an `ackedFsn >= n`
// liveness check can detect (both are driven off the same client-
// side FSN math the bug would have corrupted). The contiguity+anchor
// assertion additionally catches a skip of warm-up..f-3 (those
// frames the server DID see pre-drop, so the union alone would mask
// their loss).
func TestQwpSfSendLoopReplayIsGapFree(t *testing.T) {
	// Run over both engine backings so disk-backed gap-free replay —
	// otherwise exercised only by the jar-gated fuzz workflow — is
	// covered here too. "" selects a memory-backed engine; a TempDir
	// selects disk-backed segments under that slot directory.
	t.Run("memory", func(t *testing.T) { testQwpSfSendLoopReplayIsGapFree(t, "") })
	t.Run("disk", func(t *testing.T) { testQwpSfSendLoopReplayIsGapFree(t, t.TempDir()) })
}

func testQwpSfSendLoopReplayIsGapFree(t *testing.T, sfDir string) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		closeAfterFrames: 5,
		recordFrames:     true,
	})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine(sfDir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Warm-up frame: process one ACK deterministically before the
	// burst so the run() silent-drop guard gates on lifetime
	// totalAcks > 0 and treats the upcoming mid-burst drop as
	// transient. See the equivalent block in
	// TestQwpSfSendLoopReconnectAfterServerClose for the
	// RST-loses-in-flight-ACKs race that this dodges.
	_, err = engine.engineAppendBlocking(context.Background(), []byte("warm-up"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond, "warm-up frame should ACK before the burst")

	const n = 10
	for i := 0; i < n; i++ {
		_, err := engine.engineAppendBlocking(
			context.Background(), []byte(fmt.Sprintf("f-%d", i)))
		require.NoError(t, err)
	}
	// FSNs: warm-up=0, f-0..f-9 = 1..10. All-acked = ackedFsn >= n.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= int64(n)
	}, 5*time.Second, 1*time.Millisecond,
		"loop did not drain every frame after reconnect")
	require.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"the mid-flush drop must have forced at least one reconnect")

	frames := srv.recordedFrames()
	require.Len(t, frames, 2,
		"expected exactly two connections (one drop -> one reconnect)")
	conn1, conn2 := frames[1], frames[2]

	// conn 1: the server reads exactly the warm-up + first four
	// burst frames (5 total — the closeAfterFrames trigger), in
	// order, then drops. This is independent of how many ACKs it
	// managed to write before dropping, so this part is race-free.
	require.Equal(t, []string{"warm-up", "f-0", "f-1", "f-2", "f-3"}, conn1,
		"conn 1 must receive warm-up + first 4 burst frames before the drop")

	// conn 2: the replayed run. Its start depends on how many of
	// conn 1's ACKs the receiver had processed before the drop
	// surfaced — a benign race: fsnAtZero = engineAckedFsn()+1 at
	// swap time, somewhere in [1,4] (warm-up's ACK was waited-on so
	// fsnAtZero is at least 1; at most warm-up + f-0..f-2 were ACKed
	// before the close, so fsnAtZero is at most 4). Whatever that
	// anchor is, the replay MUST begin exactly there, be strictly
	// contiguous (no gap, no reorder), and run through the final
	// frame. fsnAtZero and the replayed bytes derive from the same
	// ackedFsn snapshot, so this assertion is race-robust and is
	// precisely the wire<->messageSequence alignment server-side
	// dedup keys on.
	require.NotEmpty(t, conn2, "reconnect must have replayed frames")
	fsnAtZero := loop.sendLoopFsnAtZero()
	require.GreaterOrEqual(t, fsnAtZero, int64(1))
	require.LessOrEqual(t, fsnAtZero, int64(4))
	for i, got := range conn2 {
		// FSN fsnAtZero+i maps to f-(fsnAtZero-1+i): warm-up holds
		// FSN 0, the burst occupies FSN 1..n.
		want := fmt.Sprintf("f-%d", fsnAtZero-1+int64(i))
		require.Equalf(t, want, got,
			"replayed frame %d not contiguous from the fsnAtZero anchor "+
				"(gap, reorder, or misaligned messageSequence)", i)
	}
	require.Equalf(t, fmt.Sprintf("f-%d", n-1), conn2[len(conn2)-1],
		"replay must run through the final frame f-%d", n-1)

	// THE data-loss guard: every appended burst row reached the
	// server at least once across the two connections. f-4..f-9 were
	// never seen on conn 1, so only a correct replay puts them in
	// this set.
	seen := make(map[string]bool, n+1)
	for _, payloads := range frames {
		for _, p := range payloads {
			seen[p] = true
		}
	}
	for i := 0; i < n; i++ {
		require.Truef(t, seen[fmt.Sprintf("f-%d", i)],
			"row f-%d never reached the server — gap-free replay violated", i)
	}

	// Duplicates are expected and correct (at-least-once + server
	// dedup). Assert at least one actually occurred so a future change
	// that silently stopped replaying can't pass this test trivially.
	// Total appended = warm-up + n burst = n+1; anything past that is
	// a replayed duplicate.
	require.Greaterf(t, srv.totalFramesReceived.Load(), int64(n+1),
		"replay must re-send >=1 already-received frame (the dup the "+
			"server dedups); got only %d total for %d rows",
		srv.totalFramesReceived.Load(), n+1)
}

func TestQwpSfSendLoopServerErrorIsTerminal(t *testing.T) {
	// Use ParseError, which the spec defaults to Halt — SchemaMismatch
	// is Drop and would no longer be terminal under the new policy
	// resolver.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking(context.Background(), []byte("bad"))
	require.NoError(t, err)

	// Loop must record a terminal error rather than entering reconnect.
	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	var senderErr *SenderError
	assert.True(t, errors.As(gotErr, &senderErr) || strings.Contains(gotErr.Error(), "rejected"))
	// reconnects should be 0 — terminal status doesn't trigger
	// reconnect (server isn't going to change its mind on retry).
	assert.Equal(t, int64(0), loop.sendLoopTotalReconnects())
}

// TestQwpSfSendLoopPreSendHaltRejectionDoesNotFabricateFsn verifies
// that a HALT-category rejection ACK arriving BEFORE any frame has
// been sent on the current connection (highestSent < 0, e.g. right
// after a fresh swapClient) surfaces the typed SenderError but does
// NOT attribute it to a fabricated fsnAtZero. The reported span must
// be the unacked [ackedFsn+1, publishedFsn] window — the same span
// the protocol-violation close path uses — not the
// fsnAtZero+cappedSeq(=0) value the old code emitted. Mirrors the
// Java client's handlePreSendRejection guard.
func TestQwpSfSendLoopPreSendHaltRejectionDoesNotFabricateFsn(t *testing.T) {
	// ParseError is HALT by default. The server fires the rejection
	// immediately on connect, before we publish anything into the
	// engine, so the receiver sees highestSent < 0.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		unsolicitedRejectAtConnect: QwpStatusParseError,
	})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	var senderErr *SenderError
	require.True(t, errors.As(gotErr, &senderErr),
		"expected typed *SenderError, got %T: %v", gotErr, gotErr)
	assert.Equal(t, CategoryParseError, senderErr.Category)
	assert.Equal(t, PolicyHalt, senderErr.AppliedPolicy)
	// Engine is empty: ackedFsn=-1, publishedFsn=-1 →
	// FromFsn = 0, ToFsn = max(0, -1) = 0.
	assert.Equal(t, int64(0), senderErr.FromFsn)
	assert.Equal(t, int64(0), senderErr.ToFsn)
	// The fabricated DROP would have advanced the engine watermark to
	// fsn 0. Verify it did NOT.
	assert.Equal(t, int64(-1), engine.engineAckedFsn(),
		"pre-send rejection must not advance the engine's acked watermark")
	assert.Equal(t, int64(1), loop.sendLoopTotalServerErrors())
	assert.Equal(t, int64(0), loop.sendLoopTotalReconnects(),
		"HALT must not trigger reconnect")
}

// TestQwpSfSendLoopPreSendDropRejectionDoesNotAdvanceWatermark
// verifies that a DROP_AND_CONTINUE rejection arriving before any
// frame has been sent on the current connection is dispatched but
// does NOT call engineAcknowledge — the old code would have advanced
// ackedFsn past the next-unsent batch (fsnAtZero == ackedFsn+1 right
// after a swap), which would let the segment manager trim sealed
// segments the I/O thread is about to replay.
func TestQwpSfSendLoopPreSendDropRejectionDoesNotAdvanceWatermark(t *testing.T) {
	// SchemaMismatch is DROP_AND_CONTINUE by default — this is the
	// dangerous case where the old code's fabricated
	// engineAcknowledge(fsnAtZero) silently advanced the watermark.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		unsolicitedRejectAtConnect: QwpStatusSchemaMismatch,
	})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Wait for the receiver to process the unsolicited rejection.
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalServerErrors() >= 1
	}, 2*time.Second, 1*time.Millisecond)

	// DROP must not latch — loop stays running, no terminal error.
	assert.NoError(t, loop.sendLoopCheckError(),
		"DROP policy must not latch a terminal error")
	// Critical: the engine watermark must be unchanged. The old code
	// would have called engineAcknowledge(fsnAtZero) = engineAcknowledge(0),
	// advancing ackedFsn from -1 to 0.
	assert.Equal(t, int64(-1), engine.engineAckedFsn(),
		"pre-send DROP rejection must not advance the engine's acked watermark")
	// And no spurious totalAcks bump either — the old code added one.
	assert.Equal(t, int64(0), loop.sendLoopTotalAcks(),
		"pre-send DROP rejection must not bump totalAcks")
}

// TestQwpSfSendLoopSilentDropAfterFrameIsTerminal verifies that when
// the server accepts the WS upgrade but silently disconnects after a
// frame (without sending any ACK) on EVERY connection, the send loop
// classifies it as a server version/config mismatch and fails fast
// instead of entering a hot reconnect loop. Without this guard, every
// dial succeeds and the receiver reset its backoff on each attempt —
// burning thousands of ephemeral ports per second until
// reconnectMaxDuration (5 minutes default) expired.
//
// The guard fires only after qwpSfMaxSilentConnStrikes consecutive
// ACK-less connections — at least one full reconnect+replay cycle
// that still met silence — so this server, which drops on every
// connection, trips it. A single such drop reconnects instead; see
// TestQwpSfSendLoopSilentDropOnFirstConnReconnects.
func TestQwpSfSendLoopSilentDropAfterFrameIsTerminal(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{silentDropAfterFrames: 1})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 5*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond, "loop should have failed fast")

	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "without ACKing",
		"error should explain the no-ACK detection")

	// The whole point: we must NOT hammer the server with thousands
	// of reconnects. With qwpSfMaxSilentConnStrikes == 2 the loop
	// gives up after exactly one reconnect+replay cycle that still
	// met silence — i.e. one reconnect and two connections.
	assert.LessOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"expected at most one reconnect before terminal classification")
	assert.LessOrEqual(t, srv.connCount.Load(), int64(2),
		"server should have seen at most 2 connections")
}

// TestQwpSfSendLoopSilentDropOnFirstConnReconnects verifies that a
// single ACK-less disconnect on the *first* connection — the
// signature of a routine server restart or LB RST landing in the
// window between a fresh sender's first frame and its first ACK —
// reconnects, replays the unacked frame, and recovers once the server
// ACKs. A repeated ACK-less pattern (>= qwpSfMaxSilentConnStrikes
// connections, i.e. at least one full reconnect+replay cycle that
// still met silence) is what trips the terminal classification; that
// case is TestQwpSfSendLoopSilentDropAfterFrameIsTerminal.
func TestQwpSfSendLoopSilentDropOnFirstConnReconnects(t *testing.T) {
	// Conn 1 reads one frame then closes without ACKing (the
	// transient restart/RST); conn 2+ ACK normally.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		silentDropAfterFrames: 1,
		silentDropUntilConn:   2,
	})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 5*time.Second, 1*time.Millisecond, 10*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// One frame: conn 1 reads it and silently drops; the loop must
	// reconnect to conn 2, replay it, and get the ACK.
	_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, 2*time.Second, 1*time.Millisecond,
		"replayed frame should be ACK'd after reconnect to a healthy conn")

	// Crucially: the first ACK-less drop must NOT have latched a
	// terminal incompatible-build SenderError.
	if gotErr := loop.sendLoopCheckError(); gotErr != nil {
		t.Fatalf("loop went terminal on a routine first-connection drop: %v", gotErr)
	}
	assert.Nil(t, loop.sendLoopLastTerminalServerError(),
		"a single ACK-less first-connection drop must not be terminal")
	// And we recovered via exactly the reconnect+replay path.
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"loop should have reconnected past the transient drop")
	assert.GreaterOrEqual(t, loop.sendLoopTotalFramesReplayed(), int64(1),
		"the unacked frame should have been replayed on the new connection")
}

// TestQwpSfSendLoopSilentDropAfterPriorAckReconnects pins the
// regression for the silent-drop guard's false-positive failure
// mode: once any ACK has been observed across this sender's
// lifetime, a subsequent silent disconnect is a transient outage
// (LB drain emitting WS 1001 GoingAway, TCP RST surfacing as 1006,
// proxy reset, 1011/1012/1013 service restarts — none of which are
// flagged terminal by qwpSfIsTerminalCloseCode), not an
// incompatible-build mismatch. The loop must keep reconnecting
// rather than latch a terminal SenderError.
func TestQwpSfSendLoopSilentDropAfterPriorAckReconnects(t *testing.T) {
	goodSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer goodSrv.Close()
	// silentSrv stands in for the LB / proxy that accepts the WS
	// upgrade but drops every frame without ACKing — what the old
	// per-connection heuristic mistook for "incompatible build".
	silentSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{silentDropAfterFrames: 1})
	defer silentSrv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(goodSrv)(context.Background(), 0)
	require.NoError(t, err)

	// Reconnect factory points at silentSrv: after goodSrv goes
	// away, every reconnect lands on the silent-drop server.
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialAt(silentSrv.URL),
		100*time.Microsecond, 30*time.Second, 1*time.Millisecond, 5*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Frame 0: goodSrv ACKs. After this, totalAcks >= 1 and the
	// silent-drop guard's "never any ACK" precondition is gone.
	_, err = engine.engineAppendBlocking(context.Background(), []byte("warm-up"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond, "warm-up frame should have been ACK'd by goodSrv")

	// Tear down goodSrv to force the loop into reconnect against silentSrv.
	close(goodSrv.kill)

	// Enqueue a frame that silentSrv will read and silently drop,
	// driving the silent-drop guard's reconnect cycle. Without
	// further work the loop would just park on a quiet silentSrv
	// connection forever and we'd observe no reconnects either way.
	_, err = engine.engineAppendBlocking(context.Background(), []byte("post-kill"))
	require.NoError(t, err)

	// Wait until the loop has accumulated several silent-drop
	// reconnect cycles against silentSrv. Under the old heuristic
	// the very first cycle would have latched a terminal
	// "incompatible build" SenderError, capping connCount at 1.
	require.Eventually(t, func() bool {
		return silentSrv.connCount.Load() >= 3
	}, 2*time.Second, 1*time.Millisecond,
		"loop should have reconnected to silentSrv multiple times")

	// The whole point: no terminal classification.
	if gotErr := loop.sendLoopCheckError(); gotErr != nil {
		t.Fatalf("loop unexpectedly went terminal after prior-ACK silent drop: %v", gotErr)
	}
	assert.Nil(t, loop.sendLoopLastTerminalServerError(),
		"no terminal SenderError should be latched once totalAcks > 0")
}

func TestQwpSfSendLoopUpgradeAuthFailureIsTerminal(t *testing.T) {
	// First server ACKs at least one frame (so the post-disconnect
	// classification is "had a real conversation, try to reconnect"
	// rather than the no-ACK protocol-mismatch terminal path); then
	// the WS conn is killed and the reconnect factory points at a
	// *different* server that rejects the upgrade with 401, which is
	// what this test actually exercises.
	authSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: 401})
	defer authSrv.Close()
	dataSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer dataSrv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(dataSrv)(context.Background(), 0)
	require.NoError(t, err)

	// Reconnect factory dials the auth-rejecting server.
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialAt(authSrv.URL),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking(context.Background(), []byte("hi"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond, "expected the warm-up frame to be ACKed by dataSrv")

	// Tear down the live WS so the loop falls into reconnect, where
	// it'll hit authSrv and surface the 401.
	close(dataSrv.kill)

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	// Phase 4 routes 401 → SECURITY_ERROR / Halt SenderError.
	var senderErr *SenderError
	require.True(t, errors.As(gotErr, &senderErr),
		"expected *SenderError, got %T: %v", gotErr, gotErr)
	assert.Equal(t, CategorySecurityError, senderErr.Category)
	assert.Equal(t, PolicyHalt, senderErr.AppliedPolicy)
	assert.Contains(t, senderErr.ServerMessage, "401")
}

func TestQwpSfSendLoopReconnectBudgetExhausted(t *testing.T) {
	// Healthy server first — get a successful ACK on the live
	// connection so the disconnect, when it comes, is NOT classified
	// as "no ACKs ever, must be a protocol mismatch" by run(). Then
	// take the server down so reconnects fail with connection-refused
	// and the per-outage budget actually gets exercised.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 200*time.Millisecond /* short cap */, 10*time.Millisecond, 50*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking(context.Background(), []byte("warm-up"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond, "expected the warm-up frame to be ACKed")

	// Tear the live WS conn (kill channel) AND shut down the
	// listener (Close) so reconnect attempts fail with connection-
	// refused. CloseClientConnections / Close do not force-close
	// hijacked WS conns, so the kill channel is required.
	close(srv.kill)
	srv.Close()

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 5*time.Second, 10*time.Millisecond)
	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "reconnect failed")
	// Should have made multiple attempts before giving up.
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnectAttempts(), int64(1))
}

func TestQwpSfSendLoopNilFactoryIsTerminalOnFailure(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 1})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	// Nil factory → wire failure is immediately terminal.
	loop := qwpSfNewSendLoop(engine, transport, nil,
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking(context.Background(), []byte("data"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	assert.Equal(t, int64(0), loop.sendLoopTotalReconnectAttempts())
}

// Spec §16: verifies the reconnect-status snapshot the loop exposes
// is non-empty while connectWithBackoff is iterating, so
// engineAppendBlocking can produce the diagnostic-rich
// "reconnecting: attempts=N, outage-elapsed=…" error.
func TestQwpSfSendLoopReconnectStatusSnapshot(t *testing.T) {
	// Pre-state: never reconnecting.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	// Factory that always fails so the loop stays inside
	// connectWithBackoff for the duration of the outage budget. We
	// pass a still-good initial transport so the loop runs once,
	// observes the close, and enters reconnect — which is the state
	// we want to sample.
	dialFails := atomic.Bool{}
	factory := func(ctx context.Context, idx int) (*qwpTransport, error) {
		if dialFails.Load() {
			return nil, errors.New("dial: connection refused")
		}
		return qwpSfDialFor(srv)(ctx, idx)
	}

	transport, err := factory(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, factory,
		100*time.Microsecond, 2*time.Second /* outage budget */, 10*time.Millisecond, 30*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Pre-reconnect snapshot: not reconnecting.
	reconnecting, attempts, _ := loop.sendLoopReconnectStatus()
	assert.False(t, reconnecting)
	assert.Equal(t, int64(0), attempts)

	_, err = engine.engineAppendBlocking(context.Background(), []byte("warm-up"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond)

	// Now flip the factory to fail and tear the live conn so the
	// loop is forced into connectWithBackoff with a short backoff
	// cap (30ms) — gives us many attempts inside the 2s budget.
	dialFails.Store(true)
	close(srv.kill)

	require.Eventually(t, func() bool {
		r, a, start := loop.sendLoopReconnectStatus()
		return r && a >= 1 && !start.IsZero()
	}, 1500*time.Millisecond, 5*time.Millisecond,
		"expected loop to enter reconnect with attempts ≥ 1 and a non-zero outage start")

	r, a, start := loop.sendLoopReconnectStatus()
	require.True(t, r)
	assert.GreaterOrEqual(t, a, int64(1))
	assert.WithinDuration(t, time.Now(), start, 2*time.Second)
}

func TestQwpSfConnectWithRetrySucceedsEventually(t *testing.T) {
	// Start with a port that nothing is listening on; flip to a
	// real server after a few attempts.
	var srv *qwpSfTestServer
	var startedSrv atomic.Bool
	var mu sync.Mutex
	factoryAttempts := 0
	factory := func(ctx context.Context, idx int) (*qwpTransport, error) {
		mu.Lock()
		factoryAttempts++
		myAttempt := factoryAttempts
		mu.Unlock()
		if myAttempt < 3 {
			// Closed-connection refused.
			return nil, errors.New("dial: connection refused")
		}
		if startedSrv.CompareAndSwap(false, true) {
			srv = newQwpSfTestServer(t, qwpSfTestServerOpts{})
			t.Cleanup(srv.Close)
		}
		return qwpSfDialFor(srv)(ctx, idx)
	}
	transport, _, err := qwpSfConnectWithRetry(context.Background(), factory, nil,
		2*time.Second, 5*time.Millisecond, 50*time.Millisecond, true, nil)
	require.NoError(t, err)
	require.NotNil(t, transport)
	_ = transport.close()
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, factoryAttempts, 3)
}

func TestQwpSfConnectWithRetryTerminalUpgrade(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: 401})
	defer srv.Close()

	_, _, err := qwpSfConnectWithRetry(context.Background(), qwpSfDialFor(srv), nil,
		200*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond, true, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebSocket upgrade failed")
}

func TestQwpSfConnectWithRetryBudgetExhausted(t *testing.T) {
	factory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		return nil, errors.New("dial tcp: connection refused")
	}
	_, _, err := qwpSfConnectWithRetry(context.Background(), factory, nil,
		100*time.Millisecond, 5*time.Millisecond, 30*time.Millisecond, true, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect failed")
}

func TestQwpSfIsTerminalUpgradeError(t *testing.T) {
	cases := []struct {
		err   error
		want  bool
		label string
	}{
		{errors.New("got 401 unauthorized"), true, "401"},
		{errors.New("got 403 forbidden"), true, "403"},
		{errors.New("got 426 upgrade required"), true, "426"},
		{errors.New("dial tcp: connection refused"), false, "transient"},
		{errors.New("websocket: bad handshake"), false, "transient"},
		{nil, false, "nil"},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			assert.Equal(t, c.want, qwpSfIsTerminalUpgradeError(c.err))
		})
	}
}

// TestQwpSfRecordFatalServerErrorPopulatesBothFields asserts that
// recordFatalServerError sets both lastError and lastTerminalServerError,
// so producer-side errors.As unwrap and the typed accessor return the
// same payload.
func TestQwpSfRecordFatalServerErrorPopulatesBothFields(t *testing.T) {
	l := &qwpSfSendLoop{}
	se := &SenderError{
		Category:         CategoryParseError,
		AppliedPolicy:    PolicyHalt,
		ServerStatusByte: int(QwpStatusParseError),
		ServerMessage:    "bad column",
		MessageSequence:  9,
		FromFsn:          17,
		ToFsn:            17,
		DetectedAt:       time.Now(),
	}
	l.recordFatalServerError(se)

	require.Equal(t, se, l.sendLoopLastTerminalServerError())

	gotErr := l.sendLoopCheckError()
	require.Error(t, gotErr)
	var unwrapped *SenderError
	require.True(t, errors.As(gotErr, &unwrapped))
	require.Equal(t, se, unwrapped)
}

// TestQwpSfRecordFatalServerErrorIdempotent asserts that a second
// recordFatalServerError call does not overwrite the first — only the
// first failure wins, matching recordFatal's CAS semantics.
func TestQwpSfRecordFatalServerErrorIdempotent(t *testing.T) {
	l := &qwpSfSendLoop{}
	first := &SenderError{Category: CategoryWriteError, AppliedPolicy: PolicyHalt}
	second := &SenderError{Category: CategorySchemaMismatch, AppliedPolicy: PolicyHalt}
	l.recordFatalServerError(first)
	l.recordFatalServerError(second)
	require.Equal(t, first, l.sendLoopLastTerminalServerError())
}

// TestQwpSfRecordFatalServerErrorNilSafe asserts that passing nil is
// a no-op rather than a panic.
func TestQwpSfRecordFatalServerErrorNilSafe(t *testing.T) {
	l := &qwpSfSendLoop{}
	l.recordFatalServerError(nil)
	require.Nil(t, l.sendLoopLastTerminalServerError())
	require.Nil(t, l.sendLoopCheckError())
}

// TestQwpSfSendLoopDropAndContinue verifies that a Drop-category
// rejection (SchemaMismatch) advances ackedFsn past the rejected
// frame instead of latching as terminal. The dispatcher receives the
// notification; sendLoopCheckError returns nil; subsequent frames
// continue draining.
func TestQwpSfSendLoopDropAndContinue(t *testing.T) {
	// Run over both engine backings so disk-backed DROP-and-advance —
	// otherwise exercised only by the jar-gated fuzz workflow — is
	// covered here too. "" selects a memory-backed engine; a TempDir
	// selects disk-backed segments under that slot directory.
	t.Run("memory", func(t *testing.T) { testQwpSfSendLoopDropAndContinue(t, "") })
	t.Run("disk", func(t *testing.T) { testQwpSfSendLoopDropAndContinue(t, t.TempDir()) })
}

func testQwpSfSendLoopDropAndContinue(t *testing.T, sfDir string) {
	// rejectStatus=SchemaMismatch (default Drop) for the very first
	// frame only; subsequent frames get OK ACKs. We need the test
	// server to support that mode — see opts.rejectFirstNFrames below.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch,
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine(sfDir, 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)

	// Capture dispatched errors to assert they fired.
	var dispatched atomic.Int64
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		if e.Category == CategorySchemaMismatch && e.AppliedPolicy == PolicyDropAndContinue {
			dispatched.Add(1)
		}
	}, 8)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// First frame is rejected → dropped. Frames 1 and 2 (0-indexed) are OK.
	for i := 0; i < 3; i++ {
		_, err := engine.engineAppendBlocking(context.Background(),
			[]byte(fmt.Sprintf("f%d", i)))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 2
	}, 5*time.Second, 1*time.Millisecond, "ackedFsn did not advance past Drop")

	// No terminal error; reconnect did not trigger.
	require.NoError(t, loop.sendLoopCheckError())
	require.Equal(t, int64(0), loop.sendLoopTotalReconnects())
	// Dispatcher saw exactly one Drop-category SenderError.
	require.GreaterOrEqual(t, dispatched.Load(), int64(1))
	// Counter bumped on the Drop path.
	require.GreaterOrEqual(t, loop.sendLoopTotalServerErrors(), int64(1))
}

// TestQwpSfSendLoopReceiverClampsForgedAckToFullySent is the
// lying-ACK regression guard. A non-compliant server ACKs a wire
// sequence whose sendMessage has not yet returned (an early or forged
// ACK for an in-flight frame). The receiver must clamp the watermark
// advance to highestFullySent — the last frame fully on the wire — so
// ackedFsn never covers a frame the send goroutine is still reading
// out of the mmap'd segment (a trim would munmap it mid-read: SIGSEGV)
// nor a frame that never went out (silent loss). nextWireSeq is one
// frame too permissive for this ceiling because it is bumped before
// the wire write.
//
// Layout for both cases: 4 frames published (FSN 0..3). The send
// goroutine has STARTED all four (nextWireSeq=4) but only frames 0..2
// have FINISHED sending (highestFullySent=2); FSN 3 is mid-sendMessage.
// The server forges an ACK naming wire sequence 3. The clamp must hold
// ackedFsn at FSN 2, never FSN 3. With the clamp keyed off
// nextWireSeq-1 (=3) instead of highestFullySent (=2) the watermark
// jumps to FSN 3 and the test fails.
func TestQwpSfSendLoopReceiverClampsForgedAckToFullySent(t *testing.T) {
	const (
		published    = 4 // FSN 0..3 live in the engine
		fsnAtZero    = 0 // fresh connection: wireSeq 0 maps to FSN 0
		started      = 4 // nextWireSeq: wireSeq 0..3 all begun
		fullySent    = 2 // highestFullySent: FSN 0..2 on the wire
		forgedSeq    = 3 // server ACKs the in-flight FSN 3
		wantAckedFsn = 2 // clamp ceiling, NOT forgedSeq (3)
	)

	// run drives receiverLoop in isolation against a server that
	// greets the connection with forgedAck. The producer/sender
	// goroutines never run, so the hand-pinned wire state (notably
	// highestFullySent) stays put — FSN 3 stuck mid-sendMessage — while
	// the receiver processes the single forged ACK. Returns the
	// resulting ackedFsn.
	run := func(t *testing.T, forgedAck []byte) int64 {
		t.Helper()
		srv := newQwpSfTestServer(t, qwpSfTestServerOpts{forgedAckAtConnect: forgedAck})
		defer srv.Close()

		engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
		require.NoError(t, err)
		defer func() { _ = engine.engineClose() }()
		for i := 0; i < published; i++ {
			_, err := engine.engineAppendBlocking(context.Background(),
				[]byte(fmt.Sprintf("f%d", i)))
			require.NoError(t, err)
		}
		require.Equal(t, int64(published-1), engine.enginePublishedFsn())
		require.Equal(t, int64(-1), engine.engineAckedFsn())

		transport, err := qwpSfDialFor(srv)(context.Background(), 0)
		require.NoError(t, err)

		loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
			100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
		// Quiet, non-blocking error sink for the drop-and-continue case.
		loop.sendLoopSetErrorHandler(func(*SenderError) {}, 8)

		// Pin wire state as if the send goroutine had begun all four
		// frames but only frames 0..2 finished sending.
		loop.fsnAtZero.Store(fsnAtZero)
		loop.nextWireSeq.Store(started)
		loop.highestFullySent.Store(fullySent)
		loop.running.Store(true)

		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = loop.receiverLoop(loop.ctx)
		}()

		require.Eventually(t, func() bool {
			return engine.engineAckedFsn() != -1
		}, 2*time.Second, time.Millisecond, "receiver never processed the forged ACK")
		got := engine.engineAckedFsn()

		_ = loop.sendLoopClose() // running=false, cancel ctx, close transport + dispatcher
		<-done
		return got
	}

	t.Run("OK ACK", func(t *testing.T) {
		got := run(t, buildAckOK(forgedSeq))
		assert.Equal(t, int64(wantAckedFsn), got,
			"OK-path clamp must hold the watermark at the last fully-sent "+
				"frame (FSN 2); FSN 3 is still mid-sendMessage")
	})

	t.Run("error ACK (drop-and-continue)", func(t *testing.T) {
		// SchemaMismatch resolves to DropAndContinue by default, so the
		// rejection path advances ackedFsn via engineAcknowledge(fsn) —
		// exercising the second clamp site.
		got := run(t, buildAckError(QwpStatusSchemaMismatch, forgedSeq, "forged"))
		assert.Equal(t, int64(wantAckedFsn), got,
			"rejection-path clamp must hold the watermark at the last "+
				"fully-sent frame (FSN 2); FSN 3 is still mid-sendMessage")
	})
}
