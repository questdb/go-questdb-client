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
			// protocol — reconnects would just hammer it.
			if opts.silentDropAfterFrames > 0 &&
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

func TestQwpSfSendLoopReconnectAfterServerClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 5})
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

	for i := 0; i < 10; i++ {
		_, err := engine.engineAppendBlocking(context.Background(), []byte(fmt.Sprintf("f-%d", i)))
		require.NoError(t, err)
	}
	// All 10 frames should eventually be ACKed despite the server
	// dropping the connection after 5. (It will accept them again on
	// the new connection; with the current test server semantics,
	// reconnect doesn't truncate.) Actually closeAfterFrames is a
	// global counter — after the close, the next connect will
	// receive frames 6..10 cleanly.
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 9
	}, 5*time.Second, 1*time.Millisecond, "loop did not drain after reconnect")
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1))
	// fsnAtZero should have advanced past 0 after the swap.
	assert.Greater(t, loop.sendLoopFsnAtZero(), int64(0))
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
// Why the scenario has teeth: closeAfterFrames:5 over 10 appends
// means the server reads f-0..f-4 on conn 1 and never sees f-5..f-9
// on conn 1 at all. The ONLY path by which f-5..f-9 ever reach the
// server is the post-reconnect replay, so a cursor-repositioning bug
// that skips any of them is permanent loss that neither the global
// frame counter nor an `ackedFsn >= 9` liveness check can detect
// (both are driven off the same client-side FSN math the bug would
// have corrupted). The contiguity+anchor assertion additionally
// catches a skip of f-0..f-4 (those f-4-class frames the server DID
// see pre-drop, so the union alone would mask their loss).
func TestQwpSfSendLoopReplayIsGapFree(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		closeAfterFrames: 5,
		recordFrames:     true,
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

	const n = 10
	for i := 0; i < n; i++ {
		_, err := engine.engineAppendBlocking(
			context.Background(), []byte(fmt.Sprintf("f-%d", i)))
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= int64(n-1)
	}, 5*time.Second, 1*time.Millisecond,
		"loop did not drain every frame after reconnect")
	require.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"the mid-flush drop must have forced at least one reconnect")

	frames := srv.recordedFrames()
	require.Len(t, frames, 2,
		"expected exactly two connections (one drop -> one reconnect)")
	conn1, conn2 := frames[1], frames[2]

	// conn 1: the server reads exactly the first five frames, in
	// order, then drops. This is independent of how many ACKs it
	// managed to write before dropping, so this part is race-free.
	require.Equal(t, []string{"f-0", "f-1", "f-2", "f-3", "f-4"}, conn1,
		"conn 1 must receive exactly the first 5 frames before the drop")

	// conn 2: the replayed run. Its start depends on how many of
	// conn 1's ACKs the receiver had processed before the drop
	// surfaced — a benign race: fsnAtZero = engineAckedFsn()+1 at
	// swap time, somewhere in [0,4]. Whatever that anchor is, the
	// replay MUST begin exactly there, be strictly contiguous (no
	// gap, no reorder), and run through the final frame. fsnAtZero
	// and the replayed bytes derive from the same ackedFsn snapshot,
	// so this assertion is race-robust and is precisely the
	// wire<->messageSequence alignment server-side dedup keys on.
	require.NotEmpty(t, conn2, "reconnect must have replayed frames")
	fsnAtZero := loop.sendLoopFsnAtZero()
	require.GreaterOrEqual(t, fsnAtZero, int64(0))
	require.LessOrEqual(t, fsnAtZero, int64(4))
	for i, got := range conn2 {
		want := fmt.Sprintf("f-%d", fsnAtZero+int64(i))
		require.Equalf(t, want, got,
			"replayed frame %d not contiguous from the fsnAtZero anchor "+
				"(gap, reorder, or misaligned messageSequence)", i)
	}
	require.Equalf(t, fmt.Sprintf("f-%d", n-1), conn2[len(conn2)-1],
		"replay must run through the final frame f-%d", n-1)

	// THE data-loss guard: every appended row reached the server at
	// least once across the two connections. f-5..f-9 were never seen
	// on conn 1, so only a correct replay puts them in this set.
	seen := make(map[string]bool, n)
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
	require.Greaterf(t, srv.totalFramesReceived.Load(), int64(n),
		"replay must re-send >=1 already-received frame (the dup the "+
			"server dedups); got only %d total for %d rows",
		srv.totalFramesReceived.Load(), n)
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

// TestQwpSfSendLoopSilentDropAfterFrameIsTerminal verifies that when
// the server accepts the WS upgrade but silently disconnects after
// the first frame (without sending any ACK), the send loop classifies
// it as a server version/config mismatch and fails fast instead of
// entering a hot reconnect loop. Without this guard, every dial
// succeeds and the receiver reset its backoff on each attempt — burning
// thousands of ephemeral ports per second until reconnectMaxDuration
// (5 minutes default) expired.
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
	// of reconnects. Cap at a small number — the loop should give up
	// after the very first connection that fails the heuristic.
	assert.LessOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"expected at most one reconnect before terminal classification")
	assert.LessOrEqual(t, srv.connCount.Load(), int64(2),
		"server should have seen at most 2 connections")
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
		2*time.Second, 5*time.Millisecond, 50*time.Millisecond)
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
		200*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebSocket upgrade failed")
}

func TestQwpSfConnectWithRetryBudgetExhausted(t *testing.T) {
	factory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		return nil, errors.New("dial tcp: connection refused")
	}
	_, _, err := qwpSfConnectWithRetry(context.Background(), factory, nil,
		100*time.Millisecond, 5*time.Millisecond, 30*time.Millisecond)
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
	// rejectStatus=SchemaMismatch (default Drop) for the very first
	// frame only; subsequent frames get OK ACKs. We need the test
	// server to support that mode — see opts.rejectFirstNFrames below.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:        QwpStatusSchemaMismatch,
		rejectFirstNFrames:  1,
	})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
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
