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
	rejectStatus qwpStatusCode
	// upgradeStatus, when non-zero, causes the server to respond
	// with that HTTP status code on the WebSocket upgrade request,
	// rejecting the connection. Used to exercise auth-terminal.
	upgradeStatus int
}

// qwpSfTestServer is a fake QWP server for send-loop tests. It
// counts received frames across all connections (so tests can
// observe replays after reconnect).
type qwpSfTestServer struct {
	*httptest.Server
	totalFramesReceived atomic.Int64
	connCount           atomic.Int64
}

func newQwpSfTestServer(t *testing.T, opts qwpSfTestServerOpts) *qwpSfTestServer {
	t.Helper()
	s := &qwpSfTestServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		myConnID := s.connCount.Add(1)
		var localSeq int64
		var localFramesReceived int
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			s.totalFramesReceived.Add(1)
			localFramesReceived++
			// closeAfterFrames triggers ONLY on the first connection:
			// we accept N frames and then drop. Subsequent reconnects
			// behave normally so the loop can drain.
			if opts.closeAfterFrames > 0 &&
				myConnID == 1 &&
				localFramesReceived >= opts.closeAfterFrames {
				return
			}
			if opts.rejectStatus != 0 {
				_ = conn.Write(context.Background(), websocket.MessageBinary,
					buildAckError(opts.rejectStatus, localSeq, "rejected"))
				localSeq++
				continue
			}
			_ = conn.Write(context.Background(), websocket.MessageBinary,
				buildAckOK(localSeq))
			localSeq++
		}
	}))
	return s
}

// qwpSfDialFor builds a transport connected to the given
// httptest server. Used as the qwpSfReconnectFactory for tests.
func qwpSfDialFor(server *qwpSfTestServer) qwpSfReconnectFactory {
	return func(ctx context.Context) (*qwpTransport, error) {
		var t qwpTransport
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		if err := t.connect(ctx, wsURL, qwpTransportOpts{}); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

// qwpSfDialAt builds a transport connected to a fixed httptest URL.
func qwpSfDialAt(url string) qwpSfReconnectFactory {
	return func(ctx context.Context) (*qwpTransport, error) {
		var t qwpTransport
		wsURL := "ws" + strings.TrimPrefix(url, "http")
		if err := t.connect(ctx, wsURL, qwpTransportOpts{}); err != nil {
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

	transport, err := qwpSfDialFor(srv)(context.Background())
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Append 10 frames.
	for i := 0; i < 10; i++ {
		_, err := engine.engineAppendBlocking([]byte(fmt.Sprintf("frame-%d", i)))
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

func TestQwpSfSendLoopReconnectAfterServerClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 5})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background())
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	for i := 0; i < 10; i++ {
		_, err := engine.engineAppendBlocking([]byte(fmt.Sprintf("f-%d", i)))
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

func TestQwpSfSendLoopServerErrorIsTerminal(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: qwpStatusSchemaMismatch})
	defer srv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background())
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking([]byte("bad"))
	require.NoError(t, err)

	// Loop must record a terminal error rather than entering reconnect.
	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	var qErr *QwpError
	assert.True(t, errors.As(gotErr, &qErr) || strings.Contains(gotErr.Error(), "rejected"))
	// reconnects should be 0 — terminal status doesn't trigger
	// reconnect (server isn't going to change its mind on retry).
	assert.Equal(t, int64(0), loop.sendLoopTotalReconnects())
}

func TestQwpSfSendLoopUpgradeAuthFailureIsTerminal(t *testing.T) {
	// First server: dies after the initial connect, but reconnect
	// goes to a *different* server that rejects with 401 — we want
	// to verify the rejection is detected as terminal.
	authSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: 401})
	defer authSrv.Close()
	dataSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 1})
	defer dataSrv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(dataSrv)(context.Background())
	require.NoError(t, err)

	// Reconnect factory dials the auth-rejecting server.
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialAt(authSrv.URL),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking([]byte("hi"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	gotErr := loop.sendLoopCheckError()
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "terminal upgrade error")
	assert.Contains(t, gotErr.Error(), "401")
}

func TestQwpSfSendLoopReconnectBudgetExhausted(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 1})

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(srv)(context.Background())
	require.NoError(t, err)

	// Take the server down after grabbing the initial transport;
	// the reconnect factory will hit "connection refused" until
	// the per-outage cap fires.
	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(srv),
		100*time.Microsecond, 200*time.Millisecond /* short cap */, 10*time.Millisecond, 50*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking([]byte("data"))
	require.NoError(t, err)

	// Send the frame, server closes, reconnect tries (server is
	// alive but only accepts 1 frame each connection — so the
	// reconnect succeeds quickly... we need to take the server
	// down).
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

	transport, err := qwpSfDialFor(srv)(context.Background())
	require.NoError(t, err)

	// Nil factory → wire failure is immediately terminal.
	loop := qwpSfNewSendLoop(engine, transport, nil,
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	_, err = engine.engineAppendBlocking([]byte("data"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	assert.Equal(t, int64(0), loop.sendLoopTotalReconnectAttempts())
}

func TestQwpSfConnectWithRetrySucceedsEventually(t *testing.T) {
	// Start with a port that nothing is listening on; flip to a
	// real server after a few attempts.
	var srv *qwpSfTestServer
	var startedSrv atomic.Bool
	var mu sync.Mutex
	factoryAttempts := 0
	factory := func(ctx context.Context) (*qwpTransport, error) {
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
		return qwpSfDialFor(srv)(ctx)
	}
	transport, err := qwpSfConnectWithRetry(context.Background(), factory,
		2*time.Second, 5*time.Millisecond, 50*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, transport)
	_ = transport.close(context.Background())
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, factoryAttempts, 3)
}

func TestQwpSfConnectWithRetryTerminalUpgrade(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: 401})
	defer srv.Close()

	_, err := qwpSfConnectWithRetry(context.Background(), qwpSfDialFor(srv),
		200*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WebSocket upgrade failed")
}

func TestQwpSfConnectWithRetryBudgetExhausted(t *testing.T) {
	factory := func(ctx context.Context) (*qwpTransport, error) {
		return nil, errors.New("dial tcp: connection refused")
	}
	_, err := qwpSfConnectWithRetry(context.Background(), factory,
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
