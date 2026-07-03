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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closeFrameTestServer accepts the WS upgrade, reads one frame, then
// closes the connection with the configured terminal close code.
func closeFrameTestServer(t *testing.T, code websocket.StatusCode, reason string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_, _, _ = conn.Read(context.Background())
		_ = conn.Close(code, reason)
	}))
}

// closeAfterNFramesServer accepts the WS upgrade, reads exactly n
// frames (never ACKing any), then closes with the given terminal
// code. Consuming every frame the producer sends before closing
// keeps senderLoop from producing a write error that would race the
// receiver's close-frame error in runOneConnection's first-error
// aggregation — so the resulting terminal SenderError is always the
// close-code one, with a deterministic [ackedFsn+1, publishedFsn]
// FSN span.
func closeAfterNFramesServer(t *testing.T, n int, code websocket.StatusCode, reason string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		for i := 0; i < n; i++ {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
		_ = conn.Close(code, reason)
	}))
}

// TestQwpSfCloseCodeRepeatPoisonEscalates drives the send loop against
// a server that reads the head frame then closes with a nominally
// protocol-violating code — on every connection. WS close codes carry
// no policy semantics under NACK policy v2: each close is a transport
// event that reconnects and replays, counting a poison strike at the
// unmoved head FSN; at qwpSfDefaultMaxFrameRejections consecutive
// strikes the loop latches the typed poisoned-frame terminal.
func TestQwpSfCloseCodeRepeatPoisonEscalates(t *testing.T) {
	codes := []struct {
		code   websocket.StatusCode
		reason string
	}{
		{websocket.StatusProtocolError, "bad framing"},
		{websocket.StatusUnsupportedData, "frame type unsupported"},
		{websocket.StatusInvalidFramePayloadData, "bad payload"},
		{websocket.StatusPolicyViolation, "policy reject"},
		{websocket.StatusMessageTooBig, "frame oversized"},
		{websocket.StatusMandatoryExtension, "extension required"},
	}
	for _, c := range codes {
		t.Run(c.code.String(), func(t *testing.T) {
			httpSrv := closeFrameTestServer(t, c.code, c.reason)
			defer httpSrv.Close()

			engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err)
			defer func() { _ = engine.engineClose() }()

			factory := qwpSfDialAt(httpSrv.URL)
			transport, err := factory(context.Background(), 0)
			require.NoError(t, err)

			// 1ms reconnectMaxDuration: the poison-episode floor stays
			// below the paced inter-strike backoff, so escalation lands
			// exactly at the strike threshold.
			loop := qwpSfNewSendLoop(engine, transport, factory,
				100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
			loop.sendLoopStart()
			defer func() { _ = loop.sendLoopClose() }()

			_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return loop.sendLoopCheckError() != nil
			}, 5*time.Second, 1*time.Millisecond,
				"loop did not escalate the repeated close for code %d", c.code)

			gotErr := loop.sendLoopCheckError()
			var senderErr *SenderError
			require.True(t, errors.As(gotErr, &senderErr),
				"expected *SenderError, got %T: %v", gotErr, gotErr)
			assert.Equal(t, CategoryProtocolViolation, senderErr.Category)
			assert.Equal(t, PolicyTerminal, senderErr.AppliedPolicy)
			assert.Equal(t, NoStatusByte, senderErr.ServerStatusByte)
			assert.Contains(t, senderErr.ServerMessage, "poisoned frame")

			// Below the threshold every close reconnected + replayed; the
			// watermark never moved and nothing was dropped.
			assert.Equal(t, int64(qwpSfDefaultMaxFrameRejections-1),
				loop.sendLoopTotalReconnects(),
				"one reconnect per strike below the threshold")
			assert.Equal(t, int64(-1), engine.engineAckedFsn())
		})
	}
}

// TestQwpSfPoisonTerminalMultiFrameFsnSpan pins the non-degenerate
// SenderError FSN span on the poisoned-frame terminal. Every other
// terminal-path test publishes a single unacked frame, so
// FromFsn == ToFsn and the span is never actually exercised. Here
// several frames are published and none are ACKed when the poison
// detector escalates, so buildPoisonedFrameSE must report
// [FromFsn, ToFsn] = [ackedFsn+1, publishedFsn] with FromFsn strictly
// < ToFsn — the multi-frame correlation window that dead-lettering and
// AwaitAckedFsn callers rely on.
func TestQwpSfPoisonTerminalMultiFrameFsnSpan(t *testing.T) {
	const nFrames = 4
	httpSrv := closeAfterNFramesServer(t, nFrames,
		websocket.StatusProtocolError, "bad framing")
	defer httpSrv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	// Publish every frame BEFORE the loop starts: publishedFsn is then
	// a stable nFrames-1 by the time the poison SE is built, and the
	// server reads exactly the nFrames each connection replays.
	for i := 0; i < nFrames; i++ {
		_, err := engine.engineAppendBlocking(context.Background(), []byte{byte(i)})
		require.NoError(t, err)
	}
	require.Equal(t, int64(nFrames-1), engine.enginePublishedFsn())
	require.Equal(t, int64(-1), engine.engineAckedFsn(),
		"precondition: nothing ACKed, so FromFsn must come out as 0")

	factory := qwpSfDialAt(httpSrv.URL)
	transport, err := factory(context.Background(), 0)
	require.NoError(t, err)

	// 1ms reconnectMaxDuration: episode floor below the paced
	// inter-strike backoff, so escalation lands at the threshold.
	loop := qwpSfNewSendLoop(engine, transport, factory,
		100*time.Microsecond, time.Millisecond, time.Millisecond, 10*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 5*time.Second, 1*time.Millisecond,
		"loop did not escalate the repeated close")

	var se *SenderError
	require.True(t, errors.As(loop.sendLoopCheckError(), &se),
		"expected *SenderError, got %v", loop.sendLoopCheckError())
	assert.Equal(t, CategoryProtocolViolation, se.Category)
	assert.Equal(t, PolicyTerminal, se.AppliedPolicy)
	assert.Contains(t, se.ServerMessage, "poisoned frame")
	// The point of the test: a real multi-frame span.
	assert.Equal(t, int64(0), se.FromFsn,
		"FromFsn = ackedFsn+1 = 0 (nothing ACKed)")
	assert.Equal(t, int64(nFrames-1), se.ToFsn,
		"ToFsn = publishedFsn = nFrames-1")
	assert.Less(t, se.FromFsn, se.ToFsn,
		"multi-frame span: FromFsn must be strictly < ToFsn (not the "+
			"degenerate single-frame FromFsn == ToFsn case)")
}

// Non-terminal close-code reconnect is already covered by
// TestQwpSfSendLoopReconnectAfterServerClose at qwp_sf_send_loop_test.go;
// no need to duplicate here. The point of this file is the new
// terminal-close-code path.

// runUpgradeFailureScenario drives the send loop against an
// initially-working server that ACKs frame 1 and drops on frame 2,
// with reconnect pointing at a server that rejects the upgrade with
// the given HTTP status. Returns the latched terminal SenderError.
func runUpgradeFailureScenario(t *testing.T, upgradeStatus int) *SenderError {
	t.Helper()
	failSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: upgradeStatus})
	t.Cleanup(failSrv.Close)

	// Data server ACKs the first frame and closes on the second:
	// frame 1 advances totalAcks, so the silent-drop guard (which
	// is gated on totalAcks == 0) won't fire when the connection
	// breaks.
	dataSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 2})
	t.Cleanup(dataSrv.Close)

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.engineClose() })

	transport, err := qwpSfDialFor(dataSrv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialFor(failSrv),
		100*time.Microsecond, 200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
	loop.sendLoopStart()
	t.Cleanup(func() { _ = loop.sendLoopClose() })

	for i := 0; i < 2; i++ {
		_, err := engine.engineAppendBlocking(context.Background(), []byte{byte(i)})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return loop.sendLoopLastTerminalServerError() != nil
	}, 3*time.Second, 1*time.Millisecond,
		"loop did not record terminal SenderError for upgrade %d", upgradeStatus)

	se := loop.sendLoopLastTerminalServerError()
	require.NotNil(t, se)
	return se
}

// TestQwpSfAuthFailureProducesSecurityError: 401 (auth) →
// CategorySecurityError.
func TestQwpSfAuthFailureProducesSecurityError(t *testing.T) {
	se := runUpgradeFailureScenario(t, 401)
	assert.Equal(t, CategorySecurityError, se.Category)
	assert.Equal(t, PolicyTerminal, se.AppliedPolicy)
	assert.Equal(t, NoStatusByte, se.ServerStatusByte)
	assert.True(t, strings.Contains(se.ServerMessage, "ws-upgrade-failed"),
		"expected ws-upgrade-failed in message, got %q", se.ServerMessage)
}

// TestQwpSfProtocolUpgradeFailureProducesProtocolViolation: 426
// (Upgrade Required) → CategoryProtocolViolation, not SecurityError.
func TestQwpSfProtocolUpgradeFailureProducesProtocolViolation(t *testing.T) {
	se := runUpgradeFailureScenario(t, 426)
	assert.Equal(t, CategoryProtocolViolation, se.Category)
	assert.Equal(t, PolicyTerminal, se.AppliedPolicy)
}
