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

// TestQwpSfTerminalCloseCodeProducesProtocolViolation drives the send
// loop against a server that closes with each terminal code; asserts
// the loop produces a CategoryProtocolViolation+Halt SenderError and
// does not enter reconnect.
func TestQwpSfTerminalCloseCodeProducesProtocolViolation(t *testing.T) {
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

			loop := qwpSfNewSendLoop(engine, transport, factory,
				100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
			loop.sendLoopStart()
			defer func() { _ = loop.sendLoopClose() }()

			_, err = engine.engineAppendBlocking(context.Background(), []byte("frame"))
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return loop.sendLoopCheckError() != nil
			}, 3*time.Second, 1*time.Millisecond,
				"loop did not record terminal error for close code %d", c.code)

			gotErr := loop.sendLoopCheckError()
			var senderErr *SenderError
			require.True(t, errors.As(gotErr, &senderErr),
				"expected *SenderError, got %T: %v", gotErr, gotErr)
			assert.Equal(t, CategoryProtocolViolation, senderErr.Category)
			assert.Equal(t, PolicyHalt, senderErr.AppliedPolicy)
			assert.Equal(t, NoStatusByte, senderErr.ServerStatusByte)
			assert.Contains(t, senderErr.ServerMessage, "ws-close[")

			// The loop did not enter reconnect — the close code is
			// terminal. Reconnect counter stays at zero.
			assert.Equal(t, int64(0), loop.sendLoopTotalReconnects())
		})
	}
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
	// frame 1 advances acksRecvOnConn, so the silent-drop guard
	// won't fire when the connection breaks.
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
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
	assert.Equal(t, NoStatusByte, se.ServerStatusByte)
	assert.True(t, strings.Contains(se.ServerMessage, "ws-upgrade-failed"),
		"expected ws-upgrade-failed in message, got %q", se.ServerMessage)
}

// TestQwpSfProtocolUpgradeFailureProducesProtocolViolation: 426
// (Upgrade Required) → CategoryProtocolViolation, not SecurityError.
func TestQwpSfProtocolUpgradeFailureProducesProtocolViolation(t *testing.T) {
	se := runUpgradeFailureScenario(t, 426)
	assert.Equal(t, CategoryProtocolViolation, se.Category)
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
}
