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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQwpSenderLastTerminalErrorAndCounters drives a HALT-policy
// rejection and asserts:
//   - LastTerminalError returns the typed payload
//   - TotalServerErrors is 1
//   - errors.As on Flush unwraps the same SenderError
//   - FlushAndGetSequence returns the expected published FSN before
//     the rejection
func TestQwpSenderLastTerminalErrorAndCounters(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// First Flush enqueues a row; the receiver classifies the rejection.
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_, _ = s.FlushAndGetSequence(context.Background())

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	se := s.LastTerminalError()
	require.NotNil(t, se, "LastTerminalError should be non-nil after halt")
	assert.Equal(t, CategoryParseError, se.Category)
	assert.Equal(t, PolicyTerminal, se.AppliedPolicy)
	assert.Equal(t, int(QwpStatusParseError), se.ServerStatusByte)
	assert.GreaterOrEqual(t, s.TotalServerErrors(), int64(1))

	// The next producer call (AtNow, after Table() polls the terminal
	// latch) returns the typed *SenderError unwrappable via errors.As.
	err := s.Table("t").Int64Column("v", 2).AtNow(context.Background())
	require.Error(t, err)
	var unwrapped *SenderError
	require.True(t, errors.As(err, &unwrapped),
		"expected *SenderError, got %T: %v", err, err)
	assert.Equal(t, CategoryParseError, unwrapped.Category)
	assert.Contains(t, unwrapped.ServerMessage, "rejected")
}

// TestQwpSenderFlushAndGetSequenceHappyPath asserts the returned FSN
// monotonically increases across successful flushes.
func TestQwpSenderFlushAndGetSequenceHappyPath(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	fsn1, err := s.FlushAndGetSequence(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, fsn1, int64(0))

	require.NoError(t, s.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	fsn2, err := s.FlushAndGetSequence(context.Background())
	require.NoError(t, err)
	assert.Greater(t, fsn2, fsn1, "FSN should advance across flushes")

	// Empty FlushAndGetSequence returns the current published FSN
	// without error.
	fsn3, err := s.FlushAndGetSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, fsn2, fsn3, "empty FlushAndGetSequence should not advance FSN")
}

// TestQwpSenderHandlerInvokedOnRetriable wires a custom error handler
// via the loop setter, drives a retriable rejection, and asserts the
// handler observes the informational SenderError while
// LastTerminalError stays nil (a retriable NACK recycles the
// connection; it never latches).
func TestQwpSenderHandlerInvokedOnRetriable(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusWriteError, // retriable
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// We need a handler to capture deliveries; default loud handler
	// just logs. Inject via loop setter (legitimate during test
	// since the sender is built but not yet receiving frames).
	gotCh := make(chan *SenderError, 4)
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		select {
		case gotCh <- e:
		default:
		}
	}, 16)

	// Need a fresh batch to actually send.
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))
	require.NoError(t, s.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))

	select {
	case se := <-gotCh:
		assert.Equal(t, CategoryWriteError, se.Category)
		assert.Equal(t, PolicyRetriable, se.AppliedPolicy)
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked within deadline")
	}
	// A retriable NACK does NOT latch terminal.
	assert.Nil(t, s.LastTerminalError())
	assert.GreaterOrEqual(t, s.TotalServerErrors(), int64(1))
	assert.GreaterOrEqual(t, s.TotalErrorNotificationsDelivered(), int64(1))
}

// TestQwpSenderInboxOverflowBumpsCounter asserts that flooding a slow
// handler bumps DroppedErrorNotifications without stalling the I/O
// path. Pre-send retriable rejections give an unbounded notification
// stream (one per recycle, no poison strikes).
func TestQwpSenderInboxOverflowBumpsCounter(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		unsolicitedRejectAtConnect: QwpStatusWriteError,
	})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	release := make(chan struct{})
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		<-release
	}, qwpSfMinErrorInboxCapacity)
	defer close(release)

	require.Eventually(t, func() bool {
		return s.DroppedErrorNotifications() > 0
	}, 10*time.Second, 10*time.Millisecond,
		"DroppedErrorNotifications never increased: dropped=%d delivered=%d",
		s.DroppedErrorNotifications(), s.TotalErrorNotificationsDelivered())
}

// TestQwpSenderLastTerminalErrorMessageContainsServerMessage drives
// rejection with an explicit message and asserts the message survives
// to the SenderError's ServerMessage field.
func TestQwpSenderLastTerminalErrorMessageContainsServerMessage(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusInternalError})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = s.Flush(context.Background())

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	se := s.LastTerminalError()
	require.NotNil(t, se)
	assert.True(t, strings.Contains(se.ServerMessage, "rejected"),
		"expected 'rejected' in ServerMessage, got %q", se.ServerMessage)
}

// TestDeprecatedQwpErrorBridge pins the v4.2.0 compatibility shim: the
// historical errors.As(err, &qwpErr) pattern must keep working against
// a *SenderError, with the documented field mapping, while the new
// errors.As(err, &se) path is left intact.
func TestDeprecatedQwpErrorBridge(t *testing.T) {
	var err error = &SenderError{
		Category:         CategorySchemaMismatch,
		ServerStatusByte: int(QwpStatusSchemaMismatch),
		ServerMessage:    "column type mismatch",
		MessageSequence:  42,
		FromFsn:          10,
		ToFsn:            12,
	}

	// Adding (*SenderError).As must not shadow the direct unwrap.
	var se *SenderError
	require.True(t, errors.As(err, &se))
	assert.Equal(t, CategorySchemaMismatch, se.Category)

	// Historical pattern keeps compiling and is populated.
	var qwpErr *QwpError
	require.True(t, errors.As(err, &qwpErr))
	assert.Equal(t, QwpStatusSchemaMismatch, qwpErr.Status)
	assert.Equal(t, int64(42), qwpErr.Sequence)
	assert.Equal(t, "column type mismatch", qwpErr.Message)
	assert.Equal(t,
		"qwp: server error SCHEMA_MISMATCH (0x03): column type mismatch",
		qwpErr.Error())

	// Protocol violations carried no status byte in v4.2.0; the shim
	// reports the zero (OK) byte rather than the -1 sentinel.
	err = &SenderError{
		Category:         CategoryProtocolViolation,
		ServerStatusByte: NoStatusByte,
		MessageSequence:  NoMessageSequence,
		ServerMessage:    "policy violation",
	}
	qwpErr = nil
	require.True(t, errors.As(err, &qwpErr))
	assert.Equal(t, QwpStatusCode(0), qwpErr.Status)
	assert.Equal(t, "policy violation", qwpErr.Message)
}
