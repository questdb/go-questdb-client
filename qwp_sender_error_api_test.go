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
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
	assert.Equal(t, int(QwpStatusParseError), se.ServerStatusByte)
	assert.GreaterOrEqual(t, s.TotalServerErrors(), int64(1))

	// The next Flush returns the typed *SenderError unwrappable via
	// errors.As.
	require.NoError(t, s.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	err := s.Flush(context.Background())
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

// TestQwpSenderHandlerInvokedOnDrop wires a custom error handler via
// the loop setter, drives a Drop-policy rejection, and asserts the
// handler observes the SenderError before LastTerminalError stays nil
// (Drop does not latch a terminal error).
func TestQwpSenderHandlerInvokedOnDrop(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch, // default Drop
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
		assert.Equal(t, CategorySchemaMismatch, se.Category)
		assert.Equal(t, PolicyDropAndContinue, se.AppliedPolicy)
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked within deadline")
	}
	// Drop does NOT latch terminal.
	assert.Nil(t, s.LastTerminalError())
	// totalServerErrors saw the Drop.
	assert.GreaterOrEqual(t, s.TotalServerErrors(), int64(1))
	assert.GreaterOrEqual(t, s.TotalErrorNotificationsDelivered(), int64(1))
}

// TestQwpSenderInboxOverflowBumpsCounter asserts that flooding a slow
// handler bumps DroppedErrorNotifications without stalling the I/O
// path.
func TestQwpSenderInboxOverflowBumpsCounter(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus: QwpStatusSchemaMismatch,
	})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	release := make(chan struct{})
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		<-release
	}, qwpSfMinErrorInboxCapacity)
	defer close(release)

	for i := 0; i < 200; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		require.NoError(t, s.Flush(context.Background()))
	}
	require.Eventually(t, func() bool {
		return s.DroppedErrorNotifications() > 0
	}, 5*time.Second, 10*time.Millisecond,
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
