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

// This file holds error-resilience tests that go beyond the unit-style
// classification / dispatcher / payload tests in
// qwp_sender_error_api_test.go and qwp_error_api_integration_test.go.
//
// Coverage focus:
//   - Public-API end-to-end: every WithError* builder option and every
//     on_*_error connect-string key is exercised through
//     LineSenderFromConf / NewLineSender, so a wiring bug between
//     conf.* and the running send loop's resolver/dispatcher is caught.
//   - Reconnect × error: rejections that surface after a reconnect
//     boundary, with FSN-span correlation against post-reconnect
//     fsnAtZero.
//   - SF disk × error: HALT survives close + reopen on the same slot
//     (matches the spec's "no resumeAfterHalt; close + rebuild =
//     recovery"); DROP-acked frames are unlinked and don't replay.
//   - Strict per-category payload assertions: every field of
//     *SenderError is checked (not just Category + Policy).
//   - Concurrent halt-vs-flush stress: many iterations, no pre-check,
//     all hammering goroutines must observe the typed error.
//   - Dispatcher swap mid-flight: the atomic.Pointer guarantee that a
//     concurrent WithErrorHandler swap doesn't lose the old handler's
//     in-flight notifications below the dropped-counter line.

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

// addrOf strips the http:// prefix from an httptest.Server URL so the
// result is suitable as the addr= value in a QWP connect string.
func addrOf(srv *qwpSfTestServer) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// asQwp type-asserts to QwpSender (the superset interface that exposes
// LastTerminalError, TotalServerErrors, etc). Every QWP sender does
// implement this — the assertion is purely to surface the extra
// methods on the LineSender returned by LineSenderFromConf.
func asQwp(t *testing.T, ls LineSender) QwpSender {
	t.Helper()
	qs, ok := ls.(QwpSender)
	require.True(t, ok, "LineSender did not implement QwpSender: %T", ls)
	return qs
}

// =============================================================================
// Public-API end-to-end: builder options
// =============================================================================

// TestErrorApiBuilderOption_WithErrorHandlerInvoked drives a HALT
// rejection through a sender built via NewLineSender + WithQwp +
// WithErrorHandler, and asserts the user-supplied handler is invoked.
// Closes a gap that the unit tests previously left wide open: there
// was no test that the public option actually wired the handler into
// the running dispatcher.
func TestErrorApiBuilderOption_WithErrorHandlerInvoked(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	gotCh := make(chan *SenderError, 4)
	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addrOf(srv)),
		WithErrorHandler(func(e *SenderError) { gotCh <- e }),
		WithErrorInboxCapacity(qwpSfMinErrorInboxCapacity),
	)
	require.NoError(t, err)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = ls.Flush(context.Background()) // expected to surface the rejection

	select {
	case got := <-gotCh:
		assert.Equal(t, CategoryParseError, got.Category)
		assert.Equal(t, PolicyHalt, got.AppliedPolicy)
	case <-time.After(3 * time.Second):
		t.Fatal("user-supplied error handler was not invoked")
	}
}

// TestErrorApiBuilderOption_WithErrorPolicyOverride uses
// WithErrorPolicy(SchemaMismatch, Halt) to flip the spec default
// (Drop) to Halt, and asserts the next Flush surfaces *SenderError.
func TestErrorApiBuilderOption_WithErrorPolicyOverride(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusSchemaMismatch})
	defer srv.Close()

	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addrOf(srv)),
		WithErrorPolicy(CategorySchemaMismatch, PolicyHalt),
	)
	require.NoError(t, err)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	// Drive the rejection. The first Flush may race the receiver; the
	// second Flush is guaranteed to surface the latched terminal
	// error if the override took effect.
	_ = ls.Flush(context.Background())
	require.Eventually(t, func() bool {
		return asQwp(t, ls).LastTerminalError() != nil
	}, 3*time.Second, 1*time.Millisecond,
		"override SchemaMismatch=Halt should latch, but LastTerminalError stayed nil")

	// AtNow surfaces the latched terminal error now that Table()
	// polls the I/O loop's HALT latch on entry.
	err = ls.Table("t").Int64Column("v", 2).AtNow(context.Background())
	require.Error(t, err)
	var se *SenderError
	require.True(t, errors.As(err, &se))
	assert.Equal(t, CategorySchemaMismatch, se.Category)
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
}

// TestErrorApiBuilderOption_WithErrorPolicyResolver registers a
// programmatic resolver that flips PARSE_ERROR (default Halt) to
// Drop, and asserts the loop drops + continues past the rejection
// instead of latching.
func TestErrorApiBuilderOption_WithErrorPolicyResolver(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusParseError,
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	gotCh := make(chan *SenderError, 4)
	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addrOf(srv)),
		WithErrorPolicyResolver(func(c Category) Policy {
			if c == CategoryParseError {
				return PolicyDropAndContinue
			}
			return PolicyAuto
		}),
		WithErrorHandler(func(e *SenderError) { gotCh <- e }),
		WithErrorInboxCapacity(qwpSfMinErrorInboxCapacity),
	)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() { _ = ls.Close(context.Background()) }()

	// Two flushes: first rejected and dropped, second OK.
	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls.Flush(context.Background()))
	require.NoError(t, ls.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	require.NoError(t, ls.Flush(context.Background()))

	select {
	case got := <-gotCh:
		assert.Equal(t, CategoryParseError, got.Category)
		assert.Equal(t, PolicyDropAndContinue, got.AppliedPolicy,
			"resolver should have flipped Halt → Drop")
	case <-time.After(3 * time.Second):
		t.Fatal("handler not invoked: resolver may not have wired through")
	}
	assert.Nil(t, qs.LastTerminalError(),
		"resolver flipped Halt→Drop; no terminal error expected")
}

// TestErrorApiBuilderOption_WithErrorInboxCapacity sets a small
// capacity and floods a slow handler, asserting the drop counter
// rises (i.e., the option actually sized the inbox).
func TestErrorApiBuilderOption_WithErrorInboxCapacity(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus: QwpStatusSchemaMismatch, // Drop policy → no halt
	})
	defer srv.Close()

	release := make(chan struct{})
	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addrOf(srv)),
		WithErrorHandler(func(e *SenderError) { <-release }),
		WithErrorInboxCapacity(qwpSfMinErrorInboxCapacity),
	)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() {
		close(release)
		_ = ls.Close(context.Background())
	}()

	for i := 0; i < 200; i++ {
		require.NoError(t, ls.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		require.NoError(t, ls.Flush(context.Background()))
	}
	require.Eventually(t, func() bool {
		return qs.DroppedErrorNotifications() > 0
	}, 5*time.Second, 10*time.Millisecond,
		"DroppedErrorNotifications never increased: dropped=%d delivered=%d",
		qs.DroppedErrorNotifications(), qs.TotalErrorNotificationsDelivered())
}

// TestErrorApiBuilderOption_ProtocolViolationOverrideIgnored asserts
// that WithErrorPolicy(ProtocolViolation, DropAndContinue) is
// silently ignored — ProtocolViolation is forced HALT regardless.
// The forced behavior protects users who would otherwise lose
// connection-gone errors; matching the spec contract documented on
// the Policy enum.
func TestErrorApiBuilderOption_ProtocolViolationOverrideIgnored(t *testing.T) {
	srv := closeFrameTestServer(t, websocket.StatusProtocolError, "bad framing")
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addr),
		// Try to flip ProtocolViolation to Drop. Should be ignored.
		WithErrorPolicy(CategoryProtocolViolation, PolicyDropAndContinue),
	)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = ls.Flush(context.Background())
	require.Eventually(t, func() bool {
		return qs.LastTerminalError() != nil
	}, 3*time.Second, 1*time.Millisecond,
		"ProtocolViolation must HALT regardless of user override")
	se := qs.LastTerminalError()
	require.NotNil(t, se)
	assert.Equal(t, CategoryProtocolViolation, se.Category)
	assert.Equal(t, PolicyHalt, se.AppliedPolicy,
		"forced HALT for ProtocolViolation should ignore user override")
}

// TestErrorApiBuilderOption_ForcedHaltCategoriesNotRecorded pins the
// config-level invariant behind the runtime ignore: WithErrorPolicy
// must not record an override for the two forced-HALT categories. The
// connect-string form has no on_protocol_violation_error /
// on_unknown_error key and rejects them outright; the builder reaches
// the same end state by never storing the slot, so the forced HALT
// does not rely on resolve() checking these categories first.
func TestErrorApiBuilderOption_ForcedHaltCategoriesNotRecorded(t *testing.T) {
	for _, c := range []Category{CategoryProtocolViolation, CategoryUnknown} {
		t.Run(c.String(), func(t *testing.T) {
			conf := newLineSenderConfig(qwpSenderType)
			WithErrorPolicy(c, PolicyDropAndContinue)(conf)
			assert.Equal(t, PolicyAuto, conf.errorPolicyPerCat[c],
				"override for %s must not be recorded", c)
			assert.False(t, conf.errorPolicyPerCatSet,
				"recording an ignored override would falsely flag the QWP-only error API as used")
		})
	}
}

// =============================================================================
// Public-API end-to-end: connect-string keys
// =============================================================================

// TestErrorApiConfString_OnParseErrorDrop builds a sender from a
// connect string with on_parse_error=drop and asserts the loop
// continues past PARSE_ERROR rejections instead of latching. End-to-
// end test of the conf-string → resolver wiring path.
func TestErrorApiConfString_OnParseErrorDrop(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusParseError,
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	conf := "ws::addr=" + addrOf(srv) + ";on_parse_error=drop;"
	ls, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls.Flush(context.Background()))
	require.NoError(t, ls.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	require.NoError(t, ls.Flush(context.Background()),
		"second Flush should succeed because on_parse_error=drop continued past the rejection")
	// Flush no longer blocks on the server ACK (cursor path, commit
	// 29a6f12), so the PARSE_ERROR rejection is processed by the send
	// loop asynchronously. Wait for the counter to reflect it before
	// asserting; checking the no-latch invariant only afterwards makes
	// it meaningful (the rejection is known to have been handled).
	require.Eventually(t, func() bool {
		return qs.TotalServerErrors() >= 1
	}, 3*time.Second, 1*time.Millisecond,
		"the rejection must still bump the server-error counter")
	assert.Nil(t, qs.LastTerminalError(),
		"on_parse_error=drop must not latch terminal")
}

// TestErrorApiConfString_OnSchemaErrorHalt builds a sender from a
// connect string with on_schema_error=halt and asserts that a
// SchemaMismatch (Drop by default) instead halts. End-to-end test of
// the conf-string → resolver wiring path going the other direction
// (default-Drop flipped to Halt).
func TestErrorApiConfString_OnSchemaErrorHalt(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusSchemaMismatch})
	defer srv.Close()

	conf := "ws::addr=" + addrOf(srv) + ";on_schema_error=halt;"
	ls, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = ls.Flush(context.Background())
	require.Eventually(t, func() bool {
		return qs.LastTerminalError() != nil
	}, 3*time.Second, 1*time.Millisecond,
		"on_schema_error=halt should latch the SchemaMismatch as terminal")
	assert.Equal(t, CategorySchemaMismatch, qs.LastTerminalError().Category)
}

// TestErrorApiConfString_OnServerErrorHaltGlobal sets the global
// override on_server_error=halt and asserts a SchemaMismatch (default
// Drop) latches as terminal — the global override takes effect since
// no per-category override is set.
func TestErrorApiConfString_OnServerErrorHaltGlobal(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusSchemaMismatch})
	defer srv.Close()

	conf := "ws::addr=" + addrOf(srv) + ";on_server_error=halt;"
	ls, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = ls.Flush(context.Background())
	require.Eventually(t, func() bool {
		return qs.LastTerminalError() != nil
	}, 3*time.Second, 1*time.Millisecond)
	assert.Equal(t, PolicyHalt, qs.LastTerminalError().AppliedPolicy)
}

// TestErrorApiConfString_PerCategoryBeatsGlobal asserts the
// precedence: per-category on_*_error overrides on_server_error.
func TestErrorApiConfString_PerCategoryBeatsGlobal(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch,
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	// Global=halt, per-category=drop. Per-category must win.
	conf := "ws::addr=" + addrOf(srv) + ";on_server_error=halt;on_schema_error=drop;"
	ls, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs := asQwp(t, ls)
	defer func() { _ = ls.Close(context.Background()) }()

	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls.Flush(context.Background()))
	require.NoError(t, ls.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	require.NoError(t, ls.Flush(context.Background()),
		"per-category drop must beat global halt")
	assert.Nil(t, qs.LastTerminalError())
}

// =============================================================================
// Reconnect × error interaction
// =============================================================================

// TestErrorApiResilience_ReconnectThenHaltFsnCorrelation drives a
// reconnect followed by a HALT, and asserts the SenderError's
// FromFsn matches the engine-side publishedFsn at rejection time —
// specifically, that fsnAtZero advanced correctly across the
// reconnect boundary so wireSeq=0 on the new connection maps to
// FSN >= 1 (the first frame ACK'd on connection 1).
func TestErrorApiResilience_ReconnectThenHaltFsnCorrelation(t *testing.T) {
	// Connection 1: ACKs the first frame, then closes after reading
	// frame 1 (without ACKing it). One ACK seen so the loop's
	// silent-drop guard does not fire, and we get a clean reconnect.
	// Connection 2: rejects everything with PARSE_ERROR.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		closeAfterFrames: 2,
		rejectStatus:     QwpStatusParseError,
		rejectFromConn:   2,
	})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Frame 0: ACK'd by conn 1.
	require.NoError(t, s.Table("t").Int64Column("v", 0).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))
	require.Eventually(t, func() bool { return engine.engineAckedFsn() >= 0 },
		2*time.Second, 1*time.Millisecond, "frame 0 should be ACK'd on conn 1")

	// Frame 1: conn 1 reads it then closes (no ACK). The loop
	// reconnects to conn 2, which rejects the replayed frame 1.
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = s.Flush(context.Background())

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 5*time.Second, 1*time.Millisecond, "expected HALT after reconnect")

	se := s.LastTerminalError()
	require.NotNil(t, se)
	assert.Equal(t, CategoryParseError, se.Category)
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
	// The rejected frame's FSN must be 1 — the second frame in the
	// publish order. This is the entire point of FSN-correlation
	// across reconnect: even though wireSeq on conn 2 starts at 0,
	// fsnAtZero=1 maps it back to the right global FSN.
	assert.Equal(t, int64(1), se.FromFsn,
		"FromFsn must reflect post-reconnect fsnAtZero (=1), not raw wireSeq (=0)")
	assert.Equal(t, se.FromFsn, se.ToFsn,
		"single-frame rejection: FromFsn == ToFsn")

	// Reconnect actually happened.
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1))
}

// TestErrorApiResilience_DropAcrossReconnect: drop frame 0 on conn 1,
// reconnect, then drop frame 1 on conn 2. Assert ackedFsn advances
// to 1 (both drops counted as "resolved by server") and no terminal
// error is latched.
func TestErrorApiResilience_DropAcrossReconnect(t *testing.T) {
	// Connection 1: drop frame 0 (rejectFirstNFrames=1), then close
	// after reading frame 1 (closeAfterFrames=2). One ACK delivered,
	// so the silent-drop guard does not fire and reconnect kicks in.
	// Connection 2: rejectFromConn=2 means reject all frames on conn ≥ 2.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch,
		rejectFirstNFrames: 1,
		closeAfterFrames:   2,
		rejectFromConn:     2,
	})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Frame 0: dropped on conn 1 (Drop policy → ackedFsn advances to 0).
	require.NoError(t, s.Table("t").Int64Column("v", 0).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))
	require.Eventually(t, func() bool { return engine.engineAckedFsn() >= 0 },
		2*time.Second, 1*time.Millisecond, "frame 0 must be drop-acked on conn 1")

	// Frame 1: conn 1 reads it then closes (no ACK). The loop reconnects
	// and replays frame 1 on conn 2, which drops it (ackedFsn → 1).
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 1
	}, 5*time.Second, 1*time.Millisecond,
		"engineAckedFsn = %d, expected >= 1 (frame 0 + frame 1 both dropped)",
		engine.engineAckedFsn())
	assert.Nil(t, s.LastTerminalError(),
		"Drop across reconnect should not latch terminal")
	assert.GreaterOrEqual(t, loop.sendLoopTotalServerErrors(), int64(2),
		"two drops should each bump the server-error counter")
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"reconnect must have happened between the two drops")
}

// TestErrorApiResilience_ReconnectThenAuthFailure exercises the
// auth-on-reconnect terminal: the live conn gets killed mid-stream,
// the reconnect factory points at an auth-rejecting server, and the
// loop must surface CategorySecurityError + PolicyHalt without
// retrying past the auth wall.
func TestErrorApiResilience_ReconnectThenAuthFailure(t *testing.T) {
	authSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{upgradeStatus: 401})
	defer authSrv.Close()
	dataSrv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer dataSrv.Close()

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := qwpSfDialFor(dataSrv)(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, qwpSfDialAt(authSrv.URL),
		100*time.Microsecond, time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Warm up: get an OK ACK on dataSrv.
	_, err = engine.engineAppendBlocking(context.Background(), []byte("warmup"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return loop.sendLoopTotalAcks() >= 1
	}, time.Second, time.Millisecond, "dataSrv should have ACK'd the warm-up frame")

	// Tear the live WS so the loop falls into reconnect against authSrv.
	close(dataSrv.kill)

	require.Eventually(t, func() bool {
		return loop.sendLoopLastTerminalServerError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	se := loop.sendLoopLastTerminalServerError()
	require.NotNil(t, se)
	assert.Equal(t, CategorySecurityError, se.Category)
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
	assert.Equal(t, NoStatusByte, se.ServerStatusByte,
		"upgrade failures carry no QWP status byte")
	assert.Equal(t, NoMessageSequence, se.MessageSequence)
	assert.Contains(t, se.ServerMessage, "401")
}

// =============================================================================
// SF disk-mode × error interaction
// =============================================================================

// TestErrorApiResilience_SfDiskHaltCloseReopenReplays exercises the
// "close + rebuild" recovery path the spec mandates in lieu of
// resumeAfterHalt. Sender 1 hits a HALT-inducing rejection, closes
// (the unacked frame stays on disk under the slot), Sender 2 opens
// the same slot and replays the same frame — server rejects again,
// HALT latches again. This is the contract that makes "client
// restart" deterministic for HALT scenarios.
func TestErrorApiResilience_SfDiskHaltCloseReopenReplays(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	tmp := t.TempDir()
	conf := strings.Join([]string{
		"ws::addr=" + addrOf(srv),
		"sf_dir=" + tmp,
		"sender_id=halt-replay",
		"sf_max_bytes=4096",
		"close_flush_timeout_millis=100;", // short — the loop will halt, not drain
	}, ";")

	// === Sender 1: induce HALT, close. ===
	ls1, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs1 := asQwp(t, ls1)

	require.NoError(t, ls1.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = ls1.Flush(context.Background())
	require.Eventually(t, func() bool {
		return qs1.LastTerminalError() != nil
	}, 3*time.Second, 1*time.Millisecond, "sender 1 should HALT")
	se1 := qs1.LastTerminalError()
	require.NotNil(t, se1)
	assert.Equal(t, CategoryParseError, se1.Category)

	// Close — drain will time out (HALT keeps ackedFsn behind
	// publishedFsn), so Close returns the timeout error. We don't
	// care about that, only that it returns.
	_ = ls1.Close(context.Background())

	// === Sender 2: open same slot, expect the unacked frame to
	// replay and trigger a fresh HALT. ===
	ls2, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs2 := asQwp(t, ls2)
	defer func() { _ = ls2.Close(context.Background()) }()

	require.Eventually(t, func() bool {
		return qs2.LastTerminalError() != nil
	}, 5*time.Second, 1*time.Millisecond,
		"sender 2 should replay the on-disk frame and re-HALT against the same server")
	se2 := qs2.LastTerminalError()
	require.NotNil(t, se2)
	assert.Equal(t, CategoryParseError, se2.Category,
		"replayed rejection should classify the same way")
	assert.Equal(t, PolicyHalt, se2.AppliedPolicy)
}

// TestErrorApiResilience_SfDiskDropPersistsAckedAcrossRestart drives
// a Drop-policy rejection through SF disk mode, closes cleanly, then
// reopens the slot and asserts a NEW frame goes through normally —
// the dropped frame must NOT replay (it was acked-via-drop, so the
// segment file should be unlinked). This is the SF flip side of the
// HALT replay test: drops are durable, halts are durable, but the
// persistence semantics differ.
func TestErrorApiResilience_SfDiskDropPersistsAckedAcrossRestart(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch, // default Drop
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	tmp := t.TempDir()
	conf := strings.Join([]string{
		"ws::addr=" + addrOf(srv),
		"sf_dir=" + tmp,
		"sender_id=drop-restart",
		"sf_max_bytes=4096",
		"close_flush_timeout_millis=2000;",
	}, ";")

	// === Sender 1: send frame 0 (rejected → dropped), close cleanly. ===
	ls1, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs1 := asQwp(t, ls1)

	require.NoError(t, ls1.Table("t").Int64Column("v", 0).AtNow(context.Background()))
	require.NoError(t, ls1.Flush(context.Background()))

	// Wait for the drop to propagate so ackedFsn catches up to
	// publishedFsn — only then does Close drain successfully.
	require.Eventually(t, func() bool {
		return qs1.AckedFsn() >= 0
	}, 2*time.Second, 1*time.Millisecond,
		"frame 0 should be acked-via-drop on sender 1")
	assert.Nil(t, qs1.LastTerminalError(), "Drop should not latch terminal")
	// Clean close — drain should complete because everything's
	// acked-via-drop.
	require.NoError(t, ls1.Close(context.Background()))

	// Server frame counter saw the rejected frame.
	frames1 := srv.totalFramesReceived.Load()
	require.GreaterOrEqual(t, frames1, int64(1))

	// === Sender 2: same slot, send a fresh frame. The dropped frame
	// must NOT replay (would surface as a duplicate frame on the
	// server side). ===
	ls2, err := LineSenderFromConf(context.Background(), conf)
	require.NoError(t, err)
	qs2 := asQwp(t, ls2)
	defer func() { _ = ls2.Close(context.Background()) }()

	require.NoError(t, ls2.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls2.Flush(context.Background()))
	require.Eventually(t, func() bool {
		return qs2.AckedFsn() >= 0
	}, 2*time.Second, 1*time.Millisecond)

	// Server should have seen exactly one additional frame on
	// sender 2 — the new one — not a replay of the dropped frame.
	frames2 := srv.totalFramesReceived.Load()
	assert.Equal(t, frames1+1, frames2,
		"sender 2 should send only the new frame; dropped frame should NOT replay")
}

// =============================================================================
// Strict per-category payload assertions
// =============================================================================

// TestErrorApiPerCategoryStrict extends TestErrorApiPerCategory with
// strict assertions on every field of *SenderError. Catches bugs
// like "ServerStatusByte set to the wrong byte" or "DetectedAt left
// at zero" that the loose Category+Policy check would miss.
func TestErrorApiPerCategoryStrict(t *testing.T) {
	cases := []struct {
		name       string
		status     QwpStatusCode
		wantCat    Category
		wantPolicy Policy
		dropPath   bool
	}{
		{"SchemaMismatch", QwpStatusSchemaMismatch, CategorySchemaMismatch, PolicyDropAndContinue, true},
		{"ParseError", QwpStatusParseError, CategoryParseError, PolicyHalt, false},
		{"InternalError", QwpStatusInternalError, CategoryInternalError, PolicyHalt, false},
		{"SecurityError", QwpStatusSecurityError, CategorySecurityError, PolicyHalt, false},
		{"WriteError", QwpStatusWriteError, CategoryWriteError, PolicyDropAndContinue, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := qwpSfTestServerOpts{rejectStatus: tc.status}
			if tc.dropPath {
				opts.rejectFirstNFrames = 1
			}
			srv := newQwpSfTestServer(t, opts)
			defer srv.Close()

			s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
			defer cleanup()

			gotCh := make(chan *SenderError, 4)
			loop.sendLoopSetErrorHandler(func(e *SenderError) {
				select {
				case gotCh <- e:
				default:
				}
			}, qwpSfMinErrorInboxCapacity)

			before := time.Now()
			require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
			_ = s.Flush(context.Background())

			var got *SenderError
			select {
			case got = <-gotCh:
			case <-time.After(3 * time.Second):
				t.Fatal("handler not invoked within deadline")
			}
			after := time.Now()

			assert.Equal(t, tc.wantCat, got.Category, "Category")
			assert.Equal(t, tc.wantPolicy, got.AppliedPolicy, "AppliedPolicy")
			assert.Equal(t, int(tc.status), got.ServerStatusByte, "ServerStatusByte")
			assert.Contains(t, got.ServerMessage, "rejected", "ServerMessage carries server text")
			assert.Equal(t, int64(0), got.MessageSequence,
				"single-frame batch starts at MessageSequence 0")
			assert.Equal(t, int64(0), got.FromFsn, "single-frame batch FromFsn=0")
			assert.Equal(t, got.FromFsn, got.ToFsn, "single-frame span")
			assert.Equal(t, "", got.TableName,
				"server doesn't attribute single-table batches yet (forward-compat)")
			assert.False(t, got.DetectedAt.IsZero(), "DetectedAt populated")
			assert.True(t, !got.DetectedAt.Before(before) && !got.DetectedAt.After(after),
				"DetectedAt within [before, after] window: detected=%v before=%v after=%v",
				got.DetectedAt, before, after)

			// Assert the Error() string contains the expected
			// human-readable bits — the producer side relies on this
			// when logging.
			s2 := got.Error()
			assert.Contains(t, s2, tc.wantCat.String())
			assert.Contains(t, s2, tc.wantPolicy.String())
			assert.Contains(t, s2, fmt.Sprintf("0x%02X", byte(tc.status)))
			assert.Contains(t, s2, "rejected")
		})
	}
}

// TestErrorApiResilience_LastTerminalErrorSurvivesClose latches a HALT,
// closes the sender, and asserts LastTerminalError still returns the
// snapshot afterward. Useful for diagnostics that want to inspect
// the error after Close() has returned.
func TestErrorApiResilience_LastTerminalErrorSurvivesClose(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusInternalError})
	defer srv.Close()

	s, _, loop, _ := newCursorSenderForTest(t, srv, 0)

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = s.Flush(context.Background())

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)
	beforeClose := s.LastTerminalError()
	require.NotNil(t, beforeClose)

	_ = s.Close(context.Background())

	afterClose := s.LastTerminalError()
	require.NotNil(t, afterClose, "LastTerminalError should still return the snapshot after Close")
	assert.Equal(t, beforeClose, afterClose,
		"LastTerminalError snapshot must not change across Close")
}

// TestErrorApiResilience_TotalServerErrorsCounterStrict drives 3
// drop-policy rejections back-to-back and asserts the counter is
// exactly 3 (not >=3, exactly). Catches off-by-one and
// double-counting bugs that the looser >= assertions in the existing
// suite would miss.
func TestErrorApiResilience_TotalServerErrorsCounterStrict(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch,
		rejectFirstNFrames: 3,
	})
	defer srv.Close()

	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	for i := 0; i < 3; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		require.NoError(t, s.Flush(context.Background()))
	}
	// Send a 4th frame that should NOT be rejected — bookmarks the
	// fact that the 3 prior rejections settled.
	require.NoError(t, s.Table("t").Int64Column("v", 99).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))

	require.Eventually(t, func() bool {
		return s.AckedFsn() >= 3
	}, 5*time.Second, 1*time.Millisecond, "all four frames should be acked")

	assert.Equal(t, int64(3), s.TotalServerErrors(),
		"exactly three drops should have happened, not more, not fewer")
	assert.Nil(t, s.LastTerminalError(), "Drops should not latch terminal")
}

// =============================================================================
// Concurrent halt-vs-flush stress
// =============================================================================

// TestErrorApiResilience_HaltVsConcurrentFlushStress pins the
// HALT-is-terminal contract under load: many iterations and a strict
// "every hammering goroutine must observe *SenderError" assertion. A
// weaker any-of-N assertion can hide a race where only one goroutine
// observes the latched state. Hammering happens AFTER the latch is
// confirmed, so the sender is quiescent (no concurrent producer) —
// matches the LineSender contract that production code must
// serialize calls.
func TestErrorApiResilience_HaltVsConcurrentFlushStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	const iters = 500
	const goroutines = 8
	for i := 0; i < iters; i++ {
		runHaltStressOnce(t, i, goroutines)
	}
}

func runHaltStressOnce(t *testing.T, iter, goroutines int) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// Single producer Flush triggers the rejection. The server
	// rejects every frame with PARSE_ERROR, so one Flush is enough
	// to latch HALT.
	require.NoError(t, s.Table("t").Int64Column("v", int64(iter)).AtNow(context.Background()))
	_ = s.Flush(context.Background())

	// Wait for the latch to be observable. After this, the sender
	// is quiescent (no concurrent producer) and Flush from many
	// goroutines is safe — each just samples the latched error.
	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, time.Microsecond, "iter %d: loop must latch", iter)

	// Hammer Flush from N goroutines. Every Flush MUST surface the
	// typed *SenderError.
	var hammerWg sync.WaitGroup
	var observed atomic.Int32
	for j := 0; j < goroutines; j++ {
		hammerWg.Add(1)
		go func() {
			defer hammerWg.Done()
			err := s.Flush(context.Background())
			if err == nil {
				return
			}
			var se *SenderError
			if errors.As(err, &se) && se.Category == CategoryParseError {
				observed.Add(1)
			}
		}()
	}
	hammerWg.Wait()

	assert.Equal(t, int32(goroutines), observed.Load(),
		"iter %d: every hammering goroutine must observe *SenderError, got %d/%d",
		iter, observed.Load(), goroutines)
}

// =============================================================================
// Dispatcher mid-flight swap
// =============================================================================

// TestErrorApiResilience_DispatcherSwapMidFlight: enqueue errors
// against a slow handler, then swap the handler via
// sendLoopSetErrorHandler. The atomic.Pointer machinery should make
// this race-free: the swap is observed by the next offer; the old
// dispatcher's drain delivers any remaining queued items (subject to
// its drain timeout) before exiting. Asserts that
//   - the new handler receives notifications offered after the swap;
//   - the counters (TotalErrorNotificationsDelivered and
//     DroppedErrorNotifications) sum consistently with TotalServerErrors.
func TestErrorApiResilience_DispatcherSwapMidFlight(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusSchemaMismatch,
		rejectFirstNFrames: 50, // 50 drops on conn 1
	})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	// First handler: counts deliveries.
	var oldDelivered atomic.Int64
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		oldDelivered.Add(1)
	}, qwpSfMinErrorInboxCapacity)

	// Drive 25 rejections, then swap the handler.
	for i := 0; i < 25; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		require.NoError(t, s.Flush(context.Background()))
	}
	require.Eventually(t, func() bool {
		return s.TotalServerErrors() >= 25
	}, 5*time.Second, 1*time.Millisecond)

	// Swap to a new handler.
	var newDelivered atomic.Int64
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		newDelivered.Add(1)
	}, qwpSfMinErrorInboxCapacity)

	// Drive 25 more rejections — these must reach the new handler.
	for i := 25; i < 50; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		require.NoError(t, s.Flush(context.Background()))
	}
	require.Eventually(t, func() bool {
		return s.TotalServerErrors() >= 50
	}, 5*time.Second, 1*time.Millisecond)

	// Wait briefly for the new dispatcher to drain.
	require.Eventually(t, func() bool {
		return newDelivered.Load() > 0
	}, 2*time.Second, 1*time.Millisecond,
		"new handler must receive at least some notifications after swap")

	// Old + new together should account for at most TotalServerErrors.
	// The strict bound is harder because (a) the old dispatcher's
	// drain may discard items still in its inbox at swap time, and
	// (b) some notifications may end up in DroppedErrorNotifications
	// if the inboxes filled up. Sanity bound: deliveries <= server
	// errors observed.
	totalDelivered := oldDelivered.Load() + newDelivered.Load()
	totalErrors := s.TotalServerErrors()
	dropped := s.DroppedErrorNotifications()
	assert.LessOrEqual(t, totalDelivered, totalErrors,
		"deliveries (%d) must not exceed total server errors (%d)",
		totalDelivered, totalErrors)
	assert.Equal(t, totalErrors, totalDelivered+dropped+0 /* lost-to-old-drain unaccounted */,
		"every server error should be either delivered or dropped (or lost to old-dispatcher drain)")

	// The new handler should have received SOMETHING (otherwise the
	// swap didn't take effect).
	assert.Greater(t, newDelivered.Load(), int64(0),
		"new handler received zero deliveries — swap did not take effect")
}

// =============================================================================
// Server restart simulation
// =============================================================================

// TestErrorApiResilience_ServerRestartReplaysCorrectly models a full
// server restart: the first transport dial lands on srv1; srv1 ACKs
// frame 0 then closes after reading frame 1; the next dial (i.e. the
// reconnect) lands on srv2 — a fresh server with zero state about
// the client's prior frames. Replay must succeed and frames 1, 2
// must arrive at srv2. This is the canonical "server restart"
// scenario the SF design targets.
func TestErrorApiResilience_ServerRestartReplaysCorrectly(t *testing.T) {
	// srv1 ACKs frame 0 (closeAfterFrames=2: ACK seq 0, then on the
	// 2nd frame returns without ACK).
	srv1 := newQwpSfTestServer(t, qwpSfTestServerOpts{closeAfterFrames: 2})
	defer srv1.Close()
	srv2 := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv2.Close()

	// Factory returns srv1 on the first call, srv2 thereafter.
	// Models "the old server died; a new one is now responsible for
	// the address" — fresh state on the server side, but the client
	// re-replays its on-disk tail.
	var attempt atomic.Int32
	factory := func(ctx context.Context, _ int) (*qwpTransport, error) {
		var t qwpTransport
		var url string
		if attempt.Add(1) == 1 {
			url = srv1.URL
		} else {
			url = srv2.URL
		}
		wsURL := "ws" + strings.TrimPrefix(url, "http")
		if err := t.connect(ctx, wsURL, qwpTransportOpts{endpointPath: qwpWritePath}); err != nil {
			return nil, err
		}
		return &t, nil
	}

	engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
	require.NoError(t, err)
	defer func() { _ = engine.engineClose() }()

	transport, err := factory(context.Background(), 0)
	require.NoError(t, err)

	loop := qwpSfNewSendLoop(engine, transport, factory,
		100*time.Microsecond, 5*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	loop.sendLoopStart()
	defer func() { _ = loop.sendLoopClose() }()

	// Push 3 frames. srv1 ACKs frame 0; srv1 closes on reading frame
	// 1. Loop reconnects, factory returns srv2 transport. srv2 sees
	// frames 1 and 2 (replays of unacked tail).
	for i := 0; i < 3; i++ {
		_, err := engine.engineAppendBlocking(context.Background(),
			[]byte(fmt.Sprintf("f%d", i)))
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 2
	}, 10*time.Second, 1*time.Millisecond,
		"after server restart, all frames should be ACK'd (acked=%d)",
		engine.engineAckedFsn())

	// srv1 only saw frames 0 and 1 (ACK'd 0, dropped before ACKing 1).
	// srv2 must have seen frames 1 and 2 — the unacked tail replayed.
	assert.GreaterOrEqual(t, srv2.totalFramesReceived.Load(), int64(2),
		"server 2 should have received the replayed unacked tail (got %d)",
		srv2.totalFramesReceived.Load())
	assert.GreaterOrEqual(t, loop.sendLoopTotalReconnects(), int64(1),
		"reconnect must have happened across the server restart")
	assert.Nil(t, loop.sendLoopLastTerminalServerError(),
		"server restart with healthy new server should not produce a terminal error")
}

// =============================================================================
// Drain timeout boundary
// =============================================================================

// TestErrorApiResilience_DispatcherDrainTimeoutCap verifies that
// closing a sender with many queued errors + slow handler completes
// within a bounded time (the dispatcher's drain timeout caps the
// wait). Without this cap, a malicious or buggy handler could stall
// shutdown indefinitely. The cap is currently 100 ms; the test
// asserts < 750 ms for headroom.
func TestErrorApiResilience_DispatcherDrainTimeoutCap(t *testing.T) {
	// rejectFromConn: 1 → every connection NACKs its first frame.
	// Each Drop rejection recycles the connection (the error frame
	// breaks its ordered pipeline), so queueing N errors takes N
	// connections.
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:   QwpStatusSchemaMismatch,
		rejectFromConn: 1,
	})
	defer srv.Close()

	s, _, loop, _ := newCursorSenderForTest(t, srv, 0)

	// Slow handler: each call takes 50 ms. With 25 queued items,
	// processing them all would take 1.25 s; the drain timeout
	// (100 ms) must cap that.
	loop.sendLoopSetErrorHandler(func(e *SenderError) {
		time.Sleep(50 * time.Millisecond)
	}, 256) // generous capacity so most drops queue rather than getting dropped

	// Drive 25 drops as fast as possible.
	for i := 0; i < 25; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		require.NoError(t, s.Flush(context.Background()))
	}
	require.Eventually(t, func() bool {
		return s.TotalServerErrors() >= 25
	}, 10*time.Second, 1*time.Millisecond)

	// Now close — the drain timeout must fire before the slow
	// handler chews through all 25 queued items.
	start := time.Now()
	_ = s.Close(context.Background())
	elapsed := time.Since(start)

	// Allow generous headroom but assert well under the 1.25 s the
	// slow handler would otherwise need.
	assert.Less(t, elapsed, 750*time.Millisecond,
		"close should not wait for a slow handler past the drain timeout")
}

// =============================================================================
// HALT after partial Drop streak
// =============================================================================

// TestErrorApiResilience_DropStreakThenHalt models a realistic
// scenario: several rows fail with WriteError (Drop policy) — each
// drop recycling the connection and replaying the tail — then a row
// hits ParseError (Halt policy) and the loop stops with a terminal
// error. The Drop counter and the terminal error should be
// independent; the FSN on the Halt should be > the FSNs of the
// Drops.
func TestErrorApiResilience_DropStreakThenHalt(t *testing.T) {
	// Custom server: WriteError for the first 3 rejected frames,
	// ParseError on the 4th. One error response per connection —
	// the error frame breaks the connection's ordered pipeline, so
	// the server drains further frames without responding (the
	// client recycles and replays them on a fresh connection),
	// matching the real server's unresolved-sequence refusal. The
	// custom handler exists because the fixture only supports one
	// rejectStatus per server.
	var nRejects atomic.Int32
	srv := &qwpSfTestServer{kill: make(chan struct{})}
	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		responded := false
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			srv.totalFramesReceived.Add(1)
			if responded {
				// Pipeline broken by the error frame: refuse silently.
				continue
			}
			n := nRejects.Add(1)
			var status QwpStatusCode
			if n <= 3 {
				status = QwpStatusWriteError // Drop
			} else {
				status = QwpStatusParseError // Halt
			}
			// Each connection's first frame is the replay head, so the
			// rejection always names wire seq 0.
			_ = conn.Write(context.Background(), websocket.MessageBinary,
				buildAckError(status, 0, "rejected"))
			responded = true
		}
	}))
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	for i := 0; i < 4; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
		_ = s.Flush(context.Background())
	}
	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 5*time.Second, 1*time.Millisecond)

	se := s.LastTerminalError()
	require.NotNil(t, se)
	assert.Equal(t, CategoryParseError, se.Category, "last terminal should be the Halt, not a Drop")
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
	assert.Equal(t, int64(3), se.FromFsn, "the Halted frame is FSN 3 (after 3 Drops at 0..2)")

	// 4 server errors total: 3 drops + 1 halt.
	assert.Equal(t, int64(4), s.TotalServerErrors())
}
