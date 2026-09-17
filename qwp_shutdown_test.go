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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// The peer never reads or acknowledges a close frame. Library-initiated discard
// must not initiate the dependency's multi-second graceful handshake.
func TestQwpDiscardDoesNotWaitForCloseReply(t *testing.T) {
	for _, mode := range []string{"ws", "wss", "missing-version", "wrong-version", "durable-gap", "bad-server-info", "dump"} {
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode != "missing-version" {
					version := fmt.Sprint(qwpVersion)
					if mode == "wrong-version" {
						version = "999"
					}
					w.Header().Set(qwpHeaderVersion, version)
				}
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				defer conn.CloseNow()
				if mode == "bad-server-info" {
					_ = conn.Write(r.Context(), websocket.MessageBinary, []byte("invalid"))
				}
				<-release
			})
			var srv *httptest.Server
			if mode == "wss" {
				srv = httptest.NewTLSServer(handler)
			} else {
				srv = httptest.NewServer(handler)
			}
			t.Cleanup(func() { close(release); srv.Close() })
			tr := &qwpTransport{}
			if mode == "dump" {
				tr.dumpWriter = io.Discard
			}
			opts := qwpTransportOpts{endpointPath: qwpWritePath, tlsInsecureSkipVerify: true}
			if mode == "durable-gap" {
				opts.requestDurableAck = true
			}
			if mode == "bad-server-info" {
				opts.serverInfoTimeout = time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			start := time.Now()
			err := tr.connect(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), opts)
			if mode == "missing-version" || mode == "wrong-version" || mode == "durable-gap" || mode == "bad-server-info" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, tr.closeContext(ctx))
			if mode == "dump" {
				select {
				case <-tr.dumpConn.done:
				default:
					t.Fatal("dump pump still running after release")
				}
			}
			require.Less(t, time.Since(start), time.Second)
		})
	}
}

func TestQwpDumpFailedDialJoinsPump(t *testing.T) {
	tr := &qwpTransport{dumpWriter: io.Discard}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tr.connect(ctx, "", qwpTransportOpts{endpointPath: qwpWritePath})
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, tr.dumpConn)
	wait, stop := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
	defer stop()
	require.NoError(t, tr.closeContext(wait))
	select {
	case <-tr.dumpConn.done:
	default:
		t.Fatal("failed Dial left its pump running")
	}
}

// Block the actual transport release worker, not the public waiter. This seam
// models a dependency or kernel close that outlives the caller's budget.
func qwpBlockTransportRelease(t *testing.T, target *qwpTransport) (<-chan struct{}, func(), *atomic.Int32) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	calls := &atomic.Int32{}
	hook := func(tr *qwpTransport) {
		if tr != target {
			return
		}
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
	}
	qwpTestBeforeTransportClose.Store(&hook)
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); qwpTestBeforeTransportClose.Store(nil) })
	return entered, unblock, calls
}

func TestQwpTransportCloseWaitersShareOneRelease(t *testing.T) {
	var tr qwpTransport
	tr.dumpWriter = io.Discard
	require.NoError(t, tr.connect(context.Background(), "", qwpTransportOpts{endpointPath: qwpWritePath}))
	entered, release, calls := qwpBlockTransportRelease(t, &tr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 10; i++ {
		err := tr.closeContext(ctx)
		require.ErrorIs(t, err, ErrCleanupPending)
		require.ErrorIs(t, err, context.Canceled)
	}
	select {
	case <-entered:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("release never started")
	}
	require.Equal(t, int32(1), calls.Load())
	release()
	wait, stop := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
	defer stop()
	require.NoError(t, tr.closeContext(wait))
	require.NoError(t, tr.closeContext(ctx), "completed result wins over an expired wait")
}

func TestQwpQueryCloseWaitersShareOneRelease(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(mc *qwpMockEgressConn) { <-mc.conn.CloseRead(context.Background()).Done() })
	defer cleanup()
	entered, release, calls := qwpBlockTransportRelease(t, c.transport())
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.Close(ctx)
	require.ErrorIs(t, err, ErrCleanupPending)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-entered:
	case <-time.After(qwpTestWaitTimeout):
		t.Fatal("release never started")
	}
	for i := 0; i < 5; i++ {
		require.ErrorIs(t, c.Close(ctx), ErrCleanupPending)
	}
	require.Equal(t, int32(1), calls.Load())
	release()
	wait, stop := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
	defer stop()
	require.NoError(t, c.Close(wait))
	require.NoError(t, c.Close(ctx), "a prior caller timeout must not become a stored failure")
}

func TestQwpSenderCancelledCloseStillPublishes(t *testing.T) {
	for _, drain := range []time.Duration{0, time.Second} {
		t.Run(drain.String(), func(t *testing.T) {
			srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
			defer srv.Close()
			s, engine, _, cleanup := newCursorSenderForTest(t, srv, 0)
			defer cleanup()
			s.closeTimeout = drain
			release := qwpBlockShutdownPublication(t)
			defer release()
			require.NoError(t, s.Table("cancelled_close").Int64Column("v", 1).AtNow(context.Background()))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, s.Close(ctx), context.Canceled)
			require.ErrorIs(t, s.Close(context.Background()), errDoubleSenderClose)
			release()
			op := s.shutdown.Load()
			select {
			case <-op.done:
			case <-time.After(3 * time.Second):
				t.Fatal("owned shutdown did not finish")
			}
			require.NoError(t, op.err)
			require.Equal(t, int64(0), engine.enginePublishedFsn(), "even zero ACK wait must publish staged rows")
			if drain > 0 {
				require.Equal(t, int64(0), engine.engineAckedFsn())
			}
		})
	}
}

func TestQwpSenderDrainFailureOutlivesCallerWait(t *testing.T) {
	srv := newSilentAckServer(t)
	defer srv.Close()
	s, _, _, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()
	s.closeTimeout = 50 * time.Millisecond
	release := qwpBlockShutdownPublication(t)
	defer release()
	require.NoError(t, s.Table("timeout").Int64Column("v", 1).AtNow(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Close(ctx), context.Canceled)
	release()
	op := s.shutdown.Load()
	select {
	case <-op.done:
	case <-time.After(2 * time.Second):
		t.Fatal("owned drain never finished")
	}
	require.ErrorContains(t, op.err, "drain timed out")
	require.True(t, s.closeCompleted(), "delivery failure does not mean resources are still owned")
}

func TestQwpQueryCloseRetainsTerminalResult(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(mc *qwpMockEgressConn) { <-mc.conn.CloseRead(context.Background()).Done() })
	defer cleanup()
	transport := c.transport()
	var calls atomic.Int32
	hook := func(tr *qwpTransport) {
		if tr == transport {
			calls.Add(1)
			panic("terminal release fault")
		}
	}
	qwpTestBeforeTransportClose.Store(&hook)
	defer qwpTestBeforeTransportClose.Store(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Close(ctx)
	require.ErrorIs(t, err, ErrCleanupFailed)
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	require.Equal(t, err, c.Close(cancelled), "a later waiter must observe the stored failure, not its cancelled context")
	require.Equal(t, int32(1), calls.Load(), "terminal cleanup is not replayed")
}

// A transport cleanup failure must survive both loop-exit cleanup and cleanup
// of an unstarted loop, without replacing an earlier sending error.
func TestQwpSenderClosePreservesTransportCleanupFailure(t *testing.T) {
	for _, mode := range []string{"unstarted", "running", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			opts := qwpSfTestServerOpts{}
			if mode == "rejected" {
				opts.rejectStatus = QwpStatusParseError
			}
			srv := newQwpSfTestServer(t, opts)
			defer srv.Close()
			engine, err := qwpSfNewCursorEngine("", 4096, qwpSfUnlimitedTotalBytes, time.Second)
			require.NoError(t, err)
			tr, err := qwpSfDialFor(srv)(context.Background(), 0)
			require.NoError(t, err)
			loop := qwpSfNewSendLoop(engine, tr, qwpSfDialFor(srv),
				time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
			s, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
			require.NoError(t, err)

			var calls atomic.Int32
			hook := func(got *qwpTransport) {
				if got == tr {
					calls.Add(1)
					// This test checks reporting, not retained socket lifetime.
					// Release the real socket before injecting the cleanup fault.
					_ = tr.conn.CloseNow()
					panic("test sender transport cleanup failure")
				}
			}
			qwpTestBeforeTransportClose.Store(&hook)
			defer qwpTestBeforeTransportClose.Store(nil)
			defer func() {
				_ = s.Close(context.Background())
				<-s.shutdown.Load().done
			}()
			if mode != "unstarted" {
				loop.sendLoopStart()
			}
			var original error
			var rejection *SenderError
			if mode == "rejected" {
				require.NoError(t, s.Table("rejected").Int64Column("value", 1).AtNow(context.Background()))
				flushErr := s.Flush(context.Background())
				select {
				case <-loop.sendLoopDone():
				case <-time.After(2 * time.Second):
					t.Fatal("terminal rejection did not stop the loop")
				}
				original = loop.sendLoopCheckError()
				require.ErrorAs(t, original, &rejection)
				if flushErr != nil {
					require.ErrorIs(t, flushErr, original, "Flush may surface the rejection eagerly")
				}
				require.Nil(t, loop.transport.Load(), "exit cleanup already detached the transport")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = s.Close(ctx)
			require.ErrorIs(t, err, ErrCleanupFailed)
			if rejection != nil {
				var reported *SenderError
				require.ErrorAs(t, err, &reported)
				require.Same(t, rejection, reported)
			}
			require.Equal(t, original, loop.sendLoopCheckError(), "cleanup must not change the producer-error latch")
			require.False(t, engine.engineCloseCompleted(), "the engine now owns transport release and retains its terminal failure")
			require.False(t, s.closeCompleted(), "terminal cleanup failure must remain visible to pool bookkeeping")
			require.ErrorIs(t, s.Close(context.Background()), errDoubleSenderClose)
			require.ErrorIs(t, s.shutdown.Load().err, ErrCleanupFailed)
			require.Equal(t, int32(1), calls.Load(), "repeated Close must not retry transport cleanup")
		})
	}
}

func qwpBlockShutdownPublication(t *testing.T) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	hook := func() { <-release }
	qwpTestCloseDrainHook.Store(&hook)
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); qwpTestCloseDrainHook.Store(nil) })
	return unblock
}

func TestQwpSenderClosePublicationKeepsAppendBudget(t *testing.T) {
	engine, err := qwpSfNewCursorEngine("", 4096, 4096, 20*time.Millisecond)
	require.NoError(t, err)
	// Fill the only segment. No transport runs, so no space can be reclaimed.
	_, err = engine.engineAppendBlocking(context.Background(), make([]byte, engine.engineMaxFrameBytes()))
	require.NoError(t, err)
	loop := qwpSfNewSendLoop(engine, nil, func(context.Context, int) (*qwpTransport, error) {
		return nil, context.Canceled
	}, time.Millisecond, time.Second, time.Millisecond, time.Millisecond)
	s, err := newQwpCursorLineSender(0, 0, 0, 0, engine, loop, 0)
	require.NoError(t, err)
	require.NoError(t, s.Table("full").Int64Column("v", 1).AtNow(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
	defer cancel()
	err = s.Close(ctx)
	require.ErrorIs(t, err, ErrBackpressureTimeout)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.NotZero(t, s.pendingRowCount, "unpublished rows must not be described as saved")
	require.Equal(t, int64(0), engine.enginePublishedFsn(), "failed publication must not create an FSN")
	require.True(t, s.closeCompleted())
}
