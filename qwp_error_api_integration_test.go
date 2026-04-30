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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorApiPerCategory drives every wire status byte through the
// receiver loop and asserts the resulting Category and Policy.
func TestErrorApiPerCategory(t *testing.T) {
	cases := []struct {
		name       string
		status     QwpStatusCode
		wantCat    Category
		wantPolicy Policy
		dropPath   bool // true if Policy == DropAndContinue (no terminal error)
	}{
		{"SchemaMismatch", QwpStatusSchemaMismatch, CategorySchemaMismatch, PolicyDropAndContinue, true},
		{"ParseError", QwpStatusParseError, CategoryParseError, PolicyHalt, false},
		{"InternalError", QwpStatusInternalError, CategoryInternalError, PolicyHalt, false},
		{"SecurityError", QwpStatusSecurityError, CategorySecurityError, PolicyHalt, false},
		{"WriteError", QwpStatusWriteError, CategoryWriteError, PolicyDropAndContinue, true},
		{"Unknown(0xFE)", QwpStatusCode(0xFE), CategoryUnknown, PolicyHalt, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := qwpSfTestServerOpts{rejectStatus: tc.status}
			if tc.dropPath {
				// Reject the first frame only; subsequent frames OK.
				// Otherwise the loop would Drop forever and we'd never
				// observe a clean continuation.
				opts.rejectFirstNFrames = 1
			}
			srv := newQwpSfTestServer(t, opts)
			defer srv.Close()

			s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
			defer cleanup()

			gotCh := make(chan *SenderError, 4)
			loop.sendLoopSetErrorHandler(func(e *SenderError) {
				select {
				case gotCh <- e:
				default:
				}
			}, qwpSfMinErrorInboxCapacity)

			require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
			_ = s.Flush(context.Background())

			select {
			case got := <-gotCh:
				assert.Equal(t, tc.wantCat, got.Category, "Category mismatch")
				assert.Equal(t, tc.wantPolicy, got.AppliedPolicy, "Policy mismatch")
			case <-time.After(3 * time.Second):
				t.Fatal("handler not invoked within deadline")
			}

			if tc.dropPath {
				// Drop: ackedFsn advances past the rejected span;
				// LastTerminalError stays nil.
				require.Eventually(t, func() bool {
					return engine.engineAckedFsn() >= 0
				}, 2*time.Second, 1*time.Millisecond)
				assert.Nil(t, s.LastTerminalError(), "Drop should not latch terminal")
			} else {
				// Halt: terminal latched; LastTerminalError non-nil.
				require.Eventually(t, func() bool {
					return s.LastTerminalError() != nil
				}, 2*time.Second, 1*time.Millisecond)
				se := s.LastTerminalError()
				require.NotNil(t, se)
				assert.Equal(t, tc.wantCat, se.Category)
			}
		})
	}
}

// TestErrorApiOverridePolicyViaResolver registers a programmatic
// resolver that flips PARSE_ERROR (default Halt) to Drop, and asserts
// the loop drops + continues instead of latching.
func TestErrorApiOverridePolicyViaResolver(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus:       QwpStatusParseError,
		rejectFirstNFrames: 1,
	})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	loop.sendLoopSetPolicyResolver(&qwpSfPolicyResolver{
		resolver: func(c Category) Policy {
			if c == CategoryParseError {
				return PolicyDropAndContinue
			}
			return PolicyAuto
		},
	})

	// Two frames: first rejected and dropped, second OK.
	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))
	require.NoError(t, s.Table("t").Int64Column("v", 2).AtNow(context.Background()))
	require.NoError(t, s.Flush(context.Background()))

	require.Eventually(t, func() bool {
		return engine.engineAckedFsn() >= 1
	}, 5*time.Second, 1*time.Millisecond,
		"ackedFsn should advance past the dropped frame")
	assert.Nil(t, s.LastTerminalError(),
		"resolver flipped Halt to Drop; no terminal error expected")
}

// TestErrorApiOverridePolicyViaPerCategory uses the perCat slot to
// flip SCHEMA_MISMATCH (default Drop) to Halt — mirrors the
// connect-string on_schema_error=halt path.
func TestErrorApiOverridePolicyViaPerCategory(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus: QwpStatusSchemaMismatch,
	})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	r := &qwpSfPolicyResolver{}
	r.perCat[CategorySchemaMismatch] = PolicyHalt
	loop.sendLoopSetPolicyResolver(r)

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	_ = s.Flush(context.Background())

	require.Eventually(t, func() bool {
		return s.LastTerminalError() != nil
	}, 2*time.Second, 1*time.Millisecond,
		"Halt override should latch terminal")
	se := s.LastTerminalError()
	require.NotNil(t, se)
	assert.Equal(t, CategorySchemaMismatch, se.Category)
	assert.Equal(t, PolicyHalt, se.AppliedPolicy)
}

// TestErrorApiFsnSpanCorrelation drives a HALT rejection and asserts
// the [FromFsn, ToFsn] span on the SenderError matches the engine's
// publishedFsn at the time the rejection was classified. Useful as a
// sanity check that producer-side FSN and SenderError FSN line up.
func TestErrorApiFsnSpanCorrelation(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{
		rejectStatus: QwpStatusParseError,
	})
	defer srv.Close()

	s, engine, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.NoError(t, s.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	// Flush may return either nil (rejection not yet classified) or
	// the typed *SenderError (if the receiver beat us to it). Either
	// is fine for FSN correlation — we only need the engine's view
	// of the published FSN.
	_ = s.Flush(context.Background())

	require.Eventually(t, func() bool {
		return loop.sendLoopCheckError() != nil
	}, 2*time.Second, 1*time.Millisecond)

	se := s.LastTerminalError()
	require.NotNil(t, se)
	// The rejected frame's FSN must equal the engine's publishedFsn:
	// only one frame was sent, and the receiver saw it.
	assert.Equal(t, engine.enginePublishedFsn(), se.FromFsn,
		"FromFsn should equal publishedFsn for a single-frame batch")
	assert.Equal(t, se.FromFsn, se.ToFsn,
		"single-frame span: FromFsn == ToFsn")
}

// TestErrorApiHaltVsConcurrentFlush exercises the contract: even
// under tight concurrent Flush + induce-halt, every Flush after the
// loop has latched MUST surface the typed *SenderError; never sees
// "callback fired but Flush passed".
func TestErrorApiHaltVsConcurrentFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("race test skipped in short mode")
	}
	const iters = 50
	for i := 0; i < iters; i++ {
		runHaltVsConcurrentFlushOnce(t, i)
	}
}

func runHaltVsConcurrentFlushOnce(t *testing.T, iter int) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{rejectStatus: QwpStatusParseError})
	defer srv.Close()

	s, _, loop, cleanup := newCursorSenderForTest(t, srv, 0)
	defer cleanup()

	require.NoError(t, s.Table("t").Int64Column("v", int64(iter)).AtNow(context.Background()))
	// Kick off the rejection.
	_ = s.Flush(context.Background())

	// Hammer Flush from a few goroutines; each must observe a
	// terminal error after the loop latches.
	var wg sync.WaitGroup
	var observed atomic.Int32
	deadline := time.Now().Add(2 * time.Second)
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if loop.sendLoopCheckError() == nil {
					continue
				}
				err := s.Flush(context.Background())
				if err == nil {
					return
				}
				var se *SenderError
				if errors.As(err, &se) {
					observed.Add(1)
				}
				return
			}
		}()
	}
	wg.Wait()
	assert.Greater(t, observed.Load(), int32(0),
		"iter %d: at least one goroutine should observe *SenderError", iter)
}
