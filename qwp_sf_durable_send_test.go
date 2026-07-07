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
	"github.com/stretchr/testify/require"
)

// durableAckTableEntry matches the anonymous struct the ack builders take.
type durableAckTableEntry = struct {
	name   string
	seqTxn int64
}

// newDurableAckMockServer stands in for a QWP ingest endpoint that speaks
// durable-ack. On the upgrade it echoes X-QWP-Durable-Ack: enabled only
// when echoEnabled is true and the client sent the request header. For
// every DATA_BATCH frame it immediately replies OK (reporting the "trades"
// table's WAL seqTxn as wireSeq+1) — the "received + local WAL" ack that
// must NOT trim in durable mode. It emits a DURABLE_ACK covering "trades"
// up to a given seqTxn only when the test writes that seqTxn to the
// durableTrigger channel — modelling the slower object-store upload.
func newDurableAckMockServer(echoEnabled bool, durableTrigger <-chan int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		if echoEnabled && r.Header.Get(qwpHeaderRequestDurableAck) != "" {
			w.Header().Set(qwpHeaderDurableAck, qwpDurableAckEnabledValue)
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx := context.Background()

		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-done:
					return
				case st, ok := <-durableTrigger:
					if !ok {
						return
					}
					_ = conn.Write(ctx, websocket.MessageBinary,
						buildAckDurable(durableAckTableEntry{"trades", st}))
				}
			}
		}()

		var seq int64
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
			_ = conn.Write(ctx, websocket.MessageBinary,
				buildAckOKWithTables(seq, durableAckTableEntry{"trades", seq + 1}))
			seq++
		}
	}))
}

func newDurableSenderForTest(t *testing.T, serverURL, extra string) (QwpSender, error) {
	t.Helper()
	addr := strings.TrimPrefix(strings.TrimPrefix(serverURL, "http://"), "https://")
	s, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";request_durable_ack=on;"+extra)
	if err != nil {
		return nil, err
	}
	qs, ok := s.(QwpSender)
	if !ok {
		_ = s.Close(context.Background())
		t.Fatalf("sender is not a QwpSender: %T", s)
	}
	return qs, nil
}

// The headline durable-ack invariant: an OK does NOT advance the acked
// watermark; only a covering DURABLE_ACK does. This is exactly the
// property that closes the kill-between-OK-and-upload data-loss window.
func TestQwpDurableAckTrimsOnDurableAckNotOk(t *testing.T) {
	trigger := make(chan int64, 4)
	srv := newDurableAckMockServer(true, trigger)
	defer srv.Close()

	// A short keepalive so the idle-connection keepalive PING path is
	// exercised while the batch waits for its durable-ack.
	s, err := newDurableSenderForTest(t, srv.URL, "durable_ack_keepalive_interval_millis=50;")
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()
	require.NoError(t, s.Table("trades").Int64Column("x", 1).At(ctx, time.Unix(0, 1_000_000_000)))
	fsn, err := s.FlushAndGetSequence(ctx)
	require.NoError(t, err)

	// The server has OK'd the frame (received + local WAL), but no
	// DURABLE_ACK has been sent, so AwaitAckedFsn must not complete.
	shortCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	err = s.AwaitAckedFsn(shortCtx, fsn)
	cancel()
	require.Error(t, err, "durable-ack mode: AwaitAckedFsn must not complete on OK alone")

	// Release a DURABLE_ACK covering trades to seqTxn 1 (the value the OK
	// reported for wireSeq 0). Now the watermark may advance to fsn.
	trigger <- 1
	longCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	require.NoError(t, s.AwaitAckedFsn(longCtx, fsn),
		"AwaitAckedFsn must complete once the covering DURABLE_ACK arrives")
	cancel()

	require.GreaterOrEqual(t, s.AckedFsn(), fsn)
}

// A multi-flush chain only advances the durable frontier as each batch's
// tables are covered, in order.
func TestQwpDurableAckAdvancesPerBatch(t *testing.T) {
	trigger := make(chan int64, 4)
	srv := newDurableAckMockServer(true, trigger)
	defer srv.Close()

	s, err := newDurableSenderForTest(t, srv.URL, "")
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()
	var lastFsn int64
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Table("trades").Int64Column("x", int64(i)).At(ctx, time.Unix(0, int64(i+1)*1_000_000_000)))
		f, err := s.FlushAndGetSequence(ctx)
		require.NoError(t, err)
		lastFsn = f
	}

	// The three OKs reported trades seqTxns 1, 2, 3. Durable-ack to 2
	// covers the first two batches; the third stays pending.
	trigger <- 2
	firstTwo, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, s.AwaitAckedFsn(firstTwo, lastFsn-1))
	cancel()

	// The last batch is not yet durable.
	notYet, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	require.Error(t, s.AwaitAckedFsn(notYet, lastFsn))
	cancel()

	// Cover the last batch.
	trigger <- 3
	all, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, s.AwaitAckedFsn(all, lastFsn))
	cancel()
}

// A server that completes the upgrade WITHOUT echoing durable-ack support
// must fail fast with a typed *QwpDurableAckMismatchError, not silently
// downgrade to OK-driven trim (which would reintroduce the loss window).
func TestQwpDurableAckMismatchFailsFast(t *testing.T) {
	srv := newDurableAckMockServer(false /* does not echo enabled */, nil)
	defer srv.Close()

	_, err := newDurableSenderForTest(t, srv.URL, "")
	require.Error(t, err, "durable-ack request against a non-durable server must fail")
	var mismatch *QwpDurableAckMismatchError
	require.True(t, errors.As(err, &mismatch),
		"want a *QwpDurableAckMismatchError, got %v", err)
}
