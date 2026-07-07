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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WithDurableAckKeepalive must not let a positive sub-millisecond duration
// truncate to 0 (which would silently DISABLE the keepalive — the opposite of
// the caller's intent). It floors a positive value to the 1ms wire minimum;
// zero and negative still disable.
func TestWithDurableAckKeepaliveSubMillisecondFloor(t *testing.T) {
	cases := []struct {
		d          time.Duration
		wantMillis int
	}{
		{500 * time.Microsecond, 1}, // sub-ms floored to 1, NOT disabled
		{1 * time.Nanosecond, 1},    // smallest positive still floors to 1
		{999 * time.Microsecond, 1},
		{1 * time.Millisecond, 1},
		{5 * time.Millisecond, 5},
		{200 * time.Millisecond, 200},
		{0, 0},                      // explicit zero disables
		{-3 * time.Millisecond, -3}, // negative disables
	}
	for _, tc := range cases {
		conf := &lineSenderConfig{}
		WithDurableAckKeepalive(tc.d)(conf)
		assert.True(t, conf.durableAckKeepaliveMillisSet,
			"WithDurableAckKeepalive(%v) must mark the interval as explicitly set", tc.d)
		assert.Equal(t, tc.wantMillis, conf.durableAckKeepaliveMillis,
			"WithDurableAckKeepalive(%v) millis", tc.d)
	}
}

// The durable-ack options are QWP-only. The connect-string parser rejects
// request_durable_ack / durable_ack_keepalive_interval_millis on non-ws/wss
// schemas; the programmatic With* path must reject them too (via
// rejectQwpOnlyOptions) so the two configuration surfaces stay consistent.
func TestQwpDurableAckOptionsRejectedOnNonQwp(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		opts []LineSenderOption
		want string
	}{
		{
			"http+request_durable_ack",
			[]LineSenderOption{WithHttp(), WithAddress("localhost:9000"), WithRequestDurableAck(true)},
			"request_durable_ack",
		},
		{
			"http+keepalive",
			[]LineSenderOption{WithHttp(), WithAddress("localhost:9000"), WithDurableAckKeepalive(200 * time.Millisecond)},
			"durable_ack_keepalive_interval_millis",
		},
		{
			"tcp+request_durable_ack",
			[]LineSenderOption{WithTcp(), WithAddress("localhost:9009"), WithRequestDurableAck(true)},
			"request_durable_ack",
		},
		{
			"tcp+keepalive",
			[]LineSenderOption{WithTcp(), WithAddress("localhost:9009"), WithDurableAckKeepalive(50 * time.Millisecond)},
			"durable_ack_keepalive_interval_millis",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewLineSender(ctx, tc.opts...)
			require.Error(t, err, "durable-ack option on a non-QWP sender must be rejected")
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "only available in the QWP client")
		})
	}

	// request_durable_ack=false is the default and must NOT trip the QWP-only
	// gate: construction proceeds past sanitize (and then fails only because no
	// HTTP server is listening at the address, not because the option was
	// rejected). So any error here must not be the QWP-only rejection.
	_, err := NewLineSender(ctx, WithHttp(), WithAddress("localhost:9000"), WithRequestDurableAck(false))
	if err != nil {
		assert.NotContains(t, err.Error(), "only available in the QWP client",
			"WithRequestDurableAck(false) is a no-op and must not be rejected as a QWP-only option")
	}
}

// TotalDurableAcks counts DURABLE_ACK frames processed. It stays 0 until a
// covering DURABLE_ACK arrives, then climbs — the heartbeat that durability
// (not just OK receipt) is progressing.
func TestQwpDurableAckCounterIncrements(t *testing.T) {
	trigger := make(chan int64, 4)
	srv := newDurableAckMockServer(true, trigger)
	defer srv.Close()

	s, err := newDurableSenderForTest(t, srv.URL, "")
	require.NoError(t, err)
	defer s.Close(context.Background())

	ctx := context.Background()
	require.Equal(t, int64(0), s.TotalDurableAcks(),
		"no DURABLE_ACK processed before the first covering frame")

	require.NoError(t, s.Table("trades").Int64Column("x", 1).At(ctx, time.Unix(0, 1_000_000_000)))
	fsn, err := s.FlushAndGetSequence(ctx)
	require.NoError(t, err)

	// The OK alone (received + local WAL) must not advance the counter.
	require.Equal(t, int64(0), s.TotalDurableAcks(),
		"an OK is not a DURABLE_ACK and must not bump the counter")

	// Release a covering DURABLE_ACK (trades seqTxn 1 covers wireSeq 0).
	trigger <- 1
	longCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	require.NoError(t, s.AwaitAckedFsn(longCtx, fsn),
		"AwaitAckedFsn must complete once the covering DURABLE_ACK is processed")
	cancel()

	require.GreaterOrEqual(t, s.TotalDurableAcks(), int64(1),
		"TotalDurableAcks must count the processed DURABLE_ACK frame")
}
