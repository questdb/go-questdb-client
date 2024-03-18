/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package questdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

const (
	testTable   = "my_test_table"
	networkName = "test-network-v3"
)

type tcpConfigTestCase struct {
	name        string
	config      string
	expectedErr string
}

func TestTcpHappyCasesFromConf(t *testing.T) {
	var (
		//user        = "test-user"
		//token       = "test-token"
		initBufSize = 1000
	)

	testServer, err := newTestTcpServer(readAndDiscard)
	assert.NoError(t, err)
	defer testServer.Close()

	addr := testServer.Addr()

	testCases := []tcpConfigTestCase{
		// TODO(sklarsa): test Client doesn't support tls auth handshake
		/*
			{
				name: "user and token",
				config: fmt.Sprintf("tcp::addr=%s;user=%s;token=%s",
					addr, user, token),
			},
		*/
		{
			name: "init_buf_size",
			config: fmt.Sprintf("tcp::addr=%s;init_buf_size=%d",
				addr, initBufSize),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sender, err := qdb.LineSenderFromConf(context.Background(), tc.config)
			assert.NoError(t, err)

			sender.Close(context.Background())
		})
	}
}

func TestTcpPathologicalCasesFromConf(t *testing.T) {

	testCases := []tcpConfigTestCase{
		{
			name:        "request_timeout",
			config:      "tcp::request_timeout=5",
			expectedErr: "requestTimeout setting is not available",
		},
		{
			name:        "retry_timeout",
			config:      "tcp::retry_timeout=5",
			expectedErr: "retryTimeout setting is not available",
		},
		{
			name:        "min_throughput",
			config:      "tcp::min_throughput=5",
			expectedErr: "minThroughput setting is not available",
		},
		{
			name:        "auto_flush_rows",
			config:      "tcp::auto_flush_rows=5",
			expectedErr: "autoFlushRows setting is not available",
		},
		{
			name:        "auto_flush_interval",
			config:      "tcp::auto_flush_interval=5",
			expectedErr: "autoFlushInterval setting is not available",
		},
		{
			name:        "tcp key but no id",
			config:      "tcp::token=test_key",
			expectedErr: "tcpKeyId is empty",
		},
		{
			name:        "tcp key id but no key",
			config:      "tcp::user=test_key_id",
			expectedErr: "tcpKey is empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qdb.LineSenderFromConf(context.Background(), tc.config)
			assert.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
func TestErrorOnFlushWhenMessageIsPending(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestTcpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "pending ILP message must be finalized with At or AtNow before calling Flush")
	assert.Empty(t, qdb.Messages(sender))
}

func TestErrorOnUnavailableServer(t *testing.T) {
	ctx := context.Background()

	_, err := qdb.NewLineSender(ctx, qdb.WithTcp())
	assert.ErrorContains(t, err, "failed to connect to server")
}

func TestErrorOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := newTestTcpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// The context is not cancelled yet, so Flush should succeed.
	err = sender.Table(testTable).StringColumn("foo", "bar").AtNow(ctx)
	assert.NoError(t, err)
	err = sender.Flush(ctx)
	assert.NoError(t, err)

	cancel()

	// The context is now cancelled, so we expect an error.
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)
	err = sender.Flush(ctx)
	assert.Error(t, err)
}

func TestErrorOnContextDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := newTestTcpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Keep writing until we get an error due to the context deadline.
	for i := 0; i < 100_000; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		if err != nil {
			return
		}
		err = sender.Flush(ctx)
		if err != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fail()
}

func BenchmarkLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestTcpServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(b, err)
	defer sender.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			sender.
				Table(testTable).
				Symbol("sym_col", "test_ilp1").
				Float64Column("double_col", float64(i)+0.42).
				Int64Column("long_col", int64(i)).
				StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
				BoolColumn("bool_col", true).
				TimestampColumn("timestamp_col", time.UnixMicro(42)).
				At(ctx, time.UnixMicro(int64(1000*i)))
		}
		sender.Flush(ctx)
	}
}

func BenchmarkLineSenderNoFlush(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestTcpServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(b, err)
	defer sender.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sender.
			Table(testTable).
			Symbol("sym_col", "test_ilp1").
			Float64Column("double_col", float64(i)+0.42).
			Int64Column("long_col", int64(i)).
			StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
			BoolColumn("bool_col", true).
			TimestampColumn("timestamp_col", time.UnixMicro(42)).
			At(ctx, time.UnixMicro(int64(1000*i)))
	}
	sender.Flush(ctx)
}
