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

package tcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/questdb/go-questdb-client/v3/pkg/buffer"
	"github.com/questdb/go-questdb-client/v3/pkg/test/utils"
	"github.com/stretchr/testify/assert"
)

const (
	testTable   = "my_test_table"
	networkName = "test-network-v3"
)

type configTestCase struct {
	name                   string
	config                 string
	expectedOpts           []LineSenderOption
	expectedErrMsgContains string
}

func TestHappyCasesFromConf(t *testing.T) {

	var (
		user        = "test-user"
		token       = "test-token"
		initBufSize = 999
		maxBufSize  = 1000
	)

	testServer, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer testServer.Close()

	addr := testServer.Addr()

	testCases := []configTestCase{
		{
			name: "user and token",
			config: fmt.Sprintf("tcp::addr=%s;user=%s;token=%s",
				addr, user, token),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithAuth(user, token),
			},
		},
		{
			name: "init_buf_size and max_buf_size",
			config: fmt.Sprintf("tcp::addr=%s;init_buf_size=%d;max_buf_size=%d",
				addr, initBufSize, maxBufSize),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithInitBufferSize(initBufSize),
				WithBufferCapacity(maxBufSize),
			},
		},
		{
			name: "with tls",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=on",
				addr),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithTls(),
			},
		},
		{
			name: "with tls and unsafe_off",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=unsafe_off",
				addr),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithTlsInsecureSkipVerify(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualOpts, err := optsFromConf(tc.config)
			assert.NoError(t, err)

			actual := &LineSender{
				buf: buffer.NewBuffer(),
			}
			for _, opt := range actualOpts {
				opt(actual)
			}
			expected := &LineSender{
				buf: buffer.NewBuffer(),
			}
			for _, opt := range tc.expectedOpts {
				opt(expected)
			}

			assert.Equal(t, expected, actual)
		})
	}
}

func TestPathologicalCasesFromConf(t *testing.T) {
	testCases := []configTestCase{
		{
			name:                   "empty config",
			config:                 "",
			expectedErrMsgContains: "no schema separator found",
		},
		{
			name:                   "invalid schema",
			config:                 "http::addr=localhost:1111",
			expectedErrMsgContains: "invalid schema",
		},
		{
			name:                   "invalid tls_verify",
			config:                 "tcp::addr=localhost:1111;tls_verify=invalid",
			expectedErrMsgContains: "invalid tls_verify",
		},
		{
			name:                   "unsupported option",
			config:                 "tcp::addr=localhost:1111;unsupported_option=invalid",
			expectedErrMsgContains: "unsupported option",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := optsFromConf(tc.config)
			assert.ErrorContains(t, err, tc.expectedErrMsgContains)
		})
	}
}

func TestErrorOnFlushWhenMessageIsPending(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "pending ILP message must be finalized with At or AtNow before calling Flush")
	assert.Empty(t, sender.buf.Messages())
}

func TestErrorOnUnavailableServer(t *testing.T) {
	ctx := context.Background()

	_, err := NewLineSender(ctx)
	assert.ErrorContains(t, err, "failed to connect to server")
}

func TestErrorOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

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

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

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

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(b, err)
	defer sender.Close()

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

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := NewLineSender(ctx, WithAddress(srv.Addr()))
	assert.NoError(b, err)
	defer sender.Close()

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
