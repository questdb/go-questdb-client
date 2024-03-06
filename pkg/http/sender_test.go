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

package http

import (
	"context"
	"fmt"
	"testing"
	"time"

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
		addr           = "localhost:1111"
		user           = "test-user"
		pass           = "test-pass"
		token          = "test-token"
		min_throughput = 999
		request_timeout  = time.Second * 88
		retry_timeout  = time.Second * 99
	)

	testCases := []configTestCase{
		{
			name: "request_timeout and retry_timeout milli conversion",
			config: fmt.Sprintf("http::addr=%s;request_timeout=%d;retry_timeout=%d",
				addr, request_timeout.Milliseconds(), retry_timeout.Milliseconds()),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithRequestTimeout(request_timeout),
				WithRetryTimeout(retry_timeout),
			},
		},
		{
			name: "pass before user",
			config: fmt.Sprintf("http::addr=%s;pass=%s;user=%s",
				addr, pass, user),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithBasicAuth(user, pass),
			},
		},
		{
			name: "min_throughput",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d",
				addr, min_throughput),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithMinThroughput(min_throughput),
			},
		},
		{
			name: "bearer token",
			config: fmt.Sprintf("http::addr=%s;token=%s",
				addr, token),
			expectedOpts: []LineSenderOption{
				WithAddress(addr),
				WithBearerToken(token),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := LineSenderFromConf(context.Background(), tc.config)
			assert.NoError(t, err)

			expected, err := NewLineSender(tc.expectedOpts...)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual)

			actual.Close()
			expected.Close()
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
			config:                 "tcp::addr=localhost:1111",
			expectedErrMsgContains: "invalid schema",
		},
		{
			name:                   "invalid tls_verify",
			config:                 "http::addr=localhost:1111;tls_verify=invalid",
			expectedErrMsgContains: "invalid tls_verify",
		},
		{
			name:                   "unsupported option",
			config:                 "http::addr=localhost:1111;unsupported_option=invalid",
			expectedErrMsgContains: "unsupported option",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LineSenderFromConf(context.Background(), tc.config)
			assert.ErrorContains(t, err, tc.expectedErrMsgContains)
		})
	}
}

func TestErrorOnFlushWhenMessageIsPending(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestTcpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close()

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "pending ILP message must be finalized with At or AtNow before calling Flush")
	assert.Empty(t, sender.Messages())
}

func TestErrorOnContextDeadlineHttp(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := utils.NewTestHttpServer(utils.ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(WithAddress(srv.Addr()))
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

func TestErrorOnInternalServerErrorHttp(t *testing.T) {
	ctx := context.Background()

	srv, err := utils.NewTestHttpServer(utils.ReturningError)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := NewLineSender(
		WithAddress(srv.Addr()),
		WithRequestTimeout(10*time.Millisecond),
	)
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	if err != nil {
		return
	}
	err = sender.Flush(ctx)
	assert.ErrorContains(t, err, "500")

}

func BenchmarkHttpLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := utils.NewTestHttpServer(utils.ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := NewLineSender(WithAddress(srv.Addr()))
	assert.NoError(b, err)

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
		sender.Close()
	}

}

func BenchmarkHttpLineSenderNoFlush(b *testing.B) {
	ctx := context.Background()

	srv, err := utils.NewTestHttpServer(utils.ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := NewLineSender(WithAddress(srv.Addr()))
	assert.NoError(b, err)

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
	sender.Close()

}
