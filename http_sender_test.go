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
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

type httpConfigTestCase struct {
	name        string
	config      string
	expectedErr string
}

func TestHttpHappyCasesFromConf(t *testing.T) {
	var (
		addr            = "localhost:1111"
		user            = "test-user"
		pass            = "test-pass"
		token           = "test-token"
		min_throughput  = 999
		request_timeout = time.Second * 88
		retry_timeout   = time.Second * 99
	)

	testCases := []httpConfigTestCase{
		{
			name: "request_timeout and retry_timeout milli conversion",
			config: fmt.Sprintf("http::addr=%s;request_timeout=%d;retry_timeout=%d;",
				addr, request_timeout.Milliseconds(), retry_timeout.Milliseconds()),
		},
		{
			name: "pass before user",
			config: fmt.Sprintf("http::addr=%s;password=%s;username=%s;",
				addr, pass, user),
		},
		{
			name: "min_throughput",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d;",
				addr, min_throughput),
		},
		{
			name: "bearer token",
			config: fmt.Sprintf("http::addr=%s;token=%s;",
				addr, token),
		},
		{
			name: "auto flush",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=1000;",
				addr),
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

func TestHttpHappyCasesFromEnv(t *testing.T) {
	var (
		addr = "localhost:1111"
	)

	testCases := []httpConfigTestCase{
		{
			name:   "addr only",
			config: fmt.Sprintf("http::addr=%s", addr),
		},
		{
			name: "auto flush",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=1000;",
				addr),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv("QDB_CLIENT_CONF", tc.config)
			sender, err := qdb.LineSenderFromEnv(context.Background())
			assert.NoError(t, err)

			sender.Close(context.Background())
			os.Unsetenv("QDB_CLIENT_CONF")
		})
	}
}

func TestHttpPathologicalCasesFromConf(t *testing.T) {
	testCases := []httpConfigTestCase{
		{
			name:        "basic_and_token_auth",
			config:      "http::username=test_user;token=test_token;",
			expectedErr: "both basic and token",
		},
		{
			name:        "negative init_buf_size",
			config:      "http::init_buf_size=-1;",
			expectedErr: "initial buffer size is negative",
		},
		{
			name:        "negative max_buf_size",
			config:      "http::max_buf_size=-1;",
			expectedErr: "max buffer size is negative",
		},
		{
			name:        "negative retry timeout",
			config:      "http::retry_timeout=-1;",
			expectedErr: "retry timeout is negative",
		},
		{
			name:        "negative request timeout",
			config:      "http::request_timeout=-1;",
			expectedErr: "request timeout is negative",
		},
		{
			name:        "negative min throughput",
			config:      "http::min_throughput=-1;",
			expectedErr: "min throughput is negative",
		},
		{
			name:        "negative auto flush rows",
			config:      "http::auto_flush_rows=-1;",
			expectedErr: "auto flush rows is negative",
		},
		{
			name:        "negative auto flush interval",
			config:      "http::auto_flush_interval=-1;",
			expectedErr: "auto flush interval is negative",
		},
		{
			name:        "schema is case-sensitive",
			config:      "hTtp::addr=localhost:1234;",
			expectedErr: "invalid schema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qdb.LineSenderFromConf(context.Background(), tc.config)
			assert.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

func TestHttpPathologicalCasesFromEnv(t *testing.T) {
	// Test a few cases just to make sure that the config is read
	// from the env variable.
	testCases := []httpConfigTestCase{
		{
			name:        "basic_and_token_auth",
			config:      "http::username=test_user;token=test_token;",
			expectedErr: "both basic and token",
		},
		{
			name:        "negative max_buf_size",
			config:      "http::max_buf_size=-1;",
			expectedErr: "max buffer size is negative",
		},
		{
			name:        "schema is case-sensitive",
			config:      "hTtp::addr=localhost:1234;",
			expectedErr: "invalid schema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			os.Setenv("QDB_CLIENT_CONF", tc.config)
			_, err := qdb.LineSenderFromEnv(context.Background())
			assert.ErrorContains(t, err, tc.expectedErr)
			os.Unsetenv("QDB_CLIENT_CONF")
		})
	}
}

func TestHttpEmptyEnvVariableCaseFromEnv(t *testing.T) {
	_, err := qdb.LineSenderFromEnv(context.Background())
	assert.ErrorContains(t, err, "QDB_CLIENT_CONF environment variable is not set")
}

func TestErrorWhenSenderTypeIsNotSpecified(t *testing.T) {
	ctx := context.Background()

	_, err := qdb.NewLineSender(ctx)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "sender type is not specified: use WithHttp or WithTcp")
}

func TestHttpErrorWhenMaxBufferSizeIsReached(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithInitBufferSize(4),
		qdb.WithMaxBufferSize(8),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Table(testTable).Symbol("sym", "foobar").AtNow(ctx)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "buffer size exceeded maximum limit")
	assert.Empty(t, qdb.Messages(sender))
}

func TestHttpErrorOnFlushWhenMessageIsPending(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "pending ILP message must be finalized with At or AtNow before calling Flush")
	assert.Empty(t, qdb.Messages(sender))
}

func TestNoOpOnFlushWhenNoMessagesAreWritten(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Flush(ctx)

	assert.NoError(t, err)
	assert.Empty(t, qdb.Messages(sender))
}

func TestErrorOnContextDeadlineHttp(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
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

func TestRetryOn500(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(returning500)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithRequestTimeout(10*time.Millisecond),
		qdb.WithRetryTimeout(50*time.Millisecond),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	if err != nil {
		return
	}
	err = sender.Flush(ctx)
	retryErr := &qdb.RetryTimeoutError{}
	assert.ErrorAs(t, err, &retryErr)
	assert.ErrorContains(t, retryErr.LastErr, "500")
}

func TestNoRetryOn400FromProxy(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(returning403)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithRequestTimeout(10*time.Millisecond),
		qdb.WithRetryTimeout(50*time.Millisecond),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	if err != nil {
		return
	}
	err = sender.Flush(ctx)
	retryTimeoutErr := &qdb.RetryTimeoutError{}
	assert.False(t, errors.As(err, &retryTimeoutErr))
	assert.ErrorContains(t, err, "Forbidden")
}

func TestNoRetryOn400FromServer(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(returning404)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithRequestTimeout(10*time.Millisecond),
		qdb.WithRetryTimeout(50*time.Millisecond),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	if err != nil {
		return
	}
	err = sender.Flush(ctx)
	httpErr := &qdb.HttpError{}
	assert.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusNotFound, httpErr.HttpStatus())
	assert.Equal(t, "Not Found", httpErr.Message)
	assert.Equal(t, 42, httpErr.Line)
}

func TestRowBasedAutoFlush(t *testing.T) {
	ctx := context.Background()
	autoFlushRows := 10

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushRows(autoFlushRows),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows - 1 messages and ensure all are buffered
	for i := 0; i < autoFlushRows-1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
	}

	assert.Equal(t, autoFlushRows-1, qdb.MsgCount(sender))

	// Send one additional message and ensure that all are flushed
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)

	assert.Equal(t, 0, qdb.MsgCount(sender))
}

func TestTimeBasedAutoFlush(t *testing.T) {
	ctx := context.Background()
	autoFlushInterval := 10 * time.Millisecond

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushRows(1000),
		qdb.WithAutoFlushInterval(autoFlushInterval),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send a message and ensure it's buffered
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, qdb.MsgCount(sender))

	time.Sleep(2 * autoFlushInterval)

	// Send one additional message and ensure that both messages are flushed
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)

	assert.Equal(t, 0, qdb.MsgCount(sender))
}

func TestNoFlushWhenAutoFlushDisabled(t *testing.T) {
	ctx := context.Background()
	autoFlushRows := 10
	autoFlushInterval := time.Duration(autoFlushRows-1) * time.Millisecond

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	// opts are processed sequentially, so AutoFlushDisabled will
	// override AutoFlushRows
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushRows(autoFlushRows),
		qdb.WithAutoFlushInterval(autoFlushInterval),
		qdb.WithAutoFlushDisabled(),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows + 1 messages and ensure all are buffered
	for i := 0; i < autoFlushRows+1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, autoFlushRows+1, qdb.MsgCount(sender))
}

func TestSenderDoubleClose(t *testing.T) {
	ctx := context.Background()
	autoFlushRows := 10

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	// opts are processed sequentially, so AutoFlushDisabled will
	// override AutoFlushRows
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushRows(autoFlushRows),
		qdb.WithAutoFlushDisabled(),
	)
	assert.NoError(t, err)

	err = sender.Close(ctx)
	assert.NoError(t, err)

	err = sender.Close(ctx)
	assert.NoError(t, err)
}

func TestErrorOnFlushWhenSenderIsClosed(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	err = sender.Close(ctx)
	assert.NoError(t, err)

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "cannot flush a closed LineSender")
}

func TestAutoFlushWhenSenderIsClosed(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)

	err = sender.Table(testTable).Symbol("abc", "def").AtNow(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, qdb.Messages(sender))

	err = sender.Close(ctx)
	assert.NoError(t, err)
	assert.Empty(t, qdb.Messages(sender))
}

func TestNoFlushWhenSenderIsClosedAndAutoFlushIsDisabled(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushDisabled(),
	)
	assert.NoError(t, err)

	err = sender.Table(testTable).Symbol("abc", "def").AtNow(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, qdb.Messages(sender))

	err = sender.Close(ctx)
	assert.NoError(t, err)
	assert.Empty(t, qdb.Messages(sender))
}

func TestBufferClearAfterFlush(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(sendToBackChannel)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Table(testTable).Symbol("abc", "def").AtNow(ctx)
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	expectLines(t, srv.BackCh, []string{fmt.Sprintf("%s,abc=def", testTable)})
	assert.Zero(t, qdb.BufLen(sender))

	err = sender.Table(testTable).Symbol("ghi", "jkl").AtNow(ctx)
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	expectLines(t, srv.BackCh, []string{fmt.Sprintf("%s,ghi=jkl", testTable)})
}

func TestCustomTransportAndTlsInit(t *testing.T) {
	ctx := context.Background()

	s1, err := qdb.NewLineSender(ctx, qdb.WithHttp())
	assert.NoError(t, err)

	s2, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithTls())
	assert.NoError(t, err)

	s3, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithTlsInsecureSkipVerify())
	assert.NoError(t, err)

	transport := http.Transport{}
	s4, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithHttpTransport(&transport),
		qdb.WithTls(),
	)
	assert.NoError(t, err)

	// s1 and s2 have successfully instantiated a sender
	// using the global transport and should be registered in the
	// global transport client count
	assert.Equal(t, int64(2), qdb.GlobalTransport.ClientCount())

	// Closing the client with the custom transport should not impact
	// the global transport client count
	s4.Close(ctx)

	// Now close all remaining clients
	s1.Close(ctx)
	s2.Close(ctx)
	s3.Close(ctx)
	assert.Equal(t, int64(0), qdb.GlobalTransport.ClientCount())
}

func BenchmarkHttpLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
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
		sender.Close(ctx)
	}
}

func BenchmarkHttpLineSenderNoFlush(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
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
	sender.Close(ctx)
}
