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
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

type httpConfigTestCase struct {
	name                   string
	config                 string
	expectedOpts           []qdb.HttpLineSenderOption
	expectedErrMsgContains string
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
			config: fmt.Sprintf("http::addr=%s;request_timeout=%d;retry_timeout=%d",
				addr, request_timeout.Milliseconds(), retry_timeout.Milliseconds()),
			expectedOpts: []qdb.HttpLineSenderOption{
				qdb.WithHttpAddress(addr),
				qdb.WithHttpRequestTimeout(request_timeout),
				qdb.WithHttpRetryTimeout(retry_timeout),
			},
		},
		{
			name: "pass before user",
			config: fmt.Sprintf("http::addr=%s;pass=%s;user=%s",
				addr, pass, user),
			expectedOpts: []qdb.HttpLineSenderOption{
				qdb.WithHttpAddress(addr),
				qdb.WithHttpBasicAuth(user, pass),
			},
		},
		{
			name: "min_throughput",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d",
				addr, min_throughput),
			expectedOpts: []qdb.HttpLineSenderOption{
				qdb.WithHttpAddress(addr),
				qdb.WithHttpMinThroughput(min_throughput),
			},
		},
		{
			name: "bearer token",
			config: fmt.Sprintf("http::addr=%s;token=%s",
				addr, token),
			expectedOpts: []qdb.HttpLineSenderOption{
				qdb.WithHttpAddress(addr),
				qdb.WithHttpBearerToken(token),
			},
		},
		{
			name: "auto flush",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=1000",
				addr),
			expectedOpts: []qdb.HttpLineSenderOption{
				qdb.WithHttpAddress(addr),
				qdb.WithHttpAutoFlushRows(100),
				qdb.WithHttpAutoFlushInterval(1000),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := qdb.HttpLineSenderFromConf(context.Background(), tc.config)
			assert.NoError(t, err)

			expected, err := qdb.NewHttpLineSender(tc.expectedOpts...)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual)

			actual.Close(context.Background())
			expected.Close(context.Background())
		})
	}
}

func TestHttpPathologicalCasesFromConf(t *testing.T) {
	testCases := []httpConfigTestCase{
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
			name:                   "invalid auto_flush",
			config:                 "http::addr=localhost:1111;auto_flush=invalid",
			expectedErrMsgContains: "invalid auto_flush",
		},
		{
			name:                   "invalid auto_flush_rows (not int)",
			config:                 "http::addr=localhost:1111;auto_flush_rows=invalid",
			expectedErrMsgContains: "invalid auto_flush_rows",
		},
		{
			name:                   "invalid auto_flush_rows (non-positive)",
			config:                 "http::addr=localhost:1111;auto_flush_rows=-1",
			expectedErrMsgContains: "invalid auto_flush_rows",
		},
		{
			name:                   "invalid auto_flush_interval (not int)",
			config:                 "http::addr=localhost:1111;auto_flush_interval=invalid",
			expectedErrMsgContains: "invalid auto_flush_interval",
		},
		{
			name:                   "invalid auto_flush_interval (non-positive)",
			config:                 "http::addr=localhost:1111;auto_flush_interval=-1",
			expectedErrMsgContains: "invalid auto_flush_interval",
		},
		{
			name:                   "unsupported option",
			config:                 "http::addr=localhost:1111;unsupported_option=invalid",
			expectedErrMsgContains: "unsupported option",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qdb.HttpLineSenderFromConf(context.Background(), tc.config)
			assert.ErrorContains(t, err, tc.expectedErrMsgContains)
		})
	}
}

func TestHttpErrorOnFlushWhenMessageIsPending(t *testing.T) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "pending ILP message must be finalized with At or AtNow before calling Flush")
	assert.Empty(t, sender.Messages())
}

func TestNoOpOnFlushWhenNoMessagesAreWritten(t *testing.T) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Flush(ctx)

	assert.NoError(t, err)
	assert.Empty(t, sender.Messages())
}

func TestErrorOnContextDeadlineHttp(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
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

	srv, err := NewTestHttpServer(Returning500)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpRequestTimeout(10*time.Millisecond),
		qdb.WithHttpRetryTimeout(50*time.Millisecond),
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

	srv, err := NewTestHttpServer(Returning403)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpRequestTimeout(10*time.Millisecond),
		qdb.WithHttpRetryTimeout(50*time.Millisecond),
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

	srv, err := NewTestHttpServer(Returning404)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpRequestTimeout(10*time.Millisecond),
		qdb.WithHttpRetryTimeout(50*time.Millisecond),
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

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpAutoFlushRows(autoFlushRows),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows - 1 messages and ensure all are buffered
	for i := 0; i < autoFlushRows-1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
	}

	assert.Equal(t, autoFlushRows-1, sender.MsgCount())

	// Send one additional message and ensure that all are flushed
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)

	assert.Equal(t, 0, sender.MsgCount())
}

func TestTimeBasedAutoFlush(t *testing.T) {
	ctx := context.Background()
	autoFlushInterval := 10 * time.Millisecond

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpAutoFlushRows(1000),
		qdb.WithHttpAutoFlushInterval(autoFlushInterval),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send a message and ensure it's buffered
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, sender.MsgCount())

	time.Sleep(2 * autoFlushInterval)

	// Send one additional message and ensure that both messages are flushed
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)

	assert.Equal(t, 0, sender.MsgCount())
}

func TestNoFlushWhenAutoFlushDisabled(t *testing.T) {
	ctx := context.Background()
	autoFlushRows := 10
	autoFlushInterval := time.Duration(autoFlushRows-1) * time.Millisecond

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	// opts are processed sequentially, so AutoFlushDisabled will
	// override AutoFlushRows
	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpAutoFlushRows(autoFlushRows),
		qdb.WithHttpAutoFlushInterval(autoFlushInterval),
		qdb.WithHttpAutoFlushDisabled(),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows + 1 messages and ensure all are buffered
	for i := 0; i < autoFlushRows+1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, autoFlushRows+1, sender.MsgCount())
}

func TestSenderDoubleClose(t *testing.T) {
	ctx := context.Background()
	autoFlushRows := 10

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	// opts are processed sequentially, so AutoFlushDisabled will
	// override AutoFlushRows
	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpAutoFlushRows(autoFlushRows),
		qdb.WithHttpAutoFlushDisabled(),
	)
	assert.NoError(t, err)

	err = sender.Close(ctx)
	assert.NoError(t, err)

	err = sender.Close(ctx)
	assert.NoError(t, err)
}

func TestErrorOnFlushWhenSenderIsClosed(t *testing.T) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
	assert.NoError(t, err)
	err = sender.Close(ctx)
	assert.NoError(t, err)

	sender.Table(testTable)
	err = sender.Flush(ctx)

	assert.ErrorContains(t, err, "cannot flush a closed LineSender")
}

func TestAutoFlushWhenSenderIsClosed(t *testing.T) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
	assert.NoError(t, err)

	err = sender.Table(testTable).Symbol("abc", "def").AtNow(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, sender.Messages())

	err = sender.Close(ctx)
	assert.NoError(t, err)
	assert.Empty(t, sender.Messages())
}

func TestNoFlushWhenSenderIsClosedAndAutoFlushIsDisabled(t *testing.T) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.Addr()),
		qdb.WithHttpAutoFlushDisabled(),
	)
	assert.NoError(t, err)

	err = sender.Table(testTable).Symbol("abc", "def").AtNow(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, sender.Messages())

	err = sender.Close(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, sender.Messages())
}

func TestBufferClearAfterFlush(t *testing.T) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(SendToBackChannel)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	err = sender.Table(testTable).Symbol("abc", "def").AtNow(ctx)
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	ExpectLines(t, srv.BackCh, []string{fmt.Sprintf("%s,abc=def", testTable)})
	assert.Zero(t, sender.BufLen())

	err = sender.Table(testTable).Symbol("ghi", "jkl").AtNow(ctx)
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	ExpectLines(t, srv.BackCh, []string{fmt.Sprintf("%s,ghi=jkl", testTable)})
}

func TestCustomTransportAndTlsInit(t *testing.T) {
	ctx := context.Background()

	s1, err := qdb.NewHttpLineSender()
	assert.NoError(t, err)

	s2, err := qdb.NewHttpLineSender(qdb.WithHttpTls())
	assert.NoError(t, err)

	s3, err := qdb.NewHttpLineSender(qdb.WithHttpTlsInsecureSkipVerify())
	assert.NoError(t, err)

	_, err = qdb.NewHttpLineSender(qdb.WithHttpTls())
	assert.ErrorContains(t, err, "once InsecureSkipVerify is used with the default")

	transport := http.Transport{}
	s4, err := qdb.NewHttpLineSender(
		qdb.WithHttpTransport(&transport),
		qdb.WithHttpTls(),
	)
	assert.NoError(t, err)

	// s1, s2, and s3 all have successfully instantiated a sender
	// using the global transport and should be registered in the
	// global transport client count
	assert.Equal(t, int64(3), qdb.ClientCt.Load())

	// Closing the client with the custom transport should not impact
	// the global transport client count
	s4.Close(ctx)

	// Now close all remaining clients
	s1.Close(ctx)
	s2.Close(ctx)
	s3.Close(ctx)
	assert.Equal(t, int64(0), qdb.ClientCt.Load())
}

func BenchmarkHttpLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
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

	srv, err := NewTestHttpServer(ReadAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.Addr()))
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
