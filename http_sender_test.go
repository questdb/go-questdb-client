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
			config: fmt.Sprintf("http::addr=%s;request_timeout=%d;retry_timeout=%d;protocol_version=2;",
				addr, request_timeout.Milliseconds(), retry_timeout.Milliseconds()),
		},
		{
			name: "pass before user",
			config: fmt.Sprintf("http::addr=%s;password=%s;username=%s;protocol_version=2;",
				addr, pass, user),
		},
		{
			name: "request_min_throughput",
			config: fmt.Sprintf("http::addr=%s;request_min_throughput=%d;protocol_version=2;",
				addr, min_throughput),
		},
		{
			name: "bearer token",
			config: fmt.Sprintf("http::addr=%s;token=%s;protocol_version=2;",
				addr, token),
		},
		{
			name: "auto flush",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=1000;protocol_version=2;",
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
			config: fmt.Sprintf("http::addr=%s;protocol_version=1;", addr),
		},
		{
			name: "auto flush",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=1000;protocol_version=2;",
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
			config:      "http::request_min_throughput=-1;",
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
		{
			name:        "protocol version",
			config:      "http::protocol_version=abc;",
			expectedErr: "invalid protocol_version value",
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

func TestTimeBasedAutoFlushWithRowBasedFlushDisabled(t *testing.T) {
	ctx := context.Background()
	autoFlushInterval := 10 * time.Millisecond

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.LineSenderFromConf(ctx,
		fmt.Sprintf("http::addr=%s;auto_flush_rows=off;auto_flush_interval=%d;", srv.Addr(), autoFlushInterval.Milliseconds()))
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

func TestRowBasedAutoFlushWithTimeBasedFlushDisabled(t *testing.T) {
	ctx := context.Background()
	autoFlushRows := 1000

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.LineSenderFromConf(ctx,
		fmt.Sprintf("http::addr=%s;auto_flush_rows=%d;auto_flush_interval=off;", srv.Addr(), autoFlushRows))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows - 1 messages and ensure all are buffered
	for i := 0; i < autoFlushRows-1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
	}

	assert.Equal(t, autoFlushRows-1, qdb.MsgCount(sender))

	// Sleep past the default interval
	time.Sleep(qdb.DefaultAutoFlushInterval + time.Millisecond)

	// Check that the number of messages hasn't changed
	assert.Equal(t, autoFlushRows-1, qdb.MsgCount(sender))

	// Send one additional message and ensure that all are flushed
	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)

	assert.Equal(t, 0, qdb.MsgCount(sender))
}

func TestNoFlushWhenAutoFlushDisabled(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	// opts are processed sequentially, so AutoFlushDisabled will
	// override AutoFlushRows
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushRows(qdb.DefaultAutoFlushRows),
		qdb.WithAutoFlushInterval(qdb.DefaultAutoFlushInterval),
		qdb.WithAutoFlushDisabled(),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows + 1 messages and ensure all are buffered
	for i := 0; i < qdb.DefaultAutoFlushRows+1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
	}

	// Sleep past the default interval
	time.Sleep(qdb.DefaultAutoFlushInterval + time.Millisecond)

	assert.Equal(t, qdb.DefaultAutoFlushRows+1, qdb.MsgCount(sender))
}

func TestNoFlushWhenAutoFlushRowsAndIntervalAre0(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	// opts are processed sequentially, so AutoFlushDisabled will
	// override AutoFlushRows
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushRows(0),
		qdb.WithAutoFlushInterval(0),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	// Send autoFlushRows + 1 messages and ensure all are buffered
	for i := 0; i < qdb.DefaultAutoFlushRows+1; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		assert.NoError(t, err)
	}

	// Sleep past the default interval
	time.Sleep(qdb.DefaultAutoFlushInterval + time.Millisecond)

	assert.Equal(t, qdb.DefaultAutoFlushRows+1, qdb.MsgCount(sender))
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
	assert.Error(t, err)
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
	assert.NotEmpty(t, qdb.Messages(sender))
}

func TestSuccessAfterRetries(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(failFirstThenSendToBackChannel)
	assert.NoError(t, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(
		ctx, qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushDisabled(),
		qdb.WithRetryTimeout(time.Minute),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

	for i := 0; i < 10; i++ {
		err = sender.Table(testTable).Int64Column("foobar", int64(i)).AtNow(ctx)
		assert.NoError(t, err)
	}

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	expected := make([]string, 0)
	for i := 0; i < 10; i++ {
		expected = append(expected, fmt.Sprintf("%s foobar=%di", testTable, i))
	}
	expectLines(t, srv.BackCh, expected)
	assert.Zero(t, qdb.BufLen(sender))
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

	s1, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithProtocolVersion(qdb.ProtocolVersion1))
	assert.NoError(t, err)

	s2, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithTls(), qdb.WithProtocolVersion(qdb.ProtocolVersion2))
	assert.NoError(t, err)

	s3, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithTlsInsecureSkipVerify(), qdb.WithProtocolVersion(qdb.ProtocolVersion2))
	assert.NoError(t, err)

	transport := http.Transport{}
	s4, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithHttpTransport(&transport),
		qdb.WithTls(),
		qdb.WithProtocolVersion(qdb.ProtocolVersion2),
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

func TestAutoDetectProtocolVersionOldServer1(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", nil)
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion1)
	assert.NoError(t, err)
}

func TestAutoDetectProtocolVersionOldServer2(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion1)
	assert.NoError(t, err)
}

func TestAutoDetectProtocolVersionOldServer3(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{1})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion1)
	assert.NoError(t, err)
}

func TestAutoDetectProtocolVersionNewServer1(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{1, 2})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion2)
	assert.NoError(t, err)
}

func TestAutoDetectProtocolVersionNewServer2(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{2})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion2)
	assert.NoError(t, err)
}

func TestAutoDetectProtocolVersionNewServer3(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{2, 3})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion2)
	assert.NoError(t, err)
}

func TestAutoDetectProtocolVersionNewServer4(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{3})
	assert.NoError(t, err)
	defer srv.Close()
	_, err = qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.ErrorContains(t, err, "server does not support current client")
}

func TestAutoDetectProtocolVersionError(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServerWithErrMsg(readAndDiscard, "Internal error")
	assert.NoError(t, err)
	defer srv.Close()
	_, err = qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.ErrorContains(t, err, "failed to detect server line protocol version [http-status=500, http-message={\"code\":\"500\",\"message\":\"Internal error\"}]")
}

func TestSpecifyProtocolVersion(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{1, 2})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()), qdb.WithProtocolVersion(qdb.ProtocolVersion1))
	assert.Equal(t, qdb.ProtocolVersion(sender), qdb.ProtocolVersion1)
	assert.NoError(t, err)
}

func TestArrayColumnUnsupportedInHttpProtocolV1(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := newTestServerWithProtocol(readAndDiscard, "http", []int{1})
	assert.NoError(t, err)
	defer srv.Close()
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(t, err)
	defer sender.Close(ctx)

	values1D := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	values2D := [][]float64{{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}}
	values3D := [][][]float64{{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}}
	arrayND, err := qdb.NewNDArray[float64](2, 2, 1, 2)
	assert.NoError(t, err)
	arrayND.Fill(11.0)

	err = sender.
		Table(testTable).
		Float64Array1DColumn("array_1d", values1D).
		At(ctx, time.UnixMicro(1))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "current protocol version does not support double-array")

	err = sender.
		Table(testTable).
		Float64Array2DColumn("array_2d", values2D).
		At(ctx, time.UnixMicro(2))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "current protocol version does not support double-array")

	err = sender.
		Table(testTable).
		Float64Array3DColumn("array_3d", values3D).
		At(ctx, time.UnixMicro(3))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "current protocol version does not support double-array")

	err = sender.
		Table(testTable).
		Float64ArrayNDColumn("array_nd", arrayND).
		At(ctx, time.UnixMicro(4))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "current protocol version does not support double-array")
}

func BenchmarkHttpLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.Close()

	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(srv.Addr()))
	assert.NoError(b, err)

	values1D := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	values2D := [][]float64{{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}}
	values3D := [][][]float64{{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}}
	arrayND, _ := qdb.NewNDArray[float64](2, 3)
	arrayND.Fill(10.0)

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
				Float64Array1DColumn("array_1d", values1D).
				Float64Array2DColumn("array_2d", values2D).
				Float64Array3DColumn("array_3d", values3D).
				Float64ArrayNDColumn("array_nd", arrayND).
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

	values1D := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	values2D := [][]float64{{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}}
	values3D := [][][]float64{{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}}
	arrayND, _ := qdb.NewNDArray[float64](2, 3)
	arrayND.Fill(10)

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
			Float64Array1DColumn("array_1d", values1D).
			Float64Array2DColumn("array_2d", values2D).
			Float64Array3DColumn("array_3d", values3D).
			Float64ArrayNDColumn("array_nd", arrayND).
			At(ctx, time.UnixMicro(int64(1000*i)))
	}
	sender.Flush(ctx)
	sender.Close(ctx)
}
