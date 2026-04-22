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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// --- QwpQueryClientFromConf parse tests ---

func TestQwpQueryClientFromConfHappyPath(t *testing.T) {
	cases := []struct {
		name string
		conf string
		chk  func(t *testing.T, c *qwpQueryClientConfig)
	}{
		{
			name: "minimal_ws",
			conf: "ws::addr=localhost:9000;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.address != "localhost:9000" {
					t.Errorf("address=%q", c.address)
				}
				if c.endpointPath != qwpReadPath {
					t.Errorf("endpointPath=%q", c.endpointPath)
				}
				if c.tlsMode != tlsDisabled {
					t.Errorf("tlsMode=%v", c.tlsMode)
				}
				if c.bufferPoolSize != qwpDefaultEgressBufferPoolSize {
					t.Errorf("bufferPoolSize=%d", c.bufferPoolSize)
				}
			},
		},
		{
			name: "wss_enables_tls",
			conf: "wss::addr=db.example:9000;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.tlsMode != tlsEnabled {
					t.Errorf("tlsMode=%v, want tlsEnabled", c.tlsMode)
				}
			},
		},
		{
			name: "all_keys",
			conf: "wss::addr=db.example:9443;path=/read/v2;" +
				"username=bob;password=hunter2;" +
				"client_id=dashboard/1.0;" +
				"buffer_pool_size=8;max_batch_rows=50000;" +
				"initial_credit=131072;" +
				"tls_verify=unsafe_off;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.address != "db.example:9443" {
					t.Errorf("address=%q", c.address)
				}
				if c.endpointPath != "/read/v2" {
					t.Errorf("endpointPath=%q", c.endpointPath)
				}
				if c.httpUser != "bob" || c.httpPass != "hunter2" {
					t.Errorf("basic auth user/pass = %q/%q", c.httpUser, c.httpPass)
				}
				if c.clientID != "dashboard/1.0" {
					t.Errorf("clientID=%q", c.clientID)
				}
				if c.bufferPoolSize != 8 {
					t.Errorf("bufferPoolSize=%d", c.bufferPoolSize)
				}
				if c.maxBatchRows != 50000 {
					t.Errorf("maxBatchRows=%d", c.maxBatchRows)
				}
				if c.initialCredit != 131072 {
					t.Errorf("initialCredit=%d", c.initialCredit)
				}
				if c.tlsMode != tlsInsecureSkipVerify {
					t.Errorf("tlsMode=%v, want insecureSkipVerify", c.tlsMode)
				}
			},
		},
		{
			name: "auth_header",
			conf: "ws::addr=a:1;auth=Bearer abc;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.authorization != "Bearer abc" {
					t.Errorf("authorization=%q", c.authorization)
				}
				if got := c.effectiveAuthorization(); got != "Bearer abc" {
					t.Errorf("effectiveAuthorization=%q", got)
				}
			},
		},
		{
			name: "bearer_token",
			conf: "ws::addr=a:1;token=xyz;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.httpToken != "xyz" {
					t.Errorf("httpToken=%q", c.httpToken)
				}
				if got := c.effectiveAuthorization(); got != "Bearer xyz" {
					t.Errorf("effectiveAuthorization=%q", got)
				}
			},
		},
		{
			name: "basic_auth_encoded",
			conf: "ws::addr=a:1;username=u;password=p;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				want := "Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
				if got := c.effectiveAuthorization(); got != want {
					t.Errorf("effectiveAuthorization=%q, want %q", got, want)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := parseQwpQueryConf(tc.conf)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			tc.chk(t, c)
		})
	}
}

func TestQwpQueryClientFromConfErrors(t *testing.T) {
	cases := []struct {
		name    string
		conf    string
		wantSub string
	}{
		{"bad_schema", "http::addr=a:1;", "invalid schema"},
		{"bad_buffer_pool", "ws::addr=a:1;buffer_pool_size=abc;", "invalid buffer_pool_size"},
		{"buffer_pool_zero", "ws::addr=a:1;buffer_pool_size=0;", "buffer pool size must be >= 1"},
		{"max_batch_rows_negative", "ws::addr=a:1;max_batch_rows=-1;", "max batch rows must be >= 0"},
		{"max_batch_rows_too_big", "ws::addr=a:1;max_batch_rows=99999999;", "exceeds client cap"},
		{"mutually_exclusive_auth", "ws::addr=a:1;auth=X;token=Y;", "mutually exclusive"},
		{"basic_missing_password", "ws::addr=a:1;username=u;", "both username and password"},
		{"unknown_key", "ws::addr=a:1;weird=1;", "unsupported option"},
		{"tls_on_ws", "ws::addr=a:1;tls_verify=on;", "tls_verify requires"},
		{"tls_bad", "wss::addr=a:1;tls_verify=off;", "invalid tls_verify"},
		{"tls_roots_rejected", "wss::addr=a:1;tls_roots=/tmp/foo;", "tls_roots is not available"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseQwpQueryConf(tc.conf)
			if err == nil {
				t.Fatalf("expected error for %q", tc.conf)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err=%v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

// --- Functional options tests ---

func TestQwpQueryClientOptionsApply(t *testing.T) {
	cfg := qwpQueryDefaultConfig()
	for _, opt := range []QwpQueryClientOption{
		WithQwpQueryAddress("example:9000"),
		WithQwpQueryEndpointPath("/read/v2"),
		WithQwpQueryBasicAuth("u", "p"),
		WithQwpQueryBufferPoolSize(16),
		WithQwpQueryMaxBatchRows(1000),
		WithQwpQueryClientID("unit-test/1.0"),
		WithQwpQueryInitialCredit(4096),
		WithQwpQueryTlsInsecureSkipVerify(),
	} {
		opt(cfg)
	}
	if cfg.address != "example:9000" {
		t.Errorf("address=%q", cfg.address)
	}
	if cfg.endpointPath != "/read/v2" {
		t.Errorf("endpointPath=%q", cfg.endpointPath)
	}
	if cfg.httpUser != "u" || cfg.httpPass != "p" {
		t.Errorf("basic=%q/%q", cfg.httpUser, cfg.httpPass)
	}
	if cfg.bufferPoolSize != 16 {
		t.Errorf("bufferPoolSize=%d", cfg.bufferPoolSize)
	}
	if cfg.maxBatchRows != 1000 {
		t.Errorf("maxBatchRows=%d", cfg.maxBatchRows)
	}
	if cfg.clientID != "unit-test/1.0" {
		t.Errorf("clientID=%q", cfg.clientID)
	}
	if cfg.initialCredit != 4096 {
		t.Errorf("initialCredit=%d", cfg.initialCredit)
	}
	if cfg.tlsMode != tlsInsecureSkipVerify {
		t.Errorf("tlsMode=%v", cfg.tlsMode)
	}
}

// --- Mock server integration tests for the public API ---

// newMockQueryClient stands up the egress mock server, dials it with a
// QwpQueryClient, and returns the client + cleanup. handler drives the
// test-side choreography.
func newMockQueryClient(
	t *testing.T,
	bufferPoolSize int,
	handler func(*qwpMockEgressConn),
) (*QwpQueryClient, func()) {
	t.Helper()
	srv := newQwpMockEgressServer(t, handler)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") // httptest.NewServer → http://
	addr := strings.TrimPrefix(wsURL, "ws://")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	poolOpts := []QwpQueryClientOption{WithQwpQueryAddress(addr)}
	if bufferPoolSize > 0 {
		poolOpts = append(poolOpts, WithQwpQueryBufferPoolSize(bufferPoolSize))
	}
	c, err := NewQwpQueryClient(ctx, poolOpts...)
	if err != nil {
		srv.Close()
		t.Fatalf("NewQwpQueryClient: %v", err)
	}
	cleanup := func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		_ = c.Close(closeCtx)
		srv.Close()
	}
	return c, cleanup
}

// TestQwpQueryHappyPath drives two batches + RESULT_END through the
// public Query cursor and verifies Batches() yields them in order,
// TotalRows() matches, and no error leaks.
func TestQwpQueryHappyPath(t *testing.T) {
	const wantSQL = "SELECT * FROM trades"
	c, cleanup := newMockQueryClient(t, 4, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, sql, _ := parseQueryRequest(t, req)
		if sql != wantSQL {
			t.Errorf("server sql=%q, want %q", sql, wantSQL)
		}
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 0, "v", 10))
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 1, "v", 20))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 1, 2)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, wantSQL)
	defer q.Close()

	var got []int64
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		got = append(got, batch.Int64(0, 0))
	}
	if len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Fatalf("rows=%v, want [10 20]", got)
	}
	if q.TotalRows() != 2 {
		t.Errorf("TotalRows=%d, want 2", q.TotalRows())
	}
}

// TestQwpQueryRequestIdsAreMonotonic runs two queries in sequence on
// the same client and verifies the client-assigned requestIds tick up
// by one, starting at 1 (matches Java nextRequestId initialization).
func TestQwpQueryRequestIdsAreMonotonic(t *testing.T) {
	seenIDs := make(chan int64, 4)
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 0; i < 2; i++ {
			req := m.readBinary(ctx)
			reqID, _, _ := parseQueryRequest(t, req)
			seenIDs <- reqID
			m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 0)))
		}
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		q := c.Query(ctx, "SELECT 1")
		for _, err := range q.Batches() {
			if err != nil {
				t.Fatalf("batch err: %v", err)
			}
		}
		q.Close()
	}
	close(seenIDs)
	var ids []int64
	for id := range seenIDs {
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("requestIds=%v, want [1 2]", ids)
	}
}

// TestQwpQueryServerErrorSurfacesAsQwpQueryError verifies the
// iterator yields a *QwpQueryError with the server's status and
// message on a QUERY_ERROR frame.
func TestQwpQueryServerErrorSurfacesAsQwpQueryError(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID, byte(qwpStatusParseError), "bad sql", -1)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "NONSENSE")
	defer q.Close()

	var lastErr error
	var batches int
	for _, err := range q.Batches() {
		if err != nil {
			lastErr = err
			continue
		}
		batches++
	}
	if batches != 0 {
		t.Errorf("batches=%d, want 0", batches)
	}
	if lastErr == nil {
		t.Fatal("expected iterator error, got nil")
	}
	var qe *QwpQueryError
	if !errors.As(lastErr, &qe) {
		t.Fatalf("err type=%T, want *QwpQueryError: %v", lastErr, lastErr)
	}
	if qe.Status != qwpStatusParseError {
		t.Errorf("Status=0x%02X, want 0x%02X", byte(qe.Status), byte(qwpStatusParseError))
	}
	if qe.Message != "bad sql" {
		t.Errorf("Message=%q", qe.Message)
	}
}

// TestQwpQueryOnNonSelectSurfacesError verifies that running Query on
// a non-SELECT statement surfaces the misuse as an error on the
// iterator (server sent EXEC_DONE where we expected RESULT_BATCHes).
func TestQwpQueryOnNonSelectSurfacesError(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, writeQwpFrame(0, buildExecDoneBody(reqID, 0x04, 99)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "INSERT INTO x VALUES (1)")
	defer q.Close()

	var lastErr error
	for _, err := range q.Batches() {
		if err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		t.Fatal("expected iterator error for Query-on-non-SELECT")
	}
	if !strings.Contains(lastErr.Error(), "non-SELECT") {
		t.Errorf("error = %v, want contains 'non-SELECT'", lastErr)
	}
}

// TestQwpQueryBreakOutSendsCancel verifies that breaking out of the
// range loop early sends a CANCEL frame to the server and drains to
// the server's CANCELLED echo cleanly.
func TestQwpQueryBreakOutSendsCancel(t *testing.T) {
	cancelSeen := make(chan int64, 1)
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 0, "v", 42))
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				cancelSeen <- int64(binary.LittleEndian.Uint64(frame[1:]))
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID, byte(qwpStatusCancelled), "cancelled", -1)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT 1")
	defer q.Close()

	var saw int64
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("unexpected iterator error: %v", err)
		}
		saw = batch.Int64(0, 0)
		break // trigger cancel
	}
	if saw != 42 {
		t.Errorf("saw=%d, want 42", saw)
	}
	select {
	case gotID := <-cancelSeen:
		if gotID != q.RequestId() {
			t.Errorf("cancel id=%d, want %d", gotID, q.RequestId())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw CANCEL")
	}
}

// TestQwpQueryCancelBeforeIterate verifies that calling Cancel before
// iterating sends a CANCEL frame and the iterator exits cleanly on
// the server's CANCELLED echo (no error yielded).
func TestQwpQueryCancelBeforeIterate(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		// Wait for CANCEL.
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID, byte(qwpStatusCancelled), "cancelled", -1)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT 1")
	defer q.Close()

	q.Cancel()

	var sawErr error
	var batches int
	for _, err := range q.Batches() {
		if err != nil {
			sawErr = err
		} else {
			batches++
		}
	}
	if sawErr != nil {
		t.Errorf("iterator err=%v, want clean end", sawErr)
	}
	if batches != 0 {
		t.Errorf("got %d batches, want 0", batches)
	}
}

// TestQwpExecHappyPath runs an Exec and expects the ExecResult parsed
// from an EXEC_DONE frame.
func TestQwpExecHappyPath(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, writeQwpFrame(0, buildExecDoneBody(reqID, 0x07, 42)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Exec(ctx, "INSERT INTO x VALUES (1)")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.OpType != 0x07 {
		t.Errorf("OpType=0x%02X, want 0x07", res.OpType)
	}
	if res.RowsAffected != 42 {
		t.Errorf("RowsAffected=%d, want 42", res.RowsAffected)
	}
}

// TestQwpExecServerErrorReturnsQwpQueryError verifies that a
// QUERY_ERROR on Exec surfaces as *QwpQueryError.
func TestQwpExecServerErrorReturnsQwpQueryError(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID, byte(qwpStatusInternalError), "boom", -1)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Exec(ctx, "DROP TABLE nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	var qe *QwpQueryError
	if !errors.As(err, &qe) {
		t.Fatalf("err type=%T, want *QwpQueryError", err)
	}
	if qe.Status != qwpStatusInternalError || qe.Message != "boom" {
		t.Errorf("err=%+v", qe)
	}
}

// TestQwpExecOnSelectSurfacesMisuse verifies that running Exec on a
// SELECT (which returns RESULT_BATCH / RESULT_END) surfaces as an
// error explaining the caller should use Query instead. We also
// verify the buffer gets released (exec returned once terminal).
func TestQwpExecOnSelectSurfacesMisuse(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 0, "v", 1))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 1)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("expected misuse error")
	}
	if !strings.Contains(err.Error(), "SELECT-style") {
		t.Errorf("err=%v, want contains 'SELECT-style'", err)
	}
}

// TestQwpQueryPoolBackpressureAcrossIterator wires a pool=1 client to
// a server that emits 3 batches + End. Public Batches() iterator must
// still surface all batches in order — auto-release per iteration
// keeps the pool alive.
func TestQwpQueryPoolBackpressureAcrossIterator(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 1, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 0, "v", 100))
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 1, "v", 200))
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 2, "v", 300))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 2, 3)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT v FROM t")
	defer q.Close()

	var got []int64
	for batch, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
		got = append(got, batch.Int64(0, 0))
	}
	if len(got) != 3 || got[0] != 100 || got[1] != 200 || got[2] != 300 {
		t.Fatalf("got=%v, want [100 200 300]", got)
	}
	if q.TotalRows() != 3 {
		t.Errorf("TotalRows=%d, want 3", q.TotalRows())
	}
}

// TestQwpQueryClientCloseTwiceOK verifies Close is idempotent.
func TestQwpQueryClientCloseTwiceOK(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		// Keep the connection alive until the client tears it down.
		// An immediate return triggers the server-side CloseNow
		// before the client even submits, and races the client's
		// own close into an EOF.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _ = m.conn.Read(ctx)
	})
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx, WithQwpQueryAddress(addr))
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close 2: %v", err)
	}
}

// TestQwpQueryOnClosedClient verifies that Query/Exec on a closed
// client surface an error instead of dialing a stale transport.
func TestQwpQueryOnClosedClient(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		// Keep the connection alive until the client tears it down.
		// An immediate return triggers the server-side CloseNow
		// before the client even submits, and races the client's
		// own close into an EOF.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _ = m.conn.Read(ctx)
	})
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx, WithQwpQueryAddress(addr))
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	_ = c.Close(ctx)

	// Query: error should surface on first iteration.
	q := c.Query(ctx, "SELECT 1")
	var gotErr error
	for _, err := range q.Batches() {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "closed") {
		t.Errorf("Query on closed client err=%v, want 'closed' substring", gotErr)
	}

	// Exec: sync error.
	if _, err := c.Exec(ctx, "DROP TABLE X"); err == nil ||
		!strings.Contains(err.Error(), "closed") {
		t.Errorf("Exec on closed client err=%v", err)
	}
}

// TestQwpQueryClientSendsEgressHeaders verifies that max_batch_rows
// and the X-QWP-Accept-Encoding header omission (step-9 deferral)
// propagate through the public client to the upgrade request.
func TestQwpQueryClientSendsEgressHeaders(t *testing.T) {
	var sawMaxBatchRows string
	var sawAcceptEnc string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawMaxBatchRows = r.Header.Get(qwpHeaderMaxBatchRows)
		sawAcceptEnc = r.Header.Get(qwpHeaderAcceptEncoding)
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(addr),
		WithQwpQueryMaxBatchRows(1234),
	)
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	defer c.Close(ctx)

	if sawMaxBatchRows != "1234" {
		t.Errorf("X-QWP-Max-Batch-Rows=%q, want 1234", sawMaxBatchRows)
	}
	if sawAcceptEnc != "" {
		t.Errorf("X-QWP-Accept-Encoding=%q, want empty (compression arrives in step 9)", sawAcceptEnc)
	}
}

// TestQwpQueryCloseAfterCtxCancel exercises the close-path drain
// fix: a break-out from the iterator after the query's ctx has been
// cancelled must still drain the dispatcher to idle so a follow-up
// Query on the same client works. With the pre-fix behavior the
// iterator's break-out drain would return ctx.Err() immediately,
// strand the server's CANCELLED echo in the events channel, and the
// next query's takeEvent would pick up that stale error.
func TestQwpQueryCloseAfterCtxCancel(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Query 1: one batch, wait for CANCEL, respond with CANCELLED echo.
		req1 := m.readBinary(ctx)
		reqID1, _, _ := parseQueryRequest(t, req1)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID1, 0, "v", 1))
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID1, byte(qwpStatusCancelled), "cancelled", -1)))
		// Query 2: one batch + RESULT_END. Proves the dispatcher
		// returned to idle after query 1's drain.
		req2 := m.readBinary(ctx)
		reqID2, _, _ := parseQueryRequest(t, req2)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID2, 0, "v", 2))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID2, 0, 1)))
	})
	defer cleanup()

	// Query 1: iterate one batch, cancel ctx, break out.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1() // belt-and-braces so vet sees every return path cancel
	q1 := c.Query(ctx1, "SELECT 1")
	var saw1 int64
	for b, err := range q1.Batches() {
		if err != nil {
			t.Fatalf("iter1 err: %v", err)
		}
		saw1 = b.Int64(0, 0)
		cancel1() // kill q1.ctx while iterating — exercises the drain path
		break
	}
	if saw1 != 1 {
		t.Fatalf("saw1=%d, want 1", saw1)
	}
	q1.Close() // no-op: break-out already set done=true via the deferred Store

	// Query 2 must succeed — dispatcher is idle iff the break-out
	// drain on query 1 used a cleanup ctx (not the dead q.ctx).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	q2 := c.Query(ctx2, "SELECT 2")
	defer q2.Close()
	var saw2 int64
	for b, err := range q2.Batches() {
		if err != nil {
			t.Fatalf("iter2 err: %v", err)
		}
		saw2 = b.Int64(0, 0)
	}
	if saw2 != 2 {
		t.Errorf("saw2=%d, want 2 (stale query-1 error leaked into query 2?)", saw2)
	}
	if q2.TotalRows() != 1 {
		t.Errorf("q2.TotalRows=%d, want 1", q2.TotalRows())
	}
}

// TestQwpQueryInitialCreditReachesWire verifies that
// WithQwpQueryInitialCredit actually sets the initial_credit varint
// on the outgoing QUERY_REQUEST frame. The option is exercised by
// other unit tests only at the config level; this is the end-to-end
// wire probe.
func TestQwpQueryInitialCreditReachesWire(t *testing.T) {
	gotCredit := make(chan int64, 1)
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, credit := parseQueryRequest(t, req)
		gotCredit <- credit
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 0)))
	})
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewQwpQueryClient(ctx,
		WithQwpQueryAddress(addr),
		WithQwpQueryInitialCredit(65536),
	)
	if err != nil {
		t.Fatalf("ctor: %v", err)
	}
	defer c.Close(ctx)

	q := c.Query(ctx, "SELECT 1")
	defer q.Close()
	for _, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
	}

	select {
	case got := <-gotCredit:
		if got != 65536 {
			t.Errorf("initial_credit on wire = %d, want 65536", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw QUERY_REQUEST")
	}
}

// TestQwpQueryCloseIdempotentAfterFinish locks in the documented
// contract that Close on an already-finished cursor is a safe no-op.
// Exercised via the CAS guard on q.done.
func TestQwpQueryCloseIdempotentAfterFinish(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		reqID, _, _ := parseQueryRequest(t, req)
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 0)))
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT 1")
	for _, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iter err: %v", err)
		}
	}
	// First Close after a normal iteration-to-End: no-op because the
	// iterator's deferred q.done.Store(true) already fired. Second
	// Close: no-op via CAS. Neither call should panic or block.
	q.Close()
	q.Close()
}
