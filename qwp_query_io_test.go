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
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// --- Mock server harness ---

// qwpMockEgressConn is the test-side view of a client's WebSocket.
// Tests drive it imperatively: read a frame (typically QUERY_REQUEST /
// CANCEL / CREDIT), send a scripted response (RESULT_BATCH,
// RESULT_END, QUERY_ERROR, EXEC_DONE), close cleanly.
//
// version, when non-zero, is the QWP wire-protocol version the mock
// claims to have negotiated in X-QWP-Version. sendBinary rewrites the
// header version byte of every frame to this value before writing —
// the shared frame builders (writeQwpFrame, buildOneRowInt64Batch)
// stamp qwpVersion unconditionally, but the strict-equality check in
// qwpQueryDecoder.parseFrameHeader requires server frames to match
// the negotiated version. Tests leave version=0 to skip the rewrite
// (frames are already stamped qwpVersion); cluster mocks that stamp
// frames explicitly set it to qwpVersion.
type qwpMockEgressConn struct {
	t       *testing.T
	conn    *websocket.Conn
	version byte
}

// readBinary reads one binary frame from the client. Skips non-binary
// frames; fails the test on read error.
func (m *qwpMockEgressConn) readBinary(ctx context.Context) []byte {
	m.t.Helper()
	for {
		typ, data, err := m.conn.Read(ctx)
		if err != nil {
			m.t.Fatalf("mock: read: %v", err)
		}
		if typ == websocket.MessageBinary {
			return data
		}
	}
}

// sendBinary sends one binary frame to the client. When m.version is
// non-zero, the frame's QWP header version byte (offset 4) is rewritten
// to that value first — see the type comment for the rationale.
func (m *qwpMockEgressConn) sendBinary(ctx context.Context, data []byte) {
	m.t.Helper()
	if m.version != 0 && len(data) > 4 {
		data[4] = m.version
	}
	if err := m.conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		m.t.Fatalf("mock: write: %v", err)
	}
}

// newQwpMockEgressServer stands up an httptest WebSocket server that
// hands control to `handler` once upgraded. handler is expected to
// perform the test-side request/response choreography, then return.
// The server stamps X-QWP-Version=1 so transport.connect accepts the
// upgrade.
func newQwpMockEgressServer(t *testing.T, handler func(*qwpMockEgressConn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("mock: accept: %v", err)
			return
		}
		defer conn.CloseNow()
		// The real server emits SERVER_INFO as the first post-upgrade
		// frame and the egress client reads it during connect (both
		// connectEgress and NewQwpQueryClient set serverInfoTimeout > 0).
		// Mirror that here so connect() does not block waiting for it.
		info := buildServerInfoFrame(qwpVersion, 0, qwpRolePrimary, 1, 0,
			1_700_000_000_000_000_000, "test-cluster", "mock-node")
		if err := conn.Write(r.Context(), websocket.MessageBinary, info); err != nil {
			t.Logf("mock: SERVER_INFO write: %v", err)
			return
		}
		handler(&qwpMockEgressConn{t: t, conn: conn})
	}))
}

// connectEgress dials the mock server with qwpReadPath. It sets a
// SERVER_INFO read timeout so the transport consumes the frame the mock
// emits post-upgrade, matching the production egress connect path.
func connectEgress(t *testing.T, url string) *qwpTransport {
	t.Helper()
	var tr qwpTransport
	wsURL := "ws" + strings.TrimPrefix(url, "http")
	if err := tr.connect(context.Background(), wsURL, qwpTransportOpts{
		endpointPath:      qwpReadPath,
		serverInfoTimeout: 2 * time.Second,
	}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return &tr
}

// --- Frame builders (reuse decoder_test.go helpers where possible) ---

// buildOneRowInt64Batch produces a RESULT_BATCH frame with a single
// column (wireType=LONG), one row, value=val. Uses the real encoder
// so the decoder exercises the positive path.
func buildOneRowInt64Batch(t *testing.T, requestId int64, batchSeq uint64, colName string, val int64) []byte {
	t.Helper()
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn(colName, qwpTypeLong, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addLong(val)
	tb.commitRow()
	var enc qwpEncoder
	return wrapAsResultBatch(enc.encodeTable(tb), requestId, batchSeq)
}

// buildOneRowVarcharBatch produces a RESULT_BATCH frame with a single
// column (wireType=VARCHAR), one row, value=val. Used by the aliasing
// test, which needs a column type whose accessor returns bytes that
// alias directly into the per-frame payload.
func buildOneRowVarcharBatch(t *testing.T, requestId int64, batchSeq uint64, colName string, val string) []byte {
	t.Helper()
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn(colName, qwpTypeVarchar, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addString(val)
	tb.commitRow()
	var enc qwpEncoder
	return wrapAsResultBatch(enc.encodeTable(tb), requestId, batchSeq)
}

// --- Parsers for frames sent by the client to the mock server ---

// parseQueryRequest decodes a client-sent QUERY_REQUEST frame. Egress
// control frames (QUERY_REQUEST / CANCEL / CREDIT) sent by the client
// carry no 12-byte QWP header — they begin with the msg_kind byte
// directly. Returns (requestId, sql, initialCredit).
func parseQueryRequest(t *testing.T, frame []byte) (int64, string, int64) {
	t.Helper()
	if len(frame) < 1+8 {
		t.Fatalf("QUERY_REQUEST frame too short: %d", len(frame))
	}
	if kind := frame[0]; kind != byte(qwpMsgKindQueryRequest) {
		t.Fatalf("expected msg_kind 0x10, got 0x%02X", kind)
	}
	p := 1
	requestId := int64(binary.LittleEndian.Uint64(frame[p:]))
	p += 8
	sqlLen, n, err := qwpReadVarint(frame[p:])
	if err != nil {
		t.Fatalf("bad sql_len varint: %v", err)
	}
	p += n
	sql := string(frame[p : p+int(sqlLen)])
	p += int(sqlLen)
	credit, n, err := qwpReadVarint(frame[p:])
	if err != nil {
		t.Fatalf("bad credit varint: %v", err)
	}
	p += n
	if _, _, err := qwpReadVarint(frame[p:]); err != nil {
		t.Fatalf("bad bind_count varint: %v", err)
	}
	return requestId, sql, int64(credit)
}

// --- Tests ---

// TestQwpEgressIOHappyPathSelect drives a SELECT-style sequence: the
// mock sends RESULT_BATCH + RESULT_BATCH + RESULT_END; the I/O loop
// decodes and surfaces Batch, Batch, End in order.
func TestQwpEgressIOHappyPathSelect(t *testing.T) {
	const wantSQL = "SELECT * FROM trades"
	const wantReqID = int64(42)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req := m.readBinary(ctx)
		reqID, sql, credit := parseQueryRequest(t, req)
		if reqID != wantReqID {
			t.Errorf("server saw requestId=%d, want %d", reqID, wantReqID)
		}
		if sql != wantSQL {
			t.Errorf("server saw sql=%q, want %q", sql, wantSQL)
		}
		if credit != 0 {
			t.Errorf("server saw credit=%d, want 0", credit)
		}

		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 0, "v", 100))
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 1, "v", 200))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(wantReqID, 1, 2)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 4)
	io.start()
	defer shutdownIO(t, io)

	submitCtx, submitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer submitCancel()
	if err := io.submitQuery(submitCtx, qwpRequest{sql: wantSQL, requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	values := drainBatchesToEnd(t, io, 2 /* expect 2 batches */)
	if len(values) != 2 || values[0] != 100 || values[1] != 200 {
		t.Fatalf("batch values = %v, want [100 200]", values)
	}
}

// TestQwpEgressIOExecDone verifies the non-SELECT path: the server
// replies with EXEC_DONE and the I/O loop emits an ExecDone event.
func TestQwpEgressIOExecDone(t *testing.T) {
	const wantReqID = int64(7)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, writeQwpFrame(0, buildExecDoneBody(wantReqID, 0x04, 99)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "INSERT INTO t VALUES (1)", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindExecDone {
		t.Fatalf("event kind = %v, want ExecDone (errMsg=%q)", ev.kind, ev.errMessage)
	}
	if ev.execResult.OpType != 0x04 {
		t.Errorf("OpType = 0x%02X, want 0x04", ev.execResult.OpType)
	}
	if ev.execResult.RowsAffected != 99 {
		t.Errorf("RowsAffected = %d, want 99", ev.execResult.RowsAffected)
	}
	if ev.requestId != wantReqID {
		t.Errorf("requestId = %d, want %d", ev.requestId, wantReqID)
	}
}

// TestQwpEgressIOQueryError exercises the server-side-error path.
func TestQwpEgressIOQueryError(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(1, byte(QwpStatusParseError), "bad sql", -1)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "BAD", requestId: 1}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindError {
		t.Fatalf("event kind = %v, want Error", ev.kind)
	}
	if ev.errStatus != QwpStatusParseError {
		t.Errorf("errStatus = 0x%02X, want 0x%02X", byte(ev.errStatus), byte(QwpStatusParseError))
	}
	if ev.errMessage != "bad sql" {
		t.Errorf("errMessage = %q, want %q", ev.errMessage, "bad sql")
	}
	if ev.requestId != 1 {
		t.Errorf("requestId = %d, want 1", ev.requestId)
	}
}

// TestQwpEgressIOCancel checks that requestCancel from a second
// goroutine produces a CANCEL frame on the wire before the query
// terminates. The mock pretends to be a streaming server: it sends one
// batch, waits for the client's CANCEL, then ends with QUERY_ERROR
// CANCELLED so the I/O loop exits cleanly.
func TestQwpEgressIOCancel(t *testing.T) {
	const wantReqID = int64(5)
	cancelSeen := make(chan int64, 1)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 0, "v", 7))

		// Wait for CANCEL. Client control frames have no QWP header —
		// they are just msg_kind + body.
		frame := m.readBinary(ctx)
		if kind := frame[0]; kind != byte(qwpMsgKindCancel) {
			t.Errorf("server expected CANCEL, got msg_kind=0x%02X", kind)
		}
		cid := int64(binary.LittleEndian.Uint64(frame[1:]))
		cancelSeen <- cid

		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(wantReqID, byte(qwpStatusCancelled), "cancelled", -1)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "SELECT 1", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// Receive the first batch, release it.
	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindBatch {
		t.Fatalf("event kind = %v, want Batch", ev.kind)
	}
	ev.batch.release()

	// Cancel from a separate goroutine; the I/O loop should flush
	// CANCEL on the next loop iteration.
	go io.requestCancel(wantReqID)

	select {
	case gotID := <-cancelSeen:
		if gotID != wantReqID {
			t.Errorf("server saw cancel id=%d, want %d", gotID, wantReqID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw CANCEL frame")
	}

	// Server follows up with QUERY_ERROR/CANCELLED to close out.
	ev = takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindError {
		t.Fatalf("event kind = %v, want Error", ev.kind)
	}
	if ev.errStatus != qwpStatusCancelled {
		t.Errorf("errStatus = 0x%02X, want 0x%02X (CANCELLED)", byte(ev.errStatus), byte(qwpStatusCancelled))
	}
}

// TestQwpEgressIOShutdownUnblocksRead forces shutdown while the I/O
// goroutine is parked on a Read with no traffic. The goroutine must
// exit within a short grace period — demonstrating the ctx-cancel
// kick wakes the Read.
func TestQwpEgressIOShutdownUnblocksRead(t *testing.T) {
	ready := make(chan struct{})
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		close(ready)
		// Sleep — don't reply. Client will shutdown.
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: 1}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}
	<-ready // I/O loop is now inside readBinaryFrame.

	// Shutdown must unblock the Read promptly.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutCancel()
	start := time.Now()
	if err := io.shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if dt := time.Since(start); dt > 500*time.Millisecond {
		t.Errorf("shutdown took %v (expected <500ms)", dt)
	}
}

// TestQwpEgressReaderRecoversPanic asserts the readerRun goroutine's
// top-level recover converts a panic into the connection's latched
// terminal error instead of crashing the host process. The panic is
// injected with a nil-conn transport: readerRun dereferences the conn
// directly (conn.Reader), bypassing the transport's own nil guards, so
// the first read panics inside coder/websocket. The recover must route
// that into setIoErr so a follow-up submitQuery surfaces it. This is the
// only wire goroutine whose panic is reachable without a test seam — the
// dispatcher's send path is nil-safe and its decode path plus the send
// loop's ACK parsers are length-validated before use — but all of them
// share the same recover idiom this guards.
func TestQwpEgressReaderRecoversPanic(t *testing.T) {
	io := newQwpEgressIO(&qwpTransport{conn: nil}, 2)
	io.start()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = io.shutdown(shutCtx)
	}()

	// The reader panics on its first conn.Reader; poll until the guard
	// latches the synthesized terminal error. Reaching the deadline means
	// the panic escaped the goroutine and the test binary would have
	// crashed.
	var ioErr error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ioErr == nil {
		ioErr = io.loadIoErr()
		time.Sleep(5 * time.Millisecond)
	}
	if ioErr == nil {
		t.Fatal("reader panic was not latched within 2s")
	}
	if !strings.Contains(ioErr.Error(), "egress reader panicked") {
		t.Fatalf("latched error = %q, want it to name the recovered reader panic", ioErr)
	}

	// A follow-up submitQuery must surface the latched cause rather than
	// running a fresh query against the dead connection.
	err := io.submitQuery(context.Background(), qwpRequest{sql: "x", requestId: 1})
	if err == nil || !strings.Contains(err.Error(), "egress reader panicked") {
		t.Fatalf("submitQuery after panic = %v, want the latched reader-panic error", err)
	}
}

// TestQwpEgressIOPoolBackpressure sizes the buffer pool to 1 and has
// the server emit two batches back-to-back. The I/O loop must not
// emit the second batch event until the user releases the first —
// the classic pool-exhaustion case.
func TestQwpEgressIOPoolBackpressure(t *testing.T) {
	const wantReqID = int64(3)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 0, "v", 10))
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 1, "v", 20))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(wantReqID, 1, 2)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 1) // pool of size 1
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// First batch arrives promptly.
	ev1 := takeEventOrFail(t, io, 2*time.Second)
	if ev1.kind != qwpEventKindBatch {
		t.Fatalf("ev1 kind = %v", ev1.kind)
	}

	// Second batch must NOT arrive until we release the first — the
	// I/O goroutine is parked in handleResultBatch waiting on the
	// pool. A short poll of takeEvent confirms nothing pending.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	if _, err := io.takeEvent(shortCtx); err == nil {
		shortCancel()
		t.Fatal("event arrived while pool was exhausted")
	}
	shortCancel()

	// Release and then the second batch + end should follow.
	val1 := ev1.batch.batch.Int64(0, 0)
	ev1.batch.release()

	ev2 := takeEventOrFail(t, io, 2*time.Second)
	if ev2.kind != qwpEventKindBatch {
		t.Fatalf("ev2 kind = %v", ev2.kind)
	}
	val2 := ev2.batch.batch.Int64(0, 0)
	ev2.batch.release()

	ev3 := takeEventOrFail(t, io, 2*time.Second)
	if ev3.kind != qwpEventKindEnd {
		t.Fatalf("ev3 kind = %v, errMsg=%q", ev3.kind, ev3.errMessage)
	}
	if val1 != 10 || val2 != 20 {
		t.Fatalf("batch values = %d, %d; want 10, 20", val1, val2)
	}
}

// TestQwpEgressIOInPlaceDecodeAliasing pins the cross-batch isolation
// invariant: while the user holds batch N, the dispatcher decoding
// batch N+1 into a DIFFERENT pool buffer must not corrupt batch N's
// view. VARCHAR makes the property visible — its accessor returns a
// byte slice aliased into the frame's payload, so any cross-buffer
// clobber would surface as wrong bytes on a re-read.
//
// In the Go architecture each qwpBatchBuffer holds its own
// QwpColumnBatch with per-batch layouts, and coder/websocket hands
// the dispatcher a fresh []byte per binary frame; holding a buffer
// pins that frame's payload via the layout's aliased slices. This
// test is the negative case the existing CopyAll-survives-pool-reuse
// tests don't cover: there we explicitly snapshot before reuse, here
// we read the live aliased view across reuse.
func TestQwpEgressIOInPlaceDecodeAliasing(t *testing.T) {
	const wantReqID = int64(7)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, buildOneRowVarcharBatch(t, wantReqID, 0, "v", "ALPHA"))
		m.sendBinary(ctx, buildOneRowVarcharBatch(t, wantReqID, 1, "v", "BRAVO"))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(wantReqID, 1, 2)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	// Pool size 2: the dispatcher can decode batch 1 into a
	// different buffer while batch 0 is still held by the user.
	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev0 := takeEventOrFail(t, io, 2*time.Second)
	if ev0.kind != qwpEventKindBatch {
		t.Fatalf("ev0 kind = %v, errMsg=%q", ev0.kind, ev0.errMessage)
	}
	if got := ev0.batch.batch.String(0, 0); got != "ALPHA" {
		t.Fatalf("batch0 first read = %q, want ALPHA", got)
	}
	// Capture the aliased byte view too — the bytes themselves must
	// stay stable, not just an accessor that happens to recopy them.
	str0Before := ev0.batch.batch.Str(0, 0)
	if string(str0Before) != "ALPHA" {
		t.Fatalf("Str(0,0) = %q, want ALPHA", str0Before)
	}

	// Pull batch 1 WITHOUT releasing batch 0. The dispatcher must
	// take the second buffer from the pool and decode payload 1 into
	// it; batch 0's view must remain untouched.
	ev1 := takeEventOrFail(t, io, 2*time.Second)
	if ev1.kind != qwpEventKindBatch {
		t.Fatalf("ev1 kind = %v, errMsg=%q", ev1.kind, ev1.errMessage)
	}
	if got := ev1.batch.batch.String(0, 0); got != "BRAVO" {
		t.Fatalf("batch1 read = %q, want BRAVO", got)
	}
	if ev1.batch == ev0.batch {
		t.Fatal("dispatcher reused the still-held batch buffer; pool isolation broken")
	}

	// Re-read batch 0 AFTER batch 1 has been decoded. Without
	// cross-batch isolation the alias would now resolve to BRAVO.
	if got := ev0.batch.batch.String(0, 0); got != "ALPHA" {
		t.Fatalf("batch0 re-read after batch1 decode = %q, want ALPHA", got)
	}
	// The aliased byte view captured before batch 1 arrived must
	// also still resolve to the same bytes — a stale slice header
	// pointing into a clobbered buffer would surface here.
	if string(str0Before) != "ALPHA" {
		t.Fatalf("aliased Str(0,0) drifted to %q after batch1 decode, want ALPHA", str0Before)
	}

	ev0.batch.release()
	ev1.batch.release()

	end := takeEventOrFail(t, io, 2*time.Second)
	if end.kind != qwpEventKindEnd {
		t.Fatalf("end kind = %v, errMsg=%q", end.kind, end.errMessage)
	}
}

// TestQwpEgressIOCreditReplenish confirms that a query opted into flow
// control emits a CREDIT frame on the wire after each batch release,
// carrying the exact payload-byte count.
func TestQwpEgressIOCreditReplenish(t *testing.T) {
	const wantReqID = int64(11)
	const initialCredit = int64(64 * 1024)

	creditFrames := make(chan []byte, 4)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		_, _, credit := parseQueryRequest(t, req)
		if credit != initialCredit {
			t.Errorf("server saw credit=%d, want %d", credit, initialCredit)
		}
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 0, "v", 1))

		// Block until the client sends CREDIT. Client control frames
		// have no QWP header — they are just msg_kind + body.
		for {
			f := m.readBinary(ctx)
			if f[0] == byte(qwpMsgKindCredit) {
				creditFrames <- f
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(wantReqID, 0, 1)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{
		sql:           "SELECT 1",
		requestId:     wantReqID,
		initialCredit: initialCredit,
	}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindBatch {
		t.Fatalf("ev kind = %v", ev.kind)
	}
	wantBytes := ev.batch.payloadLen
	ev.batch.release()

	// Credit frame should arrive at the server; check the byte count
	// on it matches the batch size. CREDIT layout: msg_kind(1) +
	// request_id(8) + additional_bytes(varint).
	select {
	case frame := <-creditFrames:
		p := 1 + 8
		got, _, err := qwpReadVarint(frame[p:])
		if err != nil {
			t.Fatalf("bad CREDIT varint: %v", err)
		}
		if int64(got) != int64(wantBytes) {
			t.Errorf("CREDIT bytes = %d, want %d", got, wantBytes)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no CREDIT frame seen")
	}

	endEv := takeEventOrFail(t, io, 2*time.Second)
	if endEv.kind != qwpEventKindEnd {
		t.Fatalf("final event kind = %v, want End", endEv.kind)
	}
}

// TestQwpEgressIOUnknownMsgKind has the server send a bogus msg_kind
// and verifies the I/O loop emits a synthesized error and terminates
// the query.
func TestQwpEgressIOUnknownMsgKind(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		// Frame with an unknown msg_kind byte (0x7F).
		m.sendBinary(ctx, writeQwpFrame(0, []byte{0x7F}))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 1)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: 1}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindTransportError {
		t.Fatalf("event kind = %v, want TransportError", ev.kind)
	}
	if !strings.Contains(ev.errMessage, "unknown msg_kind") {
		t.Errorf("errMessage = %q, want unknown-msg-kind", ev.errMessage)
	}
}

// TestQwpEgressIOCacheResetBetweenQueries drives the server-emitted
// CACHE_RESET path end-to-end: query 1's response seeds the
// connection-scoped SYMBOL dict; the server then emits CACHE_RESET
// with mask=DICT; query 2 runs afterwards. Validates three invariants:
//   - the dispatcher does not surface CACHE_RESET to the user (the
//     event stream is {Batch, End} for Q1 and {ExecDone} for Q2);
//   - the decoder's dict is cleared by the time Q2's terminal event
//     is delivered;
//   - nothing about Q2's normal completion is disturbed.
func TestQwpEgressIOCacheResetBetweenQueries(t *testing.T) {
	const q1ReqID = int64(11)
	const q2ReqID = int64(12)

	// Build Q1's RESULT_BATCH with a SYMBOL column so the delta dict
	// section feeds qwpConnDict.entries.
	globalDict := []string{"AAPL", "MSFT"}
	tb := newQwpTableBuffer("t")
	col, err := tb.getOrCreateColumn("s", qwpTypeSymbol, false)
	if err != nil {
		t.Fatalf("getOrCreateColumn: %v", err)
	}
	col.addSymbolID(0)
	tb.commitRow()
	col.addSymbolID(1)
	tb.commitRow()
	var enc qwpEncoder
	q1Batch := wrapAsResultBatch(
		enc.encodeTableWithDeltaDict(tb, globalDict, -1, 1),
		q1ReqID, 0)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Query 1: batch with symbols + RESULT_END, then CACHE_RESET.
		m.readBinary(ctx)
		m.sendBinary(ctx, q1Batch)
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(q1ReqID, 0, 2)))
		m.sendBinary(ctx, writeQwpFrame(0, buildCacheResetBody(qwpResetMaskDict)))

		// Query 2: a plain EXEC_DONE. If the dispatcher were to leak
		// CACHE_RESET as an event, the test's event sequence would pick
		// that up before the ExecDone.
		m.readBinary(ctx)
		m.sendBinary(ctx, writeQwpFrame(0, buildExecDoneBody(q2ReqID, 0x01, 0)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Query 1 → expect Batch, End; decoder state populated afterwards.
	if err := io.submitQuery(ctx, qwpRequest{sql: "SELECT s FROM t", requestId: q1ReqID}); err != nil {
		t.Fatalf("submitQuery q1: %v", err)
	}
	batchEv := takeEventOrFail(t, io, 2*time.Second)
	if batchEv.kind != qwpEventKindBatch {
		t.Fatalf("q1 first event = %v, want Batch (errMsg=%q)", batchEv.kind, batchEv.errMessage)
	}
	if got := batchEv.batch.batch.String(0, 0); got != "AAPL" {
		t.Errorf("q1 batch row 0 = %q, want AAPL", got)
	}
	batchEv.batch.release()
	endEv := takeEventOrFail(t, io, 2*time.Second)
	if endEv.kind != qwpEventKindEnd {
		t.Fatalf("q1 second event = %v, want End (errMsg=%q)", endEv.kind, endEv.errMessage)
	}

	// Query 2 → expect ExecDone only (no CACHE_RESET event surfaces).
	if err := io.submitQuery(ctx, qwpRequest{sql: "INSERT INTO t VALUES ('x')", requestId: q2ReqID}); err != nil {
		t.Fatalf("submitQuery q2: %v", err)
	}
	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindExecDone {
		t.Fatalf("q2 first event kind = %v, want ExecDone (errMsg=%q)",
			ev.kind, ev.errMessage)
	}
	if ev.requestId != q2ReqID {
		t.Errorf("q2 ExecDone requestId = %d, want %d", ev.requestId, q2ReqID)
	}

	// Shut the dispatcher down so it cannot touch the decoder while we
	// inspect — the happens-before via events channel already covers
	// correctness; the shutdown makes the intent explicit for readers.
	shutdownIO(t, io)

	if io.decoder.dict.size() != 0 {
		t.Errorf("dict not cleared after CACHE_RESET: size=%d", io.decoder.dict.size())
	}
}

// TestQwpEgressIOCacheResetTruncatedPoisons feeds a CACHE_RESET frame
// that ends right after the msg_kind byte (no reset_mask). The
// dispatcher must surface the decode error, poison the connection,
// and reject the next submitQuery immediately.
func TestQwpEgressIOCacheResetTruncatedPoisons(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, writeQwpFrame(0, []byte{byte(qwpMsgKindCacheReset)}))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 1)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: 1}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindTransportError {
		t.Fatalf("event kind = %v, want TransportError", ev.kind)
	}
	if !strings.Contains(ev.errMessage, "truncated before reset_mask") {
		t.Errorf("errMessage = %q, want truncated-reset_mask", ev.errMessage)
	}

	// A fresh submitQuery must now fail synchronously because the
	// decoder state is untrustworthy.
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: 2}); err == nil {
		t.Fatal("submitQuery after poison returned nil; expected latched ioErr")
	}
}

// TestQwpEgressIOConcurrentCancelAndShutdown stress-tests the cancel /
// shutdown races: a test-runner goroutine fires requestCancel while
// the test's main goroutine fires shutdown. Both should complete
// without a deadlock or a goroutine leak.
func TestQwpEgressIOConcurrentCancelAndShutdown(t *testing.T) {
	ready := make(chan struct{})
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		close(ready)
		// Stall.
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: 99}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}
	<-ready

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		io.requestCancel(99)
	}()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	if err := io.shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	wg.Wait()
}

// TestQwpEgressIODecodeFailure feeds a RESULT_BATCH frame whose header
// is valid but body is truncated (just the msg_kind byte with nothing
// after it). handleResultBatch must return the borrowed buffer to the
// pool — stranding it would permanently leak a slot — surface a
// synthesized decode-error event, and terminate the query cleanly.
// Connection-level poisoning behavior after this path is covered by
// TestQwpEgressIODecodeFailurePoisons.
func TestQwpEgressIODecodeFailure(t *testing.T) {
	const wantReqID = int64(17)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		// Valid header + RESULT_BATCH kind + zero-length body. decode()
		// dispatches into parseFrameHeader (accepts), then tries to
		// read the requestId int64 and fails with truncation.
		m.sendBinary(ctx, writeQwpFrame(0, []byte{byte(qwpMsgKindResultBatch)}))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	const poolSize = 2
	io := newQwpEgressIO(tr, poolSize)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "SELECT 1", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindTransportError {
		t.Fatalf("event kind = %v, want TransportError", ev.kind)
	}
	if !strings.Contains(ev.errMessage, "decode") {
		t.Errorf("errMessage = %q, expected to contain \"decode\"", ev.errMessage)
	}

	// The borrowed buffer must be back in the pool — the error branch
	// of handleResultBatch explicitly returns it before emitting the
	// event. Poll briefly because the event emit and the pool return
	// happen on the dispatcher but we read from a different goroutine.
	if !waitForPoolSize(io, poolSize, 500*time.Millisecond) {
		t.Fatalf("buffer pool size = %d, want %d — decode-error path stranded a buffer",
			len(io.buffers), poolSize)
	}
}

// TestQwpEgressIODecodeFailurePoisons verifies the terminal-flag
// contract: once a decode error desyncs the per-connection decoder
// state, ioErr is latched and every subsequent submitQuery returns
// it immediately — a fresh query must never be decoded against
// stale dict/schema state. Mirrors the ingest send loop's latched
// terminal error (recordFatal / sendLoopCheckError).
func TestQwpEgressIODecodeFailurePoisons(t *testing.T) {
	const wantReqID = int64(31)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		// Truncated RESULT_BATCH — same shape as TestQwpEgressIODecodeFailure.
		m.sendBinary(ctx, writeQwpFrame(0, []byte{byte(qwpMsgKindResultBatch)}))
		// Hold the connection open so the reader does not synthesize
		// its own "server closed" event that would race the decode
		// error we're trying to observe as the terminal event.
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "SELECT 1", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery first: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindTransportError {
		t.Fatalf("event kind = %v, want TransportError", ev.kind)
	}

	// The latch is set on the dispatcher goroutine right before the
	// error event hits the channel; by the time the user has observed
	// the event, loadIoErr must also be populated.
	gotLoad := io.loadIoErr()
	if gotLoad == nil {
		t.Fatalf("loadIoErr() = nil after decode failure, want latched error")
	}
	if !strings.Contains(gotLoad.Error(), "decode") {
		t.Errorf("loadIoErr() = %q, expected to contain \"decode\"", gotLoad.Error())
	}

	// A follow-up submitQuery must fail synchronously with the latched
	// error — not block, not succeed, not return a different error.
	// Using a generous ctx timeout ensures we are not accidentally
	// observing ctx expiry.
	gotSubmit := io.submitQuery(ctx, qwpRequest{sql: "SELECT 2", requestId: wantReqID + 1})
	if gotSubmit == nil {
		t.Fatalf("submitQuery after decode failure: got nil error, want latched decode error")
	}
	if gotSubmit != gotLoad {
		t.Errorf("submitQuery returned %q, want identity with latched %q",
			gotSubmit.Error(), gotLoad.Error())
	}
}

// TestQwpEgressIOReleaseAfterShutdown exercises the closed.Load()
// early-exit in releaseBuffer: a user that holds onto a batch across
// shutdown must be able to call release() without panicking,
// blocking, or corrupting the already-drained pool.
func TestQwpEgressIOReleaseAfterShutdown(t *testing.T) {
	const wantReqID = int64(23)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 0, "v", 1))
		// Keep the connection open so the client's shutdown drives
		// the teardown (rather than the server closing first and the
		// reader emitting its own synthetic error).
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindBatch {
		t.Fatalf("event kind = %v, want Batch", ev.kind)
	}
	heldBuf := ev.batch

	// Shutdown WITHOUT releasing the buffer the user still holds.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutCancel()
	if err := io.shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// Post-shutdown invariant: the dispatcher sets closed=true in its
	// defer before doneCh fires (which is what unblocks shutdown).
	if !io.closed.Load() {
		t.Fatal("dispatcher didn't set closed=true before exiting")
	}

	poolBefore := len(io.buffers)
	creditBefore := io.pendingCredit.Load()

	// release after shutdown must return promptly: the early-exit
	// path skips the pool send and the notify. Runs in a goroutine
	// with a timeout so a hypothetical deadlock surfaces as a test
	// failure rather than hanging the suite.
	done := make(chan struct{})
	go func() {
		defer close(done)
		heldBuf.release()
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("releaseBuffer after shutdown blocked")
	}

	// The early-exit skips pendingCredit.Add and the pool send — the
	// observable state should be unchanged. Without the closed check,
	// a post-shutdown release would leave buf dangling on io.buffers
	// with no consumer to drain it.
	if got := len(io.buffers); got != poolBefore {
		t.Errorf("pool size changed after post-shutdown release: before=%d after=%d",
			poolBefore, got)
	}
	if got := io.pendingCredit.Load(); got != creditBefore {
		t.Errorf("pendingCredit changed after post-shutdown release: before=%d after=%d",
			creditBefore, got)
	}

	// A second release on the same buffer must also stay harmless.
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		heldBuf.release()
	}()
	select {
	case <-done2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second releaseBuffer after shutdown blocked")
	}
}

// TestQwpEgressIOReleaseClosePoolRace races releaseBuffer against the
// dispatcher's exit-defer (closed.Store(true) + close(events)) across
// 200 iterations to surface any TOCTOU bug in the closed.Load() guard
// in releaseBuffer. Mirrors Java's QwpEgressIoThreadCloseRaceTest.
//
// In the Java client the concern is a leaked native scratch buffer:
// a user thread reads closed==false, pauses, lets closePool drain
// freeBuffers, then offers its buffer into the now-emptied queue and
// the buffer's native memory leaks. Go's qwpBatchBuffer holds only
// GC-managed slices, so the failure mode here is narrower — what we
// pin is that the release/exit pair never panics, never blocks, and
// has no data race detectable under -race. The existing single-shot
// TestQwpEgressIOReleaseAfterShutdown only covers the post-shutdown
// case; the close-during-release window needs the loop.
func TestQwpEgressIOReleaseClosePoolRace(t *testing.T) {
	const iterations = 50
	for iter := 0; iter < iterations; iter++ {
		runReleaseClosePoolRaceOnce(t, iter)
	}
}

func runReleaseClosePoolRaceOnce(t *testing.T, iter int) {
	// A real, started egress IO so the race runs against the REAL
	// dispatcher teardown driven by shutdown() — not a hand-rolled copy
	// of its exit defers, which would silently go stale if the teardown
	// sequence ever changed. The mock just idles; no query is needed —
	// shutdown() alone makes the dispatcher return and run its exit
	// defers (decoder.close, close(events), closed.Store(true)).
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			if _, _, err := m.conn.Read(ctx); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()

	// Pull both pool buffers out so we can release them — what the
	// dispatcher would have handed to the user as batches.
	b0 := <-io.buffers
	b1 := <-io.buffers

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		io.releaseBuffer(b0)
		io.releaseBuffer(b1)
	}()
	go func() {
		defer wg.Done()
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = io.shutdown(ctx)
	}()

	// Release the start gate so both goroutines hit the racing section
	// as close to simultaneously as the runtime allows.
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("iteration %d: race between releaseBuffer and shutdown deadlocked", iter)
	}
}

// TestQwpEgressIOTakeEventWakesOnShutdown parks a consumer on
// takeEvent with nothing queued, then shuts the dispatcher down. The
// consumer must wake with a terminal error rather than blocking on an
// open-but-silent channel until its own ctx expires. This is the
// guarantee that replaced the old best-effort postShutdownSentinel —
// closing the events channel means a parked consumer always wakes.
func TestQwpEgressIOTakeEventWakesOnShutdown(t *testing.T) {
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		// Never reply — the consumer will be parked waiting.
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()

	submitCtx, submitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer submitCancel()
	if err := io.submitQuery(submitCtx, qwpRequest{sql: "x", requestId: 1}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// Park a goroutine inside takeEvent with a ctx that won't fire
	// before our shutdown does — if the channel-close signal doesn't
	// wake takeEvent, this assertion would have to wait for the ctx.
	done := make(chan error, 1)
	go func() {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer waitCancel()
		_, err := io.takeEvent(waitCtx)
		done <- err
	}()

	// Small sleep to raise the probability that the goroutine is
	// actually parked inside the takeEvent select when shutdown
	// fires. Not a correctness requirement — even if the goroutine
	// hasn't reached the select yet, close(events) happens-before the
	// receive, so takeEvent still returns the terminal error.
	time.Sleep(50 * time.Millisecond)

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutCancel()
	if err := io.shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("takeEvent returned nil after shutdown; expected terminal error")
		}
		if !strings.Contains(err.Error(), "terminated") {
			t.Errorf("takeEvent error = %q, want substring \"terminated\"", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("takeEvent did not wake within 500ms of shutdown")
	}
}

// TestQwpEgressIOShutdownPreservesQueuedEvents verifies that events
// already buffered on io.events at shutdown aren't dropped: the
// consumer drains them normally and only afterwards sees the
// closed-channel signal. Regression guard against an over-eager
// postShutdownSentinel design that would have had to discard queued
// events to make room for its own terminal message.
func TestQwpEgressIOShutdownPreservesQueuedEvents(t *testing.T) {
	const wantReqID = int64(29)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, wantReqID, 0, "v", 42))
		// Stay connected so the client's reader doesn't see a close
		// and synthesize a transport error before the test's own
		// shutdown fires — we want the batch event to be the only
		// thing on io.events when we tear down.
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()

	io := newQwpEgressIO(tr, 2)
	io.start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "x", requestId: wantReqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// Wait for the dispatcher to actually deliver the batch event
	// onto io.events. <-serverSide is not enough — the client's
	// reader + dispatcher may not have processed the frame yet.
	// len(chan) is a safe atomic read at runtime.
	if !waitForEventsCount(io, 1, 500*time.Millisecond) {
		t.Fatalf("batch event never queued: len(events)=%d", len(io.events))
	}

	// Shut down WITHOUT draining. The batch event stays queued.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutCancel()
	if err := io.shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Drain — the batch must still be recoverable despite the
	// channel having been closed by the dispatcher's defer.
	ev := takeEventOrFail(t, io, 500*time.Millisecond)
	if ev.kind != qwpEventKindBatch {
		t.Fatalf("first event kind = %v, want Batch (errMsg=%q)", ev.kind, ev.errMessage)
	}
	if got := ev.batch.batch.Int64(0, 0); got != 42 {
		t.Errorf("queued batch value = %d, want 42", got)
	}
	ev.batch.release()

	// Next take must see the terminal signal now that the queue is
	// drained — from the channel close, not a synthesized event.
	takeCtx, takeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer takeCancel()
	if _, err := io.takeEvent(takeCtx); err == nil {
		t.Fatal("post-drain takeEvent returned no error; expected terminal error")
	} else if !strings.Contains(err.Error(), "terminated") {
		t.Errorf("post-drain takeEvent error = %q, want substring \"terminated\"", err)
	}
}

// --- shared helpers ---

func takeEventOrFail(t *testing.T, io *qwpEgressIO, timeout time.Duration) qwpEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ev, err := io.takeEvent(ctx)
	if err != nil {
		t.Fatalf("takeEvent: %v", err)
	}
	return ev
}

// drainBatchesToEnd reads events until an End event is seen, asserting
// the expected number of batches arrives first. Returns the Int64(0,0)
// value of each batch for caller-side sanity checks.
func drainBatchesToEnd(t *testing.T, io *qwpEgressIO, wantBatches int) []int64 {
	t.Helper()
	var values []int64
	for i := 0; i < wantBatches; i++ {
		ev := takeEventOrFail(t, io, 2*time.Second)
		if ev.kind != qwpEventKindBatch {
			t.Fatalf("event %d: kind = %v, errMsg=%q", i, ev.kind, ev.errMessage)
		}
		values = append(values, ev.batch.batch.Int64(0, 0))
		ev.batch.release()
	}
	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindEnd {
		t.Fatalf("final event: kind = %v, errMsg=%q", ev.kind, ev.errMessage)
	}
	return values
}

// shutdownIO wraps qwpEgressIO.shutdown with a bounded context for
// deferred cleanup in tests. Not fatal on error — the goroutine may
// already have exited on its own after a server error, in which case
// shutdown is a no-op.
func shutdownIO(t *testing.T, io *qwpEgressIO) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.shutdown(ctx); err != nil {
		t.Logf("shutdown: %v", err)
	}
}

// waitForPoolSize polls len(io.buffers) until it reaches want or the
// timeout expires. Used where the assertion races with the dispatcher
// wrapping up — e.g. after a decode error, where the pool-return and
// the event emit happen on the dispatcher but the test reads the
// event on a different goroutine.
func waitForPoolSize(io *qwpEgressIO, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if len(io.buffers) == want {
			return true
		}
		if time.Now().After(deadline) {
			return len(io.buffers) == want
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForEventsCount polls len(io.events) until it reaches at least
// want or the timeout expires. Used by the shutdown-preserves-queued
// test to synchronize on the dispatcher having actually delivered an
// event to the consumer-visible channel (rather than just read it
// from the wire).
func waitForEventsCount(io *qwpEgressIO, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if len(io.events) >= want {
			return true
		}
		if time.Now().After(deadline) {
			return len(io.events) >= want
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newStalledTransport returns a qwpTransport whose WebSocket conn is
// wired to an in-process net.Pipe. The server side completes the HTTP
// upgrade, optionally emits preSend bytes right after the upgrade
// (for seeding a valid inbound WebSocket frame before the stall), and
// then stops reading. Because net.Pipe is synchronous and unbuffered,
// any subsequent client-side Write blocks until the pipe is closed.
// Use this to simulate a hung peer (TCP zero-window, stuck
// application) without relying on OS socket buffer sizes.
//
// The caller must arrange for the returned clientConn to be closed at
// test end so a blocked Write unwinds and goroutines don't leak.
func newStalledTransport(t *testing.T, preSend []byte) (tr *qwpTransport, clientConn net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()

	stallDone := make(chan struct{})
	t.Cleanup(func() {
		close(stallDone)
	})

	go func() {
		defer serverConn.Close()
		br := bufio.NewReader(serverConn)
		var wsKey string
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			if len(line) > 20 && strings.EqualFold(line[:19], "Sec-WebSocket-Key: ") {
				wsKey = strings.TrimSpace(line[19:])
			}
		}
		h := sha1.New()
		h.Write([]byte(wsKey + wsAcceptGUID))
		accept := base64.StdEncoding.EncodeToString(h.Sum(nil))
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + accept + "\r\n" +
			qwpHeaderVersion + ": " + fmt.Sprintf("%d", qwpVersion) + "\r\n" +
			"\r\n"
		if _, err := serverConn.Write([]byte(resp)); err != nil {
			return
		}
		if len(preSend) > 0 {
			if _, err := serverConn.Write(preSend); err != nil {
				return
			}
		}
		// Stall: never read again. The client's next Write blocks
		// because net.Pipe has no buffer.
		<-stallDone
	}()

	dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(dialCtx, "ws://stall.local"+qwpReadPath, &websocket.DialOptions{
		HTTPHeader: http.Header{
			qwpHeaderMaxVersion: []string{fmt.Sprintf("%d", qwpVersion)},
			qwpHeaderClientId:   []string{qwpClientId},
		},
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return clientConn, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	conn.SetReadLimit(-1)

	return &qwpTransport{conn: conn}, clientConn
}

// TestQwpEgressIOShutdownUnblocksStuckWrite checks that qwpEgressIO's
// shutdown returns promptly even when the dispatcher is parked inside
// a conn.Write that the peer has stopped draining AND the reader is
// not currently inside conn.Read. Regression guard for sendMessage
// passing context.Background(): with that bug the Write has no ctx
// to observe shutdown, so it stays parked until the underlying
// transport is torn down externally. Cancelling readCtx does NOT
// help here — coder/websocket only tears down the underlying net.Conn
// via the AfterFunc registered during an active Read, and that
// AfterFunc has been unregistered by the time the reader parks on
// frameCh.
//
// Scenario setup:
//
//  1. Server upgrades, emits one valid binary WS frame, then stalls.
//  2. Reader receives the frame, returns from conn.Read (Read timeout
//     AfterFunc cleared), and parks on the frameCh/shutdownCh select.
//  3. User submits a query. Dispatcher picks it up and enters
//     sendQueryRequest → conn.Write; net.Pipe blocks the Write because
//     the server is no longer reading.
//  4. shutdown is called. Reader wakes via shutdownCh and exits. The
//     dispatcher must also wind down within the timeout — only a
//     shutdown-aware Write ctx can guarantee that.
func TestQwpEgressIOShutdownUnblocksStuckWrite(t *testing.T) {
	// One valid server-to-client binary WS frame: FIN+binary opcode,
	// 1-byte payload (content is irrelevant — the dispatcher never
	// decodes it, because it's stuck in Write before reaching
	// receiveLoop).
	preSend := []byte{0x82, 0x01, 0x00}
	tr, clientConn := newStalledTransport(t, preSend)
	t.Cleanup(func() { _ = clientConn.Close() })

	io := newQwpEgressIO(tr, 2)
	io.start()

	// Let the reader pull the pre-sent frame off the wire and park on
	// the frameCh send — at which point it is no longer inside
	// conn.Read and readCtx cancellation can no longer tear down the
	// underlying net.Conn via coder/websocket's read AfterFunc.
	time.Sleep(100 * time.Millisecond)

	submitCtx, submitCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer submitCancel()
	if err := io.submitQuery(submitCtx, qwpRequest{sql: "SELECT 1", requestId: 1}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// Give the dispatcher time to pick up the request and park inside
	// conn.Write on the stalled pipe.
	time.Sleep(100 * time.Millisecond)

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer shutCancel()
	start := time.Now()
	err := io.shutdown(shutCtx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("shutdown returned %v after %v; want clean return — sendMessage ctx must participate in shutdown", err, elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown took %v; want well under 500ms — dispatcher was stuck in Write past shutdown signal", elapsed)
	}
}

// fillFrameReader fills every Read fully and never reports io.EOF,
// modelling a hostile or buggy server streaming an unbounded frame.
type fillFrameReader struct{}

func (fillFrameReader) Read(p []byte) (int, error) { return len(p), nil }

// TestQwpReadFrameIntoCeiling pins the defense-in-depth ceiling: an
// unbounded inbound frame must be rejected without growing the buffer
// past qwpMaxFrameReadLimit (host-OOM hardening), while a legitimate
// frame of exactly qwpMaxBatchSize — the egress decoder's own accept
// boundary — must still be read in full. The latter is what
// qwpReadLimitSlack buys: coder/websocket's limitReader and this
// function would otherwise false-reject an exactly-cap frame whose
// terminal io.EOF arrives on a separate Read.
func TestQwpReadFrameIntoCeiling(t *testing.T) {
	buf := make([]byte, 0)
	pb := &buf
	out, err := qwpReadFrameInto(fillFrameReader{}, pb)
	if err == nil {
		t.Fatalf("unbounded frame: expected error, got nil (len=%d)", len(out))
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unbounded frame: unexpected error: %v", err)
	}
	if cap(*pb) > qwpMaxFrameReadLimit {
		t.Fatalf("buffer grew to cap %d, exceeds ceiling %d", cap(*pb), qwpMaxFrameReadLimit)
	}

	buf2 := make([]byte, 0)
	pb2 := &buf2
	out2, err := qwpReadFrameInto(io.LimitReader(fillFrameReader{}, qwpMaxBatchSize), pb2)
	if err != nil {
		t.Fatalf("exact-qwpMaxBatchSize frame rejected: %v", err)
	}
	if len(out2) != qwpMaxBatchSize {
		t.Fatalf("exact-qwpMaxBatchSize frame: got %d bytes, want %d", len(out2), qwpMaxBatchSize)
	}
}

// TestQwpEgressIOCacheResetMidQuery drives a CACHE_RESET interleaved
// between two RESULT_BATCH frames of the SAME query. The server contract
// is that CACHE_RESET arrives between queries, but the dispatcher must
// not be tripped up if one lands mid-query: it consumes the frame
// silently (no user-visible event, the query is not terminated) and
// clears the connection dict, after which the continuation batch
// re-seeds the dict from id 0 and decodes normally.
//
// The continuation's delta carries deltaStart=0, which qwpConnDict
// accepts only when the dict was actually cleared (otherwise appendDelta
// rejects it as out of sync) — so a regression that dropped or
// mis-ordered the mid-query reset surfaces here as a decode error on
// batch 1 rather than a silent pass.
func TestQwpEgressIOCacheResetMidQuery(t *testing.T) {
	const reqID = int64(21)
	globalDict := []string{"AAPL", "MSFT"}

	// batch_seq 0: rows AAPL, MSFT (ids 0,1); seeds dict ids 0..1.
	tb0 := newQwpTableBuffer("t")
	for _, id := range []int32{0, 1} {
		col, _ := tb0.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb0.commitRow()
	}
	var enc qwpEncoder
	batch0 := wrapAsResultBatch(enc.encodeTableWithDeltaDict(tb0, globalDict, -1, 1), reqID, 0)

	// batch_seq 1 (continuation): rows MSFT, AAPL (ids 1,0). Re-advertises
	// ids 0..1 from deltaStart=0 — valid only because the mid-query
	// CACHE_RESET cleared the dict first.
	tb1 := newQwpTableBuffer("t")
	for _, id := range []int32{1, 0} {
		col, _ := tb1.getOrCreateColumn("s", qwpTypeSymbol, false)
		col.addSymbolID(id)
		tb1.commitRow()
	}
	batch1 := wrapAsResultBatch(enc.encodeTableWithDeltaDict(tb1, globalDict, -1, 1), reqID, 1)

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.readBinary(ctx)
		m.sendBinary(ctx, batch0)
		m.sendBinary(ctx, writeQwpFrame(0, buildCacheResetBody(qwpResetMaskDict)))
		m.sendBinary(ctx, batch1)
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 1, 4)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()
	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{sql: "SELECT s FROM t", requestId: reqID}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// The mid-query CACHE_RESET is consumed silently: the event stream is
	// exactly {Batch, Batch, End}.
	ev0 := takeEventOrFail(t, io, 2*time.Second)
	if ev0.kind != qwpEventKindBatch {
		t.Fatalf("event 0 = %v, want Batch (errMsg=%q)", ev0.kind, ev0.errMessage)
	}
	if a, b := ev0.batch.batch.String(0, 0), ev0.batch.batch.String(0, 1); a != "AAPL" || b != "MSFT" {
		t.Errorf("batch 0 rows = %q,%q, want AAPL,MSFT", a, b)
	}
	ev0.batch.release()

	ev1 := takeEventOrFail(t, io, 2*time.Second)
	if ev1.kind != qwpEventKindBatch {
		t.Fatalf("event 1 = %v, want Batch (errMsg=%q)", ev1.kind, ev1.errMessage)
	}
	if a, b := ev1.batch.batch.String(0, 0), ev1.batch.batch.String(0, 1); a != "MSFT" || b != "AAPL" {
		t.Errorf("batch 1 rows = %q,%q, want MSFT,AAPL", a, b)
	}
	ev1.batch.release()

	end := takeEventOrFail(t, io, 2*time.Second)
	if end.kind != qwpEventKindEnd {
		t.Fatalf("event 2 = %v, want End (errMsg=%q)", end.kind, end.errMessage)
	}

	// The continuation re-seeded the dict from id 0 after the reset.
	shutdownIO(t, io)
	if got := io.decoder.dict.size(); got != 2 {
		t.Errorf("dict size after reset+reseed = %d, want 2", got)
	}
}

// TestQwpEgressIOCreditStarvationNeverReleases pins the behavior when a
// flow-controlled query's consumer reads a batch and then never releases
// it: with the buffer pool exhausted, the dispatcher parks (no busy-spin,
// no further events) and — because CREDIT is only emitted on release —
// the server is starved of credit (no CREDIT frame is sent). shutdown
// must still unblock the parked dispatcher and return cleanly, proving
// no deadlock or goroutine leak.
func TestQwpEgressIOCreditStarvationNeverReleases(t *testing.T) {
	const reqID = int64(31)
	const initialCredit = int64(64 * 1024)

	sawCredit := make(chan struct{}, 1)
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		if _, _, credit := parseQueryRequest(t, req); credit != initialCredit {
			t.Errorf("server saw credit=%d, want %d", credit, initialCredit)
		}
		// Two batches: with pool size 1 the client decodes the first and
		// parks acquiring a buffer for the second.
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 0, "v", 10))
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 1, "v", 20))
		// Watch for a CREDIT frame until the client disconnects. A
		// never-releasing consumer sends none. Read directly (not
		// readBinary) so the expected close/cancel is not fatal.
		for {
			typ, data, err := m.conn.Read(ctx)
			if err != nil {
				return
			}
			if typ == websocket.MessageBinary && len(data) > 0 && data[0] == byte(qwpMsgKindCredit) {
				select {
				case sawCredit <- struct{}{}:
				default:
				}
				return
			}
		}
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()
	io := newQwpEgressIO(tr, 1) // pool of size 1
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{
		sql:           "SELECT v FROM t",
		requestId:     reqID,
		initialCredit: initialCredit,
	}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	// Read the first batch and HOLD it — never release.
	ev := takeEventOrFail(t, io, 2*time.Second)
	if ev.kind != qwpEventKindBatch {
		t.Fatalf("first event = %v, want Batch (errMsg=%q)", ev.kind, ev.errMessage)
	}

	// The dispatcher parks on the exhausted pool: no second event arrives.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	if _, err := io.takeEvent(shortCtx); err == nil {
		shortCancel()
		t.Fatal("event arrived while the consumer starved the pool")
	}
	shortCancel()

	// No CREDIT is emitted while the batch is held.
	select {
	case <-sawCredit:
		t.Fatal("client emitted CREDIT despite the consumer never releasing")
	case <-time.After(800 * time.Millisecond):
	}

	// shutdown must unblock the parked dispatcher and return cleanly,
	// even though the held batch is never released.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	start := time.Now()
	if err := io.shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown returned %v; want clean return despite a never-releasing consumer", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("shutdown took %v; dispatcher did not unblock promptly", elapsed)
	}
}

// TestQwpEgressIOBindPath verifies the egress bind path end-to-end at the
// I/O layer: typed binds encoded via QwpBinds are carried verbatim in the
// QUERY_REQUEST after the bind_count field, and the query then completes
// normally. Unit-level coverage for bind transmission, which is otherwise
// exercised only by the server-fixture fuzz tests.
func TestQwpEgressIOBindPath(t *testing.T) {
	const reqID = int64(41)
	const wantSQL = "SELECT * FROM t WHERE a = $1 AND b = $2"

	// Encode two typed binds the way QwpQueryClient.buildRequest does.
	var binds QwpBinds
	binds.reset()
	binds.LongBind(0, 0x0123456789ABCDEF).VarcharBind(1, "needle")
	if err := binds.Err(); err != nil {
		t.Fatalf("encode binds: %v", err)
	}
	wantBindPayload := append([]byte(nil), binds.bufferBytes()...)
	wantBindCount := binds.Count()
	if wantBindCount != 2 {
		t.Fatalf("bind count = %d, want 2", wantBindCount)
	}

	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := m.readBinary(ctx)
		gotID, gotSQL, _ := parseQueryRequest(t, req)
		if gotID != reqID {
			t.Errorf("server saw requestId=%d, want %d", gotID, reqID)
		}
		if gotSQL != wantSQL {
			t.Errorf("server saw sql=%q, want %q", gotSQL, wantSQL)
		}
		// The typed bind block is the tail of QUERY_REQUEST after the
		// bind_count varint; verify it byte-for-byte against the client
		// encoding.
		if !strings.HasSuffix(string(req), string(wantBindPayload)) {
			t.Errorf("QUERY_REQUEST missing expected %d-byte bind payload suffix", len(wantBindPayload))
		}
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID, 0, "v", 777))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 1)))
	})
	defer srv.Close()

	tr := connectEgress(t, srv.URL)
	defer tr.close()
	io := newQwpEgressIO(tr, 2)
	io.start()
	defer shutdownIO(t, io)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := io.submitQuery(ctx, qwpRequest{
		sql:         wantSQL,
		requestId:   reqID,
		bindCount:   wantBindCount,
		bindPayload: wantBindPayload,
	}); err != nil {
		t.Fatalf("submitQuery: %v", err)
	}

	values := drainBatchesToEnd(t, io, 1)
	if len(values) != 1 || values[0] != 777 {
		t.Fatalf("batch values = %v, want [777]", values)
	}
}
