/*******************************************************************************
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
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// These tests cover a wss peer that resets the connection, for example a
// server process that dies mid-stream. Closing the dead connection makes
// tls.Conn.Close report that it could not send close_notify, although the
// socket is closed. The client reconnects and delivers everything, so Close
// must not report that release error.

// qwpResetTestUpgrade writes a raw 101 response, so the server side can reset
// the TCP connection without going through a WebSocket library.
func qwpResetTestUpgrade(r *http.Request, version string) string {
	h := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(h[:]) + "\r\n" +
		qwpHeaderVersion + ": " + version + "\r\n\r\n"
}

// qwpResetTestAbort resets the hijacked connection: SO_LINGER 0, then close.
func qwpResetTestAbort(conn net.Conn) {
	var tcp *net.TCPConn
	if tc, ok := conn.(*tls.Conn); ok {
		tcp = tc.NetConn().(*net.TCPConn)
	} else {
		tcp = conn.(*net.TCPConn)
	}
	_ = tcp.SetLinger(0)
	_ = tcp.Close()
}

// qwpResetTestIngestServer resets the first connection shortly after the
// upgrade and acknowledges every frame on later connections.
func qwpResetTestIngestServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var conns atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conns.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte(qwpResetTestUpgrade(r, "1")))
			time.Sleep(300 * time.Millisecond)
			qwpResetTestAbort(conn)
			return
		}
		w.Header().Set(qwpHeaderVersion, "1")
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		var seq int64
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				return
			}
			_ = c.Write(context.Background(), websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &conns
}

func qwpResetTestIngestConf(srv *httptest.Server, extra string) string {
	return "wss::addr=" + strings.TrimPrefix(srv.URL, "https://") + ";tls_verify=unsafe_off;" +
		"reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;" + extra
}

// qwpResetTestWriteAndAwait keeps publishing across the reset, then waits
// until the server has acknowledged everything.
func qwpResetTestWriteAndAwait(t *testing.T, s QwpSender) {
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		require.NoError(t, s.Table("t").Int64Column("v", int64(i)).AtNow(ctx))
		_, err := s.FlushAndGetSequence(ctx)
		require.NoError(t, err)
		time.Sleep(25 * time.Millisecond)
	}
	fsn, err := s.FlushAndGetSequence(ctx)
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, qwpTestWaitTimeout)
	defer cancel()
	require.NoError(t, s.AwaitAckedFsn(waitCtx, fsn))
}

func TestQwpWssResetThenFullDeliveryCloseReturnsNil(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra func(t *testing.T) string
	}{
		{"memory", func(*testing.T) string { return "" }},
		{"store-and-forward", func(t *testing.T) string { return "sf_dir=" + t.TempDir() + ";sender_id=reset;" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, conns := qwpResetTestIngestServer(t)
			ls, err := LineSenderFromConf(context.Background(), qwpResetTestIngestConf(srv, tc.extra(t)))
			require.NoError(t, err)
			s := ls.(QwpSender)
			qwpResetTestWriteAndAwait(t, s)
			require.GreaterOrEqual(t, conns.Load(), int32(2), "the first connection must have been reset")
			require.Positive(t, s.TotalReconnectsSucceeded())
			closeCtx, cancel := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
			defer cancel()
			require.NoError(t, s.Close(closeCtx), "every row was acknowledged and the dead socket is closed")
			require.Eventually(t, s.SlotLockReleased, qwpTestWaitTimeout, time.Millisecond)
		})
	}
}

func TestQwpFacadeWssResetThenFullDeliveryCloseReturnsNil(t *testing.T) {
	srv, conns := qwpResetTestIngestServer(t)
	ctx := context.Background()
	db, err := NewQuestDB(ctx, qwpResetTestIngestConf(srv, "lazy_connect=true;query_pool_min=0;sender_pool_min=1;"))
	require.NoError(t, err)
	ls, err := db.BorrowSender(ctx)
	require.NoError(t, err)
	qwpResetTestWriteAndAwait(t, ls.(QwpSender))
	require.GreaterOrEqual(t, conns.Load(), int32(2), "the first connection must have been reset")
	require.NoError(t, ls.Close(ctx))
	for i := 0; i < 2; i++ {
		closeCtx, cancel := context.WithTimeout(ctx, qwpTestWaitTimeout)
		err = db.Close(closeCtx)
		cancel()
		require.NoError(t, err, "QuestDB.Close call %d", i+1)
	}
}

func qwpResetTestWriteWsBinary(w io.Writer, p []byte) error {
	hdr := []byte{0x82}
	switch {
	case len(p) < 126:
		hdr = append(hdr, byte(len(p)))
	case len(p) <= 0xFFFF:
		hdr = append(hdr, 126, byte(len(p)>>8), byte(len(p)))
	default:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(len(p)))
		hdr = append(hdr, 127)
		hdr = append(hdr, b[:]...)
	}
	_, err := w.Write(append(hdr, p...))
	return err
}

func TestQwpQueryWssResetThenFailoverCloseReturnsNil(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := buildServerInfoFrame(qwpVersion, 0, qwpRolePrimary, 1, 0, time.Now().UnixNano(), "test-cluster", "node-0")
		if conns.Add(1) == 1 {
			conn, brw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte(qwpResetTestUpgrade(r, fmt.Sprintf("%d", qwpVersion))))
			_ = qwpResetTestWriteWsBinary(conn, info)
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			// Wait for the query request, then reset the connection.
			_, _ = io.ReadFull(bufio.NewReader(brw), make([]byte, 1))
			time.Sleep(50 * time.Millisecond)
			qwpResetTestAbort(conn)
			return
		}
		w.Header().Set(qwpHeaderVersion, fmt.Sprintf("%d", qwpVersion))
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		if err := c.Write(r.Context(), websocket.MessageBinary, info); err != nil {
			return
		}
		m := &qwpMockEgressConn{t: t, conn: c, version: qwpVersion}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, _, err := c.Read(ctx); err != nil {
			return
		}
		m.sendBinary(ctx, buildOneRowInt64Batch(t, 1, 0, "v", 99))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(1, 0, 1)))
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	cfg := qwpQueryDefaultConfig()
	eps, err := parseEndpointList(strings.TrimPrefix(srv.URL, "https://"), qwpDefaultPort)
	require.NoError(t, err)
	cfg.endpoints = eps
	cfg.target = qwpTargetAny
	cfg.serverInfoTimeout = 2 * time.Second
	cfg.failoverEnabled = true
	cfg.failoverMaxAttempts = 3
	cfg.failoverBackoffInitial = time.Millisecond
	cfg.failoverBackoffMax = 10 * time.Millisecond
	cfg.tlsMode = tlsInsecureSkipVerify
	ctx, cancel := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
	defer cancel()
	c, err := newQwpQueryClient(ctx, cfg)
	require.NoError(t, err)

	q := c.Query(ctx, "select v from t")
	var gotReset, gotBatch bool
	for batch, err := range q.Batches() {
		if err != nil {
			var reset *QwpFailoverReset
			require.ErrorAs(t, err, &reset)
			gotReset = true
			continue
		}
		gotBatch = batch.Int64(0, 0) == 99
	}
	q.Close()
	require.True(t, gotReset, "the query must have failed over from the reset connection")
	require.True(t, gotBatch)
	for i := 0; i < 2; i++ {
		closeCtx, cancel := context.WithTimeout(context.Background(), qwpTestWaitTimeout)
		err := c.Close(closeCtx)
		cancel()
		require.NoError(t, err, "QwpQueryClient.Close call %d", i+1)
	}
}
