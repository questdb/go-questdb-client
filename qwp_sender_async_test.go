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

package questdb

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestQwpAsyncAcquireAndRelease(t *testing.T) {
	a := newQwpAsyncState(2, nil)

	// Should acquire 2 slots without blocking.
	if err := a.acquireSlot(); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := a.acquireSlot(); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	a.mu.Lock()
	if a.inFlightCount != 2 {
		t.Fatalf("inFlightCount = %d, want 2", a.inFlightCount)
	}
	a.mu.Unlock()

	// Release one slot.
	a.releaseSlot()

	a.mu.Lock()
	if a.inFlightCount != 1 {
		t.Fatalf("inFlightCount after release = %d, want 1", a.inFlightCount)
	}
	a.mu.Unlock()

	// Should be able to acquire one more.
	if err := a.acquireSlot(); err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
}

func TestQwpAsyncAcquireBlocksAtMax(t *testing.T) {
	a := newQwpAsyncState(1, nil)

	// Fill the window.
	if err := a.acquireSlot(); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Second acquire should block. Use a goroutine to test.
	acquired := make(chan struct{})
	go func() {
		a.acquireSlot()
		close(acquired)
	}()

	// Wait a bit — should NOT have acquired.
	select {
	case <-acquired:
		t.Fatal("acquire should have blocked but didn't")
	case <-time.After(50 * time.Millisecond):
		// Good, it's blocked.
	}

	// Release the slot — should unblock.
	a.releaseSlot()

	select {
	case <-acquired:
		// Good, unblocked.
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after release")
	}
}

func TestQwpAsyncSetErrorUnblocksAcquire(t *testing.T) {
	a := newQwpAsyncState(1, nil)

	// Fill the window.
	a.acquireSlot()

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.acquireSlot()
	}()

	// Wait for the goroutine to be blocked.
	time.Sleep(20 * time.Millisecond)

	// Set an error — should unblock with error.
	testErr := fmt.Errorf("test I/O failure")
	a.setError(testErr)

	select {
	case err := <-errCh:
		if err != testErr {
			t.Fatalf("acquire returned wrong error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after setError")
	}
}

func TestQwpAsyncWaitEmpty(t *testing.T) {
	a := newQwpAsyncState(3, nil)

	// Acquire 3 slots.
	a.acquireSlot()
	a.acquireSlot()
	a.acquireSlot()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- a.waitEmpty()
	}()

	// Should still be waiting.
	select {
	case <-doneCh:
		t.Fatal("waitEmpty should be blocking")
	case <-time.After(50 * time.Millisecond):
	}

	// Release 2 — still 1 in flight.
	a.releaseSlot()
	a.releaseSlot()

	select {
	case <-doneCh:
		t.Fatal("waitEmpty should still be blocking with 1 in flight")
	case <-time.After(50 * time.Millisecond):
	}

	// Release last.
	a.releaseSlot()

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("waitEmpty: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitEmpty did not return after all released")
	}
}

func TestQwpAsyncWaitEmptyWithError(t *testing.T) {
	a := newQwpAsyncState(2, nil)

	a.acquireSlot()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- a.waitEmpty()
	}()

	time.Sleep(20 * time.Millisecond)

	testErr := fmt.Errorf("transport error")
	a.setError(testErr)

	select {
	case err := <-doneCh:
		if err != testErr {
			t.Fatalf("waitEmpty returned wrong error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitEmpty did not return after setError")
	}
}

func TestQwpAsyncCheckError(t *testing.T) {
	a := newQwpAsyncState(2, nil)

	if err := a.checkError(); err != nil {
		t.Fatalf("checkError on fresh state: %v", err)
	}

	testErr := fmt.Errorf("some error")
	a.setError(testErr)

	if err := a.checkError(); err != testErr {
		t.Fatalf("checkError = %v, want %v", err, testErr)
	}

	// Second setError should not overwrite.
	a.setError(fmt.Errorf("second error"))
	if err := a.checkError(); err != testErr {
		t.Fatalf("checkError after second setError = %v, want %v", err, testErr)
	}
}

func TestQwpAsyncMarkStopped(t *testing.T) {
	a := newQwpAsyncState(1, nil)

	// Fill window.
	a.acquireSlot()

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.acquireSlot()
	}()

	time.Sleep(20 * time.Millisecond)
	a.markStopped()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after markStopped")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after markStopped")
	}
}

func TestQwpAsyncIoLoopSendAndAck(t *testing.T) {
	// Mock WebSocket server that ACKs each message.
	var received [][]byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, data, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			received = append(received, data)
			mu.Unlock()
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	// Create transport and connect.
	var transport qwpTransport
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	if err := transport.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer transport.close(context.Background())

	// Create async state with window=2.
	a := newQwpAsyncState(2, &transport)
	a.start()

	// Send 3 batches through the I/O loop.
	for i := 0; i < 3; i++ {
		if err := a.acquireSlot(); err != nil {
			t.Fatalf("acquireSlot %d: %v", i, err)
		}
		a.sendCh <- []byte{byte(i + 1), byte(i + 2)}
	}

	// Wait for all in-flight to be ACKed.
	if err := a.waitEmpty(); err != nil {
		t.Fatalf("waitEmpty: %v", err)
	}

	// Stop the I/O goroutine.
	a.stop()

	// Verify all 3 batches were received.
	mu.Lock()
	if len(received) != 3 {
		t.Fatalf("received %d batches, want 3", len(received))
	}
	mu.Unlock()

	// Verify no error.
	if err := a.checkError(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQwpAsyncIoLoopServerError(t *testing.T) {
	// Mock server that returns an error ACK on the second message.
	msgCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			msgCount++
			if msgCount == 2 {
				// Return error ACK.
				errMsg := "bad batch"
				ack := make([]byte, 3+len(errMsg))
				ack[0] = qwpWireStatusWriteError
				ack[1] = byte(len(errMsg))
				ack[2] = 0
				copy(ack[3:], errMsg)
				conn.Write(context.Background(), websocket.MessageBinary, ack)
			} else {
				ack := make([]byte, 9)
				ack[0] = qwpWireStatusOK
				conn.Write(context.Background(), websocket.MessageBinary, ack)
			}
		}
	}))
	defer srv.Close()

	var transport qwpTransport
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	if err := transport.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}
	defer transport.close(context.Background())

	a := newQwpAsyncState(2, &transport)
	a.start()

	// Send first batch (will succeed).
	a.acquireSlot()
	a.sendCh <- []byte{0x01}

	// Give the I/O loop time to process.
	time.Sleep(20 * time.Millisecond)

	// Send second batch (will fail).
	a.acquireSlot()
	a.sendCh <- []byte{0x02}

	// Wait for error to propagate.
	a.stop()

	err := a.checkError()
	if err == nil {
		t.Fatal("expected error from server")
	}
	qErr, ok := err.(*QwpError)
	if !ok {
		t.Fatalf("expected *QwpError, got %T: %v", err, err)
	}
	if qErr.Status != qwpWireStatusWriteError {
		t.Fatalf("status = %d, want %d", qErr.Status, qwpWireStatusWriteError)
	}
}

func TestQwpAsyncConcurrentAcquireRelease(t *testing.T) {
	a := newQwpAsyncState(4, nil)

	var wg sync.WaitGroup
	const goroutines = 8
	const iterations = 100

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := a.acquireSlot(); err != nil {
					return
				}
				a.releaseSlot()
			}
		}()
	}

	wg.Wait()

	a.mu.Lock()
	if a.inFlightCount != 0 {
		t.Fatalf("inFlightCount = %d, want 0", a.inFlightCount)
	}
	a.mu.Unlock()
}

func TestQwpAsyncGoroutineLeakOnClose(t *testing.T) {
	// Verify the I/O goroutine exits cleanly after stop().
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			ack := make([]byte, 9)
			ack[0] = qwpWireStatusOK
			conn.Write(context.Background(), websocket.MessageBinary, ack)
		}
	}))
	defer srv.Close()

	var transport qwpTransport
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	if err := transport.connect(context.Background(), wsURL, qwpTransportOpts{}); err != nil {
		t.Fatal(err)
	}

	a := newQwpAsyncState(2, &transport)
	a.start()

	// Send a batch and wait for ACK.
	a.acquireSlot()
	a.sendCh <- []byte{0x01}
	if err := a.waitEmpty(); err != nil {
		t.Fatalf("waitEmpty: %v", err)
	}

	// Stop should close the channel and wait for goroutine exit.
	a.stop()

	// Verify the done channel is closed (goroutine exited).
	select {
	case <-a.done:
		// Good.
	default:
		t.Fatal("done channel not closed after stop()")
	}

	// Verify stopped flag is set.
	a.mu.Lock()
	if !a.stopped {
		t.Fatal("stopped flag not set after stop()")
	}
	a.mu.Unlock()

	transport.close(context.Background())
}

func TestQwpAsyncCloseAfterError(t *testing.T) {
	// Verify Close works correctly after an I/O error in async mode.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		// Close immediately to cause an error on the next send.
		conn.Close(websocket.StatusGoingAway, "bye")
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Add a row.
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())

	// Flush will fail (server closed connection).
	err = s.Flush(context.Background())
	// Error is expected since the server closed the connection.
	t.Logf("Flush error (expected): %v", err)

	// Close should not panic or hang.
	closeErr := s.Close(context.Background())
	t.Logf("Close error: %v", closeErr)

	// Double close should return the standard error.
	err = s.Close(context.Background())
	if err != errDoubleSenderClose {
		t.Fatalf("double close: got %v, want errDoubleSenderClose", err)
	}
}

func TestQwpAsyncCloseUnresponsiveServer(t *testing.T) {
	// Verify that Close() completes within a reasonable timeout even
	// when the server accepts the WebSocket connection and reads
	// messages but never sends ACKs. Without a cancellable context in
	// the I/O goroutine, sendMessage or readAck would block forever
	// and Close() would hang.

	// blockForever keeps the server handler alive but never sends ACKs.
	blockForever := make(chan struct{})
	defer close(blockForever)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{qwpSubprotocol},
		})
		if err != nil {
			return
		}
		defer conn.CloseNow()

		// Read messages but never ACK — simulate an unresponsive server.
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			// Block instead of sending an ACK.
			<-blockForever
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	s, err := newQwpLineSender(context.Background(), wsURL, qwpTransportOpts{}, 0, 0, 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a row and start async flush (enqueue to I/O goroutine).
	s.Table("t").Int64Column("x", 1).AtNow(context.Background())
	// Manually enqueue so we have an in-flight batch.
	s.enqueueFlush(context.Background())

	// Close must complete within 5 seconds. Without context
	// cancellation, the I/O goroutine would block forever on
	// readAck(context.Background()).
	done := make(chan error, 1)
	go func() {
		done <- s.Close(context.Background())
	}()

	select {
	case err := <-done:
		// Close completed — it should return an error (cancelled context).
		t.Logf("Close returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not complete within 5 seconds — I/O goroutine is stuck")
	}
}
