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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestSenderProgressHandlerFires drives a settled (non-durable) server that
// cumulatively OK-acks each flushed batch, and asserts the progress handler sees
// a strictly monotonic ackedFsn stream.
func TestSenderProgressHandlerFires(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(qwpHeaderVersion, "1")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var seq int64
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
			_ = conn.Write(ctx, websocket.MessageBinary, buildAckOK(seq))
			seq++
		}
	}))
	defer srv.Close()

	progress := make(chan int64, 16)
	addr := strings.TrimPrefix(srv.URL, "http://")
	s, err := NewLineSender(ctx,
		WithQwp(), WithAddress(addr),
		WithProgressHandler(func(fsn int64) { progress <- fsn }),
	)
	if err != nil {
		t.Fatalf("NewLineSender: %v", err)
	}
	defer s.Close(ctx)

	for i := 0; i < 2; i++ {
		if err := s.Table("trades").Int64Column("v", int64(i)).AtNow(ctx); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	var last int64 = -1
	for want := int64(0); want <= 1; want++ {
		select {
		case got := <-progress:
			if got <= last {
				t.Fatalf("progress not monotonic: got %d after %d", got, last)
			}
			last = got
		case <-time.After(5 * time.Second):
			t.Fatalf("progress handler did not reach %d (stuck at %d)", want, last)
		}
	}
	if last != 1 {
		t.Fatalf("final progress = %d, want 1", last)
	}
}
