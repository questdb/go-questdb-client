/******************************************************************************
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
 *****************************************************************************/

package questdb

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// Cleanup can log after the public caller returns, including when construction
// failed without returning a handle. The transport must retain the chosen sink.
func TestQwpTransportCleanupLoggerRouting(t *testing.T) {
	for _, family := range []string{"sender", "query"} {
		for _, stage := range []string{"close", "failed-setup"} {
			for _, sink := range []string{"configured", "default"} {
				t.Run(family+"/"+stage+"/"+sink, func(t *testing.T) {
					configured, fallback := &recordCapturingHandler{}, &recordCapturingHandler{}
					previous := slog.Default()
					slog.SetDefault(slog.New(fallback))
					defer slog.SetDefault(previous)

					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if stage == "close" {
							w.Header().Set(qwpHeaderVersion, "1")
						}
						conn, err := websocket.Accept(w, r, nil)
						if err != nil {
							return
						}
						defer conn.CloseNow()
						if family == "query" && stage == "close" {
							info := buildServerInfoFrame(qwpVersion, 0, qwpRolePrimary, 1, 0,
								1_700_000_000_000_000_000, "test-cluster", "log-test")
							if err := conn.Write(r.Context(), websocket.MessageBinary, info); err != nil {
								return
							}
						}
						<-conn.CloseRead(r.Context()).Done()
					}))
					defer srv.Close()

					hook := func(tr *qwpTransport) {
						// Inject the diagnostic after releasing the real connection,
						// so terminal retention does not leave live test sockets.
						_ = tr.conn.CloseNow()
						panic("test transport cleanup diagnostic")
					}
					qwpTestBeforeTransportClose.Store(&hook)
					defer qwpTestBeforeTransportClose.Store(nil)

					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					addr := strings.TrimPrefix(srv.URL, "http://")
					var closeClient func(context.Context) error
					var err error
					if family == "sender" {
						opts := []LineSenderOption{WithQwp(), WithAddress(addr), WithInitialConnectMode(InitialConnectOff), WithCloseFlushTimeout(0)}
						if sink == "configured" {
							opts = append(opts, WithLogger(slog.New(configured)))
						}
						var s LineSender
						s, err = NewLineSender(ctx, opts...)
						if s != nil {
							closeClient = s.Close
						}
					} else {
						opts := []QwpQueryClientOption{WithQwpQueryAddress(addr)}
						if sink == "configured" {
							opts = append(opts, WithQwpQueryClientLogger(slog.New(configured)))
						}
						var c *QwpQueryClient
						c, err = NewQwpQueryClient(ctx, opts...)
						if c != nil {
							closeClient = c.Close
						}
					}
					if closeClient != nil {
						defer closeClient(context.Background())
					}
					if stage == "failed-setup" {
						require.Error(t, err)
						require.Nil(t, closeClient)
					} else {
						require.NoError(t, err)
						require.NotNil(t, closeClient)
						require.ErrorIs(t, closeClient(ctx), ErrCleanupFailed)
					}

					const message = "qwp: transport release failed"
					expected, other := configured, fallback
					if sink == "default" {
						expected, other = fallback, configured
					}
					// Logging is deliberately outside the release-completion barrier.
					require.Eventually(t, func() bool {
						for _, msg := range expected.messages() {
							if msg == message {
								return true
							}
						}
						return false
					}, time.Second, time.Millisecond, "cleanup diagnostic did not reach the expected logger")
					require.NotContains(t, other.messages(), message, "cleanup diagnostic reached the wrong logger")
				})
			}
		}
	}
}
