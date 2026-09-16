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
// failed without returning a handle. Exercise the public logger options, default
// resolution timing, and a handler that records each attempted diagnostic before
// panicking. The operation must still report its intended cleanup failure.
func TestQwpTransportCleanupLoggerRouting(t *testing.T) {
	for _, family := range []string{"sender", "query", "facade"} {
		for _, stage := range []string{"close", "failed-setup"} {
			for _, sink := range []string{"configured", "default", "panicking-configured", "panicking-default"} {
				t.Run(family+"/"+stage+"/"+sink, func(t *testing.T) {
					configured := &recordCapturingHandler{}
					beforeOptions, construction, emission := &recordCapturingHandler{}, &recordCapturingHandler{}, &recordCapturingHandler{}
					newLogger := func(capture *recordCapturingHandler) *slog.Logger {
						if strings.HasPrefix(sink, "panicking-") {
							return slog.New(&panicAfterRecordingLogHandler{capture})
						}
						return slog.New(capture)
					}
					previous := slog.Default()
					t.Cleanup(func() { slog.SetDefault(previous) })
					slog.SetDefault(newLogger(beforeOptions))
					// The last constructor option changes the default after the
					// logger option was applied but before downstream resolution.
					advanceDefault := func() { slog.SetDefault(newLogger(construction)) }
					var logger *slog.Logger
					if strings.HasSuffix(sink, "configured") {
						logger = newLogger(configured)
					}
					// "default" omits the option; "panicking-default" supplies nil.
					useOption := sink != "default"

					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if stage == "close" {
							w.Header().Set(qwpHeaderVersion, "1")
						}
						conn, err := websocket.Accept(w, r, nil)
						if err != nil {
							return
						}
						defer conn.CloseNow()
						if family != "sender" && stage == "close" {
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

					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					addr := strings.TrimPrefix(srv.URL, "http://")
					var closeClient func(context.Context) error
					var err error
					if family == "sender" {
						opts := []LineSenderOption{WithQwp(), WithAddress(addr), WithInitialConnectMode(InitialConnectOff), WithCloseFlushTimeout(0)}
						if useOption {
							opts = append(opts, WithLogger(logger))
						}
						opts = append(opts, func(*lineSenderConfig) { advanceDefault() })
						var s LineSender
						s, err = NewLineSender(ctx, opts...)
						if s != nil {
							closeClient = s.Close
						}
					} else if family == "query" {
						opts := []QwpQueryClientOption{WithQwpQueryAddress(addr)}
						if useOption {
							opts = append(opts, WithQwpQueryClientLogger(logger))
						}
						opts = append(opts, func(*qwpQueryClientConfig) { advanceDefault() })
						var c *QwpQueryClient
						c, err = NewQwpQueryClient(ctx, opts...)
						if c != nil {
							closeClient = c.Close
						}
					} else {
						// One eager query connection exercises the facade's logger
						// propagation without unrelated ingest reconnect activity.
						opts := []QuestDBOption{WithSenderPoolMin(0), WithQueryPoolMin(1)}
						if useOption {
							opts = append(opts, WithQuestDBLogger(logger))
						}
						opts = append(opts, func(*questDBConfig) { advanceDefault() })
						var db *QuestDB
						db, err = NewQuestDB(ctx, "ws::addr="+addr+";", opts...)
						if db != nil {
							closeClient = db.Close
						}
					}
					if closeClient != nil {
						defer closeClient(context.Background())
					}
					if stage == "failed-setup" {
						require.ErrorContains(t, err, "server did not return "+qwpHeaderVersion+" header")
						require.Nil(t, closeClient)
					} else {
						require.NoError(t, err)
						require.NotNil(t, closeClient)
						slog.SetDefault(newLogger(emission))
						require.ErrorIs(t, closeClient(ctx), ErrCleanupFailed)
					}

					const message = "qwp: transport release failed"
					expected := configured
					if logger == nil {
						expected = construction
						if stage == "close" && family != "facade" {
							// Standalone transport cleanup resolves an unset logger
							// when emitting; facade components retain its choice.
							expected = emission
						}
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
					for _, other := range []*recordCapturingHandler{configured, beforeOptions, construction, emission} {
						if other != expected {
							require.NotContains(t, other.messages(), message, "cleanup diagnostic reached the wrong logger")
						}
					}
				})
			}
		}
	}
}

// Recording is synchronized by the embedded handler. Seeing the production
// diagnostic proves Handle was reached, rather than filtered by Enabled.
type panicAfterRecordingLogHandler struct{ *recordCapturingHandler }

func (h *panicAfterRecordingLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	_ = h.recordCapturingHandler.Handle(ctx, rec)
	panic("logging handler failed after recording the attempt")
}
