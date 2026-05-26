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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSfConfParseAcceptsAllKnobs(t *testing.T) {
	conf, err := confFromStr(strings.Join([]string{
		"ws::addr=localhost:9000",
		"sf_dir=/tmp/sf",
		"sender_id=my-sender",
		"sf_max_bytes=8388608",
		"sf_max_total_bytes=21474836480",
		"sf_durability=memory",
		"sf_append_deadline_millis=20000",
		"reconnect_max_duration_millis=120000",
		"reconnect_initial_backoff_millis=200",
		"reconnect_max_backoff_millis=10000",
		"initial_connect_retry=on",
		"close_flush_timeout_millis=2500",
		"drain_orphans=on",
		"max_background_drainers=2",
		"request_durable_ack=off",
		"durable_ack_keepalive_interval_millis=200;",
	}, ";"))
	require.NoError(t, err)
	assert.Equal(t, "/tmp/sf", conf.sfDir)
	assert.Equal(t, "my-sender", conf.senderId)
	assert.Equal(t, int64(8388608), conf.sfMaxBytes)
	assert.Equal(t, int64(21474836480), conf.sfMaxTotalBytes)
	assert.Equal(t, "memory", conf.sfDurability)
	assert.Equal(t, 20000, conf.sfAppendDeadlineMillis)
	assert.Equal(t, 120000, conf.reconnectMaxDurationMillis)
	assert.Equal(t, 200, conf.reconnectInitialBackoffMillis)
	assert.Equal(t, 10000, conf.reconnectMaxBackoffMillis)
	assert.Equal(t, InitialConnectSync, conf.initialConnectMode)
	assert.Equal(t, 2500, conf.closeFlushTimeoutMillis)
	assert.True(t, conf.closeFlushTimeoutSet)
	assert.True(t, conf.drainOrphans)
	assert.Equal(t, 2, conf.maxBackgroundDrainers)
}

func TestSfConfRejectsNonQwpSchema(t *testing.T) {
	for _, schema := range []string{"http", "https", "tcp", "tcps"} {
		t.Run(schema, func(t *testing.T) {
			_, err := confFromStr(schema + "::addr=localhost:9000;sf_dir=/tmp/sf;")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "QWP")
		})
	}
}

func TestSfConfRejectsBadSenderId(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sender_id=bad/id;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid character")
}

func TestSfConfRejectsBadDurability(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sf_durability=bogus;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "memory")
}

func TestSfConfRejectsDeferredDurabilityModes(t *testing.T) {
	for _, v := range []string{"flush", "append"} {
		_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sf_durability=" + v + ";")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "deferred")
	}
}

// WithSfDurability is the functional-option analogue of the
// sf_durability connect-string key. The parser rejects flush/append
// and bogus values up front; the option path is a thin setter, so the
// equivalent gate lives in sanitizeQwpConf via the shared
// validateSfDurability helper. These tests pin that parity (SSOT for
// the value space) — see TestSfConfRejectsDeferredDurabilityModes /
// TestSfConfRejectsBadDurability for the connect-string side.
func TestSfDurabilityOptionRejectsDeferredModes(t *testing.T) {
	for _, v := range []string{"flush", "append"} {
		t.Run(v, func(t *testing.T) {
			conf := newLineSenderConfig(qwpSenderType)
			WithSfDir("/tmp/sf")(conf)
			WithSfDurability(v)(conf)
			err := sanitizeQwpConf(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "deferred")
		})
	}
}

func TestSfDurabilityOptionRejectsBogus(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	WithSfDir("/tmp/sf")(conf)
	WithSfDurability("bogus")(conf)
	err := sanitizeQwpConf(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "memory")
}

func TestSfDurabilityOptionMemoryAccepted(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	WithSfDir("/tmp/sf")(conf)
	WithSfDurability("memory")(conf)
	require.NoError(t, sanitizeQwpConf(conf))
}

// WithSenderId is the functional-option analogue of the sender_id
// connect-string key. The parser rejects '.', '/', '\' and other
// out-of-charset bytes (TestSfConfRejectsBadSenderId pins that), but
// the option path used to assign the raw string straight to
// conf.senderId. The unsanitized value is then joined into the slot
// path under sfDir, so values like "../etc" would let a caller
// escape the sf_dir root. sanitizeQwpConf must apply the same charset
// gate the parser does — these tests pin parity.
func TestSenderIdOptionRejectsPathTraversal(t *testing.T) {
	for _, id := range []string{"../etc", "..", "a/b", `a\b`, "foo.bar"} {
		t.Run(id, func(t *testing.T) {
			conf := newLineSenderConfig(qwpSenderType)
			WithSfDir("/tmp/sf")(conf)
			WithSenderId(id)(conf)
			err := sanitizeQwpConf(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "sender_id")
		})
	}
}

func TestSenderIdOptionAcceptsValid(t *testing.T) {
	for _, id := range []string{"default", "ingest-1", "slot_42", "ABCxyz"} {
		t.Run(id, func(t *testing.T) {
			conf := newLineSenderConfig(qwpSenderType)
			WithSfDir("/tmp/sf")(conf)
			WithSenderId(id)(conf)
			require.NoError(t, sanitizeQwpConf(conf))
		})
	}
}

// Durable-ack mode is a deferred opt-in feature, but sf-client.md §19
// makes its connect-string keys normative: the parser MUST recognise
// request_durable_ack / durable_ack_keepalive_interval_millis so a
// user porting a Java connect string gets a clear deferred-feature
// message, not the generic "unsupported option".
func TestSfConfDurableAckOffParses(t *testing.T) {
	for _, v := range []string{"off", "false"} {
		t.Run(v, func(t *testing.T) {
			_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;request_durable_ack=" + v + ";")
			require.NoError(t, err)
		})
	}
}

func TestSfConfRejectsDurableAckOptIn(t *testing.T) {
	for _, v := range []string{"on", "true"} {
		t.Run(v, func(t *testing.T) {
			_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;request_durable_ack=" + v + ";")
			require.Error(t, err)
			// Must name the feature and that it is deferred -- not the
			// generic "unsupported option" the review flagged.
			assert.Contains(t, err.Error(), "not implemented")
			assert.Contains(t, err.Error(), "deferred")
			assert.NotContains(t, err.Error(), "unsupported option")
		})
	}
}

func TestSfConfRejectsBadDurableAckValue(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;request_durable_ack=maybe;")
	require.Error(t, err)
	for _, want := range []string{"on", "off", "true", "false"} {
		assert.Contains(t, err.Error(), want)
	}
}

func TestSfConfRejectsDurableAckKeysOnNonQwp(t *testing.T) {
	cases := []string{
		"request_durable_ack=off",
		"durable_ack_keepalive_interval_millis=200",
	}
	for _, schema := range []string{"http", "tcp"} {
		for _, c := range cases {
			t.Run(schema+"/"+c, func(t *testing.T) {
				_, err := confFromStr(schema + "::addr=localhost:9000;" + c + ";")
				require.Error(t, err)
				assert.Contains(t, err.Error(), "QWP")
			})
		}
	}
}

func TestSfConfDurableAckKeepaliveParses(t *testing.T) {
	// 0 and negative mean "disabled" per sf-client.md §4.3, so any
	// int is in range; only a non-int is rejected.
	for _, v := range []string{"200", "0", "-1"} {
		t.Run(v, func(t *testing.T) {
			_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;durable_ack_keepalive_interval_millis=" + v + ";")
			require.NoError(t, err)
		})
	}
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;durable_ack_keepalive_interval_millis=soon;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "int")
}

func TestSfConfRejectsNegativeNumbers(t *testing.T) {
	cases := []string{
		"sf_max_bytes=-1",
		"sf_max_total_bytes=-1",
		"sf_append_deadline_millis=0",
		"reconnect_initial_backoff_millis=0",
		"reconnect_max_backoff_millis=0",
		"max_background_drainers=-1",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;" + c + ";")
			require.Error(t, err)
		})
	}
}

// TestSfConfInitialConnectRetryValues exercises every accepted spelling
// of `initial_connect_retry` (Java spec §4.2 / §13.4) and the rejected
// one. The legacy bool spellings (`on`/`true`/`off`/`false`) and the
// Java-aligned tri-state words (`sync`/`async`) must all parse; bogus
// values must be rejected with a message that names every accepted
// value so users know what to type.
func TestSfConfInitialConnectRetryValues(t *testing.T) {
	cases := []struct {
		raw  string
		want InitialConnectMode
	}{
		{"on", InitialConnectSync},
		{"true", InitialConnectSync},
		{"sync", InitialConnectSync},
		{"off", InitialConnectOff},
		{"false", InitialConnectOff},
		{"async", InitialConnectAsync},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			conf, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;initial_connect_retry=" + c.raw + ";")
			require.NoError(t, err)
			assert.Equal(t, c.want, conf.initialConnectMode)
		})
	}
}

func TestSfConfInitialConnectRetryRejectsBogusValue(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;initial_connect_retry=maybe;")
	require.Error(t, err)
	// Error message must enumerate the accepted spellings so users
	// porting from Java know `sync`/`async` are valid.
	for _, want := range []string{"sync", "async", "on", "off", "true", "false"} {
		assert.Contains(t, err.Error(), want)
	}
}

// TestSfConfReconnectKeyPromotesInitialConnect pins the implicit
// promotion documented in the connect-string reference: if the user
// tuned any reconnect_* knob but did not pick an initial_connect_retry
// mode, sanitize promotes the mode to sync so the reconnect budget
// actually covers the *first* connect attempt. Mirrors Java's
// actualInitialConnectMode resolution in Sender.java.
//
// confFromStr alone returns the parser's raw view (mode stays unset);
// the promotion fires in sanitizeQwpConf. The assertions below
// exercise both layers so future refactors can't silently relocate the
// promotion to a layer the option-builder path bypasses.
func TestSfConfReconnectKeyPromotesInitialConnect(t *testing.T) {
	cases := []string{
		"reconnect_max_duration_millis=120000",
		"reconnect_initial_backoff_millis=200",
		"reconnect_max_backoff_millis=10000",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			conf, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;" + c + ";")
			require.NoError(t, err)
			// Parser keeps the user's view raw: the mode is unset and
			// the default-zero InitialConnectOff still reads.
			assert.False(t, conf.initialConnectModeSet)
			assert.Equal(t, InitialConnectOff, conf.initialConnectMode)
			// Sanitize promotes when no explicit mode was chosen.
			require.NoError(t, sanitizeQwpConf(conf))
			assert.Equal(t, InitialConnectSync, conf.initialConnectMode)
		})
	}
}

// Explicit initial_connect_retry=off paired with a tuned reconnect
// budget is a documented escape hatch: fail-fast on startup misconfig
// while still accepting a generous post-connect outage budget. The
// explicit choice must win over the promotion.
func TestSfConfInitialConnectRetryOffOverridesPromotion(t *testing.T) {
	conf, err := confFromStr(
		"ws::addr=localhost:9000;sf_dir=/tmp/sf;" +
			"reconnect_max_duration_millis=120000;" +
			"initial_connect_retry=off;")
	require.NoError(t, err)
	require.NoError(t, sanitizeQwpConf(conf))
	assert.Equal(t, InitialConnectOff, conf.initialConnectMode)
}

// initial_connect_retry=async paired with a tuned reconnect budget
// also wins over the promotion — the explicit choice is preserved
// verbatim, not silently coerced to sync.
func TestSfConfInitialConnectRetryAsyncSurvivesPromotion(t *testing.T) {
	conf, err := confFromStr(
		"ws::addr=localhost:9000;sf_dir=/tmp/sf;" +
			"reconnect_max_duration_millis=120000;" +
			"initial_connect_retry=async;")
	require.NoError(t, err)
	require.NoError(t, sanitizeQwpConf(conf))
	assert.Equal(t, InitialConnectAsync, conf.initialConnectMode)
}

// No reconnect_* knob set → no promotion. Defends against the
// promotion logic firing on the QWP defaults (which seed the
// reconnect fields lazily in the send loop, not at parse time).
func TestSfConfNoReconnectKeyNoPromotion(t *testing.T) {
	conf, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;")
	require.NoError(t, err)
	require.NoError(t, sanitizeQwpConf(conf))
	assert.Equal(t, InitialConnectOff, conf.initialConnectMode)
}

// Functional-option parity for the promotion. WithReconnectPolicy on
// its own must promote to sync; an explicit WithInitialConnectRetry
// (or WithInitialConnectMode) must win over it. This is the option
// path the Go builder API exposes, separate from the connect string.
func TestSfOptionsWithReconnectPolicyPromotes(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	WithSfDir("/tmp/sf")(conf)
	WithReconnectPolicy(2*time.Minute, 100*time.Millisecond, 5*time.Second)(conf)
	require.NoError(t, sanitizeQwpConf(conf))
	assert.Equal(t, InitialConnectSync, conf.initialConnectMode)
}

func TestSfOptionsWithInitialConnectRetryOffOverridesPromotion(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	WithSfDir("/tmp/sf")(conf)
	WithReconnectPolicy(2*time.Minute, 100*time.Millisecond, 5*time.Second)(conf)
	WithInitialConnectRetry(false)(conf)
	require.NoError(t, sanitizeQwpConf(conf))
	assert.Equal(t, InitialConnectOff, conf.initialConnectMode)
}

func TestSfOptionsWithInitialConnectModeAsyncSurvivesPromotion(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	WithSfDir("/tmp/sf")(conf)
	WithReconnectPolicy(2*time.Minute, 100*time.Millisecond, 5*time.Second)(conf)
	WithInitialConnectMode(InitialConnectAsync)(conf)
	require.NoError(t, sanitizeQwpConf(conf))
	assert.Equal(t, InitialConnectAsync, conf.initialConnectMode)
}

func TestSanitizeQwpConfRejectsSfKeysWithoutSfDir(t *testing.T) {
	cases := []func(c *lineSenderConfig){
		func(c *lineSenderConfig) { c.senderId = "x" },
		func(c *lineSenderConfig) { c.sfMaxBytes = 1 << 20 },
		func(c *lineSenderConfig) { c.sfMaxTotalBytes = 1 << 30 },
		func(c *lineSenderConfig) { c.sfDurability = "memory" },
		func(c *lineSenderConfig) { c.sfAppendDeadlineMillis = 5000 },
		func(c *lineSenderConfig) { c.drainOrphans = true },
		func(c *lineSenderConfig) { c.maxBackgroundDrainers = 4 },
	}
	for i, mut := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			conf := newLineSenderConfig(qwpSenderType)
			conf.address = "localhost:9000"
			mut(conf)
			err := sanitizeQwpConf(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "sf_dir")
		})
	}
}

func TestSanitizeQwpConfRejectsTotalLessThanSegment(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	conf.address = "localhost:9000"
	conf.sfDir = "/tmp/sf"
	conf.sfMaxBytes = 1 << 20
	conf.sfMaxTotalBytes = 1 << 18
	err := sanitizeQwpConf(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sf_max_total_bytes")
}

// TestSfConfEndToEnd builds a sender from a connect string with
// sf_dir set, sends rows through it, closes, and confirms the
// fake server saw the frames AND the slot dir was created on disk.
func TestSfConfEndToEnd(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	tmp := t.TempDir()
	addr := strings.TrimPrefix(srv.URL, "http://")
	confStr := strings.Join([]string{
		"ws::addr=" + addr,
		"sf_dir=" + tmp,
		"sender_id=test-slot",
		"sf_max_bytes=4096",
		"sf_max_total_bytes=" + fmt.Sprintf("%d", int64(64*1024)),
		"close_flush_timeout_millis=5000;",
	}, ";")

	ls, err := LineSenderFromConf(context.Background(), confStr)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, ls.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
	}
	require.NoError(t, ls.Close(context.Background()))

	// The slot dir must have been created.
	st, err := os.Stat(filepath.Join(tmp, "test-slot"))
	require.NoError(t, err)
	assert.True(t, st.IsDir())
	// On clean drain, residual .sfa files are unlinked. The .lock
	// file may remain (it's not unlinked on close).
	entries, err := os.ReadDir(filepath.Join(tmp, "test-slot"))
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotEqual(t, ".sfa", filepath.Ext(e.Name()),
			"unexpected leftover segment file %s", e.Name())
	}
	// Server received at least one frame.
	assert.GreaterOrEqual(t, srv.totalFramesReceived.Load(), int64(1))
}

func TestSfConfPicksDefaultSenderIdWhenUnset(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	tmp := t.TempDir()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";sf_dir="+tmp+";close_flush_timeout_millis=2000;")
	require.NoError(t, err)
	require.NoError(t, ls.Close(context.Background()))
	// Default sender_id is "default".
	st, err := os.Stat(filepath.Join(tmp, "default"))
	require.NoError(t, err)
	assert.True(t, st.IsDir())
}

func TestSfConfWithSfDirOptionBuilder(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	tmp := t.TempDir()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addr),
		WithSfDir(tmp),
		WithSenderId("opt-builder"),
		WithCloseFlushTimeout(2*time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls.Close(context.Background()))
	st, err := os.Stat(filepath.Join(tmp, "opt-builder"))
	require.NoError(t, err)
	assert.True(t, st.IsDir())
}

// reserveLocalPort grabs a free TCP port and immediately releases it.
// The returned address is suitable for "no server is listening here"
// scenarios — between the release and the test using the address,
// another process *could* in principle grab the port, but for short-
// lived test windows on localhost this is reliable enough in practice.
func reserveLocalPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// TestSfConfInitialConnectAsyncReturnsImmediately is the headline
// behavior of `initial_connect_retry=async`: LineSenderFromConf must
// return immediately even when no server is reachable. The I/O
// goroutine retries connect in the background; the producer is
// unblocked. With `reconnect_max_duration_millis=60000`, anything
// that waited on connect would hang the test for a minute — assert a
// sub-second construction time instead.
func TestSfConfInitialConnectAsyncReturnsImmediately(t *testing.T) {
	tmp := t.TempDir()
	addr := reserveLocalPort(t)
	cfg := strings.Join([]string{
		"ws::addr=" + addr,
		"sf_dir=" + tmp,
		"initial_connect_retry=async",
		"reconnect_max_duration_millis=60000",
		"reconnect_initial_backoff_millis=10",
		"reconnect_max_backoff_millis=50",
		// Fast close: don't block on a drain that can't complete
		// without a server.
		"close_flush_timeout_millis=0;",
	}, ";")

	t0 := time.Now()
	ls, err := LineSenderFromConf(context.Background(), cfg)
	require.NoError(t, err)
	elapsed := time.Since(t0)
	assert.Less(t, elapsed, 2*time.Second,
		"LineSenderFromConf must return immediately in async mode (took %s)", elapsed)

	// Producer-side calls work without a live wire — frames accumulate
	// on the cursor SF engine while the I/O goroutine is still trying
	// to connect.
	require.NoError(t, ls.Table("foo").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls.Close(context.Background()))
}

// TestSfConfInitialConnectAsyncDeliversWhenServerComesUp covers the
// late-arrival flow: the sender opens before the server is listening,
// the producer publishes a row to the cursor SF engine, then the
// server starts. The buffered frame must be delivered and ACKed by
// the I/O goroutine once the wire is up.
//
// Also pins the post-v4.2.0 flush contract (Java decision #1): with
// the server still down, FlushAndGetSequence must NOT block on the
// ACK — it returns the published FSN immediately because the frame
// is already durable in the SF engine. AwaitAckedFsn is the
// dedicated barrier that blocks until the I/O loop delivers it and
// the server ACKs.
func TestSfConfInitialConnectAsyncDeliversWhenServerComesUp(t *testing.T) {
	// Reserve a port and bind a listener on it that we'll later wrap
	// with httptest. By holding the port across the gap we avoid the
	// race where another process could steal it between reserve and
	// re-bind.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	tmp := t.TempDir()
	cfg := strings.Join([]string{
		"ws::addr=" + addr,
		"sf_dir=" + tmp,
		"initial_connect_retry=async",
		"reconnect_max_duration_millis=10000",
		"reconnect_initial_backoff_millis=20",
		"reconnect_max_backoff_millis=200",
		"close_flush_timeout_millis=5000;",
	}, ";")

	ls, err := LineSenderFromConf(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = ls.Close(context.Background()) }()

	qs, ok := ls.(QwpSender)
	require.True(t, ok, "QWP sender must satisfy QwpSender")

	// Append a row before the server is up. The frame lands in the
	// cursor SF engine; the I/O goroutine is still retrying connect.
	require.NoError(t, qs.Table("foo").Int64Column("v", 42).AtNow(context.Background()))

	// FlushAndGetSequence must return promptly even though the server
	// is still down: the frame is durable in the SF engine and flush
	// no longer blocks on the ACK. Bound it tightly so a regression
	// back to ACK-barrier semantics fails loudly here.
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fsn, err := qs.FlushAndGetSequence(flushCtx)
	require.NoError(t, err, "FlushAndGetSequence must not block on ACK while the server is down")
	require.GreaterOrEqual(t, fsn, int64(0))

	// Bring the server up on the held port. Use the same handler as
	// the standard test server (just enough to ACK frames).
	srv := newQwpSfTestServerOnListener(t, listener)
	defer srv.Close()

	// AwaitAckedFsn is the delivery barrier: block until the I/O loop
	// has delivered the buffered frame and the server ACKed it.
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer awaitCancel()
	require.NoError(t, qs.AwaitAckedFsn(awaitCtx, fsn),
		"buffered frame must be delivered and ACKed once the server is up")
	assert.GreaterOrEqual(t, srv.totalFramesReceived.Load(), int64(1))
}
