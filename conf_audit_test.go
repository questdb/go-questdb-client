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
	"strings"
	"testing"
	"time"
)

// TestConfQwpwsAlias pins the qwpws / qwpwss long-form schema aliases
// from connect-string.md §Protocols and transports: "qwpws / qwpwss
// are accepted as long-form aliases for ws / wss." Same TLS mode and
// transport selection as the short forms.
func TestConfQwpwsAlias(t *testing.T) {
	cases := []struct {
		schema string
		wantTLS tlsMode
	}{
		{"ws", tlsDisabled},
		{"qwpws", tlsDisabled},
		{"wss", tlsEnabled},
		{"qwpwss", tlsEnabled},
	}
	for _, tc := range cases {
		t.Run(tc.schema, func(t *testing.T) {
			// Ingest parser.
			c, err := confFromStr(tc.schema + "::addr=localhost:9000;")
			if err != nil {
				t.Fatalf("ingest %s parse: %v", tc.schema, err)
			}
			if c.senderType != qwpSenderType {
				t.Errorf("ingest %s senderType=%v, want qwpSenderType", tc.schema, c.senderType)
			}
			if c.tlsMode != tc.wantTLS {
				t.Errorf("ingest %s tlsMode=%v, want %v", tc.schema, c.tlsMode, tc.wantTLS)
			}

			// Egress parser.
			qc, err := parseQwpQueryConf(tc.schema + "::addr=localhost:9000;")
			if err != nil {
				t.Fatalf("egress %s parse: %v", tc.schema, err)
			}
			if qc.tlsMode != tc.wantTLS {
				t.Errorf("egress %s tlsMode=%v, want %v", tc.schema, qc.tlsMode, tc.wantTLS)
			}
		})
	}
}

// TestConfQwpwsAliasUnknownSchemaErrorMentionsAliases pins the
// improved error message — a typo like "wsq::" should mention all four
// accepted schemas on the egress side so the user knows the long form
// is also valid.
func TestConfQwpwsAliasUnknownSchemaErrorMentionsAliases(t *testing.T) {
	_, err := parseQwpQueryConf("wsq::addr=a:1;")
	if err == nil {
		t.Fatal("expected error for wsq::")
	}
	msg := err.Error()
	for _, want := range []string{"ws", "wss", "qwpws", "qwpwss"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not contain %q", msg, want)
		}
	}
}

// TestConfSizeSuffix pins the JVM-style 1024-based size-suffix grammar
// from connect-string.md §Size suffixes. Suffixes are case-
// insensitive and the long forms (kb/mb/gb/tb) match the short forms
// (k/m/g/t).
func TestConfSizeSuffix(t *testing.T) {
	cases := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1k", 1 << 10},
		{"1K", 1 << 10},
		{"1kb", 1 << 10},
		{"1KB", 1 << 10},
		{"4m", 4 << 20},
		{"4M", 4 << 20},
		{"4mb", 4 << 20},
		{"8m", 8 << 20},
		{"1g", 1 << 30},
		{"1G", 1 << 30},
		{"1gb", 1 << 30},
		{"10g", 10 << 30},
		{"1t", 1 << 40},
		{"1tb", 1 << 40},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseSizeBytes(tc.input)
			if err != nil {
				t.Fatalf("parseSizeBytes(%q): %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parseSizeBytes(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

// TestConfSizeSuffixRejected covers shapes that must error.
func TestConfSizeSuffixRejected(t *testing.T) {
	cases := []string{
		"",
		"k",       // suffix without a number
		"abc",     // non-numeric
		"1.5m",    // floats not supported
		"-1",      // negative bare
		"-1m",     // negative with suffix
		"1xb",     // unknown suffix
		"1024kb extra", // trailing garbage
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			if _, err := parseSizeBytes(in); err == nil {
				t.Errorf("parseSizeBytes(%q) expected error", in)
			}
		})
	}
}

// TestConfSizeSuffixAppliedToKeys verifies the suffix grammar is
// wired into the size-typed connect-string keys end-to-end.
func TestConfSizeSuffixAppliedToKeys(t *testing.T) {
	c, err := confFromStr("ws::addr=localhost:9000;" +
		"init_buf_size=128k;" +
		"max_buf_size=10m;" +
		"auto_flush_bytes=4m;" +
		"sf_dir=/tmp/sf;sender_id=t;" +
		"sf_max_bytes=4m;" +
		"sf_max_total_bytes=1g;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.initBufSize != 128<<10 {
		t.Errorf("initBufSize=%d, want %d", c.initBufSize, 128<<10)
	}
	if c.maxBufSize != 10<<20 {
		t.Errorf("maxBufSize=%d, want %d", c.maxBufSize, 10<<20)
	}
	if c.autoFlushBytes != 4<<20 {
		t.Errorf("autoFlushBytes=%d, want %d", c.autoFlushBytes, 4<<20)
	}
	if c.sfMaxBytes != int64(4<<20) {
		t.Errorf("sfMaxBytes=%d, want %d", c.sfMaxBytes, 4<<20)
	}
	if c.sfMaxTotalBytes != int64(1<<30) {
		t.Errorf("sfMaxTotalBytes=%d, want %d", c.sfMaxTotalBytes, 1<<30)
	}
}

// TestConfSenderIdRejectsDot pins the spec + Java charset: sender_id
// must not contain '.'. Sender.java validateSenderId allows only
// letters / digits / '_' / '-'. The legacy permissive validator
// allowed '.', which deviated from the spec's "no path separators,
// no '.', no spaces" rule.
func TestConfSenderIdRejectsDot(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sender_id=foo.bar;")
	if err == nil {
		t.Fatal("expected error for sender_id with '.'")
	}
	msg := err.Error()
	if !strings.Contains(msg, "sender_id") {
		t.Errorf("error %q does not name sender_id", msg)
	}
	if !strings.Contains(msg, ".") {
		t.Errorf("error %q does not show the offending char", msg)
	}
}

// TestConfSenderIdAccepted pins the allowed character set so future
// changes don't accidentally regress.
func TestConfSenderIdAccepted(t *testing.T) {
	for _, id := range []string{"a", "Z", "0", "abc-DEF_123", "a_b-c"} {
		t.Run(id, func(t *testing.T) {
			_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sender_id=" + id + ";")
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", id, err)
			}
		})
	}
}

// TestConfQwpAutoFlushBytesDefault pins the spec default of 8 MiB.
// connect-string.md §Auto-flushing: "Default where supported: `8m`
// (8 MiB)." Without this, byte-triggered auto-flush is silently
// disabled on the Go client.
func TestConfQwpAutoFlushBytesDefault(t *testing.T) {
	c, err := confFromStr("ws::addr=localhost:9000;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.autoFlushBytes != qwpDefaultAutoFlushBytes {
		t.Errorf("autoFlushBytes=%d, want %d (8 MiB)",
			c.autoFlushBytes, qwpDefaultAutoFlushBytes)
	}
	if qwpDefaultAutoFlushBytes != 8<<20 {
		t.Errorf("qwpDefaultAutoFlushBytes=%d, want %d", qwpDefaultAutoFlushBytes, 8<<20)
	}
}

// TestConfQwpAutoFlushOffZerosBytes pins that `auto_flush=off` also
// clears the new byte default (otherwise users who disable auto-flush
// would still see byte-triggered flushes).
func TestConfQwpAutoFlushOffZerosBytes(t *testing.T) {
	c, err := confFromStr("ws::addr=localhost:9000;auto_flush=off;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.autoFlushBytes != 0 {
		t.Errorf("auto_flush=off left autoFlushBytes=%d, want 0", c.autoFlushBytes)
	}
}

// TestConfAutoFlushOffVsExplicitTriggerIsDeterministic pins that a
// self-contradictory connect string — auto_flush=off paired with an
// explicit auto_flush_rows / _interval / _bytes trigger — resolves the
// same way on every parse. The resolution loop ranges over a map (Go
// randomizes iteration order), so without a deterministic pre-pass the
// surviving value would depend on which key the loop visited last and
// the explicit trigger would be silently zeroed on a fraction of runs.
// The contract is "explicit trigger wins over off". The string position
// of auto_flush is irrelevant (the map drops order), so both orderings
// must agree; many iterations exercise the map-order randomization.
func TestConfAutoFlushOffVsExplicitTriggerIsDeterministic(t *testing.T) {
	const iterations = 2000
	cases := []struct {
		name     string
		conf     string
		wantRows int
		wantIvl  time.Duration
		wantByts int
	}{
		{
			// QWP exercises all three triggers, including the byte trigger
			// that this PR newly lets auto_flush=off zero.
			name:     "qwp_off_before_triggers",
			conf:     "ws::addr=localhost:9000;auto_flush=off;auto_flush_rows=1000;auto_flush_interval=500;auto_flush_bytes=4m;",
			wantRows: 1000,
			wantIvl:  500 * time.Millisecond,
			wantByts: 4 << 20,
		},
		{
			name:     "qwp_off_after_triggers",
			conf:     "ws::addr=localhost:9000;auto_flush_rows=1000;auto_flush_interval=500;auto_flush_bytes=4m;auto_flush=off;",
			wantRows: 1000,
			wantIvl:  500 * time.Millisecond,
			wantByts: 4 << 20,
		},
		{
			// HTTP form (rows + interval only) — the pre-existing nondeterminism.
			name:     "http_off_with_rows_and_interval",
			conf:     "http::addr=localhost:9000;auto_flush=off;auto_flush_rows=200;auto_flush_interval=300;",
			wantRows: 200,
			wantIvl:  300 * time.Millisecond,
			wantByts: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < iterations; i++ {
				c, err := confFromStr(tc.conf)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				if c.autoFlushRows != tc.wantRows {
					t.Fatalf("iter %d: autoFlushRows=%d, want %d", i, c.autoFlushRows, tc.wantRows)
				}
				if c.autoFlushInterval != tc.wantIvl {
					t.Fatalf("iter %d: autoFlushInterval=%v, want %v", i, c.autoFlushInterval, tc.wantIvl)
				}
				if c.autoFlushBytes != tc.wantByts {
					t.Fatalf("iter %d: autoFlushBytes=%d, want %d", i, c.autoFlushBytes, tc.wantByts)
				}
			}
		})
	}
}

// TestConfIngestSilentlyAcceptsEgressKeys is the cross-direction
// silent-accept contract from connect-string.md §16-20 and §Query
// client keys: a ws:: / wss:: Sender must not error on egress-only
// keys, because the same connect string must be shareable with a
// QwpQueryClient.
func TestConfIngestSilentlyAcceptsEgressKeys(t *testing.T) {
	// One representative value per egress-only key. Values are
	// intentionally a mix of valid and "garbage-from-the-Sender's-
	// perspective" forms — the spec says the Sender does not
	// interpret them, so even invalid values must pass.
	kvs := []string{
		"buffer_pool_size=8",
		"compression=zstd",
		"compression_level=22",
		"failover=off",
		"failover_backoff_initial_ms=10",
		"failover_backoff_max_ms=2000",
		"failover_max_attempts=16",
		"failover_max_duration_ms=60000",
		"initial_credit=262144",
		"max_batch_rows=10000",
	}
	for _, kv := range kvs {
		t.Run(kv, func(t *testing.T) {
			conf := "ws::addr=localhost:9000;" + kv + ";"
			if _, err := confFromStr(conf); err != nil {
				t.Errorf("unexpected error parsing %q: %v", conf, err)
			}
		})
	}
}

// TestConfIngestRejectsEgressKeysOnHttp pins that the silent-accept
// is QWP-only — HTTP/TCP senders do not share a connect string with
// QwpQueryClient, so egress-only keys must still error there.
func TestConfIngestRejectsEgressKeysOnHttp(t *testing.T) {
	_, err := confFromStr("http::addr=localhost:9000;compression=zstd;")
	if err == nil {
		t.Fatal("expected error: compression on http:: must not be silently accepted")
	}
	if !strings.Contains(err.Error(), "unsupported option") {
		t.Errorf("error %q does not say unsupported option", err.Error())
	}
}

// TestConfEgressSilentlyAcceptsIngressKeys is the egress side of the
// shared-connect-string contract. A QwpQueryClient must not error on
// ingress-only keys (sf_*, reconnect_*, auto_flush_*, on_*_error,
// etc.).
func TestConfEgressSilentlyAcceptsIngressKeys(t *testing.T) {
	kvs := []string{
		"auto_flush=on",
		"auto_flush_bytes=8m",
		"auto_flush_interval=100",
		"auto_flush_rows=1000",
		"close_flush_timeout_millis=5000",
		"drain_orphans=off",
		"durable_ack_keepalive_interval_millis=200",
		"error_inbox_capacity=256",
		"init_buf_size=64k",
		"initial_connect_retry=off",
		"max_background_drainers=4",
		"max_buf_size=100m",
		"max_name_len=127",
		"on_internal_error=halt",
		"on_parse_error=halt",
		"on_schema_error=halt",
		"on_security_error=halt",
		"on_server_error=auto",
		"on_write_error=halt",
		"reconnect_initial_backoff_millis=100",
		"reconnect_max_backoff_millis=5000",
		"reconnect_max_duration_millis=300000",
		"request_durable_ack=off",
		"sender_id=ingest-1",
		"sf_append_deadline_millis=30000",
		"sf_dir=/tmp/sf",
		"sf_durability=memory",
		"sf_max_bytes=4m",
		"sf_max_total_bytes=10g",
	}
	for _, kv := range kvs {
		t.Run(kv, func(t *testing.T) {
			conf := "ws::addr=localhost:9000;" + kv + ";"
			if _, err := parseQwpQueryConf(conf); err != nil {
				t.Errorf("unexpected error parsing %q: %v", conf, err)
			}
		})
	}
}

// TestConfSharedConnectString is the end-to-end check on the
// shared-connect-string contract: one string with both ingress-only
// and egress-only keys must parse successfully on both sides.
func TestConfSharedConnectString(t *testing.T) {
	shared := "wss::addr=db-a:9000,db-b:9000;" +
		"token=my-token;" +
		"target=primary;" +
		"zone=eu-west-1;" +
		// ingress-only:
		"sf_dir=/tmp/sf;sender_id=ingest-1;auto_flush_rows=500;" +
		"reconnect_max_duration_millis=120000;" +
		"on_schema_error=drop;" +
		// egress-only:
		"compression=zstd;compression_level=3;" +
		"failover_max_attempts=8;failover_max_duration_ms=30000;"
	if _, err := confFromStr(shared); err != nil {
		t.Errorf("ingest parser rejected the shared connect string: %v", err)
	}
	if _, err := parseQwpQueryConf(shared); err != nil {
		t.Errorf("egress parser rejected the shared connect string: %v", err)
	}
}

// TestConfQwpRejectsRetryTimeout pins the fix for the silent-drop
// audit finding: retry_timeout is HTTP-only (legacy ILP doc) and is
// not listed in connect-string.md, and Sender.java:3412 rejects it
// on the WebSocket protocol. The Go QWP sanitizer now rejects too,
// pointing the user at the QWP analogue.
func TestConfQwpRejectsRetryTimeout(t *testing.T) {
	for _, schema := range []string{"ws", "wss", "qwpws", "qwpwss"} {
		t.Run(schema, func(t *testing.T) {
			_, err := LineSenderFromConf(context.Background(),
				schema+"::addr=localhost:9000;retry_timeout=10000;")
			if err == nil {
				t.Fatal("expected error: retry_timeout must not be accepted on QWP")
			}
			msg := err.Error()
			if !strings.Contains(msg, "retry_timeout") {
				t.Errorf("error %q does not name retry_timeout", msg)
			}
			if !strings.Contains(msg, "reconnect_max_duration_millis") {
				t.Errorf("error %q does not point to the QWP analogue", msg)
			}
		})
	}
}

// TestConfQwpRejectsWithRetryTimeoutOption pins the same reject on
// the functional-option path so users who reach for WithRetryTimeout
// on a QWP sender get the same error as the connect-string path.
// (The WithRetryTimeout doc comment already says "Only available for
// the HTTP sender"; this is the enforcement.)
func TestConfQwpRejectsWithRetryTimeoutOption(t *testing.T) {
	_, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress("localhost:9000"),
		WithRetryTimeout(5*time.Second))
	if err == nil {
		t.Fatal("expected error: WithRetryTimeout must not be accepted on QWP")
	}
	if !strings.Contains(err.Error(), "retry_timeout") {
		t.Errorf("error %q does not name retry_timeout", err.Error())
	}
}

// TestConfRejectsCloseTimeoutWithMigrationHint pins the removal of
// the Go-only `close_timeout` key. Java never accepted it (only
// close_flush_timeout_millis, Sender.java §3071), and the cursor
// architecture unified the memory and SF close paths onto the
// spec-aligned key. The parser rejects regardless of schema with a
// migration hint, not the generic "unsupported option" error.
func TestConfRejectsCloseTimeoutWithMigrationHint(t *testing.T) {
	for _, schema := range []string{"ws", "wss", "qwpws", "qwpwss", "http", "https", "tcp", "tcps"} {
		t.Run(schema, func(t *testing.T) {
			_, err := confFromStr(schema + "::addr=localhost:9000;close_timeout=1000;")
			if err == nil {
				t.Fatal("expected error: close_timeout must not be accepted")
			}
			msg := err.Error()
			if !strings.Contains(msg, "close_timeout") {
				t.Errorf("error %q does not name close_timeout", msg)
			}
			if !strings.Contains(msg, "close_flush_timeout_millis") {
				t.Errorf("error %q does not point at close_flush_timeout_millis", msg)
			}
		})
	}
}

// TestConfMemoryModeHonoursCloseFlushTimeout pins the unification:
// memory mode (no sf_dir) now reads close_flush_timeout_millis, not
// the removed close_timeout. With the cursor architecture sharing
// the same engine across memory and SF modes, both keys map to the
// same runtime field via the spec-aligned name.
func TestConfMemoryModeHonoursCloseFlushTimeout(t *testing.T) {
	c, err := confFromStr("ws::addr=localhost:9000;close_flush_timeout_millis=2500;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !c.closeFlushTimeoutSet {
		t.Error("closeFlushTimeoutSet=false after explicit user value")
	}
	if c.closeFlushTimeoutMillis != 2500 {
		t.Errorf("closeFlushTimeoutMillis=%d, want 2500", c.closeFlushTimeoutMillis)
	}
}

// TestWithCloseTimeoutSubMillisecondIsNoOverride pins that the
// deprecated alias honours its documented "d <= 0 is treated as no
// override" semantics for sub-millisecond positive durations too.
// Without this gate, d=500µs satisfies d > 0, truncates to 0 ms,
// sets closeFlushTimeoutSet=true, and routes into the fast-close
// branch (qwp_sender_cursor.go:167, sender.go:1493), contradicting
// the doc. Callers who actually want fast-close must opt in via
// WithCloseFlushTimeout.
func TestWithCloseTimeoutSubMillisecondIsNoOverride(t *testing.T) {
	for _, d := range []time.Duration{
		0,
		-1 * time.Second,
		1 * time.Nanosecond,
		500 * time.Microsecond,
		999 * time.Microsecond,
	} {
		t.Run(d.String(), func(t *testing.T) {
			c := newLineSenderConfig(qwpSenderType)
			WithCloseTimeout(d)(c)
			if c.closeFlushTimeoutSet {
				t.Errorf("closeFlushTimeoutSet=true for d=%s; want no override", d)
			}
			if c.closeFlushTimeoutMillis != 0 {
				t.Errorf("closeFlushTimeoutMillis=%d for d=%s; want 0 (untouched)", c.closeFlushTimeoutMillis, d)
			}
		})
	}
	// Sanity: the smallest representable positive value, 1ms, must
	// still override (the gate is inclusive at the ms boundary).
	c := newLineSenderConfig(qwpSenderType)
	WithCloseTimeout(time.Millisecond)(c)
	if !c.closeFlushTimeoutSet || c.closeFlushTimeoutMillis != 1 {
		t.Errorf("WithCloseTimeout(1ms): set=%v millis=%d; want set=true millis=1",
			c.closeFlushTimeoutSet, c.closeFlushTimeoutMillis)
	}
}

// TestConfRejectsUnknownKeyOnBothSides confirms that a genuinely
// unknown key (not in either spec set) still errors out, so the
// silent-accept is scoped.
func TestConfRejectsUnknownKeyOnBothSides(t *testing.T) {
	bad := "ws::addr=localhost:9000;not_a_real_key=42;"
	if _, err := confFromStr(bad); err == nil {
		t.Error("ingest: expected error for not_a_real_key")
	}
	if _, err := parseQwpQueryConf(bad); err == nil {
		t.Error("egress: expected error for not_a_real_key")
	}
}
