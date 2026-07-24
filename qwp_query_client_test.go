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
	"bytes"
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
				if got := c.addressString(); got != "localhost:9000" {
					t.Errorf("addressString=%q", got)
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
				// zone defaults to unset (zone-blind) and
				// auth_timeout_ms to the shared 15s default so a
				// connect string omitting them behaves like the
				// ingest client.
				if c.zone != "" {
					t.Errorf("zone=%q, want empty (zone-blind default)", c.zone)
				}
				if c.authTimeoutMs != qwpDefaultAuthTimeoutMs {
					t.Errorf("authTimeoutMs=%d, want %d",
						c.authTimeoutMs, qwpDefaultAuthTimeoutMs)
				}
			},
		},
		{
			// failover.md §1.1 common keys: a connect string shared
			// verbatim with the ingest client must parse here too
			// (the ingest side accepts both; the query side is where
			// zone= is actually effective).
			name: "zone_and_auth_timeout",
			conf: "ws::addr=db.example:9000;zone=eu-west-1a;" +
				"auth_timeout_ms=2500;target=replica;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.zone != "eu-west-1a" {
					t.Errorf("zone=%q, want eu-west-1a", c.zone)
				}
				if c.authTimeoutMs != 2500 {
					t.Errorf("authTimeoutMs=%d, want 2500", c.authTimeoutMs)
				}
				if c.target != qwpTargetReplica {
					t.Errorf("target=%v, want replica", c.target)
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
				if got := c.addressString(); got != "db.example:9443" {
					t.Errorf("addressString=%q", got)
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
		{
			name: "compression_default_is_raw",
			conf: "ws::addr=a:1;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.compression != qwpCompressionRaw {
					t.Errorf("compression=%q, want raw", c.compression)
				}
				if c.compressionLevel != qwpDefaultCompressionLevel {
					t.Errorf("compressionLevel=%d, want %d",
						c.compressionLevel, qwpDefaultCompressionLevel)
				}
				if got := c.buildAcceptEncodingHeader(); got != "" {
					t.Errorf("accept-encoding header=%q, want empty (raw)", got)
				}
			},
		},
		{
			name: "compression_zstd_builds_header",
			conf: "ws::addr=a:1;compression=zstd;compression_level=7;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.compression != qwpCompressionZstd {
					t.Errorf("compression=%q, want zstd", c.compression)
				}
				if c.compressionLevel != 7 {
					t.Errorf("compressionLevel=%d, want 7", c.compressionLevel)
				}
				if got := c.buildAcceptEncodingHeader(); got != "zstd;level=7,raw" {
					t.Errorf("accept-encoding=%q, want %q",
						got, "zstd;level=7,raw")
				}
			},
		},
		{
			name: "compression_auto_also_advertises_zstd",
			conf: "ws::addr=a:1;compression=auto;",
			chk: func(t *testing.T, c *qwpQueryClientConfig) {
				if c.compression != qwpCompressionAuto {
					t.Errorf("compression=%q, want auto", c.compression)
				}
				// "auto" advertises the same header value as "zstd";
				// the server picks. Level defaults to 1
				// (qwpDefaultCompressionLevel; Sender.java parity and
				// connect-string.md §Query client keys).
				if got := c.buildAcceptEncodingHeader(); got != "zstd;level=1,raw" {
					t.Errorf("accept-encoding=%q, want %q",
						got, "zstd;level=1,raw")
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
		{"mutually_exclusive_basic_token", "ws::addr=a:1;username=u;password=p;token=Y;", "mutually exclusive"},
		{"basic_missing_password", "ws::addr=a:1;username=u;", "both username and password"},
		{"auth_key_removed", "ws::addr=a:1;auth=Bearer abc;", "unsupported option"},
		{"unknown_key", "ws::addr=a:1;weird=1;", "unsupported option"},
		{"tls_on_ws", "ws::addr=a:1;tls_verify=on;", "tls_verify requires"},
		{"tls_bad", "wss::addr=a:1;tls_verify=off;", "invalid tls_verify"},
		{"tls_roots_rejected", "wss::addr=a:1;tls_roots=/tmp/foo;", "tls_roots is not available"},
		{"compression_unsupported_value", "ws::addr=a:1;compression=lzma;", "invalid compression"},
		{"compression_level_non_numeric", "ws::addr=a:1;compression=zstd;compression_level=seven;", "invalid compression_level"},
		{"compression_level_too_low", "ws::addr=a:1;compression=zstd;compression_level=0;", "compression level must be in [1, 22]"},
		{"compression_level_too_high", "ws::addr=a:1;compression=zstd;compression_level=23;", "compression level must be in [1, 22]"},
		{"server_info_timeout_zero", "ws::addr=a:1;server_info_timeout_ms=0;", "server_info_timeout_ms must be > 0"},
		{"server_info_timeout_negative", "ws::addr=a:1;server_info_timeout_ms=-1;", "server_info_timeout_ms must be > 0"},
		{"failover_max_duration_negative", "ws::addr=a:1;failover_max_duration_ms=-1;", "failover_max_duration_ms must be >= 0"},
		{"failover_max_duration_non_numeric", "ws::addr=a:1;failover_max_duration_ms=soon;", "invalid failover_max_duration_ms"},
		{"auth_timeout_non_numeric", "ws::addr=a:1;auth_timeout_ms=soon;", "invalid auth_timeout_ms"},
		{"auth_timeout_zero", "ws::addr=a:1;auth_timeout_ms=0;", "auth_timeout_ms must be > 0"},
		{"auth_timeout_negative", "ws::addr=a:1;auth_timeout_ms=-1;", "auth_timeout_ms must be > 0"},
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

// TestQwpQueryClientFromConfPortBoundaries pins the addr= port-range
// validation: ports outside [1, 65535] and non-numeric ports are
// rejected at parse time so the user sees an actionable error rather
// than an opaque dial failure later. Ports of 1 and 65535 are accepted.
// Mirrors the Java QwpQueryClientFromConfigTest port-boundary tests.
func TestQwpQueryClientFromConfPortBoundaries(t *testing.T) {
	t.Run("Reject", func(t *testing.T) {
		cases := []struct {
			conf    string
			wantSub string
		}{
			{"ws::addr=db:0;", "out of range"},
			{"ws::addr=db:-1;", "out of range"},
			{"ws::addr=db:65536;", "out of range"},
			{"ws::addr=db:2147483647;", "out of range"},
			{"ws::addr=host:abc;", "invalid port"},
		}
		for _, tc := range cases {
			t.Run(tc.conf, func(t *testing.T) {
				_, err := parseQwpQueryConf(tc.conf)
				if err == nil {
					t.Fatalf("expected error for %q", tc.conf)
				}
				if !strings.Contains(err.Error(), tc.wantSub) {
					t.Errorf("err=%v, want substring %q", err, tc.wantSub)
				}
			})
		}
	})
	t.Run("AcceptBoundaries", func(t *testing.T) {
		// 1 and 65535 are the inclusive boundaries of the legal range.
		// "addr=host" with no port is also legal — the URL scheme
		// supplies a default port at dial time.
		for _, conf := range []string{
			"ws::addr=db:1;",
			"ws::addr=db:65535;",
			"ws::addr=db.internal;",
		} {
			if _, err := parseQwpQueryConf(conf); err != nil {
				t.Errorf("unexpected error for %q: %v", conf, err)
			}
		}
	})
}

// TestQwpQueryClientFromConfIPv6 pins the bracketed-IPv6 and bare-IPv6
// parsing paths in the addr= validator. The validator accepts:
//   - bracketed with port:    [::1]:9000
//   - bracketed without port: [fe80::1]
//   - bare IPv6 (>= 2 colons): fe80::1 (no port; brackets required for port)
//
// And rejects:
//   - empty bracketed host:   [] :9000
//   - missing closing ']':    [::1:9000
//   - trailing garbage after ']': [::1]9000
//
// Mirrors the Java QwpQueryClientFromConfigTest IPv6 cases. The Go
// client targets a single endpoint; the comma-separated multi-address
// form Java accepts is rejected up front (see TestRejectsMultiAddress).
func TestQwpQueryClientFromConfIPv6(t *testing.T) {
	t.Run("Accept", func(t *testing.T) {
		for _, conf := range []string{
			"ws::addr=[::1]:9000;",
			"ws::addr=[fe80::1];",
			"ws::addr=[::1];",
			"ws::addr=fe80::1;", // bare IPv6, default port
		} {
			t.Run(conf, func(t *testing.T) {
				if _, err := parseQwpQueryConf(conf); err != nil {
					t.Errorf("unexpected error for %q: %v", conf, err)
				}
			})
		}
	})
	t.Run("Reject", func(t *testing.T) {
		cases := []struct {
			conf    string
			wantSub string
		}{
			{"ws::addr=[]:9000;", "empty host"},
			{"ws::addr=[::1:9000;", "missing closing"},
			{"ws::addr=[::1]9000;", "expected ':'"},
			{"ws::addr=[::1]:0;", "out of range"},
			{"ws::addr=[::1]:65536;", "out of range"},
		}
		for _, tc := range cases {
			t.Run(tc.conf, func(t *testing.T) {
				_, err := parseQwpQueryConf(tc.conf)
				if err == nil {
					t.Fatalf("expected error for %q", tc.conf)
				}
				if !strings.Contains(err.Error(), tc.wantSub) {
					t.Errorf("err=%v, want substring %q", err, tc.wantSub)
				}
			})
		}
	})
}

// TestQwpQueryClientFromConfAcceptsMultiAddress verifies that
// comma-separated addr= entries become an ordered endpoint list. The
// connect walk in qwp_query_failover.go consumes them in order; the
// parser's responsibility is just shape validation here.
func TestQwpQueryClientFromConfAcceptsMultiAddress(t *testing.T) {
	cases := []struct {
		conf      string
		wantHosts []string
		wantPorts []int
	}{
		{
			conf:      "ws::addr=a:9000,b:9001;",
			wantHosts: []string{"a", "b"},
			wantPorts: []int{9000, 9001},
		},
		{
			conf:      "ws::addr=a:9000,b:9000,c:9000;",
			wantHosts: []string{"a", "b", "c"},
			wantPorts: []int{9000, 9000, 9000},
		},
		{
			conf:      "ws::addr=[::1]:9000,[fe80::1]:9001;",
			wantHosts: []string{"::1", "fe80::1"},
			wantPorts: []int{9000, 9001},
		},
	}
	for _, tc := range cases {
		t.Run(tc.conf, func(t *testing.T) {
			cfg, err := parseQwpQueryConf(tc.conf)
			if err != nil {
				t.Fatalf("parseQwpQueryConf: %v", err)
			}
			if len(cfg.endpoints) != len(tc.wantHosts) {
				t.Fatalf("len(endpoints) = %d, want %d", len(cfg.endpoints), len(tc.wantHosts))
			}
			for i, ep := range cfg.endpoints {
				if ep.host != tc.wantHosts[i] || ep.port != tc.wantPorts[i] {
					t.Errorf("endpoints[%d] = %s:%d, want %s:%d",
						i, ep.host, ep.port, tc.wantHosts[i], tc.wantPorts[i])
				}
			}
		})
	}
}

// TestQwpQueryClientFromConfV2KeysParse verifies the v2 connection-
// string keys (target, failover, failover_max_attempts,
// failover_backoff_initial_ms, failover_backoff_max_ms,
// failover_max_duration_ms, server_info_timeout_ms, replay_exec)
// parse into the expected config fields and reject malformed values
// with actionable errors.
func TestQwpQueryClientFromConfV2KeysParse(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		conf := "ws::addr=a:9000;target=primary;failover=off;" +
			"failover_max_attempts=3;failover_backoff_initial_ms=10;" +
			"failover_backoff_max_ms=200;failover_max_duration_ms=1500;" +
			"server_info_timeout_ms=750;replay_exec=on;"
		cfg, err := parseQwpQueryConf(conf)
		if err != nil {
			t.Fatalf("parseQwpQueryConf: %v", err)
		}
		if cfg.target != qwpTargetPrimary {
			t.Errorf("target=%v, want primary", cfg.target)
		}
		if cfg.failoverEnabled {
			t.Errorf("failoverEnabled=true, want false")
		}
		if cfg.failoverMaxAttempts != 3 {
			t.Errorf("failoverMaxAttempts=%d, want 3", cfg.failoverMaxAttempts)
		}
		if cfg.failoverBackoffInitial != 10*time.Millisecond {
			t.Errorf("failoverBackoffInitial=%v, want 10ms", cfg.failoverBackoffInitial)
		}
		if cfg.failoverBackoffMax != 200*time.Millisecond {
			t.Errorf("failoverBackoffMax=%v, want 200ms", cfg.failoverBackoffMax)
		}
		if cfg.failoverMaxDuration != 1500*time.Millisecond {
			t.Errorf("failoverMaxDuration=%v, want 1500ms", cfg.failoverMaxDuration)
		}
		if cfg.serverInfoTimeout != 750*time.Millisecond {
			t.Errorf("serverInfoTimeout=%v, want 750ms", cfg.serverInfoTimeout)
		}
		if !cfg.replayExec {
			t.Errorf("replayExec=false, want true")
		}
	})

	t.Run("invalid_target", func(t *testing.T) {
		_, err := parseQwpQueryConf("ws::addr=a:9000;target=leader;")
		if err == nil || !strings.Contains(err.Error(), "target") {
			t.Errorf("err=%v, want target validation error", err)
		}
	})

	t.Run("invalid_failover", func(t *testing.T) {
		_, err := parseQwpQueryConf("ws::addr=a:9000;failover=maybe;")
		if err == nil || !strings.Contains(err.Error(), "failover") {
			t.Errorf("err=%v, want failover validation error", err)
		}
	})

	t.Run("backoff_max_lt_initial", func(t *testing.T) {
		_, err := parseQwpQueryConf(
			"ws::addr=a:9000;failover_backoff_initial_ms=100;" +
				"failover_backoff_max_ms=10;")
		if err == nil || !strings.Contains(err.Error(), "failover_backoff_max") {
			t.Errorf("err=%v, want max-lt-initial error", err)
		}
	})

	t.Run("failover_max_duration_default", func(t *testing.T) {
		cfg, err := parseQwpQueryConf("ws::addr=a:9000;")
		if err != nil {
			t.Fatalf("parseQwpQueryConf: %v", err)
		}
		if cfg.failoverMaxDuration != qwpDefaultFailoverMaxDuration {
			t.Errorf("failoverMaxDuration=%v, want default %v",
				cfg.failoverMaxDuration, qwpDefaultFailoverMaxDuration)
		}
	})

	t.Run("failover_max_duration_unbounded", func(t *testing.T) {
		cfg, err := parseQwpQueryConf(
			"ws::addr=a:9000;failover_max_duration_ms=0;")
		if err != nil {
			t.Fatalf("parseQwpQueryConf: %v", err)
		}
		if cfg.failoverMaxDuration != 0 {
			t.Errorf("failoverMaxDuration=%v, want 0 (unbounded)",
				cfg.failoverMaxDuration)
		}
	})
}

// TestQwpQueryClientFromConfTlsVariations exercises the tls_verify
// matrix exhaustively: on/unsafe_off accepted on wss://, both rejected
// on ws://, invalid values rejected, and the legacy tls_roots /
// tls_roots_password keys explicitly rejected on both schemas (the Go
// client uses the system trust store only). Mirrors the Java
// QwpQueryClientFromConfigTest TLS variations.
func TestQwpQueryClientFromConfTlsVariations(t *testing.T) {
	type tlsCase struct {
		name      string
		conf      string
		wantTls   tlsMode
		wantErrIn string
	}
	cases := []tlsCase{
		{
			name:    "wss_no_tls_verify_defaults_to_enabled",
			conf:    "wss::addr=db:9000;",
			wantTls: tlsEnabled,
		},
		{
			name:    "wss_tls_verify_on",
			conf:    "wss::addr=db:9000;tls_verify=on;",
			wantTls: tlsEnabled,
		},
		{
			name:    "wss_tls_verify_unsafe_off",
			conf:    "wss::addr=db:9000;tls_verify=unsafe_off;",
			wantTls: tlsInsecureSkipVerify,
		},
		{
			name:    "ws_no_tls",
			conf:    "ws::addr=db:9000;",
			wantTls: tlsDisabled,
		},
		{
			name:      "ws_tls_verify_on_rejected",
			conf:      "ws::addr=db:9000;tls_verify=on;",
			wantErrIn: "tls_verify requires",
		},
		{
			name:      "ws_tls_verify_unsafe_off_rejected",
			conf:      "ws::addr=db:9000;tls_verify=unsafe_off;",
			wantErrIn: "tls_verify requires",
		},
		{
			name:      "wss_tls_verify_invalid",
			conf:      "wss::addr=db:9000;tls_verify=strict;",
			wantErrIn: "invalid tls_verify",
		},
		{
			name:      "wss_tls_roots_rejected",
			conf:      "wss::addr=db:9000;tls_roots=/etc/ca.p12;",
			wantErrIn: "tls_roots is not available",
		},
		{
			name:      "ws_tls_roots_rejected",
			conf:      "ws::addr=db:9000;tls_roots=/etc/ca.p12;",
			wantErrIn: "tls_roots is not available",
		},
		{
			name:      "tls_roots_password_rejected",
			conf:      "wss::addr=db:9000;tls_roots_password=secret;",
			wantErrIn: "tls_roots_password is not available",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := parseQwpQueryConf(c.conf)
			if c.wantErrIn != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", c.wantErrIn)
				}
				if !strings.Contains(err.Error(), c.wantErrIn) {
					t.Errorf("err=%v, want %q", err, c.wantErrIn)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if cfg.tlsMode != c.wantTls {
				t.Errorf("tlsMode=%v, want %v", cfg.tlsMode, c.wantTls)
			}
		})
	}
}

// TestQwpQueryClientFromConfCompressionVariations exhaustively covers
// the compression and compression_level keys: every accepted value
// (raw / zstd / auto), every boundary on compression_level (1 and 22
// inclusive), and the rejected values that Java's
// QwpQueryClientFromConfigTest pins.
func TestQwpQueryClientFromConfCompressionVariations(t *testing.T) {
	type compCase struct {
		name             string
		conf             string
		wantCompression  string
		wantLevel        int
		wantErrIn        string
		wantHeaderHasZst bool
	}
	cases := []compCase{
		{
			name:            "default_is_raw",
			conf:            "ws::addr=db:9000;",
			wantCompression: qwpCompressionRaw,
			wantLevel:       qwpDefaultCompressionLevel,
		},
		{
			name:             "zstd_at_lower_bound",
			conf:             "ws::addr=db:9000;compression=zstd;compression_level=1;",
			wantCompression:  qwpCompressionZstd,
			wantLevel:        1,
			wantHeaderHasZst: true,
		},
		{
			name:             "zstd_at_upper_bound",
			conf:             "ws::addr=db:9000;compression=zstd;compression_level=22;",
			wantCompression:  qwpCompressionZstd,
			wantLevel:        22,
			wantHeaderHasZst: true,
		},
		{
			name:             "auto_also_advertises_zstd",
			conf:             "ws::addr=db:9000;compression=auto;",
			wantCompression:  qwpCompressionAuto,
			wantLevel:        qwpDefaultCompressionLevel,
			wantHeaderHasZst: true,
		},
		{
			name:            "raw_explicit",
			conf:            "ws::addr=db:9000;compression=raw;",
			wantCompression: qwpCompressionRaw,
			wantLevel:       qwpDefaultCompressionLevel,
		},
		{
			name:      "level_zero_rejected",
			conf:      "ws::addr=db:9000;compression_level=0;",
			wantErrIn: "must be in [1, 22]",
		},
		{
			name:      "level_negative_rejected",
			conf:      "ws::addr=db:9000;compression_level=-1;",
			wantErrIn: "must be in [1, 22]",
		},
		{
			name:      "level_too_large_rejected",
			conf:      "ws::addr=db:9000;compression_level=23;",
			wantErrIn: "must be in [1, 22]",
		},
		{
			name:      "level_non_numeric_rejected",
			conf:      "ws::addr=db:9000;compression_level=high;",
			wantErrIn: "invalid compression_level",
		},
		{
			name:      "compression_invalid_rejected",
			conf:      "ws::addr=db:9000;compression=gzip;",
			wantErrIn: "invalid compression",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := parseQwpQueryConf(c.conf)
			if c.wantErrIn != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", c.wantErrIn)
				}
				if !strings.Contains(err.Error(), c.wantErrIn) {
					t.Errorf("err=%v, want %q", err, c.wantErrIn)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if cfg.compression != c.wantCompression {
				t.Errorf("compression=%q, want %q", cfg.compression, c.wantCompression)
			}
			if cfg.compressionLevel != c.wantLevel {
				t.Errorf("compressionLevel=%d, want %d", cfg.compressionLevel, c.wantLevel)
			}
			h := cfg.buildAcceptEncodingHeader()
			if c.wantHeaderHasZst {
				if !strings.Contains(h, "zstd") {
					t.Errorf("buildAcceptEncodingHeader=%q, want to contain 'zstd'", h)
				}
				if !strings.Contains(h, "raw") {
					t.Errorf("buildAcceptEncodingHeader=%q, want to contain 'raw' fallback", h)
				}
			} else {
				if h != "" {
					t.Errorf("buildAcceptEncodingHeader=%q, want empty for raw", h)
				}
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
		WithQwpQueryCompression(qwpCompressionZstd),
		WithQwpQueryCompressionLevel(9),
		WithQwpQueryFailoverMaxDuration(7 * time.Second),
		WithQwpQueryConnectTimeout(3 * time.Second),
	} {
		opt(cfg)
	}
	if got := cfg.addressString(); got != "example:9000" {
		t.Errorf("addressString=%q", got)
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
	if cfg.compression != qwpCompressionZstd {
		t.Errorf("compression=%q", cfg.compression)
	}
	if cfg.compressionLevel != 9 {
		t.Errorf("compressionLevel=%d", cfg.compressionLevel)
	}
	if got := cfg.buildAcceptEncodingHeader(); got != "zstd;level=9,raw" {
		t.Errorf("accept-encoding=%q", got)
	}
	if cfg.failoverMaxDuration != 7*time.Second {
		t.Errorf("failoverMaxDuration=%v, want 7s", cfg.failoverMaxDuration)
	}
	if cfg.connectTimeoutMs != 3000 {
		t.Errorf("connectTimeoutMs=%d, want 3000", cfg.connectTimeoutMs)
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
			reqID, byte(QwpStatusParseError), "bad sql", -1)))
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
	if qe.Status != QwpStatusParseError {
		t.Errorf("Status=0x%02X, want 0x%02X", byte(qe.Status), byte(QwpStatusParseError))
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
			reqID, byte(QwpStatusInternalError), "boom", -1)))
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
	if qe.Status != QwpStatusInternalError || qe.Message != "boom" {
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

// TestQwpQueryRejectsOversizedSql verifies that buildRequest's
// preflight blocks SQL text exceeding the spec §16 1 MiB limit
// before any bytes leave the process. Both Query (iterator-yielded
// error) and Exec (sync error) surface a typed length-limit message.
func TestQwpQueryRejectsOversizedSql(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		// Hold the connection open until the client tears it down;
		// preflight rejects the SQL before any frame leaves the
		// client, so this read must never return data.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _ = m.conn.Read(ctx)
	})
	defer cleanup()

	oversized := strings.Repeat("a", qwpMaxSqlTextBytes+1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := c.Query(ctx, oversized)
	defer q.Close()
	var queryErr error
	for _, err := range q.Batches() {
		if err != nil {
			queryErr = err
		}
	}
	if queryErr == nil {
		t.Fatal("expected oversized-SQL error from Query")
	}
	if !strings.Contains(queryErr.Error(), "exceeds") ||
		!strings.Contains(queryErr.Error(), "1048576") {
		t.Errorf("Query err=%v, want size-limit message", queryErr)
	}

	_, execErr := c.Exec(ctx, oversized)
	if execErr == nil {
		t.Fatal("expected oversized-SQL error from Exec")
	}
	if !strings.Contains(execErr.Error(), "exceeds") ||
		!strings.Contains(execErr.Error(), "1048576") {
		t.Errorf("Exec err=%v, want size-limit message", execErr)
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

// TestQwpQueryClientCloseShortCtxNoReaderRace guards M2: Close(ctx) with
// an already-cancelled ctx must not race the reader goroutine over the
// transport's conn. shutdown(ctx) returns via ctx.Done() before doneCh
// fires (the reader has not joined), so the transport teardown that
// follows runs while the reader is still live inside readerRun. The
// reader re-reads io.transport.conn every loop iteration; the teardown
// must not mutate that field out from under it. Run under -race (CI uses
// `go test -race`): before the fix this trips the detector on
// io.transport.conn — readerRun's per-iteration field read vs close()'s
// t.conn=nil write — and can nil-deref the unsupervised reader goroutine.
func TestQwpQueryClientCloseShortCtxNoReaderRace(t *testing.T) {
	// Server streams stray text frames as fast as it can and drains its
	// own reads concurrently so the client's close handshake completes
	// promptly. readerRun reads io.transport.conn every iteration, skips
	// non-binary frames, and loops — so the reader goroutine spins on
	// that field read while the close lands.
	srv := newQwpMockEgressServer(t, func(m *qwpMockEgressConn) {
		go func() {
			for {
				if _, _, err := m.conn.Read(context.Background()); err != nil {
					return
				}
			}
		}()
		for {
			if err := m.conn.Write(context.Background(), websocket.MessageText, []byte("x")); err != nil {
				return
			}
		}
	})
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	// Repeat: each round stands up a fresh generation whose reader spins
	// on io.transport.conn, then closes it with an already-cancelled ctx.
	// shutdown(ctx) returns via ctx.Done() before doneCh fires (the reader
	// has not joined), so the pre-fix unconditional tr.close() nils
	// io.transport.conn concurrently with the still-spinning reader — a
	// data race the detector flags within a few rounds.
	for i := 0; i < 40; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c, err := NewQwpQueryClient(ctx, WithQwpQueryAddress(addr))
		cancel()
		if err != nil {
			t.Fatalf("round %d ctor: %v", i, err)
		}
		// Let the reader reach its read-skip loop and spin on the conn
		// field before the close writes it.
		time.Sleep(2 * time.Millisecond)
		closeCtx, closeCancel := context.WithCancel(context.Background())
		closeCancel()
		_ = c.Close(closeCtx)
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
		// The egress client reads SERVER_INFO during connect; emit one
		// so the upgrade-header assertions below are reached.
		info := buildServerInfoFrame(qwpVersion, 0, qwpRolePrimary, 1, 0,
			1_700_000_000_000_000_000, "test-cluster", "mock-node")
		_ = conn.Write(r.Context(), websocket.MessageBinary, info)
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
		t.Errorf("X-QWP-Accept-Encoding=%q, want empty (default compression=raw omits the header)", sawAcceptEnc)
	}
}

// TestQwpQueryClientSendsAcceptEncodingWhenCompressed covers the
// compression opt-in path. When the user sets compression to "zstd"
// or "auto", the client advertises zstd in the upgrade handshake;
// "raw" (default, covered above) omits the header entirely.
func TestQwpQueryClientSendsAcceptEncodingWhenCompressed(t *testing.T) {
	cases := []struct {
		name   string
		opts   []QwpQueryClientOption
		wantAE string
	}{
		{
			// qwpDefaultCompressionLevel = 1 per Sender.java and
			// connect-string.md §Query client keys ("Default `1`").
			name: "zstd_default_level",
			opts: []QwpQueryClientOption{
				WithQwpQueryCompression(qwpCompressionZstd),
			},
			wantAE: "zstd;level=1,raw",
		},
		{
			name: "zstd_explicit_level",
			opts: []QwpQueryClientOption{
				WithQwpQueryCompression(qwpCompressionZstd),
				WithQwpQueryCompressionLevel(7),
			},
			wantAE: "zstd;level=7,raw",
		},
		{
			name: "auto_also_advertises_zstd",
			opts: []QwpQueryClientOption{
				WithQwpQueryCompression(qwpCompressionAuto),
			},
			wantAE: "zstd;level=1,raw",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sawAE string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sawAE = r.Header.Get(qwpHeaderAcceptEncoding)
				w.Header().Set(qwpHeaderVersion, "1")
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				// The egress client reads SERVER_INFO during connect;
				// emit one so the header assertion below is reached.
				info := buildServerInfoFrame(qwpVersion, 0, qwpRolePrimary, 1, 0,
					1_700_000_000_000_000_000, "test-cluster", "mock-node")
				_ = conn.Write(r.Context(), websocket.MessageBinary, info)
			}))
			defer srv.Close()
			addr := strings.TrimPrefix(srv.URL, "http://")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			opts := append([]QwpQueryClientOption{WithQwpQueryAddress(addr)}, tc.opts...)
			c, err := NewQwpQueryClient(ctx, opts...)
			if err != nil {
				t.Fatalf("ctor: %v", err)
			}
			defer c.Close(ctx)
			if sawAE != tc.wantAE {
				t.Errorf("X-QWP-Accept-Encoding=%q, want %q", sawAE, tc.wantAE)
			}
		})
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
// Exercised via the CAS guard on q.state.
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
	// iterator's deferred state→Done already fired. Second Close:
	// same, the CAS from Idle fails on Done state. Neither call
	// should panic or block.
	q.Close()
	q.Close()
}

// TestQwpQueryDrainAfterIteratorCtxExpiry reproduces the bug where
// Batches() yields (nil, ctx.Err()) without sending CANCEL or
// draining, leaving the dispatcher stuck in receiveLoop for the
// abandoned query. The iterator's deferred state→Done then poisons
// the q.Close() CAS so Close early-returns too, and the next
// c.Query() deadlocks on the single-slot requests channel (or, with a
// bounded ctx, returns a stale error instead of running cleanly).
//
// Exercises the takeEvent-error path specifically: the caller's ctx
// expires mid-wait before the server has sent anything. With the fix
// the iterator must CANCEL + drain on a cleanup ctx so the dispatcher
// returns to idle.
func TestQwpQueryDrainAfterIteratorCtxExpiry(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Query 1: send nothing; just wait for CANCEL, then echo
		// CANCELLED. Mirrors a slow-server / timeout scenario.
		req1 := m.readBinary(ctx)
		reqID1, _, _ := parseQueryRequest(t, req1)
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID1, byte(qwpStatusCancelled), "cancelled", -1)))
		// Query 2: one batch + RESULT_END, proving the dispatcher
		// returned to idle after query 1 was drained.
		req2 := m.readBinary(ctx)
		reqID2, _, _ := parseQueryRequest(t, req2)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID2, 0, "v", 99))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID2, 0, 1)))
	})
	defer cleanup()

	// Query 1: short-deadline ctx. The iterator's first takeEvent
	// returns ctx.Err() because the server sends nothing. The body
	// accepts the (nil, err) without breaking so we exit via the
	// takeEvent-error return path (the branch that lacked the drain).
	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	q1 := c.Query(ctx1, "SELECT 1")
	var iter1Err error
	var iter1Batches int
	for _, err := range q1.Batches() {
		if err != nil {
			iter1Err = err
			continue
		}
		iter1Batches++
	}
	if iter1Err == nil {
		t.Fatalf("expected ctx-cancel error from iter1, got nil")
	}
	if iter1Batches != 0 {
		t.Fatalf("iter1 batches=%d, want 0", iter1Batches)
	}
	// No-op with the current bug; with the fix, already drained by
	// the iterator's exit path.
	q1.Close()

	// Query 2 must reach RESULT_END within a reasonable timeout. With
	// the bug, the dispatcher is still stuck in receiveLoop for query
	// 1 so this never produces a batch.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
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
	if saw2 != 99 {
		t.Errorf("saw2=%d, want 99 (dispatcher stranded on query 1?)", saw2)
	}
	if q2.TotalRows() != 1 {
		t.Errorf("q2.TotalRows=%d, want 1", q2.TotalRows())
	}
}

// TestQwpExecDrainAfterCtxExpiry is the Exec-side counterpart of the
// Batches drain test. Exec's takeEvent loop returns on ctx.Err
// without CANCEL + drain, leaving the dispatcher stuck on the
// unfinished server-side query. A subsequent Exec must still work
// once the first Exec has returned.
func TestQwpExecDrainAfterCtxExpiry(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Exec 1: wait for CANCEL, echo CANCELLED.
		req1 := m.readBinary(ctx)
		reqID1, _, _ := parseQueryRequest(t, req1)
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID1, byte(qwpStatusCancelled), "cancelled", -1)))
		// Exec 2: EXEC_DONE to prove the dispatcher returned to idle.
		req2 := m.readBinary(ctx)
		reqID2, _, _ := parseQueryRequest(t, req2)
		m.sendBinary(ctx, writeQwpFrame(0, buildExecDoneBody(reqID2, 0x07, 5)))
	})
	defer cleanup()

	// Exec 1: short-deadline ctx → takeEvent returns ctx.Err(); Exec
	// currently returns without cancelling/draining.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	if _, err := c.Exec(ctx1, "INSERT INTO x VALUES (1)"); err == nil {
		t.Fatalf("expected ctx error from Exec 1")
	}

	// Exec 2 must complete. With the bug the dispatcher is still stuck
	// on Exec 1, so Exec 2's takeEvent times out on ctx2.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	res, err := c.Exec(ctx2, "INSERT INTO x VALUES (2)")
	if err != nil {
		t.Fatalf("Exec 2 err (dispatcher stranded?): %v", err)
	}
	if res.OpType != 0x07 || res.RowsAffected != 5 {
		t.Errorf("Exec 2 result=%+v, want OpType=0x07 RowsAffected=5", res)
	}
}

// TestQwpQueryYieldPanicReleasesBufferAndDrains verifies that a panic
// raised inside the Batches() yield body does not permanently leak the
// current batch buffer or strand the dispatcher on the in-flight query.
// Without the panic-safe release + drain, bufferPoolSize=1 starves on
// the first panic and a follow-up Query deadlocks on the dispatcher
// still parked in receiveLoop for query 1.
func TestQwpQueryYieldPanicReleasesBufferAndDrains(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 1, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Query 1: one batch, wait for CANCEL, echo CANCELLED.
		req1 := m.readBinary(ctx)
		reqID1, _, _ := parseQueryRequest(t, req1)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID1, 0, "v", 42))
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID1, byte(qwpStatusCancelled), "cancelled", -1)))
		// Query 2: one batch + RESULT_END. Proves the pool has buffers
		// available and the dispatcher is idle.
		req2 := m.readBinary(ctx)
		reqID2, _, _ := parseQueryRequest(t, req2)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID2, 0, "v", 99))
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID2, 0, 1)))
	})
	defer cleanup()

	// Query 1: panic from inside the yield body. Recover the panic so
	// the test survives, and let defer q1.Close() run on the way out.
	func() {
		ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel1()
		q1 := c.Query(ctx1, "SELECT 1")
		defer q1.Close()
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic from yield body")
			}
		}()
		for _, err := range q1.Batches() {
			if err != nil {
				t.Fatalf("iter err: %v", err)
			}
			panic("boom")
		}
	}()

	// Query 2 must complete. With the bug:
	//   - bufferPoolSize=1 and the batch buffer from query 1 is never
	//     returned to the pool, so the dispatcher's handleResultBatch
	//     blocks forever waiting for a free buffer on the next batch.
	//   - even before that point, the dispatcher is still parked in
	//     receiveLoop for query 1 (no CANCEL was ever sent, no drain
	//     happened), so query 2's takeEvent never wakes.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	q2 := c.Query(ctx2, "SELECT 2")
	defer q2.Close()
	var saw int64
	for b, err := range q2.Batches() {
		if err != nil {
			t.Fatalf("q2 err (dispatcher stranded?): %v", err)
		}
		saw = b.Int64(0, 0)
	}
	if saw != 99 {
		t.Errorf("saw=%d, want 99", saw)
	}
	if q2.TotalRows() != 1 {
		t.Errorf("q2.TotalRows=%d, want 1", q2.TotalRows())
	}
}

// TestQwpQueryCloseIsNoOpWhileIterating verifies Close called from
// another goroutine while Batches() is in flight returns immediately
// and does not compete with the iterator for the dispatcher's single
// terminal event. Before the fix, Close's CAS guard only prevented
// double-close by the same caller; a concurrent Close and Batches
// both entered drainUntilTerminal, and whichever lost the race on the
// one terminal frame blocked until its cleanup ctx expired (5 s).
func TestQwpQueryCloseIsNoOpWhileIterating(t *testing.T) {
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Query 1: send one batch, then block until CANCEL arrives so
		// the iterator stays parked in takeEvent while the test
		// invokes Close concurrently.
		req1 := m.readBinary(ctx)
		reqID1, _, _ := parseQueryRequest(t, req1)
		m.sendBinary(ctx, buildOneRowInt64Batch(t, reqID1, 0, "v", 7))
		for {
			frame := m.readBinary(ctx)
			if frame[0] == byte(qwpMsgKindCancel) {
				break
			}
		}
		m.sendBinary(ctx, writeQwpFrame(0, buildQueryErrorBody(
			reqID1, byte(qwpStatusCancelled), "cancelled", -1)))
		// Query 2: RESULT_END only — proves the dispatcher returned
		// to idle via the iterator's own drain path.
		req2 := m.readBinary(ctx)
		reqID2, _, _ := parseQueryRequest(t, req2)
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID2, 0, 0)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT 1")

	seen := make(chan int64, 1)
	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		for b, err := range q.Batches() {
			if err != nil {
				return
			}
			seen <- b.Int64(0, 0)
		}
	}()

	// Wait until the iterator has yielded the first batch — it is
	// now parked in takeEvent waiting on the next event.
	select {
	case v := <-seen:
		if v != 7 {
			t.Fatalf("seen=%d, want 7", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("iterator never yielded a batch")
	}

	// Close must return quickly. With the bug it would race the
	// iterator for the terminal event and block up to the 5 s
	// cleanup timeout.
	closeReturned := make(chan struct{})
	go func() {
		q.Close()
		close(closeReturned)
	}()
	select {
	case <-closeReturned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close blocked while Batches iteration in flight")
	}

	// The iterator is still parked. Cancel() triggers the server's
	// CANCELLED echo, which the iterator swallows and exits cleanly.
	q.Cancel()
	select {
	case <-iterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("iterator did not end after Cancel")
	}

	// Follow-up Query must complete — the dispatcher is idle because
	// the iterator (not the racing Close) drained to the terminal
	// frame.
	q2 := c.Query(ctx, "SELECT 2")
	defer q2.Close()
	for _, err := range q2.Batches() {
		if err != nil {
			t.Fatalf("q2 err (dispatcher stranded?): %v", err)
		}
	}
}

// --- Bind parameter tests ---

// parseQueryRequestWithBinds parses a client-sent QUERY_REQUEST and
// returns the bind count plus the raw bind payload bytes, in addition
// to the usual tuple. Tests that exercise WithQwpQueryBinds assert against
// this richer view.
func parseQueryRequestWithBinds(t *testing.T, frame []byte) (int64, string, int64, int, []byte) {
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
	bindCount, n, err := qwpReadVarint(frame[p:])
	if err != nil {
		t.Fatalf("bad bind_count varint: %v", err)
	}
	p += n
	return requestId, sql, int64(credit), int(bindCount), frame[p:]
}

// TestQwpQueryWithBindsWiresBindPayload sends a query with mixed-type
// binds and asserts the server sees the pre-encoded bind bytes along
// with a matching bind_count.
func TestQwpQueryWithBindsWiresBindPayload(t *testing.T) {
	const wantSQL = "SELECT * FROM trades WHERE sym = $1 AND price >= $2 AND ts >= $3 LIMIT 1000"
	var gotFrame []byte
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gotFrame = append(gotFrame, m.readBinary(ctx)...)
		reqID, _, _, _, _ := parseQueryRequestWithBinds(t, gotFrame)
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 0)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, wantSQL, WithQwpQueryBinds(func(b *QwpBinds) {
		b.VarcharBind(0, "AAPL").
			DoubleBind(1, 100.0).
			TimestampMicrosBind(2, 1_700_000_000_000_000)
	}))
	defer q.Close()
	for _, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
	}

	_, sql, _, bindCount, bindPayload := parseQueryRequestWithBinds(t, gotFrame)
	if sql != wantSQL {
		t.Errorf("sql=%q, want %q", sql, wantSQL)
	}
	if bindCount != 3 {
		t.Fatalf("bind_count=%d, want 3", bindCount)
	}

	// Build the expected bind payload by running the same encoder
	// against a fresh QwpBinds instance. This way the expected bytes
	// live in exactly one place (the production encoder) and the test
	// asserts only the wiring, not the encoding.
	var expected QwpBinds
	expected.VarcharBind(0, "AAPL").
		DoubleBind(1, 100.0).
		TimestampMicrosBind(2, 1_700_000_000_000_000)
	if !bytes.Equal(bindPayload, expected.bufferBytes()) {
		t.Fatalf("bind payload mismatch:\n got: % x\nwant: % x",
			bindPayload, expected.bufferBytes())
	}
}

// TestQwpQueryWithBindsEmpty verifies a query with zero-argument binds
// (user passed WithQwpQueryBinds with no setter calls) sends bind_count=0
// and an empty bind payload — equivalent to not using WithQwpQueryBinds
// at all.
func TestQwpQueryWithBindsEmpty(t *testing.T) {
	var gotFrame []byte
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gotFrame = append(gotFrame, m.readBinary(ctx)...)
		reqID, _, _, _, _ := parseQueryRequestWithBinds(t, gotFrame)
		m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 0)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT 1", WithQwpQueryBinds(func(b *QwpBinds) {}))
	defer q.Close()
	for _, err := range q.Batches() {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
	}

	_, _, _, bindCount, bindPayload := parseQueryRequestWithBinds(t, gotFrame)
	if bindCount != 0 {
		t.Errorf("bind_count=%d, want 0", bindCount)
	}
	if len(bindPayload) != 0 {
		t.Errorf("bind payload should be empty, got % x", bindPayload)
	}
}

// TestQwpQueryWithBindsSurfacesEncodingError verifies a bind setter
// that produces a latched QwpBinds error (e.g. out-of-order index)
// fails the query through the iterator's first yield, without sending
// a QUERY_REQUEST to the server.
func TestQwpQueryWithBindsSurfacesEncodingError(t *testing.T) {
	done := make(chan struct{})
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		// Server should never see a frame for a bind-failing query.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_, _, err := m.conn.Read(ctx)
		if err == nil {
			t.Errorf("server received a frame despite bind error")
		}
		close(done)
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	q := c.Query(ctx, "SELECT 1", WithQwpQueryBinds(func(b *QwpBinds) {
		b.LongBind(0, 1)
		b.LongBind(5, 2) // out-of-order
	}))
	defer q.Close()

	var sawErr error
	for _, err := range q.Batches() {
		if err != nil {
			sawErr = err
			break
		}
	}
	if sawErr == nil {
		t.Fatal("expected bind error to surface via Batches")
	}
	if !strings.Contains(sawErr.Error(), "out of order") {
		t.Fatalf("unexpected error: %v", sawErr)
	}
	<-done
}

// TestQwpExecWithBinds verifies WithQwpQueryBinds is plumbed through Exec,
// not just Query. Drives an EXEC_DONE against a bind-bearing UPDATE-
// style request.
func TestQwpExecWithBinds(t *testing.T) {
	var gotFrame []byte
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gotFrame = append(gotFrame, m.readBinary(ctx)...)
		reqID, _, _, _, _ := parseQueryRequestWithBinds(t, gotFrame)
		m.sendBinary(ctx, writeQwpFrame(0, buildExecDoneBody(reqID, 0x01, 42)))
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Exec(ctx, "UPDATE trades SET price = $1 WHERE sym = $2",
		WithQwpQueryBinds(func(b *QwpBinds) {
			b.DoubleBind(0, 200.5).VarcharBind(1, "MSFT")
		}))
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.RowsAffected != 42 {
		t.Errorf("RowsAffected=%d, want 42", res.RowsAffected)
	}

	_, _, _, bindCount, bindPayload := parseQueryRequestWithBinds(t, gotFrame)
	if bindCount != 2 {
		t.Fatalf("bind_count=%d, want 2", bindCount)
	}
	var expected QwpBinds
	expected.DoubleBind(0, 200.5).VarcharBind(1, "MSFT")
	if !bytes.Equal(bindPayload, expected.bufferBytes()) {
		t.Fatalf("bind payload mismatch:\n got: % x\nwant: % x",
			bindPayload, expected.bufferBytes())
	}
}

// TestQwpQueryBindsResetAcrossCalls verifies the per-client bind
// scratch is reset between calls — a second query with fewer binds
// must not accidentally include the prior call's trailing bytes.
func TestQwpQueryBindsResetAcrossCalls(t *testing.T) {
	frames := make(chan []byte, 2)
	c, cleanup := newMockQueryClient(t, 2, func(m *qwpMockEgressConn) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 0; i < 2; i++ {
			f := m.readBinary(ctx)
			frames <- f
			reqID, _, _, _, _ := parseQueryRequestWithBinds(t, f)
			m.sendBinary(ctx, writeQwpFrame(0, buildResultEndBody(reqID, 0, 0)))
		}
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First query has 3 binds.
	q1 := c.Query(ctx, "SELECT 1", WithQwpQueryBinds(func(b *QwpBinds) {
		b.LongBind(0, 1).LongBind(1, 2).LongBind(2, 3)
	}))
	for _, err := range q1.Batches() {
		if err != nil {
			t.Fatalf("q1 err: %v", err)
		}
	}
	q1.Close()

	// Second query has 1 bind — must not carry over the first two longs.
	q2 := c.Query(ctx, "SELECT 2", WithQwpQueryBinds(func(b *QwpBinds) {
		b.IntBind(0, 99)
	}))
	for _, err := range q2.Batches() {
		if err != nil {
			t.Fatalf("q2 err: %v", err)
		}
	}
	q2.Close()

	close(frames)
	got := make([][]byte, 0, 2)
	for f := range frames {
		got = append(got, f)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(got))
	}
	_, _, _, count1, payload1 := parseQueryRequestWithBinds(t, got[0])
	_, _, _, count2, payload2 := parseQueryRequestWithBinds(t, got[1])
	if count1 != 3 {
		t.Errorf("q1 bind_count=%d, want 3", count1)
	}
	if count2 != 1 {
		t.Errorf("q2 bind_count=%d, want 1", count2)
	}

	var wantPayload2 QwpBinds
	wantPayload2.IntBind(0, 99)
	if !bytes.Equal(payload2, wantPayload2.bufferBytes()) {
		t.Fatalf("q2 payload mismatch (possible carry-over from q1):\n got: % x\nwant: % x",
			payload2, wantPayload2.bufferBytes())
	}
	// Sanity: payload1 must not be a prefix/subset of payload2 (i.e., they
	// encode different things).
	if bytes.Contains(payload2, payload1) {
		t.Fatalf("q2 payload contains q1 payload — scratch not reset")
	}
}
