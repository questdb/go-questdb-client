/*+*****************************************************************************
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

package questdb_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/stretchr/testify/assert"
)

type parseConfigTestCase struct {
	name                   string
	config                 string
	expected               qdb.ConfigData
	expectedErrMsgContains string
}

func TestParserHappyCases(t *testing.T) {
	var (
		addr            = "localhost:1111"
		user            = "test-user"
		pass            = "test-pass"
		token           = "test-token"
		min_throughput  = 999
		request_timeout = time.Second * 88
		retry_timeout   = time.Second * 99
	)

	testCases := []parseConfigTestCase{
		{
			name:   "http and ipv4 address",
			config: fmt.Sprintf("http::addr=%s;", addr),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr": addr,
				},
			},
		},
		{
			name:   "http and ipv6 address",
			config: "http::addr=::1;",
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr": "::1",
				},
			},
		},
		{
			name:   "tcp and address",
			config: fmt.Sprintf("tcp::addr=%s;", addr),
			expected: qdb.ConfigData{
				Schema: "tcp",
				KeyValuePairs: map[string]string{
					"addr": addr,
				},
			},
		},
		{
			name:   "http and username/password",
			config: fmt.Sprintf("http::addr=%s;username=%s;password=%s;", addr, user, pass),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":     addr,
					"username": user,
					"password": pass,
				},
			},
		},
		{
			name:   "http and token (with trailing ';')",
			config: fmt.Sprintf("http::addr=%s;token=%s;", addr, token),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":  addr,
					"token": token,
				},
			},
		},
		{
			name:   "tcp with key and user",
			config: fmt.Sprintf("tcp::addr=%s;token=%s;username=%s;", addr, token, user),
			expected: qdb.ConfigData{
				Schema: "tcp",
				KeyValuePairs: map[string]string{
					"addr":     addr,
					"username": user,
					"token":    token,
				},
			},
		},
		{
			name:   "https with request_min_throughput",
			config: fmt.Sprintf("https::addr=%s;request_min_throughput=%d;", addr, min_throughput),
			expected: qdb.ConfigData{
				Schema: "https",
				KeyValuePairs: map[string]string{
					"addr":                   addr,
					"request_min_throughput": fmt.Sprintf("%d", min_throughput),
				},
			},
		},
		{
			name:   "https with request_min_throughput, init_buf_size and tls_verify=unsafe_off",
			config: fmt.Sprintf("https::addr=%s;request_min_throughput=%d;init_buf_size=%d;tls_verify=unsafe_off;", addr, min_throughput, 1024),
			expected: qdb.ConfigData{
				Schema: "https",
				KeyValuePairs: map[string]string{
					"addr":                   addr,
					"request_min_throughput": fmt.Sprintf("%d", min_throughput),
					"init_buf_size":          "1024",
					"tls_verify":             "unsafe_off",
				},
			},
		},
		{
			name:   "tcps with tls_verify=unsafe_off",
			config: fmt.Sprintf("tcps::addr=%s;tls_verify=unsafe_off;", addr),
			expected: qdb.ConfigData{
				Schema: "tcps",
				KeyValuePairs: map[string]string{
					"addr":       addr,
					"tls_verify": "unsafe_off",
				},
			},
		},
		{
			name: "http with request_min_throughput, request_timeout, and retry_timeout",
			config: fmt.Sprintf("http::addr=%s;request_min_throughput=%d;request_timeout=%d;retry_timeout=%d;",
				addr, min_throughput, request_timeout.Milliseconds(), retry_timeout.Milliseconds()),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":                   addr,
					"request_min_throughput": fmt.Sprintf("%d", min_throughput),
					"request_timeout":        fmt.Sprintf("%d", request_timeout.Milliseconds()),
					"retry_timeout":          fmt.Sprintf("%d", retry_timeout.Milliseconds()),
				},
			},
		},
		{
			name:   "tcp with tls_verify=on",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=on;", addr),
			expected: qdb.ConfigData{
				Schema: "tcp",
				KeyValuePairs: map[string]string{
					"addr":       addr,
					"tls_verify": "on",
				},
			},
		},
		{
			name:   "password with an escaped semicolon (ending with a ';')",
			config: fmt.Sprintf("http::addr=%s;username=%s;password=pass;;word;", addr, user),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":     addr,
					"username": user,
					"password": "pass;word",
				},
			},
		},
		{
			name:   "password with a trailing semicolon",
			config: fmt.Sprintf("http::addr=%s;username=%s;password=password;;;", addr, user),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":     addr,
					"username": user,
					"password": "password;",
				},
			},
		},
		{
			name:   "protocol version",
			config: fmt.Sprintf("http::addr=%s;protocol_version=1;", addr),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":             addr,
					"protocol_version": "1",
				},
			},
		},
		{
			name:   "equal sign in password",
			config: fmt.Sprintf("http::addr=%s;username=%s;password=pass=word;", addr, user),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":     addr,
					"username": user,
					"password": "pass=word",
				},
			},
		},
		{
			name:   "no trailing semicolon",
			config: fmt.Sprintf("http::addr=%s", addr),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr": addr,
				},
			},
		},
		{
			name:   "ws schema",
			config: fmt.Sprintf("ws::addr=%s;", addr),
			expected: qdb.ConfigData{
				Schema: "ws",
				KeyValuePairs: map[string]string{
					"addr": addr,
				},
			},
		},
		{
			name:   "wss schema",
			config: fmt.Sprintf("wss::addr=%s;", addr),
			expected: qdb.ConfigData{
				Schema: "wss",
				KeyValuePairs: map[string]string{
					"addr": addr,
				},
			},
		},
		{
			// failover.md §1: `addr=h1;addr=h2` is an alternative
			// spelling of `addr=h1,h2`. The parser MUST accumulate
			// both forms into a single comma-joined value.
			name:   "ws addr accumulates across repeated keys",
			config: "ws::addr=h1:9000;addr=h2:9000;addr=h3:9000;",
			expected: qdb.ConfigData{
				Schema: "ws",
				KeyValuePairs: map[string]string{
					"addr": "h1:9000,h2:9000,h3:9000",
				},
			},
		},
		{
			// Comma-form already parses today; accumulator must not
			// double-comma when mixing with repeated keys.
			name:   "ws addr accumulates mixed comma and repeated forms",
			config: "ws::addr=h1:9000,h2:9000;addr=h3:9000;",
			expected: qdb.ConfigData{
				Schema: "ws",
				KeyValuePairs: map[string]string{
					"addr": "h1:9000,h2:9000,h3:9000",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := qdb.ParseConfigStr(tc.config)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestParserPathologicalCases(t *testing.T) {
	testCases := []parseConfigTestCase{
		{
			name:                   "empty config",
			config:                 "",
			expectedErrMsgContains: "no schema separator found",
		},
		{
			name:                   "empty config with semicolon",
			config:                 ";",
			expectedErrMsgContains: "no schema separator found",
		},
		{
			name:                   "no schema",
			config:                 "addr=localhost:9000",
			expectedErrMsgContains: "no schema separator found",
		},
		{
			name:                   "no address",
			config:                 "http::",
			expectedErrMsgContains: "'addr' key not found",
		},
		{
			name:                   "unescaped semicolon in password leads to unexpected end of string (with trailing semicolon)",
			config:                 "http::addr=localhost:9000;username=test;password=pass;word;",
			expectedErrMsgContains: "unexpected end of",
		},
		{
			name:                   "unescaped semicolon in password leads to unexpected end of string (no trailing semicolon)",
			config:                 "http::addr=localhost:9000;username=test;password=pass;word",
			expectedErrMsgContains: "unexpected end of",
		},
		{
			name:                   "duplicate on_server_error",
			config:                 "ws::addr=localhost:9000;on_server_error=auto;on_server_error=halt;",
			expectedErrMsgContains: `duplicate key \"on_server_error\"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qdb.ParseConfigStr(tc.config)
			var expected *qdb.InvalidConfigStrError
			assert.Error(t, err)
			assert.ErrorAs(t, err, &expected)
			assert.Contains(t, err.Error(), tc.expectedErrMsgContains)
		})
	}
}

func TestParserKeysAreCaseSensitive(t *testing.T) {
	// Same key bytes in different case are distinct, matching the Rust
	// client. The lowercase `addr` is recognized; the uppercase `ADDR`
	// is parsed but later rejected as an unsupported option.
	parsed, err := qdb.ParseConfigStr("http::addr=localhost:9000;ADDR=localhost:9001;")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:9000", parsed.KeyValuePairs["addr"])
	assert.Equal(t, "localhost:9001", parsed.KeyValuePairs["ADDR"])

	_, err = qdb.ConfFromStr("http::addr=localhost:9000;ADDR=localhost:9001;")
	assert.ErrorContains(t, err, "unsupported option")
}

type configTestCase struct {
	name                   string
	config                 string
	expectedOpts           []qdb.LineSenderOption
	expectedErrMsgContains string
}

func TestHappyCasesFromConf(t *testing.T) {
	var (
		addr            = "localhost:1111"
		user            = "test-user"
		pass            = "test-pass"
		token           = "test-token"
		minThroughput   = 999
		requestTimeout  = time.Second * 88
		retryTimeout    = time.Second * 99
		initBufSize     = 256
		maxBufSize      = 1024
		protocolVersion = qdb.ProtocolVersion2
	)

	testCases := []configTestCase{
		{
			name: "user and token",
			config: fmt.Sprintf("tcp::addr=%s;username=%s;token=%s;",
				addr, user, token),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithTcp(),
				qdb.WithAddress(addr),
				qdb.WithAuth(user, token),
			},
		},
		{
			name: "token_x and token_y (ignored)",
			config: fmt.Sprintf("tcp::addr=%s;username=%s;token=%s;token_x=xyz;token_y=xyz;",
				addr, user, token),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithTcp(),
				qdb.WithAddress(addr),
				qdb.WithAuth(user, token),
			},
		},
		{
			name: "init_buf_size and max_buf_size",
			config: fmt.Sprintf("tcp::addr=%s;init_buf_size=%d;max_buf_size=%d;",
				addr, initBufSize, maxBufSize),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithTcp(),
				qdb.WithAddress(addr),
				qdb.WithInitBufferSize(initBufSize),
				qdb.WithMaxBufferSize(maxBufSize),
			},
		},
		{
			name:   "max_name_len",
			config: fmt.Sprintf("http::addr=%s;max_name_len=64;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithFileNameLimit(64),
			},
		},
		{
			name: "with tls",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=on;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithTcp(),
				qdb.WithAddress(addr),
				qdb.WithTls(),
			},
		},
		{
			name: "with tls and unsafe_off",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=unsafe_off;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithTcp(),
				qdb.WithAddress(addr),
				qdb.WithTlsInsecureSkipVerify(),
			},
		},
		{
			name: "request_timeout and retry_timeout milli conversion",
			config: fmt.Sprintf("http::addr=%s;request_timeout=%d;retry_timeout=%d;",
				addr, requestTimeout.Milliseconds(), retryTimeout.Milliseconds()),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithRequestTimeout(requestTimeout),
				qdb.WithRetryTimeout(retryTimeout),
			},
		},
		{
			name: "password before username",
			config: fmt.Sprintf("http::addr=%s;password=%s;username=%s",
				addr, pass, user),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithBasicAuth(user, pass),
			},
		},
		{
			name: "request_min_throughput",
			config: fmt.Sprintf("http::addr=%s;request_min_throughput=%d;",
				addr, minThroughput),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithMinThroughput(minThroughput),
			},
		},
		{
			name: "protocol_version",
			config: fmt.Sprintf("http::addr=%s;protocol_version=%d;",
				addr, protocolVersion),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithProtocolVersion(protocolVersion),
			},
		},
		{
			name: "bearer token",
			config: fmt.Sprintf("http::addr=%s;token=%s",
				addr, token),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithBearerToken(token),
			},
		},
		{
			name: "auto flush",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=1000;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithAutoFlushRows(100),
				qdb.WithAutoFlushInterval(1000 * time.Millisecond),
			},
		},
		{
			name: "auto flush interval off",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=100;auto_flush_interval=off;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithAutoFlushRows(100),
				qdb.WithAutoFlushInterval(0),
			},
		},
		{
			name: "auto flush rows off",
			config: fmt.Sprintf("http::addr=%s;auto_flush_rows=off;auto_flush_interval=1000;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithAutoFlushRows(0),
				qdb.WithAutoFlushInterval(1000 * time.Millisecond),
			},
		},
		{
			name:   "ws basic",
			config: fmt.Sprintf("ws::addr=%s;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
			},
		},
		{
			name:   "wss with tls",
			config: fmt.Sprintf("wss::addr=%s;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithTls(),
			},
		},
		{
			name:   "wss with tls_verify unsafe_off",
			config: fmt.Sprintf("wss::addr=%s;tls_verify=unsafe_off;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithTlsInsecureSkipVerify(),
			},
		},
		{
			// retry_timeout is intentionally NOT paired with ws here: the
			// parser maps it to WithRetryTimeout for any schema, but the
			// QWP sanitizer rejects it (see TestQwpSanitizeRejectsRetryTimeout),
			// so a ws connect string carrying it never reaches a sender.
			name: "ws with auto_flush",
			config: fmt.Sprintf("ws::addr=%s;auto_flush_rows=100;auto_flush_interval=500;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithAutoFlushRows(100),
				qdb.WithAutoFlushInterval(500 * time.Millisecond),
			},
		},
		{
			name:   "ws with bearer token",
			config: fmt.Sprintf("ws::addr=%s;token=%s;", addr, token),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithBearerToken(token),
			},
		},
		{
			name:   "ws with basic auth",
			config: fmt.Sprintf("ws::addr=%s;username=%s;password=%s;", addr, user, pass),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithBasicAuth(user, pass),
			},
		},
		{
			name:   "ws with gorilla=off",
			config: fmt.Sprintf("ws::addr=%s;gorilla=off;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithGorilla(false),
			},
		},
		{
			name:   "ws with gorilla=on",
			config: fmt.Sprintf("ws::addr=%s;gorilla=on;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithGorilla(true),
			},
		},
		{
			name:   "ws with auth_timeout_ms",
			config: fmt.Sprintf("ws::addr=%s;auth_timeout_ms=7000;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithAuthTimeout(7 * time.Second),
			},
		},
		{
			name:   "ws with target=primary",
			config: fmt.Sprintf("ws::addr=%s;target=primary;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithTarget(qdb.QwpTargetPrimary),
			},
		},
		{
			name:   "ws with zone",
			config: fmt.Sprintf("ws::addr=%s;zone=az-1;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithZone("az-1"),
			},
		},
		{
			name:   "ws with on_server_error",
			config: fmt.Sprintf("ws::addr=%s;on_server_error=halt;", addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithServerErrorPolicy(qdb.PolicyHalt),
			},
		},
		{
			name: "ws with sf cursor knobs",
			config: fmt.Sprintf(
				"ws::addr=%s;sf_dir=/tmp/sf;sf_durability=memory;sf_append_deadline_millis=20000;drain_orphans=on;max_background_drainers=2;",
				addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithQwp(),
				qdb.WithAddress(addr),
				qdb.WithSfDir("/tmp/sf"),
				qdb.WithSfDurability("memory"),
				qdb.WithSfAppendDeadline(20 * time.Second),
				qdb.WithDrainOrphans(true),
				qdb.WithMaxBackgroundDrainers(2),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := qdb.ConfFromStr(tc.config)
			assert.NoError(t, err)

			var expected *qdb.LineSenderConfig
			if len(tc.config) >= 2 && tc.config[:2] == "ws" {
				expected = qdb.NewLineSenderConfig(qdb.QwpSenderType)
			} else {
				switch tc.config[0] {
				case 'h':
					expected = qdb.NewLineSenderConfig(qdb.HttpSenderType)
				case 't':
					expected = qdb.NewLineSenderConfig(qdb.TcpSenderType)
				default:
					assert.FailNow(t, "happy case configs must start with 'http', 'tcp', or 'ws'")
				}
			}
			for _, opt := range tc.expectedOpts {
				opt(expected)
			}

			assert.Equal(t, expected, actual)
		})
	}
}

func TestPathologicalCasesFromConf(t *testing.T) {
	testCases := []configTestCase{
		{
			name:                   "empty config",
			config:                 "",
			expectedErrMsgContains: "no schema separator found",
		},
		{
			name:                   "invalid schema",
			config:                 "foobar::addr=localhost:1111;",
			expectedErrMsgContains: "invalid schema",
		},
		{
			name:                   "invalid tls_verify 1",
			config:                 "tcp::addr=localhost:1111;tls_verify=invalid;",
			expectedErrMsgContains: "invalid tls_verify",
		},
		{
			name:                   "invalid tls_verify 2",
			config:                 "http::addr=localhost:1111;tls_verify=invalid;",
			expectedErrMsgContains: "invalid tls_verify",
		},
		{
			name:                   "unsupported option",
			config:                 "tcp::addr=localhost:1111;unsupported_option=invalid;",
			expectedErrMsgContains: "unsupported option",
		},
		{
			name:                   "invalid auto_flush",
			config:                 "http::addr=localhost:1111;auto_flush=invalid;",
			expectedErrMsgContains: "invalid auto_flush",
		},
		{
			name:                   "invalid auto_flush_rows",
			config:                 "http::addr=localhost:1111;auto_flush_rows=invalid;",
			expectedErrMsgContains: "invalid auto_flush_rows",
		},
		{
			name:                   "invalid auto_flush_interval",
			config:                 "http::addr=localhost:1111;auto_flush_interval=invalid;",
			expectedErrMsgContains: "invalid auto_flush_interval",
		},
		{
			name:                   "invalid gorilla value",
			config:                 "ws::addr=localhost:1111;gorilla=maybe;",
			expectedErrMsgContains: "not 'on' or 'off'",
		},
		{
			name:                   "in_flight_window on HTTP",
			config:                 "http::addr=localhost:1111;in_flight_window=4;",
			expectedErrMsgContains: "unsupported option",
		},
		{
			// close_timeout was a Go-only legacy key; the Java client
			// never accepted it. Removed in favour of the spec-
			// aligned close_flush_timeout_millis. The parser now
			// rejects regardless of schema, with a migration hint.
			name:                   "close_timeout rejected with migration hint",
			config:                 "tcp::addr=localhost:1111;close_timeout=1000;",
			expectedErrMsgContains: "close_timeout is no longer supported",
		},
		{
			name:                   "gorilla on TCP",
			config:                 "tcp::addr=localhost:1111;gorilla=off;",
			expectedErrMsgContains: "gorilla is only supported for QWP senders",
		},
		{
			name:                   "unsupported option",
			config:                 "http::addr=localhost:1111;unsupported_option=invalid;",
			expectedErrMsgContains: "unsupported option",
		},
		{
			name:                   "case-sensitive values",
			config:                 "http::aDdr=localhost:9000;",
			expectedErrMsgContains: "unsupported option",
		},
		{
			name:                   "partial key at end",
			config:                 "http::addr=localhost:9000;test",
			expectedErrMsgContains: "unexpected end of string",
		},
		{
			name:                   "no value at end",
			config:                 "http::addr=localhost:9000;username=",
			expectedErrMsgContains: "empty value for key",
		},
		{
			name:                   "no value at end with semicolon",
			config:                 "http::addr=localhost:9000;username=;",
			expectedErrMsgContains: "empty value for key",
		},
		{
			name:                   "auth_timeout_ms on HTTP",
			config:                 "http::addr=localhost:9000;auth_timeout_ms=5000;",
			expectedErrMsgContains: "auth_timeout_ms is only supported for QWP senders",
		},
		{
			name:                   "zone on TCP",
			config:                 "tcp::addr=localhost:9009;zone=eu-west-1a;",
			expectedErrMsgContains: "zone is only supported for QWP senders",
		},
		{
			name:                   "target on HTTP",
			config:                 "http::addr=localhost:9000;target=primary;",
			expectedErrMsgContains: "target is only supported for QWP senders",
		},
		{
			name:                   "invalid target value",
			config:                 "ws::addr=localhost:9000;target=foo;",
			expectedErrMsgContains: "invalid target",
		},
		{
			name:                   "non-positive auth_timeout_ms",
			config:                 "ws::addr=localhost:9000;auth_timeout_ms=0;",
			expectedErrMsgContains: "auth_timeout_ms",
		},
		{
			name:                   "non-numeric auth_timeout_ms",
			config:                 "ws::addr=localhost:9000;auth_timeout_ms=fast;",
			expectedErrMsgContains: "auth_timeout_ms",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qdb.ConfFromStr(tc.config)
			assert.ErrorContains(t, err, tc.expectedErrMsgContains)
		})
	}
}

// TestQwpFailoverSanitizeErrors covers sanitizer-level rejections for
// failover-related config: multi-host on HTTP/TCP (not yet wired up
// in those transports) and malformed QWP endpoint lists.
func TestQwpFailoverSanitizeErrors(t *testing.T) {
	cases := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "multi-host addr on HTTP",
			config: "http::addr=localhost:9000,localhost:9001;",
			errMsg: "multi-host addr is not supported for HTTP",
		},
		{
			name:   "multi-host addr on HTTP via repeated keys",
			config: "http::addr=localhost:9000;addr=localhost:9001;",
			errMsg: "multi-host addr is not supported for HTTP",
		},
		{
			name:   "multi-host addr on TCP",
			config: "tcp::addr=localhost:9009,localhost:9010;",
			errMsg: "multi-host addr is not supported for TCP",
		},
		{
			name:   "trailing comma in addr",
			config: "ws::addr=localhost:9000,;",
			errMsg: "empty entry in addr list",
		},
		{
			name:   "double comma in addr",
			config: "ws::addr=h1:9000,,h2:9000;",
			errMsg: "empty entry in addr list",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := qdb.ConfFromStr(tc.config)
			assert.NoError(t, err)
			assert.ErrorContains(t, qdb.SanitizeConf(c), tc.errMsg)
		})
	}
}

// TestQwpSanitizeRejectsRetryTimeout pins that retry_timeout, though
// the parser accepts it for any schema, is rejected by the QWP
// sanitizer: it is an HTTP-ILP retry knob with no QWP analogue
// (reconnect_max_duration_millis governs the per-outage budget
// instead). Guards the parser happy-case in TestHappyCasesFromConf,
// which deliberately omits the ws+retry_timeout pairing.
func TestQwpSanitizeRejectsRetryTimeout(t *testing.T) {
	c, err := qdb.ConfFromStr("ws::addr=localhost:9000;retry_timeout=5000;")
	assert.NoError(t, err, "parser accepts retry_timeout for any schema")
	assert.ErrorContains(t, qdb.SanitizeConf(c),
		"retry_timeout is not supported for QWP")
}

// TestQwpFailoverConfKeys covers the connect-string keys mandated by
// failover.md §1 (addr multi-host, auth_timeout_ms, zone, target).
// The keys are parsed but not yet consumed by the SF reconnect loop —
// these tests pin down the parser-and-sanitizer surface so the
// downstream wire-up phases can rely on it.
func TestQwpFailoverConfKeys(t *testing.T) {
	parseSanitize := func(t *testing.T, conf string) *qdb.LineSenderConfig {
		t.Helper()
		c, err := qdb.ConfFromStr(conf)
		assert.NoError(t, err)
		assert.NoError(t, qdb.SanitizeConf(c))
		return c
	}

	t.Run("single host populates endpoints[0]", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=questdb.local:9000;")
		assert.Equal(t, []string{"questdb.local:9000"}, qdb.ConfigEndpoints(c))
	})

	t.Run("comma-form addr produces ordered endpoints", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=h1:9000,h2:9000,h3:9000;")
		assert.Equal(t,
			[]string{"h1:9000", "h2:9000", "h3:9000"},
			qdb.ConfigEndpoints(c))
	})

	t.Run("repeated-key addr produces ordered endpoints", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=h1:9000;addr=h2:9000;addr=h3:9000;")
		assert.Equal(t,
			[]string{"h1:9000", "h2:9000", "h3:9000"},
			qdb.ConfigEndpoints(c))
	})

	t.Run("missing port defaults to 9000", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=h1,h2:9001,h3;")
		assert.Equal(t,
			[]string{"h1:9000", "h2:9001", "h3:9000"},
			qdb.ConfigEndpoints(c))
	})

	t.Run("IPv6 bracketed host", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=[::1]:9000,[fe80::1]:9001;")
		assert.Equal(t,
			[]string{"[::1]:9000", "[fe80::1]:9001"},
			qdb.ConfigEndpoints(c))
	})

	t.Run("auth_timeout_ms default 15s", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=localhost:9000;")
		assert.Equal(t, 15_000, qdb.ConfigAuthTimeoutMs(c))
	})

	t.Run("auth_timeout_ms explicit", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=localhost:9000;auth_timeout_ms=5000;")
		assert.Equal(t, 5_000, qdb.ConfigAuthTimeoutMs(c))
	})

	t.Run("zone is silently accepted on QWP ingress", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=localhost:9000;zone=eu-west-1a;")
		assert.Equal(t, "eu-west-1a", qdb.ConfigZone(c))
	})

	t.Run("target=any (default)", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=localhost:9000;")
		assert.Equal(t, "any", qdb.ConfigTarget(c))
	})

	t.Run("target=primary", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=localhost:9000;target=primary;")
		assert.Equal(t, "primary", qdb.ConfigTarget(c))
	})

	t.Run("target=replica", func(t *testing.T) {
		c := parseSanitize(t, "ws::addr=localhost:9000;target=replica;")
		assert.Equal(t, "replica", qdb.ConfigTarget(c))
	})

	t.Run("wss tls-mode preserved with multi-host", func(t *testing.T) {
		c := parseSanitize(t, "wss::addr=h1:9000,h2:9000;zone=dc-a;target=primary;auth_timeout_ms=8000;")
		assert.Equal(t, []string{"h1:9000", "h2:9000"}, qdb.ConfigEndpoints(c))
		assert.Equal(t, "dc-a", qdb.ConfigZone(c))
		assert.Equal(t, "primary", qdb.ConfigTarget(c))
		assert.Equal(t, 8_000, qdb.ConfigAuthTimeoutMs(c))
	})
}

// TestQwpOnlyOptionsRejectedOnHttpAndTcp pins parity between the
// connect-string parser (which rejects each QWP-only key on http/tcp
// schemas with `<key> is only supported for QWP senders`) and the
// option path. Without this gate, e.g. `WithHttp() + WithSfDir(...)`
// silently constructs an HTTP sender that ignores the setting.
func TestQwpOnlyOptionsRejectedOnHttpAndTcp(t *testing.T) {
	cases := []struct {
		name   string
		opt    qdb.LineSenderOption
		errMsg string
	}{
		{"sf_dir", qdb.WithSfDir("/tmp/sf"), "sf_dir"},
		{"sender_id", qdb.WithSenderId("ingest-1"), "sender_id"},
		{"sf_max_bytes", qdb.WithSfMaxBytes(1 << 20), "sf_max_bytes"},
		{"sf_max_total_bytes", qdb.WithSfMaxTotalBytes(1 << 30), "sf_max_total_bytes"},
		{"sf_durability", qdb.WithSfDurability("memory"), "sf_durability"},
		{"sf_append_deadline", qdb.WithSfAppendDeadline(10 * time.Second), "sf_append_deadline_millis"},
		{"drain_orphans", qdb.WithDrainOrphans(true), "drain_orphans"},
		{"max_background_drainers", qdb.WithMaxBackgroundDrainers(2), "max_background_drainers"},
		{"reconnect_policy", qdb.WithReconnectPolicy(time.Minute, 100*time.Millisecond, time.Second), "reconnect_*"},
		{"initial_connect_mode", qdb.WithInitialConnectMode(qdb.InitialConnectSync), "initial_connect_retry"},
		{"initial_connect_retry", qdb.WithInitialConnectRetry(true), "initial_connect_retry"},
		{"close_flush_timeout", qdb.WithCloseFlushTimeout(5 * time.Second), "close_flush_timeout_millis"},
		{"close_timeout_alias", qdb.WithCloseTimeout(5 * time.Second), "close_flush_timeout_millis"},
		{"gorilla", qdb.WithGorilla(false), "gorilla"},
		{"auth_timeout", qdb.WithAuthTimeout(5 * time.Second), "auth_timeout_ms"},
		{"zone", qdb.WithZone("eu-west-1a"), "zone"},
		{"target", qdb.WithTarget(qdb.QwpTargetPrimary), "target"},
		{"qwp_dump_writer", qdb.WithQwpDumpWriter(io.Discard), "QWP dump writer"},
		{"error_handler", qdb.WithErrorHandler(func(*qdb.SenderError) {}), "server-error API"},
		{"error_inbox_capacity", qdb.WithErrorInboxCapacity(64), "server-error API"},
		{"server_error_policy", qdb.WithServerErrorPolicy(qdb.PolicyHalt), "server-error API"},
	}
	for _, transport := range []struct {
		name string
		ctor qdb.LineSenderOption
	}{
		{"http", qdb.WithHttp()},
		{"tcp", qdb.WithTcp()},
	} {
		for _, tc := range cases {
			t.Run(transport.name+"/"+tc.name, func(t *testing.T) {
				_, err := qdb.NewLineSender(context.Background(), transport.ctor, tc.opt)
				assert.ErrorContains(t, err, tc.errMsg)
			})
		}
	}
}
