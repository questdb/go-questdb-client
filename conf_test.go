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

package questdb_test

import (
	"fmt"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
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
			name:   "https with min_throughput",
			config: fmt.Sprintf("https::addr=%s;min_throughput=%d;", addr, min_throughput),
			expected: qdb.ConfigData{
				Schema: "https",
				KeyValuePairs: map[string]string{
					"addr":           addr,
					"min_throughput": fmt.Sprintf("%d", min_throughput),
				},
			},
		},
		{
			name:   "https with min_throughput, init_buf_size and tls_verify=unsafe_off",
			config: fmt.Sprintf("https::addr=%s;min_throughput=%d;init_buf_size=%d;tls_verify=unsafe_off;", addr, min_throughput, 1024),
			expected: qdb.ConfigData{
				Schema: "https",
				KeyValuePairs: map[string]string{
					"addr":           addr,
					"min_throughput": fmt.Sprintf("%d", min_throughput),
					"init_buf_size":  "1024",
					"tls_verify":     "unsafe_off",
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
			name: "http with min_throughput, request_timeout, and retry_timeout",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d;request_timeout=%d;retry_timeout=%d;",
				addr, min_throughput, request_timeout.Milliseconds(), retry_timeout.Milliseconds()),
			expected: qdb.ConfigData{
				Schema: "http",
				KeyValuePairs: map[string]string{
					"addr":            addr,
					"min_throughput":  fmt.Sprintf("%d", min_throughput),
					"request_timeout": fmt.Sprintf("%d", request_timeout.Milliseconds()),
					"retry_timeout":   fmt.Sprintf("%d", retry_timeout.Milliseconds()),
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
			name:                   "unescaped semicolon in password leads to invalid key character",
			config:                 "http::addr=localhost:9000;username=test;password=pass;word;",
			expectedErrMsgContains: "unexpected end of",
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

type configTestCase struct {
	name                   string
	config                 string
	expectedOpts           []qdb.LineSenderOption
	expectedErrMsgContains string
}

func TestHappyCasesFromConf(t *testing.T) {
	var (
		addr           = "localhost:1111"
		user           = "test-user"
		pass           = "test-pass"
		token          = "test-token"
		minThroughput  = 999
		requestTimeout = time.Second * 88
		retryTimeout   = time.Second * 99
		initBufSize    = 256
		maxBufSize     = 1024
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
			config: fmt.Sprintf("http::addr=%s;password=%s;username=%s;",
				addr, pass, user),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithBasicAuth(user, pass),
			},
		},
		{
			name: "min_throughput",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d;",
				addr, minThroughput),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(addr),
				qdb.WithMinThroughput(minThroughput),
			},
		},
		{
			name: "bearer token",
			config: fmt.Sprintf("http::addr=%s;token=%s;",
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
				qdb.WithAutoFlushInterval(1000),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := qdb.ConfFromStr(tc.config)
			assert.NoError(t, err)

			expected := &qdb.LineSenderConfig{}
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
			name:                   "trailing semicolon required",
			config:                 "http::addr=localhost:9000",
			expectedErrMsgContains: "trailing semicolon",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qdb.ConfFromStr(tc.config)
			assert.ErrorContains(t, err, tc.expectedErrMsgContains)
		})
	}
}
