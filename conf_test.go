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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type configTestCase struct {
	name                   string
	config                 string
	expected               LineSender
	expectedErrMsgContains string
}

func TestHappyCasesFromConf(t *testing.T) {

	var (
		addr           = "localhost:1111"
		user           = "test-user"
		pass           = "test-pass"
		token          = "test-token"
		min_throughput = 999
		grace_timeout  = time.Second * 88
		retry_timeout  = time.Second * 99
	)

	testCases := []configTestCase{
		{
			name:   "http and address",
			config: fmt.Sprintf("http::addr=%s", addr),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "tcp and address",
			config: fmt.Sprintf("tcp::addr=%s", addr),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "http and username/password",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=%s", addr, user, pass),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "http and token (with trailing ';')",
			config: fmt.Sprintf("http::addr=%s;token=%s;", addr, token),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "tcp with user and key",
			config: fmt.Sprintf("tcp::addr=%s;user=%s;token=%s", addr, user, token),
			expected: LineSender{
				address: addr,
				keyId:   user,
				key:     token,
			},
		},
		{
			name:   "tcp with key and user",
			config: fmt.Sprintf("tcp::addr=%s;token=%s;user=%s", addr, token, user),
			expected: LineSender{
				address: addr,
				keyId:   user,
				key:     token,
			},
		},
		{
			name:   "https with min_throughput",
			config: fmt.Sprintf("https::addr=%s;min_throughput=%d", addr, min_throughput),
			expected: LineSender{
				address:                     addr,
				minThroughputBytesPerSecond: min_throughput,
				tlsMode:                     tlsEnabled,
			},
		},
		{
			name:   "https with min_throughput, init_buf_size and tls_verify=unsafe_off",
			config: fmt.Sprintf("https::addr=%s;min_throughput=%d;init_buf_size=%d;tls_verify=unsafe_off", addr, min_throughput, 1024),
			expected: LineSender{
				address:                     addr,
				minThroughputBytesPerSecond: min_throughput,
				initBufSizeBytes:            1024,
				tlsMode:                     tlsInsecureSkipVerify,
			},
		},
		{
			name:   "tcps with tls_verify=unsafe_off",
			config: fmt.Sprintf("tcps::addr=%s;tls_verify=unsafe_off", addr),
			expected: LineSender{
				address: addr,
				tlsMode: tlsInsecureSkipVerify,
			},
		},
		{
			name: "http with min_throughput, grace_timeout, and retry_timeout",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d;grace_timeout=%d;retry_timeout=%d",
				addr, min_throughput, grace_timeout.Milliseconds(), retry_timeout.Milliseconds()),
			expected: LineSender{
				address:                     addr,
				minThroughputBytesPerSecond: min_throughput,
				graceTimeout:                grace_timeout,
				retryTimeout:                retry_timeout,
			},
		},
		{
			name:   "tcp with tls_verify=on",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=on", addr),
			expected: LineSender{
				address: addr,
				tlsMode: tlsEnabled,
			},
		},
		{
			name:   "password with an escaped semicolon",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=pass;;word", addr, user),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "password with an escaped semicolon (ending with a ';')",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=pass;;word;", addr, user),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "password with a trailing semicolon",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=password;;;", addr, user),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "equal sign in password",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=pass=word", addr, user),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "basic auth with password first",
			config: fmt.Sprintf("http::addr=%s;pass=pass;user=%s", addr, user),
			expected: LineSender{
				address: addr,
			},
		},
		{
			name:   "grace_timeout millisecond conversion",
			config: fmt.Sprintf("http::addr=%s;grace_timeout=88000", addr),
			expected: LineSender{
				address:      addr,
				graceTimeout: grace_timeout,
			},
		},
		{
			name:   "retry_timeout millisecond conversion",
			config: fmt.Sprintf("http::addr=%s;retry_timeout=99000", addr),
			expected: LineSender{
				address:      addr,
				retryTimeout: retry_timeout,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			opts, err := parseConfigString(tc.config)
			actual := LineSender{}
			for _, opt := range opts {
				opt(&actual)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, actual)

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
			name:                   "invalid schema",
			config:                 "invalid::addr=localhost:9000",
			expectedErrMsgContains: "invalid schema",
		},
		{
			name:                   "auto_flush option",
			config:                 "http::addr=localhost:9000;auto_flush=on",
			expectedErrMsgContains: "auto_flush option is not supported",
		},
		{
			name:                   "auto_flush_rows option",
			config:                 "http::addr=localhost:9000;auto_flush_rows=100",
			expectedErrMsgContains: "auto_flush option is not supported",
		},
		{
			name:                   "auto_flush_bytes option",
			config:                 "http::addr=localhost:9000;auto_flush_bytes=100",
			expectedErrMsgContains: "auto_flush option is not supported",
		},
		{
			name:                   "invalid min_throughput",
			config:                 "http::addr=localhost:9000;min_throughput=not-a-number",
			expectedErrMsgContains: "invalid min_throughput value",
		},
		{
			name:                   "invalid grace_timeout",
			config:                 "http::addr=localhost:9000;grace_timeout=not-a-number",
			expectedErrMsgContains: "invalid grace_timeout value",
		},
		{
			name:                   "invalid retry_timeout",
			config:                 "http::addr=localhost:9000;retry_timeout=not-a-number",
			expectedErrMsgContains: "invalid retry_timeout value",
		},
		{
			name:                   "invalid init_buf_size",
			config:                 "http::addr=localhost:9000;init_buf_size=not-a-number",
			expectedErrMsgContains: "invalid init_buf_size value",
		},
		{
			name:                   "invalid max_buf_size",
			config:                 "http::addr=localhost:9000;max_buf_size=not-a-number",
			expectedErrMsgContains: "invalid max_buf_size value",
		},
		{
			name:                   "unescaped semicolon in password leads to unexpected end of string",
			config:                 "http::addr=localhost:9000;user=test;pass=pass;word",
			expectedErrMsgContains: "unexpected end of string",
		},
		{
			name:                   "unescaped semicolon in password leads to invalid key character",
			config:                 "http::addr=localhost:9000;user=test;pass=pass;word;",
			expectedErrMsgContains: "invalid key character ';'",
		},
		{
			name:                   "unsupported option",
			config:                 "http::addr=localhost:9000;unsupported_option=unsupported_value",
			expectedErrMsgContains: "unsupported option",
		},
		{
			name:                   "invalid tls_verify value",
			config:                 "http::addr=localhost:9000;tls_verify=invalid",
			expectedErrMsgContains: "invalid tls_verify value",
		},
		{
			name:                   "invalid tls_roots value",
			config:                 "http::addr=localhost:9000;tls_roots=invalid",
			expectedErrMsgContains: "tls_roots is not available",
		},
		{
			name:                   "invalid tls_roots_passwore value",
			config:                 "http::addr=localhost:9000;tls_roots_password=invalid",
			expectedErrMsgContains: "tls_roots_password is not available",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			_, err := parseConfigString(tc.config)
			var expected *ConfigStrParseError
			assert.Error(t, err)
			assert.ErrorAs(t, err, &expected)
			assert.Contains(t, err.Error(), tc.expectedErrMsgContains)

		})

	}
}
