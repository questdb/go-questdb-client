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

package v3

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
				address:           addr,
				transportProtocol: protocolHttp,
			},
		},
		{
			name:   "tcp and address",
			config: fmt.Sprintf("tcp::addr=%s", addr),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolTcp,
			},
		},
		{
			name:   "http and username/password",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=%s", addr, user, pass),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolHttp,
				user:              user,
				pass:              pass,
			},
		},
		{
			name:   "http and token (with trailing ';')",
			config: fmt.Sprintf("http::addr=%s;token=%s;", addr, token),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolHttp,
				token:             token,
			},
		},
		{
			name:   "tcp with user and key",
			config: fmt.Sprintf("tcp::addr=%s;user=%s;token=%s", addr, user, token),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolTcp,
				keyId:             user,
				key:               token,
			},
		},
		{
			name:   "tcp with key and user",
			config: fmt.Sprintf("tcp::addr=%s;token=%s;user=%s", addr, token, user),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolTcp,
				keyId:             user,
				key:               token,
			},
		},
		{
			name:   "https with min_throughput",
			config: fmt.Sprintf("https::addr=%s;min_throughput=%d", addr, min_throughput),
			expected: LineSender{
				address:                     addr,
				transportProtocol:           protocolHttp,
				minThroughputBytesPerSecond: min_throughput,
				tlsMode:                     tlsEnabled,
			},
		},
		{
			name:   "tcps with tls_verify=unsafe_off",
			config: fmt.Sprintf("tcps::addr=%s;tls_verify=unsafe_off", addr),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolTcp,
				tlsMode:           tlsInsecureSkipVerify,
			},
		},
		{
			name: "http with min_throughput, grace_timeout, and retry_timeout",
			config: fmt.Sprintf("http::addr=%s;min_throughput=%d;grace_timeout=%d;retry_timeout=%d",
				addr, min_throughput, int(grace_timeout), int(retry_timeout)),
			expected: LineSender{
				address:                     addr,
				transportProtocol:           protocolHttp,
				minThroughputBytesPerSecond: min_throughput,
				graceTimeout:                grace_timeout,
				retryTimeout:                retry_timeout,
			},
		},
		{
			name:   "tcp with tls_verify=on",
			config: fmt.Sprintf("tcp::addr=%s;tls_verify=on", addr),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolTcp,
				tlsMode:           tlsEnabled,
			},
		},
		{
			name:   "password with an escaped semicolon",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=pass;;word", addr, user),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolHttp,
				user:              user,
				pass:              "pass;word",
			},
		},
		{
			name:   "password with an escaped semicolon (ending with a ';')",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=pass;;word;", addr, user),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolHttp,
				user:              user,
				pass:              "pass;word",
			},
		},
		{
			name:   "password with a trailing semicolon",
			config: fmt.Sprintf("http::addr=%s;user=%s;pass=password;;;", addr, user),
			expected: LineSender{
				address:           addr,
				transportProtocol: protocolHttp,
				user:              user,
				pass:              "password;",
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
			expectedErrMsgContains: "auto_flush option not available for this client",
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
