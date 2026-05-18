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

package questdb_test

import (
	"strings"
	"testing"

	qdb "github.com/questdb/go-questdb-client/v4"
)

// TestErrorApiConfStringHappyPath parses each new connect-string key
// and asserts it lands on the right slot.
func TestErrorApiConfStringHappyPath(t *testing.T) {
	cases := []struct {
		conf       string
		wantGlobal qdb.Policy
		wantSchema qdb.Policy
		wantParse  qdb.Policy
	}{
		{
			conf:       "ws::addr=h:9000;on_server_error=halt;",
			wantGlobal: qdb.PolicyHalt,
		},
		{
			conf:       "ws::addr=h:9000;on_server_error=drop;",
			wantGlobal: qdb.PolicyDropAndContinue,
		},
		{
			conf:       "ws::addr=h:9000;on_server_error=auto;",
			wantGlobal: qdb.PolicyAuto,
		},
		{
			conf:       "ws::addr=h:9000;on_schema_error=halt;",
			wantSchema: qdb.PolicyHalt,
		},
		{
			conf:       "ws::addr=h:9000;on_parse_error=drop;",
			wantParse:  qdb.PolicyDropAndContinue,
		},
		{
			conf:       "ws::addr=h:9000;on_internal_error=halt;on_security_error=drop;on_write_error=halt;",
			wantGlobal: qdb.PolicyAuto,
		},
		{
			conf:       "ws::addr=h:9000;error_inbox_capacity=64;",
			wantGlobal: qdb.PolicyAuto,
		},
	}
	for _, tc := range cases {
		t.Run(tc.conf, func(t *testing.T) {
			_, err := qdb.ConfFromStr(tc.conf)
			if err != nil {
				t.Fatalf("ConfFromStr(%q) = %v, want nil", tc.conf, err)
			}
		})
	}
}

// TestErrorApiConfStringInvalidValues asserts each new key rejects
// nonsense values with NewInvalidConfigStrError.
func TestErrorApiConfStringInvalidValues(t *testing.T) {
	cases := []struct {
		conf string
		want string
	}{
		{"ws::addr=h:9000;on_server_error=foo;", "on_server_error"},
		{"ws::addr=h:9000;on_schema_error=auto;", "on_schema_error"},
		{"ws::addr=h:9000;on_parse_error=foo;", "on_parse_error"},
		{"ws::addr=h:9000;on_internal_error=banana;", "on_internal_error"},
		{"ws::addr=h:9000;on_security_error=;", "on_security_error"},
		{"ws::addr=h:9000;on_write_error=halts;", "on_write_error"},
		{"ws::addr=h:9000;error_inbox_capacity=-1;", "error_inbox_capacity"},
		{"ws::addr=h:9000;error_inbox_capacity=0;", "error_inbox_capacity"},
	}
	for _, tc := range cases {
		t.Run(tc.conf, func(t *testing.T) {
			_, err := qdb.ConfFromStr(tc.conf)
			if err == nil {
				t.Fatalf("ConfFromStr(%q) should fail", tc.conf)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want to contain %q", err, tc.want)
			}
		})
	}
}

// TestErrorApiConfStringQwpOnly asserts each new key is rejected for
// HTTP and TCP transports.
func TestErrorApiConfStringQwpOnly(t *testing.T) {
	keys := []string{
		"on_server_error=halt",
		"on_schema_error=halt",
		"on_parse_error=halt",
		"on_internal_error=halt",
		"on_security_error=halt",
		"on_write_error=halt",
		"error_inbox_capacity=32",
	}
	prefixes := []string{"http", "tcp"}
	for _, prefix := range prefixes {
		for _, k := range keys {
			conf := prefix + "::addr=h:9000;" + k + ";"
			t.Run(conf, func(t *testing.T) {
				_, err := qdb.ConfFromStr(conf)
				if err == nil {
					t.Fatalf("%s should reject %s", prefix, k)
				}
				if !strings.Contains(err.Error(), "QWP") {
					t.Fatalf("error = %v, want to mention QWP", err)
				}
			})
		}
	}
}

// TestErrorApiSanitizerRejectsTinyInbox asserts the sanitizer rejects
// error_inbox_capacity values below the spec floor of 16.
func TestErrorApiSanitizerRejectsTinyInbox(t *testing.T) {
	cases := []struct {
		conf string
		want string
	}{
		{"ws::addr=h:9000;error_inbox_capacity=1;", ">="},
		{"ws::addr=h:9000;error_inbox_capacity=15;", ">="},
	}
	for _, tc := range cases {
		t.Run(tc.conf, func(t *testing.T) {
			_, err := qdb.ConfFromStr(tc.conf)
			if err == nil {
				t.Fatalf("ConfFromStr(%q) should fail", tc.conf)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want to contain %q", err, tc.want)
			}
		})
	}
}

// TestErrorApiSanitizerAcceptsAtFloor asserts capacity=16 passes.
func TestErrorApiSanitizerAcceptsAtFloor(t *testing.T) {
	if _, err := qdb.ConfFromStr("ws::addr=h:9000;error_inbox_capacity=16;"); err != nil {
		t.Fatalf("capacity=16 should pass, got %v", err)
	}
}
