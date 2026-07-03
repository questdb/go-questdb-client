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
			conf:       "ws::addr=h:9000;on_server_error=terminal;",
			wantGlobal: qdb.PolicyTerminal,
		},
		{
			conf:       "ws::addr=h:9000;on_server_error=retriable;",
			wantGlobal: qdb.PolicyRetriable,
		},
		{
			conf:       "ws::addr=h:9000;on_server_error=auto;",
			wantGlobal: qdb.PolicyAuto,
		},
		{
			conf:       "ws::addr=h:9000;on_schema_error=terminal;",
			wantSchema: qdb.PolicyTerminal,
		},
		{
			conf:      "ws::addr=h:9000;on_parse_error=retriable;",
			wantParse: qdb.PolicyRetriable,
		},
		{
			conf:       "ws::addr=h:9000;on_internal_error=terminal;on_security_error=retriable;on_write_error=terminal;",
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
		{"ws::addr=h:9000;on_write_error=terminals;", "on_write_error"},
		{"ws::addr=h:9000;error_inbox_capacity=-1;", "error_inbox_capacity"},
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
		"on_server_error=terminal",
		"on_schema_error=terminal",
		"on_parse_error=terminal",
		"on_internal_error=terminal",
		"on_security_error=terminal",
		"on_write_error=terminal",
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

// TestErrorApiSanitizerRejectsTinyInbox asserts the parser rejects
// inbox capacities outside [16, 1<<20]: sub-floor values are undersized,
// and over-ceiling values would panic make(chan) at construction.
func TestErrorApiSanitizerRejectsTinyInbox(t *testing.T) {
	cases := []struct {
		conf string
		want string
	}{
		{"ws::addr=h:9000;error_inbox_capacity=1;", "must be in"},
		{"ws::addr=h:9000;error_inbox_capacity=15;", "must be in"},
		{"ws::addr=h:9000;error_inbox_capacity=1099511627776;", "must be in"},
		{"ws::addr=h:9000;connection_listener_inbox_capacity=15;", "must be in"},
		{"ws::addr=h:9000;connection_listener_inbox_capacity=1099511627776;", "must be in"},
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

// TestErrorApiSanitizerAcceptsAtCeiling asserts the ceiling value (1<<20)
// passes for both inbox keys — the rejection is strictly above it.
func TestErrorApiSanitizerAcceptsAtCeiling(t *testing.T) {
	if _, err := qdb.ConfFromStr("ws::addr=h:9000;error_inbox_capacity=1048576;"); err != nil {
		t.Fatalf("error_inbox_capacity=1048576 should pass, got %v", err)
	}
	if _, err := qdb.ConfFromStr("ws::addr=h:9000;connection_listener_inbox_capacity=1048576;"); err != nil {
		t.Fatalf("connection_listener_inbox_capacity=1048576 should pass, got %v", err)
	}
}

// TestErrorApiConfStringZeroMeansDefault asserts error_inbox_capacity=0
// is accepted as the "use the default capacity" sentinel, matching the
// WithErrorInboxCapacity option path. The default is resolved at
// construction (qwpSfDefaultErrorInboxCapacity), so 0 bypasses the
// >= 16 floor instead of being rejected as undersized.
func TestErrorApiConfStringZeroMeansDefault(t *testing.T) {
	if _, err := qdb.ConfFromStr("ws::addr=h:9000;error_inbox_capacity=0;"); err != nil {
		t.Fatalf("error_inbox_capacity=0 should be accepted (use-default), got %v", err)
	}
}

// TestErrorApiWithErrorPolicyAutoClearsPerCatSet asserts that a
// non-Auto override followed by PolicyAuto on the same category
// nets out to "no per-category override set", so the HTTP/TCP
// sanitizers do not falsely reject the build as a QWP-only API use.
func TestErrorApiWithErrorPolicyAutoClearsPerCatSet(t *testing.T) {
	cases := []struct {
		name string
		st   qdb.SenderType
	}{
		{"http", qdb.HttpSenderType},
		{"tcp", qdb.TcpSenderType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := qdb.NewLineSenderConfig(tc.st)
			qdb.WithAddress("h:9000")(conf)
			qdb.WithErrorPolicy(qdb.CategorySchemaMismatch, qdb.PolicyTerminal)(conf)
			qdb.WithErrorPolicy(qdb.CategorySchemaMismatch, qdb.PolicyAuto)(conf)
			if err := qdb.SanitizeConf(conf); err != nil {
				t.Fatalf("sanitizer should not reject net-Auto per-cat override, got %v", err)
			}
		})
	}
}
