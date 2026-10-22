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

//go:build windows

package questdb

import (
	"os"
	"strings"
	"testing"
)

// The live-server fixture in qwp_fuzz_fixture_test.go controls the QuestDB
// JVM with Unix-only process signals, so it is not built on Windows. This
// stand-in lets the integration helpers compile. Every test that needs a live
// server skips here, as it does elsewhere when no server resolves, and fails
// instead when QDB_FUZZ_STRICT is set.
type qwpFuzzServer struct{}

func (*qwpFuzzServer) wsAddr() string { return "" }

// fuzzStrict applies the same QDB_FUZZ_STRICT rule as the Unix fixture.
func fuzzStrict() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("QDB_FUZZ_STRICT")))
	return v != "" && v != "0" && v != "false" && v != "no"
}

func fuzzServer(t *testing.T) *qwpFuzzServer {
	t.Helper()
	const reason = "the live QuestDB fixture is not supported on Windows"
	if fuzzStrict() {
		t.Fatal("QDB_FUZZ_STRICT is set but " + reason)
	}
	t.Skip(reason)
	return nil
}
