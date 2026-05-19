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

// Shared seeded-RNG helper for the QWP fuzz tests. Kept in its own
// build-tag-free file so both the server-bound fuzz tests (which carry
// //go:build !windows for graceful server teardown) and the pure,
// server-free decoder fuzz (which must run on every platform under the
// normal `go test ./...`) can use it.

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// newFuzzRand builds a reproducible RNG. QWP_FUZZ_SEED pins the seed for
// replaying a failure; otherwise it is clock-derived and logged so a
// failing run is always reproducible.
func newFuzzRand(t *testing.T) *rand.Rand {
	t.Helper()
	var seed int64
	if s := os.Getenv("QWP_FUZZ_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("QWP_FUZZ_SEED=%q: %v", s, err)
		}
		seed = v
	} else {
		seed = time.Now().UnixNano()
	}
	t.Logf("QWP_FUZZ_SEED=%d (set this env var to reproduce)", seed)
	return rand.New(rand.NewSource(seed))
}
