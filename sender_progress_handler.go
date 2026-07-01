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

import "strconv"

// SenderProgressHandler is invoked, on a dedicated dispatcher goroutine, each
// time the QWP sender's acknowledged frame sequence number advances. ackedFsn is
// strictly monotonic across calls.
//
// # Settled vs durable
//
// By default ackedFsn is a SETTLED watermark (the server committed the batch to
// its WAL), and a DROP_AND_CONTINUE rejection advances it past the dropped span
// exactly like a commit — so on its own it does not prove the data survived, and
// must be cross-referenced with a SenderErrorHandler to see drops. Under
// request_durable_ack=on it is a DURABLE watermark: it advances only after the
// data is uploaded to object storage.
//
// Registered via WithProgressHandler. Unlike the error / connection listeners it
// has no loud default — progress is high-frequency, so it is opt-in and does
// nothing when unset.
type SenderProgressHandler func(ackedFsn int64)

// newQwpProgressDispatcher builds the off-loop dispatcher that delivers ackedFsn
// advances to handler. Returns nil when handler is nil (no dispatch, no
// goroutine).
func newQwpProgressDispatcher(handler SenderProgressHandler, capacity int) *qwpDispatcher[int64] {
	if handler == nil {
		return nil
	}
	return newQwpDispatcher(
		func(fsn int64) { handler(fsn) },
		func(fsn int64) string { return strconv.FormatInt(fsn, 10) },
		nil,
		"qwp/progress",
		capacity,
	)
}
