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
// By default ackedFsn is a SETTLED watermark: it advances only on server OK
// ACKs (the server committed the batch to its WAL) — a rejection never
// advances it; a rejected batch is either replayed or halts the sender with
// the bytes preserved. Under request_durable_ack=on it is a DURABLE
// watermark: it advances only after the data is uploaded to object storage.
//
// Registered via WithProgressHandler. Unlike the error / connection listeners it
// has no loud default — progress is high-frequency, so it is opt-in and does
// nothing when unset.
//
// # Calling back into the sender
//
// The handler may call Close() or Flush() on the sender without deadlocking.
// Because it runs on the dispatcher goroutine, not the producer goroutine,
// those calls deliberately do NOT touch in-progress producer state: they
// surface only a latched terminal error and will not flush rows the producer
// has staged but not yet flushed itself. Same contract as SenderErrorHandler.
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
