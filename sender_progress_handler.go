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

// SenderProgressHandler is invoked, on a dedicated dispatcher goroutine, as the
// QWP sender's acknowledged frame sequence number advances. Delivery is strictly
// monotonic but may coalesce: under a delivery backlog the bounded dispatcher
// drops older values, so the reported sequence never goes backwards but may
// skip values. Newer queued values replace older ones; shutdown may drop even
// the latest notification. Read [QwpSender.AckedFsn] to check acknowledgement
// progress. Callback delivery is not a reliable way to wait for an
// acknowledgement or for shutdown to finish.
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
// The handler does not run on the goroutines building rows or doing network
// I/O. If it panics, the client catches and logs the panic; the sender and
// callback delivery continue. A handler may run while the application is using
// the sender. From the handler, do not call Close, Flush, methods that add rows
// or column values, any other method that changes the sender, or QuestDB.Close.
// Instead, send a value through a channel or cancel a context. The code using
// the sender can then stop its current work and call Flush or Close. Starting
// either method in another goroutine does not make concurrent use safe. The
// handler may call a method only if that method's documentation says it returns
// data without changing the sender.
//
// Shutdown stops accepting notifications and may drop queued values. A handler
// already running may finish after Close returns, even after resources are
// released. See [SenderErrorHandler] and README's "QWP shutdown and ownership".
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
