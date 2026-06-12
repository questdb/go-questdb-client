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

// SenderErrorHandler is the user-supplied callback invoked when the
// asynchronous SF send loop observes a server-side batch rejection.
// Registered via WithErrorHandler(...) on the LineSender builder.
//
// # Threading
//
// Implementations are invoked on a dedicated dispatcher goroutine,
// never on the I/O goroutine or the producer goroutine. Slow handlers
// cannot stall publishing; if the bounded inbox fills up, surplus
// notifications are dropped (visible via
// QwpSender.DroppedErrorNotifications()).
//
// # Panics
//
// Any panic from the handler is recovered and logged by the
// dispatcher. The dispatcher and the sender continue running.
//
// # Calling back into the sender
//
// The handler may call Close() or Flush() on the sender — e.g. to shut
// down on a HALT-category error. The terminal *SenderError is latched
// before the handler is invoked, so a synchronous Flush() returns it
// promptly rather than blocking. Close() called from the handler is
// honored and returns without deadlocking; the dispatcher goroutine
// (this goroutine) finishes unwinding on its own once the handler
// returns, so any error notifications still queued at that moment are
// subject to the dispatcher's short best-effort drain and may be
// dropped (visible via QwpSender.DroppedErrorNotifications()).
//
// Because the handler runs on the dispatcher goroutine — not the
// producer goroutine — these calls deliberately do NOT touch producer-
// buffered state: a handler-invoked Close() or Flush() will not flush
// rows the producer has staged but not yet flushed itself (those are
// owned by the producer goroutine and may be mid-assembly). Close()
// still tears down the wire, drains already-published frames up to
// close_flush_timeout, and releases resources; Flush() still surfaces
// the latched error. To guarantee a specific batch is flushed, flush it
// from the producer goroutine before relying on the handler to close.
//
// # What this callback is for
//
// Dead-lettering rejected data, alerting, metrics. Producer-thread
// retry/abort logic should not live here — that belongs on the
// producer side, where errors.As(err, &senderErr) unpacks the typed
// error after a HALT-policy latch.
type SenderErrorHandler func(*SenderError)
