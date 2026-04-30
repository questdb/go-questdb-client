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
// # What this callback is for
//
// Dead-lettering rejected data, alerting, metrics. Producer-thread
// retry/abort logic should not live here — that belongs on the
// producer side, where errors.As(err, &senderErr) unpacks the typed
// error after a HALT-policy latch.
type SenderErrorHandler func(*SenderError)
