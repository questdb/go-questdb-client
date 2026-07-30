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

import "log/slog"

// The client's diagnostics go through an injectable *slog.Logger rather than
// the global log package, so an embedding application controls the sink,
// format, and verbosity of everything the QWP transport emits. Register one
// with WithLogger (standalone sender) or WithQuestDBLogger (facade). When
// none is registered the client falls back to slog.Default(), so a
// misconfigured or rejecting server is never silent — the native-client
// "loud defaults" contract — while low-value chatter (replayed rejections,
// transient failover windows, watermark clamps) is emitted at slog.LevelDebug
// and stays hidden until the operator lowers the level. Inject a Discard
// handler to silence everything, or a custom handler to route it into the
// application's logging stack.

// qwpEffectiveLogger resolves the configured logger, substituting
// slog.Default() when the caller registered none. The result is always
// non-nil, so every call site can log unconditionally.
func qwpEffectiveLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}
