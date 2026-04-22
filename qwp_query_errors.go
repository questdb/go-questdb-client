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

import "fmt"

// QwpQueryError is a server-side error reported during query egress. It
// corresponds to a QUERY_ERROR frame (msg_kind 0x13) and is distinct from
// QwpError, which carries ingress ACK status. CANCELLED and LIMIT_EXCEEDED
// are egress-specific statuses that surface here.
type QwpQueryError struct {
	// RequestId correlates the error with the query that produced it.
	RequestId int64

	// Status is the server-reported egress status byte (e.g.
	// qwpStatusCancelled, qwpStatusLimitExceeded, qwpStatusParseError).
	Status qwpStatusCode

	// Message is the server-supplied UTF-8 description, or empty if the
	// server sent a zero-length message.
	Message string
}

// Error implements the error interface.
func (e *QwpQueryError) Error() string {
	name := qwpStatusName(e.Status)
	if e.Message != "" {
		return fmt.Sprintf("qwp: query error %s (0x%02X): %s",
			name, byte(e.Status), e.Message)
	}
	return fmt.Sprintf("qwp: query error %s (0x%02X)", name, byte(e.Status))
}
