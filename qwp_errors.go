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

// qwpStatusName returns a human-readable name for a QWP status code.
// Used by (*SenderError).Error() to format the wire-byte component of
// rejection messages.
func qwpStatusName(status QwpStatusCode) string {
	switch status {
	case QwpStatusOK:
		return "OK"
	case QwpStatusDurableAck:
		return "DURABLE_ACK"
	case QwpStatusSchemaMismatch:
		return "SCHEMA_MISMATCH"
	case QwpStatusParseError:
		return "PARSE_ERROR"
	case QwpStatusInternalError:
		return "INTERNAL_ERROR"
	case QwpStatusSecurityError:
		return "SECURITY_ERROR"
	case QwpStatusWriteError:
		return "WRITE_ERROR"
	case qwpStatusCancelled:
		return "CANCELLED"
	case qwpStatusLimitExceeded:
		return "LIMIT_EXCEEDED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", status)
	}
}

// parseAckErrorPayload extracts the status code, cumulative sequence
// number, and server error message from a non-OK ACK frame. Used by
// the SF send loop's receiver to assemble a *SenderError with the
// surrounding FSN-span context.
//
// Precondition: data has already been validated by readAck, which
// guarantees the layout invariants documented on readAck.
func parseAckErrorPayload(data []byte) (status QwpStatusCode, seq int64, msg string) {
	status = QwpStatusCode(data[0])
	if status == QwpStatusOK || status == QwpStatusDurableAck {
		return status, 0, ""
	}
	return status, parseAckSequence(data), parseAckError(data)
}
