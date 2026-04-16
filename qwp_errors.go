/*******************************************************************************
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
func qwpStatusName(status qwpStatusCode) string {
	switch status {
	case qwpStatusOK:
		return "OK"
	case qwpStatusSchemaMismatch:
		return "SCHEMA_MISMATCH"
	case qwpStatusParseError:
		return "PARSE_ERROR"
	case qwpStatusInternalError:
		return "INTERNAL_ERROR"
	case qwpStatusSecurityError:
		return "SECURITY_ERROR"
	case qwpStatusWriteError:
		return "WRITE_ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", status)
	}
}

// QwpError represents an error returned by the QuestDB server in
// a QWP ACK response. It contains the status code, the
// sequence number from the response, and an optional error message.
type QwpError struct {
	// Status is the status code from the ACK response.
	Status qwpStatusCode

	// Sequence is the cumulative sequence number from the ACK, used
	// to correlate responses with requests in async mode.
	Sequence int64

	// Message is the server's error description, or empty if
	// no error message was included in the response.
	Message string
}

// Error implements the error interface.
func (e *QwpError) Error() string {
	name := qwpStatusName(e.Status)
	if e.Message != "" {
		return fmt.Sprintf("qwp: server error %s (0x%02X): %s", name, byte(e.Status), e.Message)
	}
	return fmt.Sprintf("qwp: server error %s (0x%02X)", name, byte(e.Status))
}

// newQwpErrorFromAck creates a QwpError from a raw ACK payload.
// Returns nil if the status is OK.
//
// Precondition: data has already been validated by readAck, which
// guarantees qwpAckOKSize bytes for OK status and at least
// qwpAckErrorHeaderSize + msg_len bytes for non-OK statuses.
func newQwpErrorFromAck(data []byte) *QwpError {
	status := qwpStatusCode(data[0])
	if status == qwpStatusOK {
		return nil
	}
	return &QwpError{
		Status:   status,
		Sequence: parseAckSequence(data),
		Message:  parseAckError(data),
	}
}
