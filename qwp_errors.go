/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

// QWP WebSocket wire status codes. These are the values actually
// sent by the QuestDB server in ACK responses over WebSocket,
// which differ from the internal QWP protocol constants.
const (
	qwpWireStatusOK            byte = 0   // Batch accepted
	qwpWireStatusParseError    byte = 1   // Malformed message
	qwpWireStatusSchemaError   byte = 2   // Schema hash not recognized
	qwpWireStatusWriteError    byte = 3   // Write failure (type mismatch, table error)
	qwpWireStatusSecurityError byte = 4   // Authentication/authorization failure
	qwpWireStatusInternalError byte = 255 // Server internal error
)

// qwpWireStatusName returns a human-readable name for a wire status code.
func qwpWireStatusName(status byte) string {
	switch status {
	case qwpWireStatusOK:
		return "OK"
	case qwpWireStatusParseError:
		return "PARSE_ERROR"
	case qwpWireStatusSchemaError:
		return "SCHEMA_ERROR"
	case qwpWireStatusWriteError:
		return "WRITE_ERROR"
	case qwpWireStatusSecurityError:
		return "SECURITY_ERROR"
	case qwpWireStatusInternalError:
		return "INTERNAL_ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", status)
	}
}

// QwpError represents an error returned by the QuestDB server in
// a QWP ACK response. It contains the wire status code, the
// sequence number from the response, and an optional error message.
type QwpError struct {
	// Status is the wire status byte from the ACK response.
	Status byte

	// Sequence is the sequence number from the ACK, used to
	// correlate responses with requests in async mode.
	Sequence int64

	// Message is the server's error description, or empty if
	// no error message was included in the response.
	Message string
}

// Error implements the error interface.
func (e *QwpError) Error() string {
	name := qwpWireStatusName(e.Status)
	if e.Message != "" {
		return fmt.Sprintf("qwp: server error %s (0x%02X): %s", name, e.Status, e.Message)
	}
	return fmt.Sprintf("qwp: server error %s (0x%02X)", name, e.Status)
}

// IsRetriable reports whether the error indicates a transient
// condition that may succeed on retry. Currently only
// INTERNAL_ERROR is retriable (the server may recover).
func (e *QwpError) IsRetriable() bool {
	return e.Status == qwpWireStatusInternalError
}

// IsSchemaError reports whether the server rejected the schema
// hash and wants the client to resend with full schema.
func (e *QwpError) IsSchemaError() bool {
	return e.Status == qwpWireStatusSchemaError
}

// newQwpErrorFromAck creates a QwpError from a raw ACK response
// payload. Returns nil if the status is OK.
func newQwpErrorFromAck(data []byte) *QwpError {
	if len(data) < 1 {
		return &QwpError{Message: "empty ACK response"}
	}

	status := data[0]
	if status == qwpWireStatusOK {
		return nil
	}

	return &QwpError{
		Status:   status,
		Sequence: parseAckSequence(data),
		Message:  parseAckError(data),
	}
}
