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

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSenderErrorImplementsError(t *testing.T) {
	se := &SenderError{
		Category:         CategoryParseError,
		AppliedPolicy:    PolicyHalt,
		ServerStatusByte: int(QwpStatusParseError),
		ServerMessage:    "bad column type",
		MessageSequence:  42,
		FromFsn:          100,
		ToFsn:            100,
		DetectedAt:       time.Now(),
	}
	var err error = se
	s := err.Error()
	for _, want := range []string{"PARSE_ERROR", "bad column type", "0x05", "HALT", "fsn=[100,100]", "seq=42"} {
		if !strings.Contains(s, want) {
			t.Fatalf("error string missing %q: %s", want, s)
		}
	}
}

func TestSenderErrorNoMessage(t *testing.T) {
	se := &SenderError{
		Category:         CategoryWriteError,
		AppliedPolicy:    PolicyDropAndContinue,
		ServerStatusByte: int(QwpStatusWriteError),
		MessageSequence:  1,
		FromFsn:          5,
		ToFsn:            5,
	}
	s := se.Error()
	for _, want := range []string{"WRITE_ERROR", "DROP_AND_CONTINUE", "fsn=[5,5]"} {
		if !strings.Contains(s, want) {
			t.Fatalf("error string missing %q: %s", want, s)
		}
	}
	if strings.Contains(s, "—") {
		t.Fatalf("expected no trailing message separator: %s", s)
	}
}

func TestSenderErrorProtocolViolationNoStatus(t *testing.T) {
	se := &SenderError{
		Category:         CategoryProtocolViolation,
		AppliedPolicy:    PolicyHalt,
		ServerStatusByte: NoStatusByte,
		MessageSequence:  NoMessageSequence,
		ServerMessage:    "ws-close[1002]: bad framing",
		FromFsn:          7,
		ToFsn:            12,
	}
	s := se.Error()
	for _, want := range []string{"PROTOCOL_VIOLATION", "ws-close[1002]: bad framing", "fsn=[7,12]"} {
		if !strings.Contains(s, want) {
			t.Fatalf("error string missing %q: %s", want, s)
		}
	}
	for _, unwanted := range []string{"status=", "seq="} {
		if strings.Contains(s, unwanted) {
			t.Fatalf("error string should omit %q for ProtocolViolation: %s", unwanted, s)
		}
	}
}

func TestSenderErrorIsErrorsAsTarget(t *testing.T) {
	se := &SenderError{Category: CategoryParseError, AppliedPolicy: PolicyHalt}
	var err error = se
	var got *SenderError
	if !errors.As(err, &got) {
		t.Fatal("errors.As did not unwrap *SenderError")
	}
	if got.Category != CategoryParseError {
		t.Fatalf("unwrapped Category = %s, want PARSE_ERROR", got.Category)
	}
}

func TestSenderErrorNilSafe(t *testing.T) {
	var se *SenderError
	if got := se.Error(); got != "<nil *SenderError>" {
		t.Fatalf("nil error string = %q", got)
	}
}

func TestCategoryString(t *testing.T) {
	tests := []struct {
		c    Category
		want string
	}{
		{CategoryUnknown, "UNKNOWN"},
		{CategorySchemaMismatch, "SCHEMA_MISMATCH"},
		{CategoryParseError, "PARSE_ERROR"},
		{CategoryInternalError, "INTERNAL_ERROR"},
		{CategorySecurityError, "SECURITY_ERROR"},
		{CategoryWriteError, "WRITE_ERROR"},
		{CategoryProtocolViolation, "PROTOCOL_VIOLATION"},
		{Category(99), "Category(99)"},
	}
	for _, tc := range tests {
		if got := tc.c.String(); got != tc.want {
			t.Fatalf("Category(%d).String() = %q, want %q", tc.c, got, tc.want)
		}
	}
}

func TestPolicyString(t *testing.T) {
	tests := []struct {
		p    Policy
		want string
	}{
		{PolicyAuto, "AUTO"},
		{PolicyDropAndContinue, "DROP_AND_CONTINUE"},
		{PolicyHalt, "HALT"},
		{Policy(7), "Policy(7)"},
	}
	for _, tc := range tests {
		if got := tc.p.String(); got != tc.want {
			t.Fatalf("Policy(%d).String() = %q, want %q", tc.p, got, tc.want)
		}
	}
}

func TestQwpStatusName(t *testing.T) {
	tests := []struct {
		status QwpStatusCode
		want   string
	}{
		{QwpStatusOK, "OK"},
		{QwpStatusDurableAck, "DURABLE_ACK"},
		{QwpStatusSchemaMismatch, "SCHEMA_MISMATCH"},
		{QwpStatusParseError, "PARSE_ERROR"},
		{QwpStatusInternalError, "INTERNAL_ERROR"},
		{QwpStatusSecurityError, "SECURITY_ERROR"},
		{QwpStatusWriteError, "WRITE_ERROR"},
		{QwpStatusCode(42), "UNKNOWN(42)"},
	}
	for _, tc := range tests {
		if got := qwpStatusName(tc.status); got != tc.want {
			t.Fatalf("qwpStatusName(0x%02X) = %q, want %q",
				byte(tc.status), got, tc.want)
		}
	}
}

func TestParseAckErrorPayload(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		data := make([]byte, 11)
		data[0] = byte(QwpStatusOK)
		status, seq, msg := parseAckErrorPayload(data)
		if status != QwpStatusOK || seq != 0 || msg != "" {
			t.Fatalf("OK payload: status=%d seq=%d msg=%q", status, seq, msg)
		}
	})

	t.Run("DurableAck", func(t *testing.T) {
		data := make([]byte, 3)
		data[0] = byte(QwpStatusDurableAck)
		status, seq, msg := parseAckErrorPayload(data)
		if status != QwpStatusDurableAck || seq != 0 || msg != "" {
			t.Fatalf("DurableAck payload: status=%d seq=%d msg=%q", status, seq, msg)
		}
	})

	t.Run("ParseError", func(t *testing.T) {
		errMsg := "invalid column"
		data := make([]byte, 11+len(errMsg))
		data[0] = byte(QwpStatusParseError)
		binary.LittleEndian.PutUint64(data[1:9], 7)
		binary.LittleEndian.PutUint16(data[9:11], uint16(len(errMsg)))
		copy(data[11:], errMsg)

		status, seq, msg := parseAckErrorPayload(data)
		if status != QwpStatusParseError {
			t.Fatalf("status = %d, want PARSE_ERROR", status)
		}
		if seq != 7 {
			t.Fatalf("seq = %d, want 7", seq)
		}
		if msg != errMsg {
			t.Fatalf("msg = %q, want %q", msg, errMsg)
		}
	})

	t.Run("WriteErrorNoMessage", func(t *testing.T) {
		data := make([]byte, 11)
		data[0] = byte(QwpStatusWriteError)
		binary.LittleEndian.PutUint64(data[1:9], 99)

		status, seq, msg := parseAckErrorPayload(data)
		if status != QwpStatusWriteError {
			t.Fatalf("status = %d, want WRITE_ERROR", status)
		}
		if seq != 99 {
			t.Fatalf("seq = %d, want 99", seq)
		}
		if msg != "" {
			t.Fatalf("msg = %q, want empty", msg)
		}
	})
}
