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

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestQwpErrorInterface(t *testing.T) {
	e := &QwpError{
		Status:   qwpStatusParseError,
		Sequence: 42,
		Message:  "bad column type",
	}

	// Verify it implements error interface.
	var err error = e
	s := err.Error()
	if !strings.Contains(s, "PARSE_ERROR") {
		t.Fatalf("error string should contain PARSE_ERROR, got: %s", s)
	}
	if !strings.Contains(s, "bad column type") {
		t.Fatalf("error string should contain message, got: %s", s)
	}
	if !strings.Contains(s, "0x05") {
		t.Fatalf("error string should contain hex status, got: %s", s)
	}
}

func TestQwpErrorNoMessage(t *testing.T) {
	e := &QwpError{
		Status:   qwpStatusWriteError,
		Sequence: 1,
	}
	s := e.Error()
	if !strings.Contains(s, "WRITE_ERROR") {
		t.Fatalf("error string should contain WRITE_ERROR, got: %s", s)
	}
}

func TestQwpErrorIsRetriable(t *testing.T) {
	tests := []struct {
		status qwpStatusCode
		want   bool
	}{
		{qwpStatusOK, false},
		{qwpStatusPartial, false},
		{qwpStatusSchemaMismatch, false},
		{qwpStatusTableNotFound, false},
		{qwpStatusParseError, false},
		{qwpStatusInternalError, false},
		{qwpStatusOverloaded, true},
		{qwpStatusSecurityError, false},
		{qwpStatusWriteError, false},
	}
	for _, tc := range tests {
		e := &QwpError{Status: tc.status}
		if e.IsRetriable() != tc.want {
			t.Fatalf("IsRetriable for status 0x%02X: got %v, want %v",
				byte(tc.status), e.IsRetriable(), tc.want)
		}
	}
}

func TestQwpStatusName(t *testing.T) {
	tests := []struct {
		status qwpStatusCode
		want   string
	}{
		{qwpStatusOK, "OK"},
		{qwpStatusPartial, "PARTIAL"},
		{qwpStatusSchemaMismatch, "SCHEMA_MISMATCH"},
		{qwpStatusTableNotFound, "TABLE_NOT_FOUND"},
		{qwpStatusParseError, "PARSE_ERROR"},
		{qwpStatusInternalError, "INTERNAL_ERROR"},
		{qwpStatusOverloaded, "OVERLOADED"},
		{qwpStatusSecurityError, "SECURITY_ERROR"},
		{qwpStatusWriteError, "WRITE_ERROR"},
		{qwpStatusCode(42), "UNKNOWN(42)"},
	}
	for _, tc := range tests {
		got := qwpStatusName(tc.status)
		if got != tc.want {
			t.Fatalf("qwpStatusName(0x%02X) = %q, want %q",
				byte(tc.status), got, tc.want)
		}
	}
}

func TestNewQwpErrorFromAck(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		data := make([]byte, 9)
		data[0] = byte(qwpStatusOK)
		err := newQwpErrorFromAck(data)
		if err != nil {
			t.Fatalf("expected nil for OK status, got: %v", err)
		}
	})

	t.Run("ParseError", func(t *testing.T) {
		errMsg := "invalid column"
		data := make([]byte, 11+len(errMsg))
		data[0] = byte(qwpStatusParseError)
		binary.LittleEndian.PutUint64(data[1:9], 7)
		binary.LittleEndian.PutUint16(data[9:11], uint16(len(errMsg)))
		copy(data[11:], errMsg)

		e := newQwpErrorFromAck(data)
		if e == nil {
			t.Fatal("expected error, got nil")
		}
		if e.Status != qwpStatusParseError {
			t.Fatalf("status = %d, want %d", e.Status, qwpStatusParseError)
		}
		if e.Sequence != 7 {
			t.Fatalf("sequence = %d, want 7", e.Sequence)
		}
		if e.Message != errMsg {
			t.Fatalf("message = %q, want %q", e.Message, errMsg)
		}
	})

	t.Run("WriteErrorNoMessage", func(t *testing.T) {
		data := make([]byte, 9)
		data[0] = byte(qwpStatusWriteError)
		binary.LittleEndian.PutUint64(data[1:9], 99)

		e := newQwpErrorFromAck(data)
		if e == nil {
			t.Fatal("expected error, got nil")
		}
		if e.Status != qwpStatusWriteError {
			t.Fatalf("status = %d, want %d", e.Status, qwpStatusWriteError)
		}
		if e.Sequence != 99 {
			t.Fatalf("sequence = %d, want 99", e.Sequence)
		}
		if e.Message != "" {
			t.Fatalf("message = %q, want empty", e.Message)
		}
	})

	t.Run("Overloaded", func(t *testing.T) {
		data := make([]byte, 9)
		data[0] = byte(qwpStatusOverloaded)

		e := newQwpErrorFromAck(data)
		if e == nil {
			t.Fatal("expected error, got nil")
		}
		if !e.IsRetriable() {
			t.Fatal("overloaded should be retriable")
		}
	})

	t.Run("EmptyPayload", func(t *testing.T) {
		e := newQwpErrorFromAck([]byte{})
		if e == nil {
			t.Fatal("expected error for empty payload")
		}
		if e.Message == "" {
			t.Fatal("expected error message for empty payload")
		}
	})

	t.Run("InternalErrorNotRetriable", func(t *testing.T) {
		data := make([]byte, 9)
		data[0] = byte(qwpStatusInternalError)

		e := newQwpErrorFromAck(data)
		if e == nil {
			t.Fatal("expected error, got nil")
		}
		if e.IsRetriable() {
			t.Fatal("internal error should not be retriable per spec")
		}
	})
}
