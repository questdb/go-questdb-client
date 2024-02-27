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
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
)

// buffer is a wrapper on top of bytes.buffer. It extends the
// original struct with methods for writing int64 and float64
// numbers without unnecessary allocations.
type buffer struct {
	bytes.Buffer

	bufCap           int
	initBufSizeBytes int
	fileNameLimit    int
	lastMsgPos       int
	lastErr          error
	hasTable         bool
	hasTags          bool
	hasFields        bool
}

func (b *buffer) WriteInt(i int64) {
	// We need up to 20 bytes to fit an int64, including a sign.
	var a [20]byte
	s := strconv.AppendInt(a[0:0], i, 10)
	b.Write(s)
}

func (b *buffer) WriteFloat(f float64) {
	if math.IsNaN(f) {
		b.WriteString("NaN")
		return
	} else if math.IsInf(f, -1) {
		b.WriteString("-Infinity")
		return
	} else if math.IsInf(f, 1) {
		b.WriteString("Infinity")
		return
	}
	// We need up to 24 bytes to fit a float64, including a sign.
	var a [24]byte
	s := strconv.AppendFloat(a[0:0], f, 'G', -1, 64)
	b.Write(s)
}

func (b *buffer) WriteBigInt(i *big.Int) {
	// We need up to 64 bytes to fit an unsigned 256-bit number.
	var a [64]byte
	s := i.Append(a[0:0], 16)
	b.Write(s)
}

func (buf *buffer) writeTableName(str string) error {
	if str == "" {
		return fmt.Errorf("table name cannot be empty: %w", ErrInvalidMsg)
	}
	// We use string length in bytes as an approximation. That's to
	// avoid calculating the number of runes.
	if len(str) > buf.fileNameLimit {
		return fmt.Errorf("table name length exceeds the limit: %w", ErrInvalidMsg)
	}
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		b := str[i]
		switch b {
		case ' ':
			buf.WriteByte('\\')
		case '=':
			buf.WriteByte('\\')
		case '.':
			if i == 0 || i == len(str)-1 {
				return fmt.Errorf("table name contains '.' char at the start or end: %s: %w", str, ErrInvalidMsg)
			}
		default:
			if illegalTableNameChar(b) {
				return fmt.Errorf("table name contains an illegal char: "+
					"'\\n', '\\r', '?', ',', ''', '\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: %s: %w",
					str, ErrInvalidMsg)
			}
		}
		buf.WriteByte(b)
	}
	return nil
}

func illegalTableNameChar(ch byte) bool {
	switch ch {
	case '\n':
		return true
	case '\r':
		return true
	case '?':
		return true
	case ',':
		return true
	case '\'':
		return true
	case '"':
		return true
	case '\\':
		return true
	case '/':
		return true
	case ':':
		return true
	case ')':
		return true
	case '(':
		return true
	case '+':
		return true
	case '*':
		return true
	case '%':
		return true
	case '~':
		return true
	case '\u0000':
		return true
	case '\u0001':
		return true
	case '\u0002':
		return true
	case '\u0003':
		return true
	case '\u0004':
		return true
	case '\u0005':
		return true
	case '\u0006':
		return true
	case '\u0007':
		return true
	case '\u0008':
		return true
	case '\u0009':
		return true
	case '\u000b':
		return true
	case '\u000c':
		return true
	case '\u000e':
		return true
	case '\u000f':
		return true
	case '\u007f':
		return true
	}
	return false
}

func (buf *buffer) writeColumnName(str string) error {
	if str == "" {
		return fmt.Errorf("column name cannot be empty: %w", ErrInvalidMsg)
	}
	// We use string length in bytes as an approximation. That's to
	// avoid calculating the number of runes.
	if len(str) > buf.fileNameLimit {
		return fmt.Errorf("column name length exceeds the limit: %w", ErrInvalidMsg)
	}
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		b := str[i]
		switch b {
		case ' ':
			buf.WriteByte('\\')
		case '=':
			buf.WriteByte('\\')
		default:
			if illegalColumnNameChar(b) {
				return fmt.Errorf("column name contains an illegal char: "+
					"'\\n', '\\r', '?', '.', ',', ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: %s: %w",
					str, ErrInvalidMsg)
			}
		}
		buf.WriteByte(b)
	}
	return nil
}

func illegalColumnNameChar(ch byte) bool {
	switch ch {
	case '\n':
		return true
	case '\r':
		return true
	case '?':
		return true
	case '.':
		return true
	case ',':
		return true
	case '\'':
		return true
	case '"':
		return true
	case '\\':
		return true
	case '/':
		return true
	case ':':
		return true
	case ')':
		return true
	case '(':
		return true
	case '+':
		return true
	case '-':
		return true
	case '*':
		return true
	case '%':
		return true
	case '~':
		return true
	case '\u0000':
		return true
	case '\u0001':
		return true
	case '\u0002':
		return true
	case '\u0003':
		return true
	case '\u0004':
		return true
	case '\u0005':
		return true
	case '\u0006':
		return true
	case '\u0007':
		return true
	case '\u0008':
		return true
	case '\u0009':
		return true
	case '\u000b':
		return true
	case '\u000c':
		return true
	case '\u000e':
		return true
	case '\u000f':
		return true
	case '\u007f':
		return true
	}
	return false
}

func (buf *buffer) writeStrValue(str string, quoted bool) error {
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		b := str[i]
		switch b {
		case ' ':
			if !quoted {
				buf.WriteByte('\\')
			}
		case ',':
			if !quoted {
				buf.WriteByte('\\')
			}
		case '=':
			if !quoted {
				buf.WriteByte('\\')
			}
		case '"':
			if quoted {
				buf.WriteByte('\\')
			}
		case '\n':
			buf.WriteByte('\\')
		case '\r':
			buf.WriteByte('\\')
		case '\\':
			buf.WriteByte('\\')
		}
		buf.WriteByte(b)
	}
	return nil
}

func (b *buffer) prepareForField() bool {
	if b.lastErr != nil {
		return false
	}
	if !b.hasTable {
		b.lastErr = fmt.Errorf("table name was not provided: %w", ErrInvalidMsg)
		return false
	}
	if !b.hasFields {
		b.WriteByte(' ')
	} else {
		b.WriteByte(',')
	}
	return true
}

func (b *buffer) discardPendingMsg() {
	b.Truncate(b.lastMsgPos)
	b.resetMsgFlags()
}

func (b *buffer) resetMsgFlags() {
	b.hasTable = false
	b.hasTags = false
	b.hasFields = false
}

// Messages returns a copy of accumulated ILP messages that are not
// flushed to the TCP connection yet. Useful for debugging purposes.
func (b *buffer) Messages() string {
	return b.String()
}

// Table sets the table name (metric) for a new ILP message. Should be
// called before any Symbol or Column method.
//
// Table name cannot contain any of the following characters:
// '\n', '\r', '?', ',', ”', '"', '\', '/', ':', ')', '(', '+', '*',
// '%', '~', starting '.', trailing '.', or a non-printable char.
func (b *buffer) table(name string) *buffer {
	if b.lastErr != nil {
		return b
	}
	if b.hasTable {
		b.lastErr = fmt.Errorf("table name already provided: %w", ErrInvalidMsg)
		return b
	}
	b.lastErr = b.writeTableName(name)
	if b.lastErr != nil {
		return b
	}
	b.hasTable = true
	return b
}

// Symbol adds a symbol column value to the ILP message. Should be called
// before any Column method.
//
// Symbol name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (b *buffer) symbol(name, val string) *buffer {
	if b.lastErr != nil {
		return b
	}
	if !b.hasTable {
		b.lastErr = fmt.Errorf("table name was not provided: %w", ErrInvalidMsg)
		return b
	}
	if b.hasFields {
		b.lastErr = fmt.Errorf("symbols have to be written before any other column: %w", ErrInvalidMsg)
		return b
	}
	b.WriteByte(',')
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.lastErr = b.writeStrValue(val, false)
	if b.lastErr != nil {
		return b
	}
	b.hasTags = true
	return b
}

// Int64Column adds a 64-bit integer (long) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (b *buffer) int64Column(name string, val int64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.WriteInt(val)
	b.WriteByte('i')
	b.hasFields = true
	return b
}

// Long256Column adds a 256-bit unsigned integer (long256) column
// value to the ILP message.
//
// Only non-negative numbers that fit into 256-bit unsigned integer are
// supported and any other input value would lead to an error.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (b *buffer) long256Column(name string, val *big.Int) *buffer {
	if val.Sign() < 0 {
		if b.lastErr != nil {
			return b
		}
		b.lastErr = fmt.Errorf("long256 cannot be negative: %s", val.String())
		return b
	}
	if val.BitLen() > 256 {
		if b.lastErr != nil {
			return b
		}
		b.lastErr = fmt.Errorf("long256 cannot be larger than 256-bit: %v", val.BitLen())
		return b
	}
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.WriteByte('0')
	b.WriteByte('x')
	b.WriteBigInt(val)
	b.WriteByte('i')
	if b.lastErr != nil {
		return b
	}
	b.hasFields = true
	return b
}

// TimestampColumn adds a timestamp column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (b *buffer) timestampColumn(name string, ts time.Time) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.WriteInt(ts.UnixMicro())
	b.WriteByte('t')
	b.hasFields = true
	return b
}

// Float64Column adds a 64-bit float (double) column value to the ILP
// message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (b *buffer) float64Column(name string, val float64) {
	if !b.prepareForField() {
		return
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return
	}
	b.WriteByte('=')
	b.WriteFloat(val)
	b.hasFields = true
}

// StringColumn adds a string column value to the ILP message.
//
// Column name cannot contain any of the following characters:
// '\n', '\r', '?', '.', ',', ”', '"', '\', '/', ':', ')', '(', '+',
// '-', '*' '%%', '~', or a non-printable char.
func (b *buffer) stringColumn(name, val string) {
	if !b.prepareForField() {
		return
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return
	}
	b.WriteByte('=')
	b.WriteByte('"')
	b.lastErr = b.writeStrValue(val, true)
	if b.lastErr != nil {
		return
	}
	b.WriteByte('"')
	b.hasFields = true
}

func (b *buffer) boolColumn(name string, val bool) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	if val {
		b.WriteByte('t')
	} else {
		b.WriteByte('f')
	}
	b.hasFields = true
	return b
}

func (b *buffer) at(ts time.Time, sendTs bool) error {
	err := b.lastErr
	b.lastErr = nil
	if err != nil {
		b.discardPendingMsg()
		return err
	}
	if !b.hasTable {
		b.discardPendingMsg()
		return fmt.Errorf("table name was not provided: %w", ErrInvalidMsg)
	}
	if !b.hasTags && !b.hasFields {
		b.discardPendingMsg()
		return fmt.Errorf("no symbols or columns were provided: %w", ErrInvalidMsg)
	}

	if sendTs {
		b.WriteByte(' ')
		b.WriteInt(ts.UnixNano())
	}
	b.WriteByte('\n')

	b.lastMsgPos = b.Len()
	b.resetMsgFlags()
	return nil
}
