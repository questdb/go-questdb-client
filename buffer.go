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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
	"unsafe"
)

// errInvalidMsg indicates a failed attempt to construct an ILP
// message, e.g. duplicate calls to Table method or illegal
// chars found in table or column name.
var errInvalidMsg = errors.New("invalid message")

type binaryCode byte

const (
	arrayCode   binaryCode = 14
	float64Code binaryCode = 16
)

var isLittleEndian = func() bool {
	var i int32 = 0x01020304
	return *(*byte)(unsafe.Pointer(&i)) == 0x04
}()

// MaxArrayElements defines the maximum total number of elements of Array
const MaxArrayElements = (1 << 28) - 1

// writeFloat64Data optimally writes float64 slice data to buffer
// Uses batch memory copy on little-endian machines for better performance
func (b *buffer) writeFloat64Data(data []float64) {
	if isLittleEndian && len(data) > 0 {
		b.Write(unsafe.Slice((*byte)(unsafe.Pointer(&data[0])), len(data)*8))
	} else {
		bytes := make([]byte, 8)
		for _, val := range data {
			binary.LittleEndian.PutUint64(bytes[0:], math.Float64bits(val))
			b.Write(bytes)
		}
	}
}

func (b *buffer) writeInt32(val int32) {
	if isLittleEndian {
		// On little-endian machines, we can directly write the uint32 as bytes
		b.Write((*[4]byte)(unsafe.Pointer(&val))[:])
	} else {
		// On big-endian machines, use the standard conversion
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, uint32(val))
		b.Write(data)
	}
}

type arrayElemType byte

const (
	arrayElemDouble arrayElemType = 10
	arrayElemNull                 = 33
)

// buffer is a wrapper on top of bytes.Buffer. It extends the
// original struct with methods for writing int64 and float64
// numbers without unnecessary allocations.
type buffer struct {
	bytes.Buffer

	initBufSize   int
	maxBufSize    int
	fileNameLimit int

	lastMsgPos int
	lastErr    error
	hasTable   bool
	hasTags    bool
	hasFields  bool
	msgCount   int
}

func newBuffer(initBufSize int, maxBufSize int, fileNameLimit int) buffer {
	var b buffer
	b.initBufSize = initBufSize
	b.maxBufSize = maxBufSize
	b.fileNameLimit = fileNameLimit
	b.ResetSize()
	return b
}

func (b *buffer) ResetSize() {
	b.Buffer = *bytes.NewBuffer(make([]byte, 0, b.initBufSize))
}

func (b *buffer) HasTable() bool {
	return b.hasTable
}

func (b *buffer) HasTags() bool {
	return b.hasTags
}

func (b *buffer) HasFields() bool {
	return b.hasFields
}

func (b *buffer) LastErr() error {
	return b.lastErr
}

func (b *buffer) ClearLastErr() {
	b.lastErr = nil
}

func (b *buffer) SetLastErr(err error) {
	if b.lastErr == nil {
		b.lastErr = err
	}
}

func (b *buffer) writeInt(i int64) {
	// We need up to 20 bytes to fit an int64, including a sign.
	var a [20]byte
	s := strconv.AppendInt(a[0:0], i, 10)
	b.Write(s)
}

func (b *buffer) writeFloat(f float64) {
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

func (b *buffer) writeBigInt(i *big.Int) {
	// We need up to 64 bytes to fit an unsigned 256-bit number.
	var a [64]byte
	s := i.Append(a[0:0], 16)
	b.Write(s)
}

func (b *buffer) writeTableName(str string) error {
	if str == "" {
		return fmt.Errorf("table name cannot be empty: %w", errInvalidMsg)
	}
	// We use string length in bytes as an approximation. That's to
	// avoid calculating the number of runes.
	if len(str) > b.fileNameLimit {
		return fmt.Errorf("table name length exceeds the limit: %w", errInvalidMsg)
	}
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		ch := str[i]
		switch ch {
		case ' ':
			b.WriteByte('\\')
		case '=':
			b.WriteByte('\\')
		case '.':
			if i == 0 || i == len(str)-1 {
				return fmt.Errorf("table name contains '.' char at the start or end: %s: %w", str, errInvalidMsg)
			}
		default:
			if illegalTableNameChar(ch) {
				return fmt.Errorf("table name contains an illegal char: "+
					"'\\n', '\\r', '?', ',', ''', '\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: %s: %w",
					str, errInvalidMsg)
			}
		}
		b.WriteByte(ch)
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

func (b *buffer) writeColumnName(str string) error {
	if str == "" {
		return fmt.Errorf("column name cannot be empty: %w", errInvalidMsg)
	}
	// We use string length in bytes as an approximation. That's to
	// avoid calculating the number of runes.
	if len(str) > b.fileNameLimit {
		return fmt.Errorf("column name length exceeds the limit: %w", errInvalidMsg)
	}
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		ch := str[i]
		switch ch {
		case ' ':
			b.WriteByte('\\')
		case '=':
			b.WriteByte('\\')
		default:
			if illegalColumnNameChar(ch) {
				return fmt.Errorf("column name contains an illegal char: "+
					"'\\n', '\\r', '?', '.', ',', ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: %s: %w",
					str, errInvalidMsg)
			}
		}
		b.WriteByte(ch)
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

func (b *buffer) writeStrValue(str string, quoted bool) error {
	// Since we're interested in ASCII chars, it's fine to iterate
	// through bytes instead of runes.
	for i := 0; i < len(str); i++ {
		ch := str[i]
		switch ch {
		case ' ':
			if !quoted {
				b.WriteByte('\\')
			}
		case ',':
			if !quoted {
				b.WriteByte('\\')
			}
		case '=':
			if !quoted {
				b.WriteByte('\\')
			}
		case '"':
			if quoted {
				b.WriteByte('\\')
			}
		case '\n':
			b.WriteByte('\\')
		case '\r':
			b.WriteByte('\\')
		case '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(ch)
	}
	return nil
}

func (b *buffer) prepareForField() bool {
	if b.lastErr != nil {
		return false
	}
	if !b.hasTable {
		b.lastErr = fmt.Errorf("table name was not provided: %w", errInvalidMsg)
		return false
	}
	if !b.hasFields {
		b.WriteByte(' ')
	} else {
		b.WriteByte(',')
	}
	return true
}

func (b *buffer) Bytes() []byte {
	return b.Buffer.Bytes()
}

func (b *buffer) Reset() {
	b.Buffer.Reset()
	b.lastMsgPos = 0
	b.msgCount = 0
	b.resetMsgFlags()
}

func (b *buffer) DiscardPendingMsg() {
	b.Truncate(b.lastMsgPos)
	b.resetMsgFlags()
}

func (b *buffer) resetMsgFlags() {
	b.hasTable = false
	b.hasTags = false
	b.hasFields = false
}

func (b *buffer) Messages() []byte {
	return b.Buffer.Bytes()
}

func (b *buffer) Table(name string) *buffer {
	if b.lastErr != nil {
		return b
	}
	if b.hasTable {
		b.lastErr = fmt.Errorf("table name already provided: %w", errInvalidMsg)
		return b
	}
	b.lastErr = b.writeTableName(name)
	if b.lastErr != nil {
		return b
	}
	b.hasTable = true
	return b
}

func (b *buffer) Symbol(name, val string) *buffer {
	if b.lastErr != nil {
		return b
	}
	if !b.hasTable {
		b.lastErr = fmt.Errorf("table name was not provided: %w", errInvalidMsg)
		return b
	}
	if b.hasFields {
		b.lastErr = fmt.Errorf("symbols have to be written before any other column: %w", errInvalidMsg)
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

func (b *buffer) Int64Column(name string, val int64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.writeInt(val)
	b.WriteByte('i')
	b.hasFields = true
	return b
}

func (b *buffer) Long256Column(name string, val *big.Int) *buffer {
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
	b.writeBigInt(val)
	b.WriteByte('i')
	if b.lastErr != nil {
		return b
	}
	b.hasFields = true
	return b
}

func (b *buffer) TimestampColumn(name string, ts time.Time) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.writeInt(ts.UnixMicro())
	b.WriteByte('t')
	b.hasFields = true
	return b
}

func (b *buffer) Float64Column(name string, val float64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.writeFloat(val)
	b.hasFields = true
	return b
}

func (b *buffer) Float64ColumnBinary(name string, val float64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	// binary format flag
	b.WriteByte('=')
	b.WriteByte(byte(float64Code))
	if isLittleEndian {
		b.Write((*[8]byte)(unsafe.Pointer(&val))[:])
	} else {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, math.Float64bits(val))
		b.Write(data)
	}
	b.hasFields = true
	return b
}

func (b *buffer) Float64Array1DColumn(name string, values []float64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	if values == nil {
		b.writeNullArray()
		return b
	}

	dim1 := len(values)
	if dim1 > MaxArrayElements {
		b.lastErr = fmt.Errorf("array size %d exceeds maximum limit %d", dim1, MaxArrayElements)
		return b
	}
	b.writeFloat64ArrayHeader(1)

	// Write shape
	b.writeInt32(int32(dim1))

	// Write values
	if len(values) > 0 {
		b.writeFloat64Data(values)
	}

	b.hasFields = true
	return b
}

func (b *buffer) Float64Array2DColumn(name string, values [][]float64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}

	if values == nil {
		b.writeNullArray()
		return b
	}

	// Validate array shape
	dim1 := len(values)
	var dim2 int
	if dim1 > 0 {
		dim2 = len(values[0])
		totalElements := dim1 * dim2
		if dim1 > MaxArrayElements || dim2 > MaxArrayElements || totalElements > MaxArrayElements || totalElements < 0 {
			b.lastErr = fmt.Errorf("array size %d exceeds maximum limit %d", totalElements, MaxArrayElements)
			return b
		}
		for i, row := range values {
			if len(row) != dim2 {
				b.lastErr = fmt.Errorf("irregular 2D array shape: row %d has length %d, expected %d", i, len(row), dim2)
				return b
			}
		}
	}

	b.writeFloat64ArrayHeader(2)

	// Write shape
	b.writeInt32(int32(dim1))
	b.writeInt32(int32(dim2))

	// Write values
	for _, row := range values {
		if len(row) > 0 {
			b.writeFloat64Data(row)
		}
	}

	b.hasFields = true
	return b
}

func (b *buffer) Float64Array3DColumn(name string, values [][][]float64) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}

	if values == nil {
		b.writeNullArray()
		return b
	}

	// Validate array shape
	dim1 := len(values)
	var dim2, dim3 int
	if dim1 > 0 {
		dim2 = len(values[0])
		if dim2 > 0 {
			dim3 = len(values[0][0])
		}
		totalElements := dim1 * dim2 * dim3
		if dim1 > MaxArrayElements || dim2 > MaxArrayElements || dim3 > MaxArrayElements || totalElements > MaxArrayElements || totalElements < 0 {
			b.lastErr = fmt.Errorf("array size %d exceeds maximum limit %d", totalElements, MaxArrayElements)
			return b
		}

		for i, level1 := range values {
			if len(level1) != dim2 {
				b.lastErr = fmt.Errorf("irregular 3D array shape: level1[%d] has length %d, expected %d", i, len(level1), dim2)
				return b
			}
			for j, level2 := range level1 {
				if len(level2) != dim3 {
					b.lastErr = fmt.Errorf("irregular 3D array shape: level2[%d][%d] has length %d, expected %d", i, j, len(level2), dim3)
					return b
				}
			}
		}
	}

	b.writeFloat64ArrayHeader(3)

	// Write shape
	b.writeInt32(int32(dim1))
	b.writeInt32(int32(dim2))
	b.writeInt32(int32(dim3))

	// Write values
	for _, level1 := range values {
		for _, level2 := range level1 {
			if len(level2) > 0 {
				b.writeFloat64Data(level2)
			}
		}
	}

	b.hasFields = true
	return b
}

func (b *buffer) Float64ArrayNDColumn(name string, value *NdArray[float64]) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}

	if value == nil {
		b.writeNullArray()
		return b
	}

	shape := value.Shape()
	numDims := value.NDims()
	// Write nDims
	b.writeFloat64ArrayHeader(byte(numDims))

	// Write shape
	for _, dim := range shape {
		b.writeInt32(int32(dim))
	}

	// Write data
	data := value.Data()
	if len(data) > 0 {
		b.writeFloat64Data(data)
	}

	b.hasFields = true
	return b
}

func (b *buffer) StringColumn(name, val string) *buffer {
	if !b.prepareForField() {
		return b
	}
	b.lastErr = b.writeColumnName(name)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('=')
	b.WriteByte('"')
	b.lastErr = b.writeStrValue(val, true)
	if b.lastErr != nil {
		return b
	}
	b.WriteByte('"')
	b.hasFields = true
	return b
}

func (b *buffer) BoolColumn(name string, val bool) *buffer {
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

func (b *buffer) At(ts time.Time, sendTs bool) error {
	err := b.lastErr
	b.lastErr = nil
	if err != nil {
		b.DiscardPendingMsg()
		return err
	}

	// Post-factum check for the max buffer size limit.
	// Since we embed bytes.Buffer, it's impossible to hook into its
	// grow() method properly to have the check before we write
	// a value to the buffer.
	if b.maxBufSize > 0 && b.Cap() > b.maxBufSize {
		b.DiscardPendingMsg()
		return fmt.Errorf("buffer size exceeded maximum limit: size=%d, limit=%d", b.Cap(), b.maxBufSize)
	}

	if !b.hasTable {
		b.DiscardPendingMsg()
		return fmt.Errorf("table name was not provided: %w", errInvalidMsg)
	}
	if !b.hasTags && !b.hasFields {
		b.DiscardPendingMsg()
		return fmt.Errorf("no symbols or columns were provided: %w", errInvalidMsg)
	}

	if sendTs {
		b.WriteByte(' ')
		b.writeInt(ts.UnixNano())
	}
	b.WriteByte('\n')

	b.lastMsgPos = b.Len()
	b.msgCount++
	b.resetMsgFlags()
	return nil
}

func (b *buffer) writeFloat64ArrayHeader(dims byte) {
	b.WriteByte('=')
	b.WriteByte('=')
	b.WriteByte(byte(arrayCode))
	b.WriteByte(byte(arrayElemDouble))
	b.WriteByte(dims)
}

func (b *buffer) writeNullArray() {
	b.WriteByte('=')
	b.WriteByte('=')
	b.WriteByte(byte(arrayCode))
	b.WriteByte(byte(arrayElemNull))
	b.hasFields = true
}
