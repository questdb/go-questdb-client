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

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"time"
)

// QwpSender extends LineSender with column types only available
// in the QWP binary protocol. These types are not part of ILP
// and cannot be used with HTTP or TCP senders.
type QwpSender interface {
	LineSender

	// ByteColumn adds a BYTE (int8) column value.
	ByteColumn(name string, val int8) QwpSender

	// ShortColumn adds a SHORT (int16) column value.
	ShortColumn(name string, val int16) QwpSender

	// Int32Column adds an INT (int32) column value.
	Int32Column(name string, val int32) QwpSender

	// Float32Column adds a FLOAT (float32) column value.
	Float32Column(name string, val float32) QwpSender

	// CharColumn adds a CHAR column value stored as a UTF-16 code unit.
	CharColumn(name string, val rune) QwpSender

	// DateColumn adds a DATE column value (milliseconds since epoch).
	DateColumn(name string, val time.Time) QwpSender

	// TimestampNanosColumn adds a TIMESTAMP column value (nanoseconds since epoch).
	TimestampNanosColumn(name string, val time.Time) QwpSender

	// UuidColumn adds a UUID column value from high and low 64-bit parts.
	UuidColumn(name string, hi, lo uint64) QwpSender

	// VarcharColumn adds a VARCHAR column value.
	VarcharColumn(name string, val string) QwpSender

	// GeohashColumn adds a GEOHASH column value with the given bit precision.
	GeohashColumn(name string, hash uint64, precision int) QwpSender

	// Int64Array1DColumn adds a 1-dimensional LONG array column.
	Int64Array1DColumn(name string, values []int64) QwpSender

	// Int64Array2DColumn adds a 2-dimensional LONG array column.
	Int64Array2DColumn(name string, values [][]int64) QwpSender

	// Int64Array3DColumn adds a 3-dimensional LONG array column.
	Int64Array3DColumn(name string, values [][][]int64) QwpSender

	// Decimal64Column adds a DECIMAL64 column value (8 bytes on the wire,
	// 18 digits of precision). Returns an error on the next At/AtNow/Flush
	// if the value's unscaled coefficient does not fit in 64 bits.
	Decimal64Column(name string, val Decimal) QwpSender

	// Decimal128Column adds a DECIMAL128 column value (16 bytes on the wire,
	// 38 digits of precision). Returns an error on the next At/AtNow/Flush
	// if the value's unscaled coefficient does not fit in 128 bits.
	Decimal128Column(name string, val Decimal) QwpSender

	// Decimal256Column adds a DECIMAL256 column value (32 bytes on the wire,
	// 77 digits of precision). Equivalent to DecimalColumn.
	Decimal256Column(name string, val Decimal) QwpSender
}

// Compile-time check that qwpLineSender implements QwpSender.
var _ QwpSender = (*qwpLineSender)(nil)

// qwpLineSender implements LineSender for the QWP WebSocket protocol.
// In sync mode (in-flight window = 1), each Flush() encodes and
// sends one batch at a time, blocking until the server ACKs.
type qwpLineSender struct {
	// transport manages the WebSocket connection.
	transport qwpTransport

	// tableBuffers stores one columnar buffer per active table.
	tableBuffers map[string]*qwpTableBuffer
	// currentTable is the table buffer for the current in-progress row.
	currentTable *qwpTableBuffer

	// encoders provides double-buffered QWP message encoders for async
	// mode. In sync mode, only encoders[0] is used. In async mode, the
	// two encoders alternate: while one encoder's output is being sent
	// over the wire, the other can encode the next batch.
	encoders          [2]qwpEncoder
	currentEncoderIdx int
	// encoderReady signals when an encoder's buffer is safe to reuse.
	// A token is placed after sendMessage completes for that buffer.
	// In sync mode, these are nil (not used).
	encoderReady [2]chan struct{}

	// encodeInfoBuf is a reusable scratch slice for buildTableEncodeInfo,
	// avoiding allocation on every flush.
	encodeInfoBuf []qwpTableEncodeInfo

	// globalSymbols maps symbol strings to global IDs.
	globalSymbols map[string]int32
	// globalSymbolList maps IDs to symbol strings (for delta dict).
	globalSymbolList []string
	// maxSentSymbolId is the highest symbol ID ACKed by the server.
	// -1 means no symbols have been sent yet.
	maxSentSymbolId int
	// batchMaxSymbolId is the highest symbol ID used in the current batch.
	batchMaxSymbolId int

	// Schema registry (per QWP spec §16).
	// Schema IDs are small integers assigned sequentially by the
	// client and scoped to the connection lifetime. They are global
	// across all tables; the server indexes its registry by ID.
	// nextSchemaId is the next unassigned ID.
	// maxSentSchemaId is the highest ID ACKed by the server; a table
	// whose schemaId <= maxSentSchemaId is safe to encode in
	// reference mode.
	// batchMaxSchemaId is the highest schemaId used in the pending
	// batch — set by buildTableEncodeInfo, promoted to
	// maxSentSchemaId on ACK.
	nextSchemaId     int
	maxSentSchemaId  int
	batchMaxSchemaId int
	// maxSchemasPerConnection caps nextSchemaId. 0 disables the cap.
	// When the cap is hit, Flush returns an error and the caller must
	// close and re-open the sender.
	maxSchemasPerConnection int

	// Row state.
	hasTable bool
	lastErr  error

	// Auto-flush configuration.
	autoFlushRows     int
	autoFlushInterval time.Duration
	autoFlushBytes    int // 0 disables the byte-size trigger
	flushDeadline     time.Time
	pendingRowCount   int

	// Buffer size limit. 0 means no limit.
	maxBufSize int

	// Maximum length for table and column names.
	fileNameLimit int

	// Connection and retry config.
	retryTimeout time.Duration

	// syncSequence is the sequence of the next batch to send in sync
	// mode (inFlightWindow == 1). First batch is 0. Incremented after
	// each successful send so flushSync can recognise its own ACK and
	// ignore stale ACKs for earlier batches on the same connection.
	syncSequence int64

	// Async mode (in-flight window > 1).
	asyncState     *qwpAsyncState
	inFlightWindow int

	// closeTimeout is the time Close() waits for the async I/O
	// goroutine to finish before force-cancelling. Defaults to 5s.
	closeTimeout time.Duration

	// Lifecycle.
	closed bool
}

// newQwpLineSender creates a new QWP sender and establishes a
// WebSocket connection to the server. If inFlightWindow > 1, async
// mode is enabled with a dedicated I/O goroutine. If dumpWriter is
// non-nil, outgoing TCP bytes are recorded (see WithQwpDumpWriter).
func newQwpLineSender(ctx context.Context, address string, opts qwpTransportOpts, retryTimeout time.Duration, autoFlushRows int, autoFlushInterval time.Duration, dumpWriter io.Writer, inFlightWindow ...int) (*qwpLineSender, error) {
	window := 1
	if len(inFlightWindow) > 0 && inFlightWindow[0] > 1 {
		window = inFlightWindow[0]
	}

	s := &qwpLineSender{
		tableBuffers:      make(map[string]*qwpTableBuffer),
		globalSymbols:     make(map[string]int32),
		maxSentSymbolId:   -1,
		batchMaxSymbolId:  -1,
		nextSchemaId:      0,
		maxSentSchemaId:   -1,
		batchMaxSchemaId:  -1,
		retryTimeout:      retryTimeout,
		autoFlushRows:     autoFlushRows,
		autoFlushInterval: autoFlushInterval,
		inFlightWindow:    window,
		closeTimeout:      5 * time.Second,
	}
	// Initial encoder buffer capacity. Sync mode uses the small 8 KB
	// default. Async mode uses 1 MB: the user goroutine fills it while the
	// I/O goroutine transmits the other one. The size can be further grown
	// by newQwpLineSenderFromConf when autoFlushBytes is large enough to need
	// max(1 MB, 2*autoFlushBytes).
	initEncoderCap := qwpDefaultInitEncoderBufSize
	if window > 1 {
		initEncoderCap = qwpDefaultMicrobatchBufSize
	}
	s.encoders[0].wb.preallocate(initEncoderCap)
	s.encoders[1].wb.preallocate(initEncoderCap)

	s.transport.dumpWriter = dumpWriter
	if err := s.transport.connect(ctx, address, opts); err != nil {
		return nil, err
	}

	// Start async I/O goroutine if window > 1.
	if window > 1 {
		s.asyncState = newQwpAsyncState(window, &s.transport)
		s.asyncState.start()
		// Initialize double-buffered encoder ready channels.
		// Both start with a token (both encoders available).
		s.encoderReady[0] = make(chan struct{}, 1)
		s.encoderReady[1] = make(chan struct{}, 1)
		s.encoderReady[0] <- struct{}{}
		s.encoderReady[1] <- struct{}{}
	}

	return s, nil
}

// --- LineSender interface: Table and Symbol ---

func (s *qwpLineSender) Table(name string) LineSender {
	if s.lastErr != nil {
		return s
	}
	if s.hasTable {
		s.lastErr = fmt.Errorf("qwp: table %q already set; call At() or AtNow() to finalize the row first", s.currentTable.tableName)
		return s
	}
	if err := qwpValidateTableName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}

	tb, ok := s.tableBuffers[name]
	if !ok {
		tb = newQwpTableBuffer(name)
		s.tableBuffers[name] = tb
	}
	s.currentTable = tb
	s.hasTable = true
	return s
}

func (s *qwpLineSender) Symbol(name, val string) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Symbol() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}

	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeSymbol, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	// Look up or assign global symbol ID.
	id, ok := s.globalSymbols[val]
	if !ok {
		id = int32(len(s.globalSymbolList))
		s.globalSymbols[val] = id
		s.globalSymbolList = append(s.globalSymbolList, val)
	}

	col.addSymbolID(id)

	if int(id) > s.batchMaxSymbolId {
		s.batchMaxSymbolId = int(id)
	}

	return s
}

// --- LineSender interface: Column methods ---

func (s *qwpLineSender) Int64Column(name string, val int64) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Int64Column() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLong, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addLong(val)
	return s
}

func (s *qwpLineSender) Long256Column(name string, val *big.Int) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Long256Column() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLong256, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	if val == nil {
		col.addNull()
	} else {
		// Convert big.Int to four uint64 limbs in LE order.
		var mask big.Int
		mask.SetUint64(0xFFFFFFFFFFFFFFFF)
		var tmp big.Int
		l0 := tmp.And(val, &mask).Uint64()
		tmp.Rsh(val, 64)
		l1 := tmp.And(&tmp, &mask).Uint64()
		tmp.Rsh(val, 128)
		l2 := tmp.And(&tmp, &mask).Uint64()
		tmp.Rsh(val, 192)
		l3 := tmp.And(&tmp, &mask).Uint64()
		col.addLong256(l0, l1, l2, l3)
	}
	return s
}

func (s *qwpLineSender) TimestampColumn(name string, ts time.Time) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: TimestampColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeTimestamp, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addTimestamp(ts.UnixMicro())
	return s
}

func (s *qwpLineSender) Float64Column(name string, val float64) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Float64Column() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDouble, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addDouble(val)
	return s
}

func (s *qwpLineSender) StringColumn(name, val string) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: StringColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeString, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addString(val)
	return s
}

func (s *qwpLineSender) BoolColumn(name string, val bool) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: BoolColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeBoolean, false)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addBool(val)
	return s
}

// addDecimalColumn is the shared implementation for DecimalColumn and
// the Decimal{64,128,256}Column methods. It binds the column to the
// given QWP decimal type on first use; subsequent rows must use the
// same width or getOrCreateColumn returns a type-conflict error.
func (s *qwpLineSender) addDecimalColumn(apiName, colName string, typeCode qwpTypeCode, val Decimal) {
	if s.lastErr != nil {
		return
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: %s() called without Table()", apiName)
		return
	}
	if err := qwpValidateColumnName(colName, s.fileNameLimit); err != nil {
		s.lastErr = err
		return
	}
	col, err := s.currentTable.getOrCreateColumn(colName, typeCode, true)
	if err != nil {
		s.lastErr = err
		return
	}
	if err := col.addDecimal(val); err != nil {
		s.lastErr = err
	}
}

func (s *qwpLineSender) DecimalColumn(name string, val Decimal) LineSender {
	s.addDecimalColumn("DecimalColumn", name, qwpTypeDecimal256, val)
	return s
}

func (s *qwpLineSender) Decimal64Column(name string, val Decimal) QwpSender {
	s.addDecimalColumn("Decimal64Column", name, qwpTypeDecimal64, val)
	return s
}

func (s *qwpLineSender) Decimal128Column(name string, val Decimal) QwpSender {
	s.addDecimalColumn("Decimal128Column", name, qwpTypeDecimal128, val)
	return s
}

func (s *qwpLineSender) Decimal256Column(name string, val Decimal) QwpSender {
	s.addDecimalColumn("Decimal256Column", name, qwpTypeDecimal256, val)
	return s
}

func (s *qwpLineSender) DecimalColumnFromString(name string, val string) LineSender {
	if s.lastErr != nil {
		return s
	}
	d, err := parseDecimalFromString(val)
	if err != nil {
		s.lastErr = err
		return s
	}
	return s.DecimalColumn(name, d)
}

func (s *qwpLineSender) DecimalColumnShopspring(name string, val ShopspringDecimal) LineSender {
	if s.lastErr != nil {
		return s
	}
	d, err := convertShopspringDecimal(val)
	if err != nil {
		s.lastErr = err
		return s
	}
	return s.DecimalColumn(name, d)
}

func (s *qwpLineSender) Float64Array1DColumn(name string, values []float64) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Float64Array1DColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	if err := qwpValidateArrayShape([]int{len(values)}); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addDoubleArray(1, []int32{int32(len(values))}, values)
	return s
}

func (s *qwpLineSender) Float64Array2DColumn(name string, values [][]float64) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Float64Array2DColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addDoubleArray(2, []int32{0, 0}, nil)
		return s
	}
	dim0 := len(values)
	dim1 := len(values[0])
	if err := qwpValidateArrayShape([]int{dim0, dim1}); err != nil {
		s.lastErr = err
		return s
	}
	// Flatten.
	flat := make([]float64, 0, dim0*dim1)
	for _, row := range values {
		if len(row) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 2D array: row lengths differ")
			return s
		}
		flat = append(flat, row...)
	}
	col.addDoubleArray(2, []int32{int32(dim0), int32(dim1)}, flat)
	return s
}

func (s *qwpLineSender) Float64Array3DColumn(name string, values [][][]float64) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Float64Array3DColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addDoubleArray(3, []int32{0, 0, 0}, nil)
		return s
	}
	dim0 := len(values)
	dim1 := len(values[0])
	dim2 := 0
	if len(values[0]) > 0 {
		dim2 = len(values[0][0])
	}
	if err := qwpValidateArrayShape([]int{dim0, dim1, dim2}); err != nil {
		s.lastErr = err
		return s
	}
	flat := make([]float64, 0, dim0*dim1*dim2)
	for _, plane := range values {
		if len(plane) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 3D array")
			return s
		}
		for _, row := range plane {
			if len(row) != dim2 {
				s.lastErr = fmt.Errorf("qwp: irregular 3D array")
				return s
			}
			flat = append(flat, row...)
		}
	}
	col.addDoubleArray(3, []int32{int32(dim0), int32(dim1), int32(dim2)}, flat)
	return s
}

func (s *qwpLineSender) Float64ArrayNDColumn(name string, values *NdArray[float64]) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Float64ArrayNDColumn() called without Table()")
		return s
	}
	if values == nil {
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	shape := values.Shape()
	shapeInt := make([]int, len(shape))
	for i, d := range shape {
		if d > uint(MaxArrayElements) {
			s.lastErr = fmt.Errorf(
				"qwp: array dimension %d size %d exceeds maximum %d",
				i, d, MaxArrayElements,
			)
			return s
		}
		shapeInt[i] = int(d)
	}
	if err := qwpValidateArrayShape(shapeInt); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	shapeI32 := make([]int32, len(shapeInt))
	for i, d := range shapeInt {
		shapeI32[i] = int32(d)
	}
	col.addDoubleArray(uint8(len(shape)), shapeI32, values.Data())
	return s
}

// --- LineSender interface: At / AtNow ---

func (s *qwpLineSender) At(ctx context.Context, ts time.Time) error {
	if s.closed {
		return errClosedSenderAt
	}

	err := s.lastErr
	s.lastErr = nil
	if err != nil {
		if s.currentTable != nil {
			s.currentTable.cancelRow()
		}
		s.hasTable = false
		s.currentTable = nil
		return err
	}

	if !s.hasTable {
		return fmt.Errorf("qwp: At() called without Table()")
	}

	// Add designated timestamp column. The Java client uses an empty
	// string name for the designated timestamp, distinguishing it from
	// regular columns (which cannot have empty names).
	if !ts.IsZero() {
		col, err := s.currentTable.getOrCreateDesignatedTimestamp(qwpTypeTimestamp)
		if err != nil {
			s.currentTable.cancelRow()
			s.hasTable = false
			s.currentTable = nil
			return err
		}
		col.addTimestamp(ts.UnixMicro())
	}

	// Commit the row (gap-fills missing columns).
	s.currentTable.commitRow()
	s.hasTable = false
	s.currentTable = nil
	s.pendingRowCount++

	// Size-based triggers: maxBufSize is a hard cap (forced flush to
	// prevent unbounded memory growth); autoFlushBytes is a soft
	// trigger. One iteration covers both.
	if s.maxBufSize > 0 || s.autoFlushBytes > 0 {
		total := 0
		for _, tb := range s.tableBuffers {
			total += tb.approxDataSize()
		}
		triggered := (s.maxBufSize > 0 && total > s.maxBufSize) ||
			(s.autoFlushBytes > 0 && total >= s.autoFlushBytes)
		if triggered {
			if s.asyncState != nil {
				return s.enqueueFlush(ctx)
			}
			return s.Flush(ctx)
		}
	}

	// Check auto-flush thresholds.
	if s.autoFlushRows > 0 && s.pendingRowCount >= s.autoFlushRows {
		// In async mode, enqueue without waiting for ACKs so the
		// user goroutine isn't blocked on every auto-flush.
		if s.asyncState != nil {
			return s.enqueueFlush(ctx)
		}
		return s.Flush(ctx)
	}

	if s.autoFlushInterval > 0 {
		if s.flushDeadline.IsZero() {
			s.flushDeadline = time.Now().Add(s.autoFlushInterval)
		} else if time.Now().After(s.flushDeadline) {
			if s.asyncState != nil {
				return s.enqueueFlush(ctx)
			}
			return s.Flush(ctx)
		}
	}

	return nil
}

func (s *qwpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

// --- LineSender interface: Flush ---

func (s *qwpLineSender) Flush(ctx context.Context) error {
	if s.closed {
		return errClosedSenderFlush
	}
	if s.hasTable {
		return errFlushWithPendingMessage
	}
	if s.pendingRowCount == 0 {
		// In async mode, wait for any in-flight batches from
		// previous auto-flushes to complete. This lets the user
		// call Flush() as a barrier to confirm all data was ACKed.
		if s.asyncState != nil {
			return s.asyncState.waitEmpty(ctx)
		}
		return nil
	}

	defer s.resetAfterFlush()

	if s.asyncState != nil {
		return s.flushAsync(ctx)
	}
	return s.flushSync(ctx)
}

// flushSync encodes all non-empty tables into a single multi-table
// QWP message, sends it, and reads ACKs until one whose sequence is
// at least the just-sent batch's sequence arrives. Earlier sequences
// are absorbed the way the Java client does in waitForAck — a defensive
// measure against stale ACKs that can otherwise be mistaken for a
// response to the wrong batch.
func (s *qwpLineSender) flushSync(ctx context.Context) error {
	tables, err := s.buildTableEncodeInfo()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	msg := s.encoders[0].encodeMultiTableWithDeltaDict(
		tables,
		s.globalSymbolList,
		s.maxSentSymbolId,
		s.batchMaxSymbolId,
	)
	if err := s.transport.sendMessage(ctx, msg); err != nil {
		return err
	}
	expected := s.syncSequence
	s.syncSequence++

	for {
		status, data, err := s.transport.readAck(ctx)
		if err != nil {
			return err
		}
		seq := parseAckSequence(data)
		if status != qwpStatusOK {
			qErr := newQwpErrorFromAck(data)
			if qErr == nil {
				qErr = &QwpError{Status: status, Sequence: seq, Message: "unknown error"}
			}
			return qErr
		}
		if seq >= expected {
			break
		}
		// Stale ACK for an earlier batch on this connection — absorb
		// and keep reading. Matches Java's waitForAck.
	}

	// Advance ACKed state: all schema IDs in this batch are now
	// known to the server; bump the highest-ACKed symbol ID too.
	if s.batchMaxSchemaId > s.maxSentSchemaId {
		s.maxSentSchemaId = s.batchMaxSchemaId
	}
	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	return nil
}

// buildTableEncodeInfo collects non-empty tables, assigning fresh
// schema IDs to any that lack one and selecting full or reference
// mode based on whether the ID has already been ACKed by the
// server. Reuses s.encodeInfoBuf to avoid allocating per flush.
// Also sets s.batchMaxSchemaId to the highest schema ID in the batch.
// Returns an error if assigning a new schema ID would exceed
// maxSchemasPerConnection (when > 0).
func (s *qwpLineSender) buildTableEncodeInfo() ([]qwpTableEncodeInfo, error) {
	s.encodeInfoBuf = s.encodeInfoBuf[:0]
	batchMax := s.maxSentSchemaId
	for _, tb := range s.tableBuffers {
		if tb.rowCount == 0 {
			continue
		}
		// QWP wire format encodes table count as uint16.
		if len(s.encodeInfoBuf) == qwpMaxTablesPerBatch {
			return nil, fmt.Errorf(
				"qwp: too many tables in one batch: exceeded %d",
				qwpMaxTablesPerBatch,
			)
		}
		if tb.schemaId < 0 {
			if s.maxSchemasPerConnection > 0 && s.nextSchemaId >= s.maxSchemasPerConnection {
				return nil, fmt.Errorf(
					"qwp: schema registry exhausted (limit %d); close and re-open the sender to reset",
					s.maxSchemasPerConnection,
				)
			}
			tb.schemaId = s.nextSchemaId
			s.nextSchemaId++
		}
		mode := qwpSchemaModeFull
		if tb.schemaId <= s.maxSentSchemaId {
			mode = qwpSchemaModeReference
		}
		if tb.schemaId > batchMax {
			batchMax = tb.schemaId
		}
		s.encodeInfoBuf = append(s.encodeInfoBuf, qwpTableEncodeInfo{
			tb:         tb,
			schemaMode: mode,
			schemaId:   tb.schemaId,
		})
	}
	s.batchMaxSchemaId = batchMax
	return s.encodeInfoBuf, nil
}

// flushAsync encodes all tables into a single multi-table message,
// acquires a slot, enqueues the batch, and waits for all in-flight
// batches to drain before returning. Used by the public Flush() in
// async mode.
//
// Matches the Java client's flushPendingRows() + awaitPendingAcks():
// schema and symbol IDs are advanced immediately after a successful
// enqueue, not after the ACK. If a later batch fails, the I/O
// goroutine stores the error into asyncState.ioErr; every subsequent
// user-facing call hits checkError() at the top of the flush path
// and returns the error. Stale cache state can therefore never
// reach the wire on a live connection.
func (s *qwpLineSender) flushAsync(ctx context.Context) error {
	// Check for I/O errors before encoding.
	if err := s.asyncState.checkError(); err != nil {
		return err
	}

	tables, err := s.buildTableEncodeInfo()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	// Wait for the current encoder to be available (double-buffered).
	encIdx := s.currentEncoderIdx
	<-s.encoderReady[encIdx]

	// Encode all tables into a single multi-table message.
	encoded := s.encoders[encIdx].encodeMultiTableWithDeltaDict(
		tables,
		s.globalSymbolList,
		s.maxSentSymbolId,
		s.batchMaxSymbolId,
	)

	// Acquire a slot in the in-flight window.
	if err := s.asyncState.acquireSlot(ctx); err != nil {
		// Return the encoder token since we won't enqueue.
		s.encoderReady[encIdx] <- struct{}{}
		return err
	}

	// Enqueue the batch with the encoder's ready signal.
	// No copy needed — the ioLoop signals encoderReady after
	// sendMessage, at which point the buffer is safe to reuse.
	batch := qwpAsyncBatch{
		data:        encoded,
		readySignal: s.encoderReady[encIdx],
	}
	select {
	case s.asyncState.sendCh <- batch:
	case <-ctx.Done():
		s.encoderReady[encIdx] <- struct{}{}
		s.asyncState.releaseSlot()
		return ctx.Err()
	}

	// Swap to the other encoder for the next flush.
	s.currentEncoderIdx = 1 - s.currentEncoderIdx

	// Advance highest-sent schema and symbol IDs immediately after
	// enqueue — same semantics as Java's flushPendingRows. If a
	// subsequent ACK fails, asyncState.ioErr poisons the sender.
	if s.batchMaxSchemaId > s.maxSentSchemaId {
		s.maxSentSchemaId = s.batchMaxSchemaId
	}
	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	// Drain all in-flight batches before returning (Flush semantics).
	return s.asyncState.waitEmpty(ctx)
}

// enqueueFlush encodes all pending table buffers and enqueues them
// for the I/O goroutine without waiting for ACKs. This is the
// auto-flush path for async mode — At() returns promptly instead of
// blocking on a full round-trip. Schema and symbol caches are
// updated optimistically; if the I/O goroutine later fails, ioErr
// is set and all subsequent operations return that error (the
// sender is terminal, so stale cache entries can never reach the
// wire). Mirrors the Java client's flushPendingRows().
func (s *qwpLineSender) enqueueFlush(ctx context.Context) error {
	if s.pendingRowCount == 0 {
		return nil
	}

	// Check for I/O errors before encoding.
	if err := s.asyncState.checkError(); err != nil {
		return err
	}

	tables, err := s.buildTableEncodeInfo()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		s.resetAfterFlush()
		return nil
	}

	// Wait for the current encoder to be available (double-buffered).
	encIdx := s.currentEncoderIdx
	<-s.encoderReady[encIdx]

	// Encode all tables into a single multi-table message.
	encoded := s.encoders[encIdx].encodeMultiTableWithDeltaDict(
		tables,
		s.globalSymbolList,
		s.maxSentSymbolId,
		s.batchMaxSymbolId,
	)

	if err := s.asyncState.acquireSlot(ctx); err != nil {
		s.encoderReady[encIdx] <- struct{}{}
		return err
	}

	// No copy needed — the ioLoop signals encoderReady after
	// sendMessage, at which point the buffer is safe to reuse.
	batch := qwpAsyncBatch{
		data:        encoded,
		readySignal: s.encoderReady[encIdx],
	}
	select {
	case s.asyncState.sendCh <- batch:
	case <-ctx.Done():
		s.encoderReady[encIdx] <- struct{}{}
		s.asyncState.releaseSlot()
		return ctx.Err()
	}

	// Swap to the other encoder for the next flush.
	s.currentEncoderIdx = 1 - s.currentEncoderIdx

	// Optimistic cache: if the batch fails, ioErr prevents further
	// operations so stale cache entries are harmless.
	if s.batchMaxSchemaId > s.maxSentSchemaId {
		s.maxSentSchemaId = s.batchMaxSchemaId
	}
	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	s.resetAfterFlush()
	return nil
}

// resetAfterFlush clears all table buffers and resets counters.
func (s *qwpLineSender) resetAfterFlush() {
	for _, tb := range s.tableBuffers {
		tb.reset()
	}
	s.pendingRowCount = 0
	s.batchMaxSymbolId = s.maxSentSymbolId

	// Refresh flush deadline.
	if s.autoFlushInterval > 0 {
		s.flushDeadline = time.Now().Add(s.autoFlushInterval)
	} else {
		s.flushDeadline = time.Time{}
	}
}

// --- LineSender interface: Close ---

func (s *qwpLineSender) Close(ctx context.Context) error {
	if s.closed {
		return errDoubleSenderClose
	}

	s.closed = true

	var flushErr error
	if s.asyncState != nil {
		// Async close: enqueue pending rows non-blocking, then
		// stop the I/O goroutine (cancel context + close channel
		// + wait). For a guaranteed graceful flush, call Flush()
		// before Close().
		if s.hasTable {
			if s.currentTable != nil {
				s.currentTable.cancelRow()
			}
			s.hasTable = false
			s.currentTable = nil
		}
		if s.pendingRowCount > 0 {
			flushErr = s.enqueueFlush(ctx)
		}
		s.asyncState.stop(s.closeTimeout)
		if flushErr == nil {
			flushErr = s.asyncState.checkError()
		}
	} else {
		flushErr = s.flush0(ctx)
	}

	closeErr := s.transport.close(ctx)

	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// flush0 is the internal flush used by Close in sync mode. The async
// Close path uses enqueueFlush + stop() directly, so this function
// is only called when asyncState == nil.
func (s *qwpLineSender) flush0(ctx context.Context) error {
	if s.hasTable {
		// Drop the pending row silently on close.
		if s.currentTable != nil {
			s.currentTable.cancelRow()
		}
		s.hasTable = false
		s.currentTable = nil
	}
	if s.pendingRowCount == 0 {
		return nil
	}

	defer s.resetAfterFlush()
	return s.flushSync(ctx)
}

// --- QwpSender interface: extended column types ---

func (s *qwpLineSender) ByteColumn(name string, val int8) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: ByteColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeByte, false)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addByte(val)
	return s
}

func (s *qwpLineSender) ShortColumn(name string, val int16) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: ShortColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeShort, false)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addShort(val)
	return s
}

func (s *qwpLineSender) Int32Column(name string, val int32) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Int32Column() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeInt, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addInt32(val)
	return s
}

func (s *qwpLineSender) Float32Column(name string, val float32) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Float32Column() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeFloat, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addFloat32(val)
	return s
}

func (s *qwpLineSender) CharColumn(name string, val rune) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: CharColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeChar, false)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addChar(val)
	return s
}

func (s *qwpLineSender) DateColumn(name string, val time.Time) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: DateColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDate, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addLong(val.UnixMilli())
	return s
}

func (s *qwpLineSender) TimestampNanosColumn(name string, val time.Time) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: TimestampNanosColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeTimestampNano, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	// Store nanoseconds directly — qwpTypeTimestampNano (0x10) is int64 nanos.
	col.addTimestamp(val.UnixNano())
	return s
}

func (s *qwpLineSender) UuidColumn(name string, hi, lo uint64) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: UuidColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeUuid, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addUuid(hi, lo)
	return s
}

func (s *qwpLineSender) VarcharColumn(name string, val string) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: VarcharColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeVarchar, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addString(val)
	return s
}

func (s *qwpLineSender) GeohashColumn(name string, hash uint64, precision int) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: GeohashColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	if precision < 1 || precision > 60 {
		s.lastErr = fmt.Errorf("qwp: GeohashColumn() precision %d out of range [1, 60]", precision)
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeGeohash, false)
	if err != nil {
		s.lastErr = err
		return s
	}
	if err := col.addGeohash(hash, int8(precision)); err != nil {
		s.lastErr = err
	}
	return s
}

func (s *qwpLineSender) Int64Array1DColumn(name string, values []int64) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Int64Array1DColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	if err := qwpValidateArrayShape([]int{len(values)}); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLongArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	col.addLongArray(1, []int32{int32(len(values))}, values)
	return s
}

func (s *qwpLineSender) Int64Array2DColumn(name string, values [][]int64) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Int64Array2DColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLongArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addLongArray(2, []int32{0, 0}, nil)
		return s
	}
	dim0 := len(values)
	dim1 := len(values[0])
	if err := qwpValidateArrayShape([]int{dim0, dim1}); err != nil {
		s.lastErr = err
		return s
	}
	flat := make([]int64, 0, dim0*dim1)
	for _, row := range values {
		if len(row) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 2D array: row lengths differ")
			return s
		}
		flat = append(flat, row...)
	}
	col.addLongArray(2, []int32{int32(dim0), int32(dim1)}, flat)
	return s
}

func (s *qwpLineSender) Int64Array3DColumn(name string, values [][][]int64) QwpSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: Int64Array3DColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLongArray, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addLongArray(3, []int32{0, 0, 0}, nil)
		return s
	}
	dim0 := len(values)
	dim1 := len(values[0])
	dim2 := 0
	if len(values[0]) > 0 {
		dim2 = len(values[0][0])
	}
	if err := qwpValidateArrayShape([]int{dim0, dim1, dim2}); err != nil {
		s.lastErr = err
		return s
	}
	flat := make([]int64, 0, dim0*dim1*dim2)
	for _, plane := range values {
		if len(plane) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 3D array")
			return s
		}
		for _, row := range plane {
			if len(row) != dim2 {
				s.lastErr = fmt.Errorf("qwp: irregular 3D array")
				return s
			}
			flat = append(flat, row...)
		}
	}
	col.addLongArray(3, []int32{int32(dim0), int32(dim1), int32(dim2)}, flat)
	return s
}
