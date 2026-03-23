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
	"context"
	"fmt"
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

	// encoder is the reusable QWP message encoder.
	encoder qwpEncoder

	// globalSymbols maps symbol strings to global IDs.
	globalSymbols map[string]int32
	// globalSymbolList maps IDs to symbol strings (for delta dict).
	globalSymbolList []string
	// maxSentSymbolId is the highest symbol ID ACKed by the server.
	// -1 means no symbols have been sent yet.
	maxSentSymbolId int
	// batchMaxSymbolId is the highest symbol ID used in the current batch.
	batchMaxSymbolId int

	// sentSchemaHashes tracks which (table, schema) pairs have been
	// successfully sent to the server.
	sentSchemaHashes map[int64]struct{}

	// Row state.
	hasTable bool
	lastErr  error

	// Auto-flush configuration.
	autoFlushRows     int
	autoFlushInterval time.Duration
	flushDeadline     time.Time
	pendingRowCount   int

	// Connection and retry config.
	retryTimeout time.Duration

	// Async mode (in-flight window > 1).
	asyncState      *qwpAsyncState
	inFlightWindow  int

	// Lifecycle.
	closed bool
}

// newQwpLineSender creates a new QWP sender and establishes a
// WebSocket connection to the server. If inFlightWindow > 1, async
// mode is enabled with a dedicated I/O goroutine.
func newQwpLineSender(ctx context.Context, address string, opts qwpTransportOpts, retryTimeout time.Duration, autoFlushRows int, autoFlushInterval time.Duration, inFlightWindow ...int) (*qwpLineSender, error) {
	window := 1
	if len(inFlightWindow) > 0 && inFlightWindow[0] > 1 {
		window = inFlightWindow[0]
	}

	s := &qwpLineSender{
		tableBuffers:      make(map[string]*qwpTableBuffer),
		globalSymbols:     make(map[string]int32),
		sentSchemaHashes:  make(map[int64]struct{}),
		maxSentSymbolId:   -1,
		batchMaxSymbolId:  -1,
		retryTimeout:      retryTimeout,
		autoFlushRows:     autoFlushRows,
		autoFlushInterval: autoFlushInterval,
		inFlightWindow:    window,
	}

	if err := s.transport.connect(ctx, address, opts); err != nil {
		return nil, err
	}

	// Start async I/O goroutine if window > 1.
	if window > 1 {
		s.asyncState = newQwpAsyncState(window, &s.transport)
		s.asyncState.start()
	}

	return s, nil
}

// --- name validation ---

// qwpValidateTableName validates a table name using the same rules
// as the existing ILP buffer.
func qwpValidateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("qwp: table name cannot be empty")
	}
	if name[0] == '.' || name[len(name)-1] == '.' {
		return fmt.Errorf("qwp: table name %q cannot start or end with '.'", name)
	}
	for i := 0; i < len(name); i++ {
		if illegalTableNameChar(name[i]) {
			return fmt.Errorf("qwp: table name %q contains illegal character", name)
		}
	}
	return nil
}

// qwpValidateColumnName validates a column name using the same
// rules as the existing ILP buffer.
func qwpValidateColumnName(name string) error {
	if name == "" {
		return fmt.Errorf("qwp: column name cannot be empty")
	}
	for i := 0; i < len(name); i++ {
		if illegalColumnNameChar(name[i]) {
			return fmt.Errorf("qwp: column name %q contains illegal character", name)
		}
	}
	return nil
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
	if err := qwpValidateTableName(name); err != nil {
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}

	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeSymbol, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLong, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLong256, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeTimestamp, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDouble, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeString, false)
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
	if err := qwpValidateColumnName(name); err != nil {
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

func (s *qwpLineSender) DecimalColumn(name string, val Decimal) LineSender {
	if s.lastErr != nil {
		return s
	}
	if !s.hasTable {
		s.lastErr = fmt.Errorf("qwp: DecimalColumn() called without Table()")
		return s
	}
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDecimal256, true)
	if err != nil {
		s.lastErr = err
		return s
	}
	if err := col.addDecimal(val); err != nil {
		s.lastErr = err
	}
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, false)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addDoubleArray(2, []int32{0, 0}, nil)
		return s
	}
	dim0 := int32(len(values))
	dim1 := int32(len(values[0]))
	// Flatten.
	flat := make([]float64, 0, int(dim0)*int(dim1))
	for _, row := range values {
		if int32(len(row)) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 2D array: row lengths differ")
			return s
		}
		flat = append(flat, row...)
	}
	col.addDoubleArray(2, []int32{dim0, dim1}, flat)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, false)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addDoubleArray(3, []int32{0, 0, 0}, nil)
		return s
	}
	dim0 := int32(len(values))
	dim1 := int32(len(values[0]))
	dim2 := int32(0)
	if len(values[0]) > 0 {
		dim2 = int32(len(values[0][0]))
	}
	flat := make([]float64, 0, int(dim0)*int(dim1)*int(dim2))
	for _, plane := range values {
		if int32(len(plane)) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 3D array")
			return s
		}
		for _, row := range plane {
			if int32(len(row)) != dim2 {
				s.lastErr = fmt.Errorf("qwp: irregular 3D array")
				return s
			}
			flat = append(flat, row...)
		}
	}
	col.addDoubleArray(3, []int32{dim0, dim1, dim2}, flat)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, false)
	if err != nil {
		s.lastErr = err
		return s
	}

	shape := values.Shape()
	shapeI32 := make([]int32, len(shape))
	for i, d := range shape {
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

	// Check auto-flush thresholds.
	if s.autoFlushRows > 0 && s.pendingRowCount >= s.autoFlushRows {
		return s.Flush(ctx)
	}

	if s.autoFlushInterval > 0 {
		if s.flushDeadline.IsZero() {
			s.flushDeadline = time.Now().Add(s.autoFlushInterval)
		} else if time.Now().After(s.flushDeadline) {
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
		return nil
	}

	defer s.resetAfterFlush()

	if s.asyncState != nil {
		return s.flushAsync(ctx)
	}
	return s.flushSync(ctx)
}

// flushSync sends each table buffer synchronously with retry.
func (s *qwpLineSender) flushSync(ctx context.Context) error {
	for _, tb := range s.tableBuffers {
		if tb.rowCount == 0 {
			continue
		}
		if err := s.flushTable(ctx, tb); err != nil {
			return err
		}
	}

	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	return nil
}

// flushAsync encodes each table, acquires a slot, and enqueues
// the batch for the I/O goroutine to send.
func (s *qwpLineSender) flushAsync(ctx context.Context) error {
	// Check for I/O errors before encoding.
	if err := s.asyncState.checkError(); err != nil {
		return err
	}

	for _, tb := range s.tableBuffers {
		if tb.rowCount == 0 {
			continue
		}

		// Encode the table into a batch payload.
		schemaHash := tb.getSchemaHash()
		skey := qwpSchemaKey(tb.tableName, schemaHash)
		_, schemaKnown := s.sentSchemaHashes[skey]

		mode := qwpSchemaModeFull
		if schemaKnown {
			mode = qwpSchemaModeReference
		}
		encoded := s.encoder.encodeTableWithDeltaDict(
			tb,
			s.globalSymbolList,
			s.maxSentSymbolId,
			s.batchMaxSymbolId,
			mode,
			schemaHash,
		)

		// Copy the encoded data since the encoder reuses its buffer.
		batch := make([]byte, len(encoded))
		copy(batch, encoded)

		// Acquire a slot in the in-flight window.
		if err := s.asyncState.acquireSlot(); err != nil {
			return err
		}

		// Enqueue the batch for the I/O goroutine.
		select {
		case s.asyncState.sendCh <- batch:
		case <-ctx.Done():
			s.asyncState.releaseSlot()
			return ctx.Err()
		}

		// Optimistically mark schema as sent.
		s.sentSchemaHashes[skey] = struct{}{}
	}

	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	// Wait for all in-flight batches to be ACKed before returning.
	return s.asyncState.waitEmpty()
}

// flushTable encodes and sends a single table buffer, handling
// schema caching and retry logic.
func (s *qwpLineSender) flushTable(ctx context.Context, tb *qwpTableBuffer) error {
	schemaHash := tb.getSchemaHash()
	skey := qwpSchemaKey(tb.tableName, schemaHash)
	_, schemaKnown := s.sentSchemaHashes[skey]

	err := s.transport.sendWithRetry(ctx, s.retryTimeout,
		func() []byte {
			mode := qwpSchemaModeFull
			if schemaKnown {
				mode = qwpSchemaModeReference
			}
			return s.encoder.encodeTableWithDeltaDict(
				tb,
				s.globalSymbolList,
				s.maxSentSymbolId,
				s.batchMaxSymbolId,
				mode,
				schemaHash,
			)
		},
		func() {
			// Schema error callback: switch to full schema.
			delete(s.sentSchemaHashes, skey)
			schemaKnown = false
		},
	)

	if err != nil {
		return err
	}

	// Mark schema as sent on success.
	s.sentSchemaHashes[skey] = struct{}{}
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
	if s.pendingRowCount > 0 {
		flushErr = s.flush0(ctx)
	}

	// Stop the async I/O goroutine before closing the connection.
	if s.asyncState != nil {
		s.asyncState.stop()
	}

	closeErr := s.transport.close(ctx)

	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// flush0 is the internal flush used by Close. It doesn't check
// the closed flag.
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

	// Route through the async path if async mode is active,
	// so the ioLoop goroutine owns all transport access.
	if s.asyncState != nil {
		return s.flushAsync(ctx)
	}

	for _, tb := range s.tableBuffers {
		if tb.rowCount == 0 {
			continue
		}
		if err := s.flushTable(ctx, tb); err != nil {
			return err
		}
	}

	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	return nil
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
	if err := qwpValidateColumnName(name); err != nil {
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
	if err := qwpValidateColumnName(name); err != nil {
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeInt, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeFloat, false)
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
	if err := qwpValidateColumnName(name); err != nil {
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDate, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeTimestamp, false)
	if err != nil {
		s.lastErr = err
		return s
	}
	// Store nanoseconds converted to microseconds for QWP wire format.
	col.addTimestamp(val.UnixNano() / 1000)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeUuid, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeVarchar, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLongArray, false)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLongArray, false)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addLongArray(2, []int32{0, 0}, nil)
		return s
	}
	dim0 := int32(len(values))
	dim1 := int32(len(values[0]))
	flat := make([]int64, 0, int(dim0)*int(dim1))
	for _, row := range values {
		if int32(len(row)) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 2D array: row lengths differ")
			return s
		}
		flat = append(flat, row...)
	}
	col.addLongArray(2, []int32{dim0, dim1}, flat)
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
	if err := qwpValidateColumnName(name); err != nil {
		s.lastErr = err
		return s
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLongArray, false)
	if err != nil {
		s.lastErr = err
		return s
	}

	if len(values) == 0 {
		col.addLongArray(3, []int32{0, 0, 0}, nil)
		return s
	}
	dim0 := int32(len(values))
	dim1 := int32(len(values[0]))
	dim2 := int32(0)
	if len(values[0]) > 0 {
		dim2 = int32(len(values[0][0]))
	}
	flat := make([]int64, 0, int(dim0)*int(dim1)*int(dim2))
	for _, plane := range values {
		if int32(len(plane)) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 3D array")
			return s
		}
		for _, row := range plane {
			if int32(len(row)) != dim2 {
				s.lastErr = fmt.Errorf("qwp: irregular 3D array")
				return s
			}
			flat = append(flat, row...)
		}
	}
	col.addLongArray(3, []int32{dim0, dim1, dim2}, flat)
	return s
}
