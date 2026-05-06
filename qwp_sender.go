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
	"context"
	"encoding/binary"
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

	// AtNano closes the current row with a nanosecond-resolution
	// designated timestamp (TYPE_TIMESTAMP_NANO on the wire). It is
	// the nanosecond counterpart of LineSender.At, which uses
	// microseconds.
	//
	// A table's designated timestamp resolution is fixed by its first
	// row: mixing At and AtNano on rows of the same table within one
	// flush returns a type-conflict error.
	AtNano(ctx context.Context, ts time.Time) error

	// AckedFsn returns the highest server-acknowledged frame
	// sequence number, or -1 if no batch has been ACK'd yet.
	// Snapshot accessor — for a bounded wait, use AwaitAckedFsn.
	AckedFsn() int64

	// AwaitAckedFsn blocks until AckedFsn() >= target, the timeout
	// elapses, or the I/O loop latches a terminal error. Returns
	// true on success, false on timeout.
	//
	// Useful for tests and user code that need to confirm a specific
	// publish has been server-acknowledged. The timeout does not
	// extend Flush's own ACK wait — pair AwaitAckedFsn with the
	// auto-flush path (which enqueues without waiting), not with
	// Flush (which already blocks on ACK).
	AwaitAckedFsn(target int64, timeout time.Duration) (bool, error)

	// FlushAndGetSequence behaves identically to Flush but returns
	// the published FSN (highest committed-to-disk-and-queued-for-
	// wire frame sequence) post-flush. Distinct from AckedFsn(),
	// which is the highest *server-acknowledged* sequence — the
	// returned FSN is the upper bound of any SenderError.ToFsn that
	// could surface for this batch. Use AwaitAckedFsn for ack
	// confirmation.
	FlushAndGetSequence(ctx context.Context) (int64, error)

	// LastTerminalError returns a snapshot of the most recent
	// terminal SenderError the I/O loop latched (server rejection,
	// WS protocol violation, auth failure, reconnect-budget
	// exhaustion). Returns nil if the sender has not gone terminal
	// yet, or if it failed for a non-server reason (transport
	// error before classification).
	LastTerminalError() *SenderError

	// TotalServerErrors returns the cumulative count of SenderError
	// payloads the I/O loop has built (DROP and HALT combined).
	// Includes batches where the user handler dropped the
	// notification due to inbox overflow.
	TotalServerErrors() int64

	// DroppedErrorNotifications returns the cumulative count of
	// SenderError payloads that did not reach the user-supplied
	// handler because the bounded inbox was full at offer time.
	// Non-zero means the handler is too slow for the error rate;
	// raise WithErrorInboxCapacity or speed up the handler.
	DroppedErrorNotifications() int64

	// TotalErrorNotificationsDelivered returns the cumulative count
	// of SenderError payloads delivered to the user-supplied
	// handler. Includes deliveries where the handler panicked
	// (caught by the dispatcher).
	TotalErrorNotificationsDelivered() int64

	// TotalReconnectAttempts returns the cumulative count of
	// reconnect attempts the I/O loop has issued — succeeded plus
	// failed. Diverges from TotalReconnectsSucceeded when the server
	// is flapping. Always 0 when the sender is configured without
	// reconnect.
	TotalReconnectAttempts() int64

	// TotalReconnectsSucceeded returns the cumulative count of
	// successful reconnects. Useful as a heartbeat for outage
	// recovery.
	TotalReconnectsSucceeded() int64

	// TotalFramesReplayed returns the cumulative count of frames
	// re-emitted on a post-reconnect catch-up — i.e. frames whose
	// FSN was already on the wire before the drop. Useful for
	// verifying replay actually re-issued the unacked tail.
	TotalFramesReplayed() int64

	// TotalBackpressureStalls returns the cumulative count of times
	// engineAppendBlocking had to wait for the manager to free
	// buffer space. One increment per blocking call, not per spin-
	// park. Non-zero values mean the producer is outpacing the wire.
	TotalBackpressureStalls() int64

	// BackgroundDrainers returns a snapshot of the drainers the
	// foreground sender has dispatched for orphan slot adoption.
	// Returns nil when the sender was not configured with
	// drain_orphans (or when no orphans were found at startup).
	// Snapshots are point-in-time copies; the underlying drainer
	// goroutines keep running.
	BackgroundDrainers() []QwpBackgroundDrainer
}

// QwpBackgroundDrainer is a point-in-time snapshot of one
// background-drainer goroutine, surfaced via
// QwpSender.BackgroundDrainers for ops dashboards. The fields
// mirror the Java client's BackgroundDrainer accessors.
type QwpBackgroundDrainer struct {
	// Dir is the absolute path of the orphan slot directory the
	// drainer adopted.
	Dir string
	// FramesPending is the snapshot of the slot's published FSN
	// the drainer captured at startup — the upper bound the drain
	// must reach before the slot is fully empty. -1 before the
	// drainer has opened its engine.
	FramesPending int64
	// FramesAcked is the latest server-acknowledged FSN the
	// drainer has observed. -1 before the drainer's first poll.
	FramesAcked int64
	// LastError is the most recent error message the drainer
	// recorded, or "" if no error has been recorded.
	LastError string
	// Failed is true if the drainer ended in the FAILED outcome
	// (exhausted reconnect budget, auth failure, recovery error)
	// and dropped a .failed sentinel in the slot.
	Failed bool
}

// Compile-time check that qwpLineSender implements QwpSender.
var _ QwpSender = (*qwpLineSender)(nil)

// qwpLineSender implements LineSender for the QWP WebSocket
// protocol. All wire I/O goes through the cursor engine + send
// loop, regardless of whether store-and-forward (sf_dir) is set —
// sf_dir picks disk-backed segments, the empty value picks
// memory-backed segments. The producer encodes a batch into the
// engine; the I/O goroutine pair drains the engine to the wire and
// processes ACKs.
type qwpLineSender struct {
	// tableBuffers stores one columnar buffer per active table.
	tableBuffers map[string]*qwpTableBuffer
	// currentTable is the table buffer for the current in-progress row.
	currentTable *qwpTableBuffer

	// encoder builds the next QWP message. The cursor engine takes
	// a copy of the encoded bytes via tryAppend, so a single slot
	// is enough — no double-buffering needed.
	encoder qwpEncoder

	// encodeInfoBuf is a reusable scratch slice for
	// buildCursorTableEncodeInfo, avoiding allocation on every
	// flush.
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

	// cachedDesignatedTs is the last-seen designated timestamp
	// column for the current table, used to skip the map lookup
	// inside getOrCreateDesignatedTimestamp on every row. The
	// back-pointer col.table makes invalidation trivial — if the
	// user switched tables, col.table no longer matches
	// s.currentTable and we re-lookup. Mirrors cachedTimestampColumn
	// in the Java QwpWebSocketSender.
	cachedDesignatedTs *qwpColumnBuffer

	// lastTableName / lastTable cache the most recent successful
	// Table() resolution so repeated calls for the same table skip
	// validation and the tableBuffers map lookup. Table buffers are
	// never removed from the map, so a cached pointer is valid for
	// the lifetime of the sender. Mirrors the same-table
	// short-circuit in QwpWebSocketSender.table().
	lastTableName string
	lastTable     *qwpTableBuffer

	// Auto-flush configuration.
	autoFlushRows     int
	autoFlushInterval time.Duration
	autoFlushBytes    int // 0 disables the byte-size trigger
	flushDeadline     time.Time
	pendingRowCount   int

	// pendingBytes tracks the approximate buffered byte total across
	// all table buffers. Maintained incrementally on each commitRow:
	// delta = currentTable.approxDataSize() - currentTableBytesBefore.
	// Reset to 0 in resetAfterFlush. Only updated when the byte-size
	// triggers (autoFlushBytes, maxBufSize) are enabled, otherwise the
	// counter and snapshot are dead fields. Mirrors pendingBytes in
	// the Java QwpWebSocketSender.
	pendingBytes            int
	currentTableBytesBefore int

	// Buffer size limit. 0 means no limit.
	maxBufSize int

	// Maximum length for table and column names.
	fileNameLimit int

	// Connection and retry config.
	retryTimeout time.Duration

	// inFlightWindow is retained as a config knob for backwards
	// compat but is a no-op in cursor mode — the engine handles
	// concurrency via its own backpressure model.
	inFlightWindow int

	// cursorEngine + cursorSendLoop are set on every sender. The
	// engine is memory-backed when sf_dir is empty and disk-backed
	// otherwise. The send loop owns the WebSocket connection;
	// reconnect is its responsibility.
	cursorEngine   *qwpSfCursorEngine
	cursorSendLoop *qwpSfSendLoop

	// closeTimeout bounds Close()'s wait for the engine's
	// ackedFsn to catch up to publishedFsn. <= 0 means fast close
	// (skip the drain). Defaults to 5s.
	closeTimeout time.Duration

	// drainerPool is non-nil only when the user opted into
	// drain_orphans (SF mode only). Closed alongside the cursor
	// engine in closeCursor.
	drainerPool *qwpSfDrainerPool

	// Lifecycle.
	closed bool
}

// newQwpLineSender creates a new QWP sender backed by an
// in-memory cursor engine. The send loop establishes the
// WebSocket connection synchronously; on failure, the constructor
// returns the dial / upgrade error directly. inFlightWindow is
// accepted for backwards compatibility but is a no-op (the cursor
// engine handles concurrency via its own backpressure model). If
// dumpWriter is non-nil, outgoing bytes are recorded across every
// transport instance the send loop creates (initial connect plus
// reconnects).
func newQwpLineSender(ctx context.Context, address string, opts qwpTransportOpts, retryTimeout time.Duration, autoFlushRows int, autoFlushInterval time.Duration, dumpWriter io.Writer, inFlightWindow ...int) (*qwpLineSender, error) {
	s, err := newQwpLineSenderUnstarted(ctx, address, opts, retryTimeout,
		autoFlushRows, autoFlushInterval, dumpWriter, inFlightWindow...)
	if err != nil {
		return nil, err
	}
	s.cursorSendLoop.sendLoopStart()
	return s, nil
}

// newQwpLineSenderUnstarted builds the sender, engine, and loop but
// does NOT call sendLoopStart. Used by newQwpLineSenderFromConf so the
// resolver / handler / capacity from connect-string + builder options
// can be applied to the loop before it starts processing — otherwise
// the very first received frame races against the post-construction
// setters and could be classified with the default resolver / handled
// by the default handler instead of the user-configured ones.
func newQwpLineSenderUnstarted(ctx context.Context, address string, opts qwpTransportOpts, retryTimeout time.Duration, autoFlushRows int, autoFlushInterval time.Duration, dumpWriter io.Writer, inFlightWindow ...int) (*qwpLineSender, error) {
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
	s.encoder.wb.preallocate(qwpDefaultMicrobatchBufSize)

	// Build a memory-backed cursor engine. Same architecture as SF
	// mode, just no disk involvement.
	engine, err := qwpSfNewCursorEngine("", qwpSfDefaultMaxBytes, qwpSfDefaultMemoryMaxTotalBytes, qwpSfEngineDefaultAppendDeadline)
	if err != nil {
		return nil, err
	}
	factory := qwpSfBuildReconnectFactory(address, opts, dumpWriter)
	transport, err := factory(ctx)
	if err != nil {
		_ = engine.engineClose()
		return nil, err
	}
	loop := qwpSfNewSendLoop(engine, transport, factory,
		qwpSfDefaultParkInterval,
		qwpSfDefaultReconnectMaxDuration,
		qwpSfDefaultReconnectInitialBackoff,
		qwpSfDefaultReconnectMaxBackoff)
	engine.engineSetReconnectStatusGetter(loop.sendLoopReconnectStatus)
	s.cursorEngine = engine
	s.cursorSendLoop = loop
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

	var tb *qwpTableBuffer
	if s.lastTable != nil && s.lastTableName == name {
		tb = s.lastTable
	} else {
		if err := qwpValidateTableName(name, s.fileNameLimit); err != nil {
			s.lastErr = err
			return s
		}
		var ok bool
		tb, ok = s.tableBuffers[name]
		if !ok {
			tb = newQwpTableBuffer(name)
			s.tableBuffers[name] = tb
		}
		s.lastTableName = name
		s.lastTable = tb
	}

	s.currentTable = tb
	if s.maxBufSize > 0 || s.autoFlushBytes > 0 {
		s.currentTableBytesBefore = tb.approxDataSize()
	}
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
	if val != nil {
		if val.Sign() < 0 {
			s.lastErr = fmt.Errorf("qwp: long256 cannot be negative: %s", val.String())
			return s
		}
		if val.BitLen() > 256 {
			s.lastErr = fmt.Errorf("qwp: long256 cannot be larger than 256-bit: %d", val.BitLen())
			return s
		}
	}
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeLong256, true)
	if err != nil {
		s.lastErr = err
		return s
	}

	if val == nil {
		col.addNull()
	} else {
		// FillBytes writes the magnitude as big-endian; negatives and
		// oversize values were rejected above. Reinterpret as four
		// little-endian uint64 limbs.
		var buf [32]byte
		val.FillBytes(buf[:])
		l3 := binary.BigEndian.Uint64(buf[0:8])
		l2 := binary.BigEndian.Uint64(buf[8:16])
		l1 := binary.BigEndian.Uint64(buf[16:24])
		l0 := binary.BigEndian.Uint64(buf[24:32])
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
	col, err := s.currentTable.getOrCreateColumn(name, qwpTypeVarchar, true)
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
	return s.atWithTimestamp(ctx, ts, qwpTypeTimestamp)
}

func (s *qwpLineSender) AtNano(ctx context.Context, ts time.Time) error {
	return s.atWithTimestamp(ctx, ts, qwpTypeTimestampNano)
}

// atWithTimestamp is the shared implementation of At and AtNano. The
// typeCode selects the designated timestamp column type, which also
// determines the unit used to convert ts: qwpTypeTimestamp → micros,
// qwpTypeTimestampNano → nanos.
func (s *qwpLineSender) atWithTimestamp(ctx context.Context, ts time.Time, typeCode qwpTypeCode) error {
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
		col := s.cachedDesignatedTs
		if col == nil || col.table != s.currentTable || col.typeCode != typeCode {
			var err error
			col, err = s.currentTable.getOrCreateDesignatedTimestamp(typeCode)
			if err != nil {
				s.currentTable.cancelRow()
				s.hasTable = false
				s.currentTable = nil
				return err
			}
			s.cachedDesignatedTs = col
		}
		var v int64
		switch typeCode {
		case qwpTypeTimestamp:
			v = ts.UnixMicro()
		case qwpTypeTimestampNano:
			v = ts.UnixNano()
		default:
			return fmt.Errorf("qwp: invalid designated timestamp type 0x%02X", typeCode)
		}
		col.addTimestamp(v)
	}

	// Commit the row (gap-fills missing columns).
	s.currentTable.commitRow()

	// Size-based triggers: maxBufSize is a hard cap (forced flush to
	// prevent unbounded memory growth); autoFlushBytes is a soft
	// trigger. pendingBytes is a running counter updated by the
	// delta since Table() was called, keeping this O(1) per row.
	if s.maxBufSize > 0 || s.autoFlushBytes > 0 {
		s.pendingBytes += s.currentTable.approxDataSize() - s.currentTableBytesBefore
	}

	s.hasTable = false
	s.currentTable = nil
	s.pendingRowCount++

	if s.maxBufSize > 0 || s.autoFlushBytes > 0 {
		triggered := (s.maxBufSize > 0 && s.pendingBytes > s.maxBufSize) ||
			(s.autoFlushBytes > 0 && s.pendingBytes >= s.autoFlushBytes)
		if triggered {
			return s.autoFlush(ctx)
		}
	}

	// Auto-flush thresholds use enqueueCursor — never wait for
	// server ACKs from the user goroutine. Explicit Flush() is
	// where the drain barrier lives.
	if s.autoFlushRows > 0 && s.pendingRowCount >= s.autoFlushRows {
		return s.autoFlush(ctx)
	}

	if s.autoFlushInterval > 0 {
		if s.flushDeadline.IsZero() {
			s.flushDeadline = time.Now().Add(s.autoFlushInterval)
		} else if time.Now().After(s.flushDeadline) {
			return s.autoFlush(ctx)
		}
	}

	return nil
}

// autoFlush dispatches an auto-flush trigger from atWithTimestamp.
// Resets pending state on success so subsequent rows hit a clean
// trigger window. Errors propagate to the user.
func (s *qwpLineSender) autoFlush(ctx context.Context) error {
	if err := s.enqueueCursor(ctx); err != nil {
		return err
	}
	s.resetAfterFlush()
	return nil
}

func (s *qwpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

// --- LineSender interface: Flush ---

func (s *qwpLineSender) Flush(ctx context.Context) error {
	_, err := s.FlushAndGetSequence(ctx)
	return err
}

// FlushAndGetSequence implements QwpSender.FlushAndGetSequence.
// Flushes any pending rows and returns the published FSN — the
// upper bound on any SenderError.ToFsn that could surface for this
// batch. Callers wanting server-ack confirmation should pair the
// returned FSN with AwaitAckedFsn.
func (s *qwpLineSender) FlushAndGetSequence(ctx context.Context) (int64, error) {
	if s.closed {
		return -1, errClosedSenderFlush
	}
	if s.hasTable {
		return -1, errFlushWithPendingMessage
	}
	if s.pendingRowCount == 0 {
		// Flush() never waits for server ACK on the cursor path
		// (Java spec — design decision #1 in
		// qwp-cursor-durability.md). Surface any terminal I/O
		// error the loop has recorded so producers don't keep
		// silently buffering into a dead engine; otherwise return
		// the current published FSN.
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			return -1, err
		}
		return s.cursorEngine.enginePublishedFsn(), nil
	}
	defer s.resetAfterFlush()
	if err := s.flushCursor(ctx); err != nil {
		return -1, err
	}
	return s.cursorEngine.enginePublishedFsn(), nil
}

// resetAfterFlush clears all table buffers and resets counters.
func (s *qwpLineSender) resetAfterFlush() {
	for _, tb := range s.tableBuffers {
		tb.reset()
	}
	s.pendingRowCount = 0
	s.pendingBytes = 0
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
	// All wire I/O goes through the cursor engine + send loop,
	// regardless of whether sf_dir was set. closeCursor drains
	// (up to closeTimeout), stops the loop, closes the engine,
	// and tears down the orphan-drainer pool if one was started.
	return s.closeCursor(ctx)
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
