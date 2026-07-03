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
	"sync/atomic"
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
	// Runes outside the BMP (< 0 or > U+FFFF) are rejected — QuestDB's
	// CHAR is a single UTF-16 code unit, matching Java char semantics.
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
	//
	// A nil values slice yields a NULL array; a non-nil empty slice yields
	// a distinct, non-null empty array (cardinality 0).
	Int64Array1DColumn(name string, values []int64) QwpSender

	// Int64Array2DColumn adds a 2-dimensional LONG array column.
	//
	// A nil values slice yields a NULL array; a non-nil empty slice yields
	// a distinct, non-null empty array (cardinality 0).
	Int64Array2DColumn(name string, values [][]int64) QwpSender

	// Int64Array3DColumn adds a 3-dimensional LONG array column.
	//
	// A nil values slice yields a NULL array; a non-nil empty slice yields
	// a distinct, non-null empty array (cardinality 0).
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

	// AwaitAckedFsn blocks until AckedFsn() >= target, ctx is
	// cancelled / deadlines, or the I/O loop latches a terminal
	// error. Returns nil on success; ctx.Err() on cancellation /
	// deadline; *SenderError on a terminal server rejection.
	//
	// Useful for tests and user code that need to confirm a specific
	// publish has been server-acknowledged. Wrap with
	// context.WithTimeout for a bounded wait. Pair AwaitAckedFsn with
	// the FSN returned by FlushAndGetSequence — none of the flush
	// paths (explicit Flush, FlushAndGetSequence, auto-flush) wait
	// for ACK, so AwaitAckedFsn is the only API that blocks on server
	// acknowledgement.
	AwaitAckedFsn(ctx context.Context, target int64) error

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
	// WS protocol violation, auth failure). Returns nil if the
	// sender has not gone terminal yet, or if it failed for a
	// non-server reason (transport error before classification).
	LastTerminalError() *SenderError

	// TotalServerErrors returns the cumulative count of SenderError
	// payloads the I/O loop has built (retriable and terminal
	// combined). Includes batches where the user handler dropped the
	// notification due to inbox overflow.
	TotalServerErrors() int64

	// DroppedErrorNotifications returns the cumulative count of
	// SenderError payloads that did not reach the user-supplied
	// handler because the bounded inbox was full at offer time.
	// Non-zero means the handler is too slow for the error rate;
	// raise WithErrorInboxCapacity or speed up the handler.
	DroppedErrorNotifications() int64

	// DroppedConnectionNotifications returns the cumulative count of
	// SenderConnectionEvent payloads dropped because the listener's
	// bounded inbox was full. Non-zero means the listener is too slow;
	// raise WithConnectionListenerInboxCapacity or speed it up.
	DroppedConnectionNotifications() int64

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

	// TotalDurableAcks returns the cumulative count of STATUS_DURABLE_ACK
	// frames processed. Always 0 unless request_durable_ack is on.
	TotalDurableAcks() int64

	// TotalDurableTrimAdvances returns the cumulative count of times a
	// durable ACK advanced the trim/replay/await watermark. Always 0
	// unless request_durable_ack is on.
	TotalDurableTrimAdvances() int64

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
	// (auth failure, durable-ack settle exhaustion, recovery error,
	// wedged no-progress connection) and dropped a .failed sentinel
	// in the slot.
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
	// buildTableEncodeInfo, avoiding allocation on every flush.
	encodeInfoBuf []*qwpTableBuffer

	// dirtyTables lists the table buffers selected by Table() since the
	// last full flush — i.e. the only buffers that can hold pending
	// rows. buildTableEncodeInfo, resetAfterFlush, and
	// recomputePendingFromBuffers iterate this set instead of the whole
	// tableBuffers map, so a sender juggling hundreds of tables pays per
	// flush only for the handful actually written that cycle. Truncated
	// to [:0] (capacity retained) by resetAfterFlush. Producer-owned,
	// like tableBuffers and encodeInfoBuf.
	dirtyTables []*qwpTableBuffer

	// globalSymbols maps symbol strings to global IDs.
	globalSymbols map[string]int32
	// globalSymbolList maps IDs to symbol strings (for delta dict).
	globalSymbolList []string
	// maxSentSymbolId is the highest symbol ID included in a frame
	// appended to the cursor engine — advanced at append time, not on
	// server ACK. It is the cross-flush high-water mark that
	// resetAfterFlush rewinds batchMaxSymbolId to. -1 means no symbols
	// appended yet.
	maxSentSymbolId int
	// batchMaxSymbolId is the highest symbol ID used in the current batch.
	batchMaxSymbolId int

	// Schemas are intentionally NOT tracked on the cursor wire path.
	// Every frame is self-sufficient: it carries the full inline column
	// definitions and the full symbol dict from id 0. There is no
	// per-connection schema registry on the client side and no
	// schema-change detection; the server reads the inline column
	// definitions on every frame regardless.

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
	// effectiveAutoFlushBytes is the per-connection clamped variant
	// of autoFlushBytes. Computed from the server-advertised
	// X-QWP-Max-Batch-Size on every successful connect / reconnect
	// via the send loop's onTransportSwap callback:
	//   - autoFlushBytes <= 0 (user opted out):           store 0
	//   - server cap <= 0      (header absent / older):    store autoFlushBytes
	//   - otherwise:                                       store min(autoFlushBytes, cap*9/10)
	// Read by the producer in atWithTimestamp to drive the byte-size
	// auto-flush trigger; atomic so a reconnect from the I/O
	// goroutine cannot race the producer's per-row trigger check.
	// Initialised to autoFlushBytes in the constructors so the
	// trigger fires correctly even before the first transport-swap
	// callback runs.
	effectiveAutoFlushBytes atomic.Int64
	// serverMaxBatchSize mirrors the just-bound transport's
	// serverMaxBatchSize so the producer can apply the per-row hard
	// guard (atWithTimestamp) and the flush-time defensive cap
	// check (enqueueCursor) without dereferencing the loop's
	// transport pointer on every call. Updated together with
	// effectiveAutoFlushBytes from applyServerBatchSizeLimit; 0
	// means "no cap advertised" and both guards short-circuit.
	// Mirrors Java's volatile-int serverMaxBatchSize field.
	serverMaxBatchSize atomic.Int32
	// maxFrameBytes is the largest encoded frame the cursor engine's
	// segments can hold (segment size minus header overhead, from
	// engineMaxFrameBytes). A frame above this can never be appended,
	// so it bounds two things, exactly like serverMaxBatchSize:
	//   - the effectiveAutoFlushBytes clamp, so the soft byte trigger
	//     fires before a batch can grow past what a segment holds; and
	//   - the flush-time hard guard in enqueueCursor, which drops an
	//     over-cap frame with a typed error instead of retaining it and
	//     re-failing forever.
	// Constant for the sender's lifetime (the segment size never
	// changes). 0 in the bench / hand-built test senders that have no
	// engine, where both uses short-circuit.
	maxFrameBytes   int64
	flushDeadline   time.Time
	pendingRowCount int

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

	// Lifecycle. atomic so a contract-violating concurrent
	// double-Close has a defined (idempotent) outcome rather than a
	// data race that could double-close the engine's channels. The
	// single-producer At/Flush reads are racy only under the same
	// contract violation; the atomic load keeps them well-defined too.
	closed atomic.Bool
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
func newQwpLineSender(ctx context.Context, address string, opts qwpTransportOpts, autoFlushRows int, autoFlushInterval time.Duration, dumpWriter io.Writer, inFlightWindow ...int) (*qwpLineSender, error) {
	s, err := newQwpLineSenderUnstarted(ctx, address, opts,
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
func newQwpLineSenderUnstarted(ctx context.Context, address string, opts qwpTransportOpts, autoFlushRows int, autoFlushInterval time.Duration, dumpWriter io.Writer, inFlightWindow ...int) (*qwpLineSender, error) {
	window := 1
	if len(inFlightWindow) > 0 && inFlightWindow[0] > 1 {
		window = inFlightWindow[0]
	}

	s := &qwpLineSender{
		tableBuffers:      make(map[string]*qwpTableBuffer),
		globalSymbols:     make(map[string]int32),
		maxSentSymbolId:   -1,
		batchMaxSymbolId:  -1,
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
	transport, err := factory(ctx, 0)
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
	engine.engineSetTerminalErrorGetter(loop.sendLoopCheckError)
	s.cursorEngine = engine
	s.cursorSendLoop = loop
	// The memory-mode segment is the fixed qwpSfDefaultMaxBytes; record
	// the largest frame it can hold so the byte-trigger clamp and the
	// flush-time drop guard bound batches to it.
	s.maxFrameBytes = engine.engineMaxFrameBytes()
	return s, nil
}

// --- LineSender interface: Table and Symbol ---

func (s *qwpLineSender) Table(name string) LineSender {
	if s.lastErr != nil {
		return s
	}
	// Poll the I/O loop's terminal latch at the start of a new row so a
	// HALT surfaces on the next At/AtNow without forcing the user to
	// Flush first. Subsequent Symbol/*Column calls short-circuit on the
	// latched s.lastErr, preserving the fluent buffer-latch pattern.
	// The nil guard matches the accessor pattern in qwp_sender_cursor.go
	// and keeps the bench harness (which hand-builds a sender without
	// an I/O loop) working.
	if s.cursorSendLoop != nil {
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			s.lastErr = err
			return s
		}
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
	// Track this table in the dirty set the first time it is selected
	// in a flush cycle, so flush + reset visit only written tables. The
	// dirty flag dedupes: repeated Table() calls for the same table (or
	// the lastTable fast-path above) skip the append. resetAfterFlush
	// clears the flag and empties the list.
	if !tb.dirty {
		tb.dirty = true
		s.dirtyTables = append(s.dirtyTables, tb)
	}
	// Snapshot the table's buffered-byte count at row-start so both
	// the auto-flush byte-size trigger (post-commit pendingBytes
	// delta) and the per-row hard guard (pre-commit rowBytes delta
	// vs serverMaxBatchSize) can read it. Always snapshot — the
	// async-initial-connect path may flip serverMaxBatchSize from 0
	// to positive between Table() and At(), and a gated snapshot
	// would leave currentTableBytesBefore stale (carrying over from
	// a previous row, or 0 if never set) so the per-row guard reads
	// (current size - 0) as the row's bytes and falsely rejects a
	// valid row whose true delta fits. approxDataSize is O(1) and
	// the int assignment doesn't allocate, so unconditional snapshot
	// preserves the zero-alloc hot path.
	s.currentTableBytesBefore = tb.approxDataSize()
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
	// A nil slice is a NULL array (carried in the null bitmap); a non-nil
	// empty slice is a zero-length 1D array (shape {0}), which QuestDB
	// stores as a distinct, non-null value.
	if values == nil {
		col.addNull()
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

	// A nil slice is a NULL array; a non-nil empty slice is an empty 2D
	// array (shape {0, 0}), stored distinct from NULL.
	if values == nil {
		col.addNull()
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
	// Validate row regularity before reserving so the streamed write
	// fills exactly the reserved payload — no intermediate flat copy.
	for _, row := range values {
		if len(row) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 2D array: row lengths differ")
			return s
		}
	}
	col.addDoubleArray2D(dim0, dim1, values)
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

	// A nil slice is a NULL array; a non-nil empty slice is an empty 3D
	// array (shape {0, 0, 0}), stored distinct from NULL.
	if values == nil {
		col.addNull()
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
	// Validate shape regularity before reserving so the streamed write
	// fills exactly the reserved payload — no intermediate flat copy.
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
		}
	}
	col.addDoubleArray3D(dim0, dim1, dim2, values)
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
	if err := qwpValidateColumnName(name, s.fileNameLimit); err != nil {
		s.lastErr = err
		return s
	}
	// A nil NdArray is a NULL array, consistent with the typed 1D/2D/3D
	// setters: ensure the column exists and mark this row NULL via the
	// null bitmap. A non-nil NdArray with a zero-length dimension is a
	// distinct, non-null empty array and flows through the normal path.
	if values == nil {
		col, err := s.currentTable.getOrCreateColumn(name, qwpTypeDoubleArray, true)
		if err != nil {
			s.lastErr = err
			return s
		}
		col.addNull()
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
	if s.closed.Load() {
		return errClosedSenderAt
	}

	err := s.lastErr
	s.lastErr = nil
	if err != nil {
		if s.currentTable != nil {
			s.currentTable.cancelRow()
		}
		// cancelRow may have wiped the designated-TS column out
		// of tb.columns / tb.columnIndex (first row of a fresh
		// or just-flushed table). Drop the cache so the next row
		// re-runs getOrCreateDesignatedTimestamp and re-inserts
		// the column — otherwise the stale pointer satisfies the
		// staleness check and the row commits without a "" column.
		s.cachedDesignatedTs = nil
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
				s.cachedDesignatedTs = nil
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
			s.currentTable.cancelRow()
			s.cachedDesignatedTs = nil
			s.hasTable = false
			s.currentTable = nil
			return fmt.Errorf("qwp: invalid designated timestamp type 0x%02X", typeCode)
		}
		col.addTimestamp(v)
	}

	// Per-row hard guard: if THIS row's buffered bytes already
	// exceed the server's wire cap, the flush would produce an
	// oversize WS frame the server closes with ws-close[1009].
	// Catches the case where a single row is too big to ever ship,
	// so the user sees a clear error instead of a delayed
	// terminal-error from a downstream auto-flush. Checked BEFORE
	// commitRow so the buffered column bytes can be discarded via
	// cancelRow — prior committed rows in the batch stay intact
	// and can still be flushed by the caller. The check ignores
	// the null-padding bytes commitRow will add (bounded by
	// numColumns * elemSize, far below any realistic cap).
	// Mirrors Java QwpWebSocketSender.sendRow's pre-nextRow guard.
	if cap := s.serverMaxBatchSize.Load(); cap > 0 {
		rowBytes := s.currentTable.approxDataSize() - s.currentTableBytesBefore
		if int64(rowBytes) > int64(cap) {
			s.currentTable.cancelRow()
			s.cachedDesignatedTs = nil
			s.hasTable = false
			s.currentTable = nil
			return fmt.Errorf(
				"qwp: row too large for server batch cap [rowBytes=%d, serverMaxBatchSize=%d]",
				rowBytes, cap)
		}
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
		// The byte-size trigger compares against effectiveAutoFlushBytes,
		// not the raw configured autoFlushBytes: the send loop's
		// onTransportSwap callback clamps the threshold down to 90%
		// of the server-advertised X-QWP-Max-Batch-Size on every
		// connect, so the soft auto-flush fires before the encoded
		// batch can exceed the server's hard cap. effectiveAutoFlushBytes
		// is seeded from autoFlushBytes in the constructor; it is
		// always > 0 iff the user opted in, regardless of whether a
		// transport-swap callback has fired yet, so the gate on
		// s.autoFlushBytes > 0 above stays sound.
		effective := int(s.effectiveAutoFlushBytes.Load())
		triggered := (s.maxBufSize > 0 && s.pendingBytes > s.maxBufSize) ||
			(effective > 0 && s.pendingBytes >= effective)
		if triggered {
			return s.autoFlush(ctx)
		}
	}

	// Auto-flush thresholds use enqueueCursor — never wait for
	// server ACKs from the user goroutine. Explicit Flush() follows
	// the same publish-only path; the send loop drains and replays
	// in the background.
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

// applyServerBatchSizeLimit refreshes effectiveAutoFlushBytes and
// serverMaxBatchSize from the cap the just-bound transport advertised
// in X-QWP-Max-Batch-Size. Registered as the send loop's
// onTransportSwap callback, so it runs after every successful connect
// — initial bind and every reconnect. A rolling upgrade can leave
// neighbouring endpoints with different caps, so the clamp is
// re-evaluated on every swap; never increase past the configured
// autoFlushBytes, never override an explicit opt-out.
//
// Resolution (mirrors Java QwpWebSocketSender.applyServerBatchSizeLimit):
//   - s.autoFlushBytes <= 0: the user disabled byte-size auto-flush;
//     keep it disabled even when the server advertises a cap.
//   - transport == nil OR cap <= 0: the server did not advertise a
//     cap (older build or async-pending initial connect); the
//     configured autoFlushBytes is kept verbatim.
//   - otherwise: store min(autoFlushBytes, cap*9/10). The 10%
//     headroom covers schema + dict-delta encoding overhead the
//     soft trigger does not see — without it, an at-the-limit
//     auto-flush could still emit a frame the server closes with
//     ws-close[1009].
//
// Also mirrors the raw cap onto s.serverMaxBatchSize so the per-row
// hard guard in atWithTimestamp and the flush-time defensive cap
// check in enqueueCursor can sample it cheaply without dereferencing
// the loop's transport pointer. Always-update (independent of the
// opt-out branch) so the guards fire against the freshly-advertised
// value even when the user opted out of the soft trigger.
//
// Safe to call from any goroutine: atomic stores on both fields.
// Cheap; no allocations.
func (s *qwpLineSender) applyServerBatchSizeLimit(t *qwpTransport) {
	var cap int32
	if t != nil {
		cap = t.serverMaxBatchSize
	}
	s.serverMaxBatchSize.Store(cap)
	if s.autoFlushBytes <= 0 {
		s.effectiveAutoFlushBytes.Store(0)
		return
	}
	effective := int64(s.autoFlushBytes)
	// Clamp to 90% of the server-advertised cap. The 10% headroom
	// covers schema + dict-delta encoding overhead the soft trigger
	// does not see. cap <= 0 means the server advertised none (older
	// build or async-pending initial connect): keep the configured
	// value for this term.
	if cap > 0 {
		if safe := int64(cap) * 9 / 10; safe < effective {
			effective = safe
		}
	}
	// Clamp to 90% of the per-segment frame cap as well. A frame larger
	// than a single segment can hold can never be appended to the cursor
	// engine, so the soft trigger must fire before a batch crosses it —
	// independent of the server cap. This is what keeps the shipped
	// defaults (8 MiB trigger over a 4 MiB segment) from self-wedging.
	// Fixed for the sender's lifetime, so re-applying it on every
	// transport swap is a no-op past the first. maxFrameBytes is 0 in
	// the hand-built test senders that exercise the server-cap table in
	// isolation, so this term short-circuits there.
	if s.maxFrameBytes > 0 {
		if safe := s.maxFrameBytes * 9 / 10; safe < effective {
			effective = safe
		}
	}
	s.effectiveAutoFlushBytes.Store(effective)
}

func (s *qwpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

// --- LineSender interface: Flush ---

func (s *qwpLineSender) Flush(ctx context.Context) error {
	_, err := s.FlushAndGetSequence(ctx)
	return err
}

// flushForReturn readies the delegate for return to the pool: it captures and
// clears any latched fluent-API error, drops any in-progress (un-At'd) row,
// and — crucially — still flushes the committed rows into the cursor engine so
// the slot returns clean. It mirrors closeCursor's producer-state handling
// (capture latch, drop open row, enqueue pending rows) minus the teardown; the
// slot stays live for the next borrower. Producer-goroutine only.
//
// The pool must route lease return through this, not Flush:
// FlushAndGetSequence early-returns a latched error ahead of its pending-rows
// branch, so a lease closed with a committed row AND a latched error (e.g. one
// illegal column name on untrusted input, after a good row) would leave that
// committed row buffered in the recycled slot — the next borrower would then
// silently ship it as a phantom/duplicate row. A latched error must surface
// (retain-on-error) without suppressing the flush of already-committed rows.
//
// Returns the first error: the captured latch (the original user-facing cause)
// takes precedence, else an enqueue or terminal I/O failure. On a latch-only
// error the committed rows are flushed regardless, and any terminal fault is
// surfaced so the pool discards the slot instead of recycling a dead one.
//
// The first return value, retained, reports whether committed rows are STILL
// buffered un-enqueued after the attempt — the enqueue hit the backpressure
// append deadline (e.g. the cursor ring saturated during an outage) or the
// engine was closed, neither of which is a terminal send-loop HALT. Such a slot
// is DIRTY: recycling it would ship this borrower's rows under the next
// borrower's FSN (borrower-isolation). The pool must discard it rather than
// recycle. retained is only meaningful on the producer goroutine; a call from
// a dispatcher goroutine can't inspect producer state, so it reports true —
// unable to prove the slot clean, it errs on discard.
func (s *qwpLineSender) flushForReturn(ctx context.Context) (retained bool, err error) {
	if s.closed.Load() {
		return false, errClosedSenderFlush
	}
	if s.calledFromDispatcherGoroutine() {
		// Running on a dispatcher goroutine (Close invoked from inside a
		// user callback): touching producer state (lastErr / hasTable /
		// pendingRowCount / buffers) would race the producer.
		// Surface only the latched terminal error, like closeCursor does. We
		// can't safely read pendingRowCount, so we can't prove the slot is
		// clean either — report retained=true so the pool discards it instead
		// of recycling a possibly-dirty slot under the next borrower.
		return true, s.cursorSendLoop.sendLoopCheckError()
	}
	firstErr := s.lastErr
	s.lastErr = nil
	if s.hasTable {
		if s.currentTable != nil {
			s.currentTable.cancelRow()
		}
		s.hasTable = false
		s.currentTable = nil
		s.cachedDesignatedTs = nil
	}
	if s.pendingRowCount > 0 {
		if err := s.enqueueCursor(ctx); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			s.resetAfterFlush()
		}
	}
	// Surface a terminal I/O error the send loop latched (in the background
	// or during the enqueue above) so a poisoned slot is discarded, not
	// recycled — parity with flushCursor's tail. A captured fluent latch
	// takes precedence as the original user-facing cause.
	if firstErr == nil {
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			firstErr = err
		}
	}
	// pendingRowCount > 0 here means enqueueCursor could not seal the
	// committed rows (backpressure deadline / engine closed); enqueueCursor
	// leaves them retained for a later flush. The slot is dirty.
	return s.pendingRowCount > 0, firstErr
}

// FlushAndGetSequence implements QwpSender.FlushAndGetSequence.
// Flushes any pending rows and returns the published FSN — the
// upper bound on any SenderError.ToFsn that could surface for this
// batch.
//
// It does NOT wait for the server ACK (Java decision #1 in
// design/qwp-cursor-durability.md — "flush() never waits for ACK;
// ACKs are async"): it returns once the batch is published into the
// cursor engine (in-RAM for memory mode, on-disk for SF) and the
// send loop delivers + replays it in the background. Callers
// wanting server-ACK confirmation pair the returned FSN with
// AwaitAckedFsn.
func (s *qwpLineSender) FlushAndGetSequence(ctx context.Context) (int64, error) {
	if s.closed.Load() {
		return -1, errClosedSenderFlush
	}
	if s.calledFromDispatcherGoroutine() {
		// Flush() invoked from inside a user callback (error handler,
		// connection listener, or progress handler) runs on that
		// dispatcher goroutine. The callback's documented use of Flush()
		// is to surface the latched terminal error promptly (it is
		// latched before the error handler runs). We must not read or flush
		// producer-owned state (hasTable / pendingRowCount / tableBuffers
		// / the encoder) from this goroutine — that races the producer,
		// the producer-state hazard. Surface any latched error and
		// return the published FSN without touching producer state.
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			return -1, err
		}
		return s.cursorEngine.enginePublishedFsn(), nil
	}
	// Drain any latched fluent-API error (a Symbol/*Column/Table
	// validation failure) ahead of the hasTable/pendingRowCount
	// branches, fulfilling the latch contract's "surfaces on the next
	// At/AtNow/Flush". Taking it first stops a real producer-side fault
	// from being masked by errFlushWithPendingMessage or dropped on a
	// Flush-then-Close. Cancel the in-progress row exactly as At does so
	// clearing the latch can't strand a partial row a later At would
	// commit. The clear happens only when the latch is set, so a Flush
	// with no latched error performs no producer-state write at all and
	// stays a pure read — several goroutines sampling a quiescent
	// (post-HALT) sender to observe the latched terminal error therefore
	// race only on read-only state. The calledFromDispatcherGoroutine path above
	// skips this: lastErr is producer-owned, and reading it from the
	// dispatcher goroutine races the producer.
	if err := s.lastErr; err != nil {
		s.lastErr = nil
		if s.currentTable != nil {
			s.currentTable.cancelRow()
		}
		s.cachedDesignatedTs = nil
		s.hasTable = false
		s.currentTable = nil
		return -1, err
	}
	if s.hasTable {
		return -1, errFlushWithPendingMessage
	}
	if s.pendingRowCount == 0 {
		// Nothing to encode, so skip straight to flushCursor's tail:
		// surface any terminal I/O error the loop has recorded (so
		// producers don't keep silently buffering into a dead engine),
		// then return the current published FSN. Same no-ACK-wait
		// contract as the pending-rows path below — this branch only
		// elides the empty encode/append.
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			return -1, err
		}
		return s.cursorEngine.enginePublishedFsn(), nil
	}
	if err := s.flushCursor(ctx); err != nil {
		// flushCursor resets the table buffers as soon as the enqueue
		// succeeds (the rows are sealed in a segment), so an error here
		// is one of two already-handled cases:
		//   - enqueueCursor failed before sealing — ring full + wire
		//     stalled past the append deadline, or ctx cancelled: the
		//     rows were never persisted and are RETAINED for the next
		//     flush attempt (or, in SF mode, recoverable by reopening
		//     the same sf_dir). Mirrors Java's flushPendingRows()
		//     reset-after-seal contract.
		//   - enqueueCursor sealed the rows but the eager error check
		//     then surfaced a HALT latched by a previous batch: the
		//     buffers were already reset inside flushCursor, so the
		//     published rows are not double-written when the user
		//     re-sends after the documented close+rebuild recovery.
		return -1, err
	}
	return s.cursorEngine.enginePublishedFsn(), nil
}

// resetAfterFlush clears the table buffers touched this cycle and
// resets counters. Only dirtyTables can hold rows, so resetting the
// rest of the tableBuffers map would be wasted work; the dirty flag is
// cleared here (the one place that empties the list) so the next
// Table() re-lists the buffer.
func (s *qwpLineSender) resetAfterFlush() {
	for _, tb := range s.dirtyTables {
		tb.reset()
		tb.dirty = false
	}
	s.dirtyTables = s.dirtyTables[:0]
	s.pendingRowCount = 0
	s.pendingBytes = 0
	s.batchMaxSymbolId = s.maxSentSymbolId
	// Defense in depth: tb.reset() keeps the column structure but
	// sets committedColumnCount=0, so a post-flush cancelRow would
	// wipe the designated-TS column out of tb.columns. Drop the
	// cache here so the first row after a flush always re-runs
	// getOrCreateDesignatedTimestamp.
	s.cachedDesignatedTs = nil

	// Refresh flush deadline.
	if s.autoFlushInterval > 0 {
		s.flushDeadline = time.Now().Add(s.autoFlushInterval)
	} else {
		s.flushDeadline = time.Time{}
	}
}

// --- LineSender interface: Close ---

func (s *qwpLineSender) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return errDoubleSenderClose
	}
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
	if val < 0 || val > 0xFFFF {
		s.lastErr = fmt.Errorf("qwp: CharColumn() CHAR rune %U does not fit in a UTF-16 code unit", val)
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
	// A nil slice is a NULL array (carried in the null bitmap); a non-nil
	// empty slice is a zero-length 1D array (shape {0}), which QuestDB
	// stores as a distinct, non-null value.
	if values == nil {
		col.addNull()
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

	// A nil slice is a NULL array; a non-nil empty slice is an empty 2D
	// array (shape {0, 0}), stored distinct from NULL.
	if values == nil {
		col.addNull()
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
	// Validate row regularity before reserving so the streamed write
	// fills exactly the reserved payload — no intermediate flat copy.
	for _, row := range values {
		if len(row) != dim1 {
			s.lastErr = fmt.Errorf("qwp: irregular 2D array: row lengths differ")
			return s
		}
	}
	col.addLongArray2D(dim0, dim1, values)
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

	// A nil slice is a NULL array; a non-nil empty slice is an empty 3D
	// array (shape {0, 0, 0}), stored distinct from NULL.
	if values == nil {
		col.addNull()
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
	// Validate shape regularity before reserving so the streamed write
	// fills exactly the reserved payload — no intermediate flat copy.
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
		}
	}
	col.addLongArray3D(dim0, dim1, dim2, values)
	return s
}
