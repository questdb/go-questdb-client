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
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"
)

// qwpSfDefaultSenderId is used when sf_dir is set but sender_id is
// not. Single-sender deployments get zero-config; multi-sender
// users must override per spec.
const qwpSfDefaultSenderId = "default"

// qwpSfDefaultMaxBytes is the default per-segment cap. Mirrors
// Java's 4 MiB.
const qwpSfDefaultMaxBytes int64 = 4 * 1024 * 1024

// qwpSfDefaultMaxTotalBytes is the default total cap when sf_dir
// is set. Mirrors Java's 10 GiB SF default.
const qwpSfDefaultMaxTotalBytes int64 = 10 * 1024 * 1024 * 1024

// qwpSfDefaultMemoryMaxTotalBytes is the default total cap when
// sf_dir is empty (memory mode cursor). Mirrors Java's 128 MiB
// memory-mode default.
const qwpSfDefaultMemoryMaxTotalBytes int64 = 128 * 1024 * 1024

// qwpSfDefaultCloseFlushTimeout mirrors Java's 5-second default.
const qwpSfDefaultCloseFlushTimeout = 5 * time.Second

// qwpCursorMode reports whether the sender is wired to the cursor
// engine + send loop. Memory mode (the only mode in this PR's
// initial cut) returns false.
func (s *qwpLineSender) qwpCursorMode() bool {
	return s.cursorEngine != nil
}

// newQwpCursorLineSender constructs a sender that publishes its
// flushed batches into the supplied cursor engine. The send loop
// (already started) is responsible for transmitting frames and
// processing ACKs; the sender itself never opens a WebSocket
// connection. Used by the SF (`sf_dir=...`) and — eventually —
// memory-mode cursor paths.
//
// The caller retains ownership of the engine and send loop until
// Close, at which point the sender takes responsibility for
// draining + closing them in order. Reusing an engine across
// senders is not supported.
//
// closeFlushTimeout bounds Close's wait for the engine's ackedFsn
// to catch up to publishedFsn. 0 or negative means "fast close"
// (skip the drain — pending data lives on disk and will be replayed
// on the next sender start in SF mode, or is lost in memory mode).
func newQwpCursorLineSender(
	autoFlushRows int,
	autoFlushInterval time.Duration,
	autoFlushBytes int,
	maxBufSize int,
	maxSchemasPerConnection int,
	cursorEngine *qwpSfCursorEngine,
	cursorSendLoop *qwpSfSendLoop,
	closeFlushTimeout time.Duration,
) (*qwpLineSender, error) {
	if cursorEngine == nil || cursorSendLoop == nil {
		return nil, errors.New("qwp/cursor: engine and send loop must be non-nil")
	}
	s := &qwpLineSender{
		tableBuffers:            make(map[string]*qwpTableBuffer),
		globalSymbols:           make(map[string]int32),
		maxSentSymbolId:         -1,
		batchMaxSymbolId:        -1,
		nextSchemaId:            0,
		maxSentSchemaId:         -1,
		batchMaxSchemaId:        -1,
		autoFlushRows:           autoFlushRows,
		autoFlushInterval:       autoFlushInterval,
		autoFlushBytes:          autoFlushBytes,
		maxBufSize:              maxBufSize,
		maxSchemasPerConnection: maxSchemasPerConnection,
		inFlightWindow: 1,
		closeTimeout:   closeFlushTimeout,
		cursorEngine:   cursorEngine,
		cursorSendLoop: cursorSendLoop,
	}
	// Single encoder slot is enough — the cursor engine takes a copy
	// of the bytes via tryAppend, so the encoder buffer can be reused
	// immediately. No double-buffering needed here.
	s.encoder.wb.preallocate(qwpDefaultMicrobatchBufSize)
	return s, nil
}

// newQwpCursorLineSenderFromConf wires a cursor-mode sender from
// the parsed config. Resolves SF defaults, builds the cursor
// engine + send loop, runs an initial connect (optionally with
// retry-on-failure), and returns a sender ready for the user.
//
// Owns the cursor engine and the send loop; both are torn down on
// sender.Close.
func newQwpCursorLineSenderFromConf(ctx context.Context, conf *lineSenderConfig, address string, opts qwpTransportOpts) (LineSender, error) {
	// Resolve defaults.
	senderId := conf.senderId
	if senderId == "" {
		senderId = qwpSfDefaultSenderId
	}
	sfMaxBytes := conf.sfMaxBytes
	if sfMaxBytes <= 0 {
		sfMaxBytes = qwpSfDefaultMaxBytes
	}
	sfMaxTotalBytes := conf.sfMaxTotalBytes
	if sfMaxTotalBytes <= 0 {
		sfMaxTotalBytes = qwpSfDefaultMaxTotalBytes
	}
	if sfMaxTotalBytes < sfMaxBytes {
		// Caught earlier in sanitizeQwpConf, but defend in depth
		// since defaults could in principle skew this.
		return nil, fmt.Errorf("sf_max_total_bytes (%d) must be >= sf_max_bytes (%d)",
			sfMaxTotalBytes, sfMaxBytes)
	}
	appendDeadline := time.Duration(conf.sfAppendDeadlineMillis) * time.Millisecond
	if appendDeadline <= 0 {
		appendDeadline = qwpSfEngineDefaultAppendDeadline
	}
	reconnectMaxDuration := time.Duration(conf.reconnectMaxDurationMillis) * time.Millisecond
	if reconnectMaxDuration <= 0 {
		reconnectMaxDuration = qwpSfDefaultReconnectMaxDuration
	}
	reconnectInitialBackoff := time.Duration(conf.reconnectInitialBackoffMillis) * time.Millisecond
	if reconnectInitialBackoff <= 0 {
		reconnectInitialBackoff = qwpSfDefaultReconnectInitialBackoff
	}
	reconnectMaxBackoff := time.Duration(conf.reconnectMaxBackoffMillis) * time.Millisecond
	if reconnectMaxBackoff <= 0 {
		reconnectMaxBackoff = qwpSfDefaultReconnectMaxBackoff
	}
	closeFlushTimeout := qwpSfDefaultCloseFlushTimeout
	if conf.closeFlushTimeoutSet {
		// User explicitly set the value. <= 0 means "fast close".
		closeFlushTimeout = time.Duration(conf.closeFlushTimeoutMillis) * time.Millisecond
	}

	// Slot path = <sfDir>/<senderId>/.
	slotPath := filepath.Join(conf.sfDir, senderId)

	// Build the cursor engine first — it owns the slot lock and on-disk
	// recovery.
	engine, err := qwpSfNewCursorEngine(slotPath, sfMaxBytes, sfMaxTotalBytes, appendDeadline)
	if err != nil {
		return nil, err
	}

	// Reconnect factory: rebuilds a fresh transport against the same
	// address+opts on every call. Captures the dumpWriter so the
	// post-reconnect transport also dumps if the user opted in.
	factory := qwpSfBuildReconnectFactory(address, opts, conf.dumpWriter)

	// Initial connect — apply retry-with-backoff iff opted in.
	var transport *qwpTransport
	if conf.initialConnectRetry {
		transport, err = qwpSfConnectWithRetry(ctx, factory,
			reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff)
	} else {
		transport, err = factory(ctx)
	}
	if err != nil {
		_ = engine.engineClose()
		return nil, err
	}

	loop := qwpSfNewSendLoop(engine, transport, factory,
		qwpSfDefaultParkInterval,
		reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff)
	loop.sendLoopStart()

	s, err := newQwpCursorLineSender(
		conf.autoFlushRows,
		conf.autoFlushInterval,
		conf.autoFlushBytes,
		conf.maxBufSize,
		conf.maxSchemasPerConnection,
		engine, loop,
		closeFlushTimeout,
	)
	if err != nil {
		_ = loop.sendLoopClose()
		_ = engine.engineClose()
		return nil, err
	}
	s.fileNameLimit = conf.fileNameLimit
	s.encoder.gorillaDisabled = conf.gorillaDisabled

	// Orphan adoption (drain_orphans=on). At foreground startup,
	// scan <sf_dir>/* for sibling slots that hold unacked data and
	// spawn a drainer per orphan, capped at max_background_drainers
	// concurrent goroutines. Failures drop a .failed sentinel into
	// the slot so future foreground starts skip it.
	if conf.drainOrphans {
		maxDrainers := conf.maxBackgroundDrainers
		if maxDrainers <= 0 {
			maxDrainers = 4 // matches Java default
		}
		ownSlot := filepath.Base(slotPath)
		orphans := qwpSfScanOrphans(conf.sfDir, ownSlot)
		if len(orphans) > 0 {
			pool := qwpSfNewDrainerPool(maxDrainers)
			for _, orphan := range orphans {
				drainer := qwpSfNewOrphanDrainer(
					orphan,
					sfMaxBytes, sfMaxTotalBytes,
					factory,
					reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff,
				)
				_ = pool.drainerPoolSubmit(ctx, drainer)
			}
			s.drainerPool = pool
		}
	}

	return s, nil
}

// qwpSfBuildReconnectFactory returns a factory that dials the given
// address with the given options on each call. Used for both the
// initial connect (when initial_connect_retry is on) and subsequent
// reconnects from the send loop.
func qwpSfBuildReconnectFactory(address string, opts qwpTransportOpts, dumpWriter io.Writer) qwpSfReconnectFactory {
	return func(ctx context.Context) (*qwpTransport, error) {
		var t qwpTransport
		t.dumpWriter = dumpWriter
		if err := t.connect(ctx, address, opts); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

// flushCursor encodes the pending rows as a self-sufficient QWP
// frame, appends it to the cursor engine, and (for explicit
// Flush() callers) blocks until ackedFsn catches up. Used by
// Flush and auto-flush in cursor mode.
//
// Self-sufficient = full schema definitions for every table + full
// symbol-dict delta from id 0 (mirrors Java decision #14). The
// frame must replay correctly against any fresh server connection
// (post-reconnect, post-restart, drainer adopting an orphan slot)
// — refs to schema/symbol IDs the new server has never seen would
// be unrecoverable. Producer-side maxSentSchemaId / maxSentSymbolId
// retention is therefore a no-op on the cursor path.
//
// The Go API contract — `Flush() returns once the server has
// confirmed the batch` — predates the cursor unification and is
// what existing users rely on. We deviate from the Java spec's
// `flush() never waits for ACK` here in favor of preserving the
// Go contract. Use auto-flush for non-blocking enqueue.
func (s *qwpLineSender) flushCursor(ctx context.Context) error {
	if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
		return err
	}
	tables, err := s.buildTableEncodeInfo()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	// Encoder slot 0 is reused on every flush — engine.tryAppend
	// copies the bytes into the segment, so the encoder buffer is
	// safe to overwrite immediately.
	encoded := s.encoder.encodeMultiTableWithDeltaDict(
		tables,
		s.globalSymbolList,
		-1, // maxSentSymbolId=-1 → emit the full dict from id 0
		s.batchMaxSymbolId,
	)
	// engineAppendBlocking spins on backpressure for up to the
	// engine's deadline OR until ctx fires, whichever comes first.
	// The synchronous call avoids the orphan-goroutine race against
	// the encoder buffer (which is reused on the next flush).
	if _, err := s.cursorEngine.engineAppendBlocking(ctx, encoded); err != nil {
		return err
	}
	// Surface any wire failure observed during the append window —
	// the loop may have hit a server-rejected status that won't be
	// fixed by reconnecting.
	if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
		return err
	}
	// Drain barrier: wait for the server to ACK every published
	// frame. Bounded by ctx; falls through on a terminal loop
	// error so the producer surfaces it immediately.
	if err := s.waitCursorEmpty(ctx); err != nil {
		return err
	}
	// Bump the producer-side ACK trackers. Cursor frames are
	// self-sufficient so this is informational only — we never
	// emit refs — but tests and external observers still inspect
	// these counters to confirm a flush has been ACK'd by the
	// server.
	if s.batchMaxSchemaId > s.maxSentSchemaId {
		s.maxSentSchemaId = s.batchMaxSchemaId
	}
	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}
	return nil
}

// enqueueCursor is the auto-flush path's append-only counterpart
// of flushCursor. It encodes pending rows and appends them into
// the cursor engine, but does NOT wait for ACKs — so the user
// goroutine isn't blocked on every auto-flush trigger. Mirrors the
// Java client's flushPendingRows contract: schema and symbol
// trackers advance optimistically because the send loop is
// terminal on I/O error (ioErr poisons every subsequent call), so
// stale tracker state cannot reach the wire.
func (s *qwpLineSender) enqueueCursor(ctx context.Context) error {
	if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
		return err
	}
	tables, err := s.buildTableEncodeInfo()
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	encoded := s.encoder.encodeMultiTableWithDeltaDict(
		tables,
		s.globalSymbolList,
		-1, // self-sufficient: full dict from id 0
		s.batchMaxSymbolId,
	)
	if _, err := s.cursorEngine.engineAppendBlocking(ctx, encoded); err != nil {
		return err
	}
	if s.batchMaxSchemaId > s.maxSentSchemaId {
		s.maxSentSchemaId = s.batchMaxSchemaId
	}
	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}
	return nil
}

// waitCursorEmpty blocks until ackedFsn ≥ publishedFsn, ctx
// cancels, or the send loop records a terminal error. Unlike
// waitCursorDrain it has no internal timeout — Flush is bounded by
// the user's ctx, not by closeFlushTimeout.
func (s *qwpLineSender) waitCursorEmpty(ctx context.Context) error {
	const pollInterval = 5 * time.Millisecond
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		if s.cursorEngine.engineAckedFsn() >= s.cursorEngine.enginePublishedFsn() {
			return nil
		}
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			return err
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// buildTableEncodeInfo collects non-empty tables, assigns fresh
// schema IDs to any that lack one, and emits every table in FULL
// schema mode. Mirrors the Java client's "self-sufficient frames"
// contract — refs to schema/symbol IDs the new server has never
// seen would be unrecoverable on replay (post-reconnect, post-
// restart, drainer adopting an orphan slot), so the cursor wire
// path always carries the schema in full.
//
// Schema IDs are still assigned monotonically so the connection-
// scoped server-side registry stays consistent across the lifetime
// of a single connection; but useSchemaRef is forced to false on
// every encode regardless of maxSentSchemaId.
func (s *qwpLineSender) buildTableEncodeInfo() ([]qwpTableEncodeInfo, error) {
	s.encodeInfoBuf = s.encodeInfoBuf[:0]
	batchMax := s.maxSentSchemaId
	for _, tb := range s.tableBuffers {
		if tb.rowCount == 0 {
			continue
		}
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
		// Cursor path forces full schema on every batch — see
		// "self-sufficient frames" decision (Java spec #14).
		mode := qwpSchemaModeFull
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

// closeCursor drains the cursor engine and closes the send loop.
// Returns the first non-nil error from drain / loop shutdown /
// engine close. Always best-effort: every subsystem is asked to
// close even if an earlier step errored.
//
// Drain semantics:
//   - closeFlushTimeout > 0: block up to that long for ackedFsn ≥
//     publishedFsn. Logs a warning on timeout (returns nil and
//     proceeds with shutdown — pending data is on disk and will
//     replay on the next sender start in SF mode, or is lost in
//     memory mode).
//   - closeFlushTimeout <= 0: skip the drain entirely (fast close).
func (s *qwpLineSender) closeCursor(ctx context.Context) error {
	// Encode any pending rows from the open API call into the engine
	// first. Drop the pending in-progress row (no At/AtNow yet) the
	// same way Close does in memory mode.
	if s.hasTable {
		if s.currentTable != nil {
			s.currentTable.cancelRow()
		}
		s.hasTable = false
		s.currentTable = nil
	}
	var firstErr error
	if s.pendingRowCount > 0 {
		if err := s.flushCursor(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		s.resetAfterFlush()
	}
	// Wait for drain.
	if s.closeTimeout > 0 {
		if err := s.waitCursorDrain(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Stop the send loop (closes its current transport).
	if err := s.cursorSendLoop.sendLoopClose(); err != nil && firstErr == nil {
		firstErr = err
	}
	// Close the engine (closes ring, manager if owned, and slot lock).
	if err := s.cursorEngine.engineClose(); err != nil && firstErr == nil {
		firstErr = err
	}
	// Stop the drainer pool last — drainers may still be using the
	// reconnect factory (which captures the foreground's address +
	// auth) and we want their wire shutdowns to overlap with the
	// engine teardown rather than serialize after it.
	if s.drainerPool != nil {
		s.drainerPool.drainerPoolClose()
	}
	return firstErr
}

// waitCursorDrain blocks until ackedFsn ≥ publishedFsn, the
// send-loop reports a terminal error, or the user's ctx /
// closeFlushTimeout expires. On timeout, returns nil so the caller
// (closeCursor) proceeds with shutdown rather than failing — the
// data is durable on disk in SF mode and will be replayed.
func (s *qwpLineSender) waitCursorDrain(ctx context.Context) error {
	deadline := time.Now().Add(s.closeTimeout)
	timer := time.NewTimer(s.closeTimeout)
	defer timer.Stop()
	const pollInterval = 5 * time.Millisecond
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		if s.cursorEngine.engineAckedFsn() >= s.cursorEngine.enginePublishedFsn() {
			return nil
		}
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		select {
		case <-tick.C:
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
