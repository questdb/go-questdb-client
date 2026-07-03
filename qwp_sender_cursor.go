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
	cursorEngine *qwpSfCursorEngine,
	cursorSendLoop *qwpSfSendLoop,
	closeFlushTimeout time.Duration,
) (*qwpLineSender, error) {
	if cursorEngine == nil || cursorSendLoop == nil {
		return nil, errors.New("qwp/cursor: engine and send loop must be non-nil")
	}
	s := &qwpLineSender{
		tableBuffers:      make(map[string]*qwpTableBuffer),
		globalSymbols:     make(map[string]int32),
		maxSentSymbolId:   -1,
		batchMaxSymbolId:  -1,
		autoFlushRows:     autoFlushRows,
		autoFlushInterval: autoFlushInterval,
		autoFlushBytes:    autoFlushBytes,
		maxBufSize:        maxBufSize,
		inFlightWindow:    1,
		closeTimeout:      closeFlushTimeout,
		cursorEngine:      cursorEngine,
		cursorSendLoop:    cursorSendLoop,
	}
	// Record the per-segment frame cap (constant for the sender's
	// lifetime) so the byte-trigger clamp and the flush-time drop guard
	// bound batches to what a single segment can actually hold.
	s.maxFrameBytes = cursorEngine.engineMaxFrameBytes()
	// Seed effectiveAutoFlushBytes via the same clamp the transport-swap
	// callback applies, with no transport yet (server cap unknown). With
	// no server cap this yields min(autoFlushBytes, maxFrameBytes*9/10),
	// already segment-safe before the first connect. Covers the test
	// paths that build a sender directly without wiring the callback, and
	// the window in the conf-driven paths between construction and the
	// callback install; those then refine the server-cap term via
	// applyServerBatchSizeLimit using the connected transport's cap.
	s.applyServerBatchSizeLimit(nil)
	// Single encoder slot is enough — the cursor engine takes a copy
	// of the bytes via tryAppend, so the encoder buffer can be reused
	// immediately. No double-buffering needed here.
	s.encoder.wb.preallocate(qwpDefaultMicrobatchBufSize)
	return s, nil
}

// newQwpCursorLineSenderFromConf wires a cursor-mode sender from the
// parsed config. Handles BOTH memory mode (sf_dir empty → RAM-backed
// cursor engine) and store-and-forward (sf_dir set → mmapped on-disk
// segments). Resolves the mode-specific defaults, builds the cursor
// engine + send loop with the shared multi-host failover plumbing
// (host tracker, endpoint factory, initial-connect mode, reconnect
// budgets), runs the initial connect (optionally with
// retry-on-failure), and returns a sender ready for the user.
//
// Owns the cursor engine and the send loop; both are torn down on
// sender.Close.
func newQwpCursorLineSenderFromConf(ctx context.Context, conf *lineSenderConfig, opts qwpTransportOpts) (LineSender, error) {
	// Resolve defaults. memMode (no sf_dir) selects a RAM-backed cursor
	// engine (empty slot path) and the smaller memory-mode total-bytes
	// ceiling; everything else — including the multi-host failover
	// plumbing below — is shared with store-and-forward.
	memMode := conf.sfDir == ""
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
		if memMode {
			sfMaxTotalBytes = qwpSfDefaultMemoryMaxTotalBytes
		}
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

	// Slot path = <sfDir>/<senderId>/. Empty in memory mode → the
	// cursor engine allocates RAM-backed segments instead of opening
	// mmapped files under the slot directory.
	slotPath := ""
	if !memMode {
		slotPath = filepath.Join(conf.sfDir, senderId)
	}

	// Build the cursor engine first — it owns the slot lock and on-disk
	// recovery.
	engine, err := qwpSfNewCursorEngine(slotPath, sfMaxBytes, sfMaxTotalBytes, appendDeadline)
	if err != nil {
		return nil, err
	}

	// Failover plumbing (failover.md §2 / §13.6). The tracker is
	// shared across every caller drawing from this addr= list: the
	// foreground I/O loop, the initial-connect-sync path, and each
	// orphan drainer spawned below. Per-caller `previousIdx` slots
	// (§2.3) live on the qwpSfSendLoop instances, not on the tracker
	// — mid-stream demotes stay scoped to their loop while PickNext
	// classifications inform every caller on the next walk.
	scheme := "ws"
	if conf.tlsMode != tlsDisabled {
		scheme = "wss"
	}
	// The ingress endpoint never sends SERVER_INFO and the client never
	// expects one (per the wire spec, ingress is role- and zone-blind);
	// role/zone-aware endpoint selection is egress-only. Pass "" for
	// clientZone and qwpTargetAny for the role filter so every reachable
	// host binds regardless of the configured zone=/target=. Both hints
	// are accepted at config time but inert on ingest; the server's
	// 421 + X-QuestDB-Role upgrade reject keeps writes off replicas
	// (see qwp_sf_round_walk.go).
	tracker := newQwpHostTracker(len(conf.endpoints), "", qwpTargetAny)
	factory := qwpSfBuildEndpointFactory(conf.endpoints, scheme, opts, conf.dumpWriter)

	// Initial connect — three modes:
	//   - InitialConnectOff:   one single-round walk through every
	//                          configured endpoint, terminal if all
	//                          fail (no inter-round retry).
	//   - InitialConnectSync:  retry-with-backoff on the calling goroutine.
	//   - InitialConnectAsync: skip the dial here; the I/O goroutine
	//                          dials in-band on its first iteration.
	//                          The producer experiences backpressure
	//                          (engineAppendBlocking spins) until the
	//                          wire comes up.
	var (
		transport       *qwpTransport
		initialBoundIdx = -1
	)
	switch conf.initialConnectMode {
	case InitialConnectSync:
		transport, initialBoundIdx, err = qwpSfConnectWithRetry(ctx, factory, tracker,
			reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff, true, nil)
	case InitialConnectAsync:
		transport = nil
	default: // InitialConnectOff
		// Single-round walk through every configured endpoint — no
		// inter-host backoff, no retry across rounds. Mirrors Java's
		// QwpWebSocketSender.buildAndConnect (failover.md §1.2 /
		// §4.2): multi-host config gets a full sweep on initial
		// connect, but only one sweep. Use initial_connect_retry for
		// retry-with-backoff across multiple sweeps.
		walkStart := time.Now()
		rr := qwpSfRunSingleRound(ctx, nil, qwpSfRoundWalkParams{
			Factory:                 factory,
			Tracker:                 tracker,
			Endpoints:               conf.endpoints,
			DurableMismatchTerminal: true,
		}, -1)
		switch {
		case rr.Transport != nil:
			transport = rr.Transport
			initialBoundIdx = rr.Idx
		case rr.Terminal != nil:
			err = fmt.Errorf("qwp/sf: WebSocket upgrade failed (won't retry): %w", rr.Terminal)
		case rr.Cancelled != nil:
			err = rr.Cancelled
		default:
			// Round exhausted: every endpoint dialed without binding.
			err = fmt.Errorf("qwp/sf: initial connect failed; %w",
				buildExhaustedError(tracker, conf.endpoints,
					time.Since(walkStart), rr.Attempts, rr.LastError))
		}
	}
	if err != nil {
		// A terminal durable-ack mismatch on the InitialConnectOff / Sync
		// connect paths arrives as a plain error (Off wraps rr.Terminal in
		// fmt.Errorf; Sync via qwpSfConnectWithRetry). Upgrade it to the typed
		// *SenderError of category PROTOCOL_VIOLATION that WithRequestDurableAck
		// and QwpDurableAckMismatchError document — the async reconnect path
		// already latches this via connectWithBackoff. The underlying
		// *QwpDurableAckMismatchError is preserved as the SenderError cause, so
		// errors.As reaches either type.
		var mismatch *QwpDurableAckMismatchError
		if errors.As(err, &mismatch) {
			err = qwpSfUpgradeFailureSE(
				engine.engineAckedFsn()+1, engine.enginePublishedFsn(), mismatch)
		}
		_ = engine.engineClose()
		return nil, err
	}

	loop := qwpSfNewSendLoop(engine, transport, factory,
		qwpSfDefaultParkInterval,
		reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff)
	loop.sendLoopSetHostTracker(tracker, initialBoundIdx)
	engine.engineSetReconnectStatusGetter(loop.sendLoopReconnectStatus)
	engine.engineSetTerminalErrorGetter(loop.sendLoopCheckError)
	// Wire the user-configured server-error API knobs (Phase 5)
	// before sendLoopStart so they're visible from the receiver
	// goroutine the moment it starts.
	resolver := &qwpSfPolicyResolver{
		resolver: conf.errorPolicyResolver,
		perCat:   conf.errorPolicyPerCat,
		global:   conf.errorPolicyGlobal,
	}
	loop.sendLoopSetPolicyResolver(resolver)
	loop.sendLoopSetErrorHandler(conf.errorHandler, conf.errorInboxCapacity)
	loop.sendLoopSetEndpoints(conf.endpoints)
	loop.sendLoopSetConnectionListener(conf.connectionListener, conf.connectionListenerInboxCapacity)
	durableKeepalive := qwpDurableAckKeepaliveDefault
	if conf.durableAckKeepaliveMillisSet {
		durableKeepalive = time.Duration(conf.durableAckKeepaliveMillis) * time.Millisecond
	}
	loop.sendLoopSetDurableAck(conf.requestDurableAck, durableKeepalive, true)
	loop.sendLoopSetMaxFrameRejections(conf.maxFrameRejections)
	loop.sendLoopSetProgressHandler(conf.progressHandler, conf.errorInboxCapacity)

	s, err := newQwpCursorLineSender(
		conf.autoFlushRows,
		conf.autoFlushInterval,
		conf.autoFlushBytes,
		conf.maxBufSize,
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
	// Pre-size the encoder buffer for the microbatch role: the cursor
	// engine copies each frame on append so one encoder slot suffices,
	// but a large auto_flush_bytes warrants a bigger initial buffer to
	// avoid repeated grows on the hot path. The qwpDefaultMicrobatchBufSize
	// (1 MB) floor was already applied in newQwpCursorLineSender.
	if conf.autoFlushBytes*2 > qwpDefaultMicrobatchBufSize {
		s.encoder.wb.preallocate(conf.autoFlushBytes * 2)
	}
	// Seed the byte-trigger clamp from the initial transport (the
	// sync-connect branches above populated loop.transport; the
	// async branch leaves it nil and the first reconnect callback
	// will refresh) and install the swap callback so every
	// subsequent connect re-applies the clamp. Both happen before
	// sendLoopStart so the I/O goroutine sees the installed
	// callback on the very first swap.
	loop.sendLoopSetOnTransportSwap(s.applyServerBatchSizeLimit)
	s.applyServerBatchSizeLimit(loop.transport.Load())
	// Sync/Off connected synchronously at construction (transport != nil);
	// fire the one-shot CONNECTED now that the sender is fully built, so a
	// construction failure above never leaves a listener believing a sender
	// connected that never came back. The async path (transport == nil) fires
	// CONNECTED from connectWithBackoff instead.
	if transport != nil {
		loop.emitInitialConnected(initialBoundIdx)
	}
	loop.sendLoopStart()

	// Orphan adoption (drain_orphans=on). At foreground startup,
	// scan <sf_dir>/* for sibling slots that hold unacked data and
	// spawn a drainer per orphan, capped at max_background_drainers
	// concurrent goroutines. Failures drop a .failed sentinel into
	// the slot so future foreground starts skip it.
	//
	// `s` already owns engine + loop at this point. Any failure in
	// the orphan-setup block must close `s` (which closes both),
	// otherwise we leak the connected sender plus its I/O goroutine,
	// transport, and segment manager. defer+success flag covers
	// panics; explicit error returns cover any future error path
	// added below.
	if conf.drainOrphans {
		setupOK := false
		defer func() {
			if !setupOK {
				_ = s.closeCursor(ctx)
			}
		}()
		maxDrainers := conf.maxBackgroundDrainers
		if maxDrainers <= 0 {
			maxDrainers = 4 // matches Java default
		}
		ownSlot := filepath.Base(slotPath)
		// Exclude this sender's own slot always, plus any slot the pool fences
		// off as a live in-range sibling (Hazard G); nil fence on standalone.
		orphans := qwpSfScanOrphans(conf.sfDir, func(name string) bool {
			return name == ownSlot ||
				(conf.orphanDrainExclude != nil && conf.orphanDrainExclude(name))
		})
		if len(orphans) > 0 {
			pool := qwpSfNewDrainerPool(maxDrainers)
			// Publish the pool onto the sender before submitting any
			// drainers. A panic mid-loop then still routes through the
			// deferred closeCursor, whose drainerPoolClose() reaches the
			// pool and shuts down the drainers already submitted; leaving
			// the assignment past the loop would strand those goroutines
			// and their connections.
			s.drainerPool = pool
			// Drainer dials get a finite default TCP-connect deadline when
			// connect_timeout is unset: a black-holed SYN would otherwise
			// ride the OS connect timeout on a goroutine only pool close can
			// unwind. The foreground factory keeps the untimed default.
			drainerFactory := factory
			if opts.connectTimeoutMs <= 0 {
				drainerOpts := opts
				drainerOpts.connectTimeoutMs = qwpSfDrainerDefaultConnectTimeoutMs
				drainerFactory = qwpSfBuildEndpointFactory(
					conf.endpoints, scheme, drainerOpts, conf.dumpWriter)
			}
			for _, orphan := range orphans {
				drainer := qwpSfNewOrphanDrainer(
					orphan,
					sfMaxBytes, sfMaxTotalBytes,
					drainerFactory,
					tracker,
					reconnectMaxDuration, reconnectInitialBackoff, reconnectMaxBackoff,
				)
				// Set before submit (which starts the goroutine): a durable-ack
				// sender's drainers must also trim only on STATUS_DURABLE_ACK.
				drainer.durableAckMode = conf.requestDurableAck
				drainer.durableKeepalive = durableKeepalive
				drainer.maxFrameRejections = conf.maxFrameRejections
				drainer.listener = conf.backgroundDrainerListener
				_ = pool.drainerPoolSubmit(ctx, drainer)
			}
		}
		setupOK = true
	}

	return s, nil
}

// qwpSfBuildReconnectFactory returns a factory that dials the given
// address with the given options on each call. Used by drainers and
// legacy single-host paths; the idx parameter is accepted for
// signature symmetry with qwpSfBuildEndpointFactory and ignored.
func qwpSfBuildReconnectFactory(address string, opts qwpTransportOpts, dumpWriter io.Writer) qwpSfReconnectFactory {
	return func(ctx context.Context, _ int) (*qwpTransport, error) {
		var t qwpTransport
		t.dumpWriter = dumpWriter
		if err := t.connect(ctx, address, opts); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

// qwpSfBuildEndpointFactory returns a factory that dials the
// endpoint at the supplied idx. Used by the foreground SF loop's
// round-walk, where PickNext selects the host. Out-of-range idx
// returns an explicit error so a tracker bug surfaces loudly rather
// than dialing a random peer.
func qwpSfBuildEndpointFactory(endpoints []qwpEndpoint, scheme string, opts qwpTransportOpts, dumpWriter io.Writer) qwpSfReconnectFactory {
	return func(ctx context.Context, idx int) (*qwpTransport, error) {
		if idx < 0 || idx >= len(endpoints) {
			return nil, fmt.Errorf("qwp/sf: endpoint index %d out of range [0, %d)",
				idx, len(endpoints))
		}
		var t qwpTransport
		t.dumpWriter = dumpWriter
		wsURL := scheme + "://" + endpoints[idx].String()
		if err := t.connect(ctx, wsURL, opts); err != nil {
			return nil, err
		}
		return &t, nil
	}
}

// flushCursor is the explicit-Flush() wire path. It shares
// encoding and the (non-blocking, no-ACK-wait) engine append with
// auto-flush via enqueueCursor, then eagerly surfaces any wire
// failure observed during the append window so a terminal error
// reaches the producer immediately instead of on its next call.
// Mirrors Java: flushAndGetSequence() = flushPendingRows() +
// checkError() (design/qwp-cursor-durability.md decision #1 —
// "flush() never waits for ACK; ACKs are async"). Callers wanting
// server-ACK confirmation pair FlushAndGetSequence with
// AwaitAckedFsn.
//
// A successful enqueue resets the table buffers BEFORE the eager
// error check. Once enqueueCursor returns nil the rows are sealed in
// a segment (an FSN is assigned, the frame is queued for replay), so
// they are no longer pending. The eager check can still return an
// error — a HALT latched by a PREVIOUS batch in the window between
// enqueueCursor's own pre-append check and this one — and that error
// is for an already-published batch. Resetting first keeps those rows
// from being retained: re-sending them after the documented
// close+rebuild recovery would double-write the batch once the SF
// slot replays the sealed frame. Mirrors Java flushPendingRows
// resetting before checkError() throws.
func (s *qwpLineSender) flushCursor(ctx context.Context) error {
	if err := s.enqueueCursor(ctx); err != nil {
		return err
	}
	s.resetAfterFlush()
	return s.cursorSendLoop.sendLoopCheckError()
}

// enqueueCursor encodes the pending rows as a self-sufficient QWP
// frame and appends it to the cursor engine. It does NOT wait for
// the server ACK (Java decision #1 in
// design/qwp-cursor-durability.md: "flush() never waits for ACK;
// ACKs are async") — the frame is durable once appended (in-RAM
// for memory mode, on-disk for SF) and the send loop drains +
// replays it in the background. Shared by the auto-flush trigger
// and by flushCursor (explicit Flush()), so the user goroutine is
// never blocked on a server round-trip.
//
// Self-sufficient = full schema definitions for every table + full
// symbol-dict delta from id 0 (Java decision #14). The frame must
// replay correctly against any fresh server connection (post-
// reconnect, post-restart, drainer adopting an orphan slot) — refs
// to schema/symbol IDs the new server has never seen would be
// unrecoverable.
//
// Schema-side: every table block carries its full inline column
// definitions. There is no producer-side schema registry to advance.
//
// Symbol-side: the dict uses a delta encoding (varint-prefixed
// length, then names). We always pass `-1` as the encoder's maxSentId
// so the delta starts at id 0 (self-sufficient frame), and
// batchMaxSymbolId — passed as batchMaxId — bounds how much of
// globalSymbolList goes out (ids 0..batchMaxSymbolId). maxSentSymbolId
// carries the high-water mark across flushes so resetAfterFlush can
// rewind batchMaxSymbolId to it. Both fields do real work here.
func (s *qwpLineSender) enqueueCursor(ctx context.Context) error {
	if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
		return err
	}
	tables, err := s.buildTableEncodeInfo()
	if err != nil {
		// The only error here is "too many tables in one batch": the
		// wire encodes the table count as a uint16, so this fails
		// identically on every retry. Like an irreducible over-cap table
		// in the per-table split below, retaining the rows would re-fail
		// forever and wedge the sender; drop them with a typed error
		// naming the count so the sender stays usable.
		droppedRows := s.pendingRowCount
		s.resetAfterFlush()
		return fmt.Errorf("%w [droppedRows=%d]", err, droppedRows)
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
	// Flush-time cap check. The per-row guard in atWithTimestamp bounds
	// individual rows, but the schema and dict-delta bytes the encoder
	// adds at message-build time can push a batch of legitimately-sized
	// rows past a wire cap — the server-advertised batch cap
	// (serverMaxBatchSize) or the per-segment frame cap (maxFrameBytes,
	// the largest payload one cursor segment holds). A combined frame
	// over either cap cannot go out as-is: the server answers
	// ws-close[1009 Message Too Big] and the engine can never append a
	// frame larger than one segment.
	//
	// Such a frame is not doomed when it overruns only because it
	// aggregates many tables: enqueueCursorSplit re-encodes each table as
	// its own self-sufficient frame and appends every table that fits on
	// its own, dropping only a table that is individually over-cap.
	// Mirrors Java QwpWebSocketSender.flushPendingRows ->
	// flushPendingRowsSplit.
	if kind, _ := s.frameCapExceeded(len(encoded)); kind != qwpFrameCapNone {
		return s.enqueueCursorSplit(ctx, tables)
	}
	if _, err := s.cursorEngine.engineAppendBlocking(ctx, encoded); err != nil {
		return err
	}
	if s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}
	return nil
}

// qwpFrameCapKind identifies which wire cap an encoded frame overruns.
// Both caps treat a non-positive limit as "no limit".
type qwpFrameCapKind int

const (
	qwpFrameCapNone    qwpFrameCapKind = iota // fits every active cap
	qwpFrameCapServer                         // over the server-advertised batch cap
	qwpFrameCapSegment                        // over the per-segment frame cap
)

// frameCapExceeded reports which wire cap, if any, an encoded frame of
// frameLen bytes overruns. The server-advertised batch cap
// (serverMaxBatchSize) is checked before the per-segment frame cap
// (maxFrameBytes) so its diagnostics win when both bind; an appendable
// frame must satisfy both. Returns (qwpFrameCapNone, 0) when the frame
// fits. No allocation — safe on the flush hot path.
func (s *qwpLineSender) frameCapExceeded(frameLen int) (qwpFrameCapKind, int64) {
	if cap := int64(s.serverMaxBatchSize.Load()); cap > 0 && int64(frameLen) > cap {
		return qwpFrameCapServer, cap
	}
	if s.maxFrameBytes > 0 && int64(frameLen) > s.maxFrameBytes {
		return qwpFrameCapSegment, s.maxFrameBytes
	}
	return qwpFrameCapNone, 0
}

// enqueueCursorSplit is enqueueCursor's over-cap fallback: it re-encodes
// each pending table as its own self-sufficient single-table frame and
// appends every table whose frame fits a wire cap. A combined frame that
// overruns a cap only because it aggregates many tables flushes in full
// this way — one frame per table. Only a table whose own frame is still
// over-cap is irreducible: re-encoding it on the next flush would fail
// identically forever and wedge the sender, so its rows are dropped and
// named in a typed error while every other table goes out. Mirrors Java
// QwpWebSocketSender.flushPendingRowsSplit.
//
// Each single-table frame carries the full symbol dict from id 0 and the
// full inline schema, exactly like the combined frame, so it replays
// against a fresh server connection on its own.
//
// The retain-on-error contract holds per table: a table is reset only
// once its frame is in a segment, so a transient engineAppendBlocking
// failure (ring full + wire stalled, or ctx cancelled) retains the table
// that failed and every table after it for the next flush, without
// re-sending the tables already appended.
func (s *qwpLineSender) enqueueCursorSplit(ctx context.Context, tables []*qwpTableBuffer) error {
	var (
		appended    int
		droppedRows int
		oversize    []string
		worstKind   qwpFrameCapKind
		worstCap    int64
		worstSize   int
		txErr       error
	)
	for _, tb := range tables {
		if tb.rowCount == 0 {
			continue
		}
		frame := s.encoder.encodeTableWithDeltaDict(
			tb, s.globalSymbolList, -1, s.batchMaxSymbolId)
		if kind, capVal := s.frameCapExceeded(len(frame)); kind != qwpFrameCapNone {
			// Irreducible: a single table over the cap can never be sent.
			// Drop it; the other tables are unaffected.
			oversize = append(oversize, tb.tableName)
			droppedRows += tb.rowCount
			if len(frame) > worstSize {
				worstKind, worstCap, worstSize = kind, capVal, len(frame)
			}
			tb.reset()
			continue
		}
		if _, err := s.cursorEngine.engineAppendBlocking(ctx, frame); err != nil {
			// Transient: retain this table and the unprocessed tail for
			// the next flush. Tables already appended stay reset so they
			// are not re-sent.
			txErr = err
			break
		}
		appended++
		tb.reset()
	}

	if appended > 0 && s.batchMaxSymbolId > s.maxSentSymbolId {
		s.maxSentSymbolId = s.batchMaxSymbolId
	}

	if txErr != nil {
		// Retain-on-error: the failed and not-yet-reached tables still
		// hold their rows. Bring the aggregate counters back in line with
		// the surviving buffers; the caller must not reset on error.
		s.recomputePendingFromBuffers()
		return txErr
	}

	// Every table was appended or dropped, so all buffers are empty.
	// Resetting here (rather than leaving it to the caller, as the
	// single-frame success path does) keeps the producer counters
	// consistent with the emptied buffers even when an irreducible-table
	// error is returned or a caller skips its own post-flush reset.
	s.resetAfterFlush()
	if len(oversize) > 0 {
		return s.oversizeTableError(worstKind, worstCap, worstSize, oversize, droppedRows)
	}
	return nil
}

// recomputePendingFromBuffers rebuilds the aggregate pending-row and
// pending-byte counters from the table buffers, the source of truth.
// Used after a partial flush — a per-table split that stopped on a
// transient append failure — where some buffers were reset and others
// still hold rows. The designated-timestamp cache is dropped so the next
// row re-resolves it against whichever table buffer survives.
func (s *qwpLineSender) recomputePendingFromBuffers() {
	rows, bytes := 0, 0
	// dirtyTables is the source of truth for what can hold rows: a
	// split flush resets some entries (rowCount 0, contributing
	// nothing) and retains the rest; both stay listed.
	for _, tb := range s.dirtyTables {
		rows += tb.rowCount
		bytes += tb.approxDataSize()
	}
	s.pendingRowCount = rows
	s.pendingBytes = bytes
	s.cachedDesignatedTs = nil
}

// oversizeTableError builds the typed error returned when the per-table
// split dropped one or more individually-over-cap tables. It names the
// binding cap of the largest dropped frame, lists the dropped tables,
// and reports the total dropped row count.
func (s *qwpLineSender) oversizeTableError(kind qwpFrameCapKind, capVal int64, msgSize int, tables []string, droppedRows int) error {
	switch kind {
	case qwpFrameCapServer:
		return fmt.Errorf(
			"qwp: batch too large for server batch cap, even split per table [oversizeTables=%v, messageSize=%d, serverMaxBatchSize=%d, droppedRows=%d]",
			tables, msgSize, capVal, droppedRows)
	default: // qwpFrameCapSegment
		return fmt.Errorf(
			"qwp: batch too large to fit one cursor segment, even split per table [oversizeTables=%v, messageSize=%d, maxFrameBytes=%d, droppedRows=%d]; send fewer rows per flush (or raise sf_max_bytes)",
			tables, msgSize, capVal, droppedRows)
	}
}

// buildTableEncodeInfo collects non-empty tables for encoding.
// Every table block carries its full inline column definitions. There
// is no schema-change detection and no per-connection schema registry
// on the client side — matching the c-questdb-client live path.
// Mirrors the Java client's "self-sufficient frames" contract (Java
// spec #14): every replayed frame must stand alone against a fresh
// server connection, so the cursor wire path always carries the
// schema in full.
func (s *qwpLineSender) buildTableEncodeInfo() ([]*qwpTableBuffer, error) {
	s.encodeInfoBuf = s.encodeInfoBuf[:0]
	// Only dirtyTables can hold rows. The rowCount==0 skip still
	// matters: a buffer can be dirty-but-empty (a cancelled row, or a
	// per-table reset mid-split that left it listed).
	for _, tb := range s.dirtyTables {
		if tb.rowCount == 0 {
			continue
		}
		if len(s.encodeInfoBuf) == qwpMaxTablesPerBatch {
			return nil, fmt.Errorf(
				"qwp: too many tables in one batch: exceeded %d",
				qwpMaxTablesPerBatch,
			)
		}
		s.encodeInfoBuf = append(s.encodeInfoBuf, tb)
	}
	return s.encodeInfoBuf, nil
}

// calledFromErrorHandler reports whether the current goroutine is the
// error dispatcher's loop goroutine — i.e. we are running inside a
// user SenderErrorHandler invocation. The handler is documented as
// allowed to call Close() / Flush(); when it does, those calls run off
// the producer goroutine. The producer owns lastErr / hasTable /
// currentTable / pendingRowCount / the tableBuffers map / the
// dirtyTables list / the encoder with no happens-before against this
// goroutine, so the Close()/Flush() paths must NOT touch that state —
// doing so races a producer mid-At(): buildTableEncodeInfo ranges
// dirtyTables while Table() appends to it, and Table() writes the
// tableBuffers map, either of which corrupts state (a racing slice
// range/append, or Go's fatal "concurrent map iteration and map write").
//
// Cheap on the common path: loopGoid is 0 whenever the dispatcher
// goroutine is not running (no server error has ever been delivered),
// so the runtime.Stack cost of qwpGoid() is only paid once an error has
// actually spun the dispatcher up. The g != 0 guard keeps a goid parse
// failure from matching the loopGoid==0 "not running" sentinel.
func (s *qwpLineSender) calledFromErrorHandler() bool {
	if s.cursorSendLoop == nil {
		return false
	}
	d := s.cursorSendLoop.sendLoopDispatcher()
	if d == nil {
		return false
	}
	lg := d.loopGoid.Load()
	if lg == 0 {
		return false
	}
	g := qwpGoid()
	return g != 0 && g == lg
}

// closeCursor drains the cursor engine and closes the send loop.
// Returns the first non-nil error from drain / loop shutdown /
// engine close. Always best-effort: every subsystem is asked to
// close even if an earlier step errored.
//
// Drain semantics:
//   - closeFlushTimeout > 0: block up to that long for ackedFsn ≥
//     publishedFsn. On timeout, returns a drain-timeout error so
//     the caller cannot silently lose data — shutdown still
//     completes. SF-mode users can recover the unacked tail by
//     reopening on the same sf_dir; memory-mode users have no
//     recovery path and must treat the timeout as fatal.
//   - closeFlushTimeout <= 0: skip the drain entirely (fast close).
func (s *qwpLineSender) closeCursor(ctx context.Context) error {
	// A Close() invoked from inside a SenderErrorHandler runs on the
	// dispatcher goroutine, not the producer goroutine. Flushing pending
	// rows or even reading lastErr / hasTable / pendingRowCount here
	// would race a producer still mid-Table()/At() (the C3
	// producer-state race). Skip every producer-state access in that
	// case and run only the goroutine-safe teardown below (drain wait,
	// send-loop close, engine close, drainer pool). The producer
	// surfaces the latched terminal error and then the closed-sender
	// error on its next call; its un-flushed in-progress rows were never
	// handed off and remain its own to retry (SF mode replays whatever
	// was already persisted on the next open).
	var firstErr error
	if !s.calledFromErrorHandler() {
		// Surface any latched fluent-API error (e.g. validation failure
		// on Symbol/*Column/Table) so Close() doesn't silently swallow
		// it — mirrors the HTTP sender's flush0, which drains
		// buf.LastErr() on the close path. Captured first so any
		// subsequent enqueue / drain / shutdown error doesn't override
		// it: the latched fault is the original user-facing cause and
		// downstream failures usually follow from it.
		firstErr = s.lastErr
		s.lastErr = nil
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
		if s.pendingRowCount > 0 {
			// Enqueue the pending rows but do NOT block on ACK here —
			// flushCursor's ACK wait is unbounded by ctx alone, and
			// would deadlock against a silent server. waitCursorDrain
			// below is the single bounded ACK wait, governed by
			// closeFlushTimeout. Mirrors Java's flushPendingRows() +
			// drainOnClose() split.
			if err := s.enqueueCursor(ctx); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				// Retain-on-error: only reset the table buffers once the
				// rows are in a segment. A failed enqueue (ring full +
				// wire stalled, or ctx cancelled) never persisted them —
				// resetting here would silently destroy data. SF-mode
				// users recover the tail by reopening on the same sf_dir;
				// memory-mode users at least see firstErr. Mirrors the
				// autoFlush path and Java's flushPendingRows() contract.
				s.resetAfterFlush()
			}
		}
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
// closeFlushTimeout expires. On timeout, returns a drain-timeout
// error carrying publishedFsn, ackedFsn, and the count of unacked
// batches — closeCursor captures it as firstErr but still proceeds
// with shutdown so the I/O thread, transport, and segment manager
// always tear down cleanly. Mirrors Java QwpWebSocketSender's
// drainOnClose contract: silently swallowing the timeout would
// hide data loss from users who only call Close() and never call
// Flush() afterwards.
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
			return s.drainTimeoutError()
		}
		select {
		case <-tick.C:
		case <-timer.C:
			return s.drainTimeoutError()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// drainTimeoutError builds the close-drain timeout error. Snapshot
// publishedFsn first so the (target - acked) count cannot go
// negative under a concurrent ACK that lands between the two reads.
func (s *qwpLineSender) drainTimeoutError() error {
	target := s.cursorEngine.enginePublishedFsn()
	acked := s.cursorEngine.engineAckedFsn()
	return fmt.Errorf(
		"qwp/cursor: close drain timed out after %s [publishedFsn=%d, ackedFsn=%d] - server did not acknowledge %d pending batches; data may be lost (use a larger close_flush_timeout or smaller batches)",
		s.closeTimeout, target, acked, target-acked,
	)
}

// AckedFsn implements QwpSender.AckedFsn.
func (s *qwpLineSender) AckedFsn() int64 {
	return s.cursorEngine.engineAckedFsn()
}

// hasUnackedRows reports whether the engine holds published rows the server has
// not yet acknowledged. The pool uses it to keep an idle memory-mode slot alive
// through a transient outage instead of reaping (and destroying) its in-RAM rows.
func (s *qwpLineSender) hasUnackedRows() bool {
	return s.cursorEngine.enginePublishedFsn() > s.cursorEngine.engineAckedFsn()
}

// AwaitAckedFsn implements QwpSender.AwaitAckedFsn. This is the
// server-ACK confirmation primitive: Flush never blocks on ACKs
// (Java decision #1), so callers wanting delivery confirmation pair
// FlushAndGetSequence's returned FSN with this. It blocks until an ACK
// advances ackedFsn to target — woken directly by the send loop's
// ack-notify channel rather than a poll, so confirmation latency
// tracks the ACK itself — or until the send loop dies, the sender is
// closed, or ctx fires. Send-loop terminal errors surface
// synchronously so the caller can distinguish "still in flight" from
// "permanently failed".
func (s *qwpLineSender) AwaitAckedFsn(ctx context.Context, target int64) error {
	if s.closed.Load() {
		return errClosedSenderFlush
	}
	for {
		// Subscribe before sampling ackedFsn: acknowledge stores the new
		// FSN before it closes this channel, so an ACK that lands between
		// the sample below and the blocking select still wakes us.
		ackCh := s.cursorEngine.engineAckNotify()
		if s.cursorEngine.engineAckedFsn() >= target {
			return nil
		}
		if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
			return err
		}
		if s.closed.Load() {
			// Concurrent Close() stopped the send loop, so ackedFsn is
			// frozen and will never advance. Re-check once in case the
			// ACK landed between the read above and this load; otherwise
			// fail fast rather than wait until ctx fires.
			if s.cursorEngine.engineAckedFsn() >= target {
				return nil
			}
			return errClosedSenderFlush
		}
		select {
		case <-ackCh:
			// ackedFsn advanced — loop and re-test target.
		case <-s.cursorSendLoop.sendLoopDone():
			// The send loop exited: a HALT latched a terminal error or
			// Close() tore it down, so ackedFsn is now frozen. A final
			// ACK may have landed in the same instant, so re-test target
			// before reporting the terminal error or closed state.
			if s.cursorEngine.engineAckedFsn() >= target {
				return nil
			}
			if err := s.cursorSendLoop.sendLoopCheckError(); err != nil {
				return err
			}
			return errClosedSenderFlush
		case <-ctx.Done():
			if s.cursorEngine.engineAckedFsn() >= target {
				return nil
			}
			return ctx.Err()
		}
	}
}

// LastTerminalError implements QwpSender.LastTerminalError.
func (s *qwpLineSender) LastTerminalError() *SenderError {
	if s.cursorSendLoop == nil {
		return nil
	}
	return s.cursorSendLoop.sendLoopLastTerminalServerError()
}

// terminallyFailed reports whether the background send loop has latched a
// terminal error and exited. Unlike LastTerminalError (typed server HALTs only)
// this also covers the untyped transport/segment-fault paths (recordFatal), so
// it is the complete "this connection is poisoned" signal — the qwpLineSender
// analogue of QwpQueryClient.terminalError. It never reports true during a
// normal transient reconnect: lastError is latched only on the loop's terminal
// exit and is never cleared. The pool uses it to discard a slot poisoned by a
// background HALT instead of leaking it to the next borrower (M1) or recycling
// it after a benign producer error (M4).
func (s *qwpLineSender) terminallyFailed() bool {
	return s.cursorSendLoop != nil && s.cursorSendLoop.sendLoopCheckError() != nil
}

// TotalServerErrors implements QwpSender.TotalServerErrors.
func (s *qwpLineSender) TotalServerErrors() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopTotalServerErrors()
}

// DroppedErrorNotifications implements QwpSender.DroppedErrorNotifications.
func (s *qwpLineSender) DroppedErrorNotifications() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopDispatcher().droppedNotifications()
}

// DroppedConnectionNotifications implements QwpSender.DroppedConnectionNotifications.
func (s *qwpLineSender) DroppedConnectionNotifications() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopConnDispatcher().droppedNotifications()
}

// TotalErrorNotificationsDelivered implements
// QwpSender.TotalErrorNotificationsDelivered.
func (s *qwpLineSender) TotalErrorNotificationsDelivered() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopDispatcher().totalDelivered()
}

// TotalReconnectAttempts implements QwpSender.TotalReconnectAttempts.
func (s *qwpLineSender) TotalReconnectAttempts() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopTotalReconnectAttempts()
}

// TotalReconnectsSucceeded implements QwpSender.TotalReconnectsSucceeded.
func (s *qwpLineSender) TotalReconnectsSucceeded() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopTotalReconnects()
}

// TotalFramesReplayed implements QwpSender.TotalFramesReplayed.
func (s *qwpLineSender) TotalFramesReplayed() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopTotalFramesReplayed()
}

// TotalDurableAcks implements QwpSender.TotalDurableAcks.
func (s *qwpLineSender) TotalDurableAcks() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopTotalDurableAcks()
}

// TotalDurableTrimAdvances implements QwpSender.TotalDurableTrimAdvances.
func (s *qwpLineSender) TotalDurableTrimAdvances() int64 {
	if s.cursorSendLoop == nil {
		return 0
	}
	return s.cursorSendLoop.sendLoopTotalDurableTrimAdvances()
}

// TotalBackpressureStalls implements QwpSender.TotalBackpressureStalls.
func (s *qwpLineSender) TotalBackpressureStalls() int64 {
	if s.cursorEngine == nil {
		return 0
	}
	return s.cursorEngine.engineTotalBackpressureStalls()
}

// BackgroundDrainers implements QwpSender.BackgroundDrainers.
func (s *qwpLineSender) BackgroundDrainers() []QwpBackgroundDrainer {
	if s.drainerPool == nil {
		return nil
	}
	active := s.drainerPool.drainerPoolSnapshot()
	if len(active) == 0 {
		return nil
	}
	out := make([]QwpBackgroundDrainer, len(active))
	for i, d := range active {
		out[i] = QwpBackgroundDrainer{
			Dir:           d.drainerSlotPath(),
			FramesPending: d.drainerTargetFsn(),
			FramesAcked:   d.drainerAckedFsn(),
			LastError:     d.drainerLastError(),
			Failed:        d.drainerOutcome() == qwpSfDrainOutcomeFailed,
		}
	}
	return out
}
