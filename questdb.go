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
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Pool sizing defaults.
const (
	qwpDefaultPoolMin             = 1
	qwpDefaultPoolMax             = 4
	qwpDefaultAcquireTimeout      = 5 * time.Second
	qwpDefaultIdleTimeout         = 60 * time.Second
	qwpDefaultMaxLifetime         = 30 * time.Minute
	qwpDefaultHousekeeperInterval = 5 * time.Second
)

// ErrQueryDesynced is returned by a Query lease's Query / Exec (surfaced from a
// cursor's first Batches yield for Query) when the leased connection's
// single-stream wire was left desynced by an abandoned statement drain. The
// lease's Close evicts such a worker; the caller should close the lease and
// borrow a fresh one. Match it with errors.Is. It is the exported name for the
// query client's internal desync sentinel; the two are the same error value.
var ErrQueryDesynced = errExecDesynced

// QuestDB is a high-level handle to a QuestDB cluster reached over QWP for both
// ingest and query. It owns elastic connection pools for both directions; one
// ws/wss config string (one addr server list) drives the whole cluster.
// Construct once with Connect or NewQuestDB and share across goroutines:
// BorrowSender, BorrowQuery, Close, and returns of separate borrowed handles
// may run concurrently. Coordinate use of each handle in your application and
// stop using it before returning it to the pool. See [QuestDB.Close] for what
// shutdown waits for and what its result means.
//
// To tolerate the server being down at startup, set lazy_connect=true in the
// config: ingest connects asynchronously (writes buffer until the wire is up)
// and the read pool connects lazily on first borrow. Reads stay enabled.
type QuestDB struct {
	senderPool  *qwpSenderPool
	queryPool   *qwpQueryPool
	housekeeper *qwpPoolHousekeeper
	closeOnce   sync.Once

	shutdown *qwpFacadeShutdown
}

// QuestDBOption configures the QuestDB facade. An explicit option always wins
// over the matching connect-string key.
type QuestDBOption func(*questDBConfig)

// questDBConfig collects builder state. Each tunable carries a "set" flag so an
// explicit value (including 0, e.g. query_pool_min=0, or a negative the resolver
// must reject) is distinguishable from "not set" without a sentinel that a real
// argument could collide with.
type questDBConfig struct {
	senderPoolMin, senderPoolMax       int
	senderPoolMinSet, senderPoolMaxSet bool
	queryPoolMin, queryPoolMax         int
	queryPoolMinSet, queryPoolMaxSet   bool
	acquireTimeout                     time.Duration
	acquireTimeoutSet                  bool
	idleTimeout                        time.Duration
	idleTimeoutSet                     bool
	maxLifetime                        time.Duration
	maxLifetimeSet                     bool
	housekeeperInterval                time.Duration
	housekeeperIntervalSet             bool
	lazyConnect                        bool
	lazyConnectSet                     bool
	errorHandler                       SenderErrorHandler
	connectionListener                 SenderConnectionListener
	drainerListener                    QwpBackgroundDrainerListener
	logger                             *slog.Logger
}

func defaultQuestDBConfig() *questDBConfig { return &questDBConfig{} }

// WithSenderPoolMin sets the warm/minimum ingest pool size (default 1).
// Equivalent to the sender_pool_min connect-string key; the option wins.
func WithSenderPoolMin(n int) QuestDBOption {
	return func(c *questDBConfig) { c.senderPoolMin = n; c.senderPoolMinSet = true }
}

// WithSenderPoolMax sets the maximum ingest pool size (default 4).
// Equivalent to the sender_pool_max connect-string key; the option wins.
func WithSenderPoolMax(n int) QuestDBOption {
	return func(c *questDBConfig) { c.senderPoolMax = n; c.senderPoolMaxSet = true }
}

// WithQueryPoolMin sets the warm/minimum query pool size (default 1; 0 with
// lazy_connect). Equivalent to the query_pool_min connect-string key; the
// option wins.
func WithQueryPoolMin(n int) QuestDBOption {
	return func(c *questDBConfig) { c.queryPoolMin = n; c.queryPoolMinSet = true }
}

// WithQueryPoolMax sets the maximum query pool size (default 4). Equivalent to
// the query_pool_max connect-string key; the option wins.
func WithQueryPoolMax(n int) QuestDBOption {
	return func(c *questDBConfig) { c.queryPoolMax = n; c.queryPoolMaxSet = true }
}

// WithAcquireTimeout bounds how long BorrowSender/BorrowQuery block when the
// pool is exhausted (default 5s; must be positive). Equivalent to the
// acquire_timeout_ms connect-string key; the option wins.
func WithAcquireTimeout(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.acquireTimeout = d; c.acquireTimeoutSet = true }
}

// WithIdleTimeout sets how long an above-min slot may stay idle before the
// housekeeper reaps it (default 60s; 0 disables idle reaping). Equivalent to
// the idle_timeout_ms connect-string key; the option wins.
func WithIdleTimeout(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.idleTimeout = d; c.idleTimeoutSet = true }
}

// WithMaxLifetime sets the maximum age of a pooled slot before recycling
// (default 30m; 0 disables age recycling). Equivalent to the max_lifetime_ms
// connect-string key; the option wins.
func WithMaxLifetime(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.maxLifetime = d; c.maxLifetimeSet = true }
}

// WithHousekeeperInterval sets the reaper sweep interval (default 5s). 0
// disables the housekeeper entirely — no idle/age reaping runs. Equivalent to
// the housekeeper_interval_ms connect-string key; the option wins.
func WithHousekeeperInterval(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.housekeeperInterval = d; c.housekeeperIntervalSet = true }
}

// WithLazyConnect tolerates the server being down at startup: ingest connects
// asynchronously (writes buffer until the wire is up) and the read pool connects
// lazily on first borrow (query_pool_min defaults to 0). Equivalent to the
// lazy_connect connect-string key; an explicit setter wins over the key. It is
// incompatible with a non-async initial_connect_retry or an explicit
// query_pool_min > 0, which build() rejects.
func WithLazyConnect(v bool) QuestDBOption {
	return func(c *questDBConfig) { c.lazyConnect = v; c.lazyConnectSet = true }
}

// WithQuestDBErrorHandler applies an ingest SenderErrorHandler to every pooled
// sender. Only one call to this handler runs at a time, even across senders.
// Signal the application code that uses the sender; do not change or close a
// sender, or call QuestDB.Close directly from the handler. You may call
// documented methods that return read-only snapshots of the sender's state.
// See [WithErrorHandler] and [SenderErrorHandler] for how panics are handled
// and which notifications may be lost during shutdown.
func WithQuestDBErrorHandler(h SenderErrorHandler) QuestDBOption {
	return func(c *questDBConfig) { c.errorHandler = h }
}

// WithQuestDBConnectionListener applies an ingest SenderConnectionListener to
// every pooled sender. Only one call to this listener runs at a time, even
// across senders. Signal the application code that uses the sender; do not
// change or close a sender, or call QuestDB.Close directly from the listener.
// You may call documented methods that return read-only snapshots of state.
// See [WithConnectionListener] and [SenderConnectionListener] for how panics
// are handled and which notifications may be lost during shutdown.
func WithQuestDBConnectionListener(l SenderConnectionListener) QuestDBOption {
	return func(c *questDBConfig) { c.connectionListener = l }
}

// WithQuestDBBackgroundDrainerListener applies a QwpBackgroundDrainerListener
// to every pooled sender, covering both orphan adoption (drain_orphans) and the
// pool's crash-stranded-slot recovery senders. Callbacks may fire concurrently
// from multiple drainers, so protect any state shared by callbacks. Signal
// the application code that uses the sender. Do not change or close a sender,
// or call QuestDB.Close directly from a callback. You may call documented
// methods that return read-only snapshots of state. See
// [WithBackgroundDrainerListener] and [QwpBackgroundDrainerListener] for where
// callbacks run and which notifications may be lost during shutdown.
func WithQuestDBBackgroundDrainerListener(l QwpBackgroundDrainerListener) QuestDBOption {
	return func(c *questDBConfig) { c.drainerListener = l }
}

// WithQuestDBLogger sets the *slog.Logger applied to both pools and every
// pooled sender and query session. See WithLogger.
func WithQuestDBLogger(l *slog.Logger) QuestDBOption {
	// Guarded at the door, like WithLogger: only panic-guarded loggers are
	// stored (see qwp_log.go).
	return func(c *questDBConfig) { c.logger = qwpGuardLogger(l) }
}

// serializeErrorHandler wraps h so concurrent invocations from the pool's
// per-sender dispatchers are serialized, preserving the single-goroutine
// delivery contract a single sender's handler enjoys. Returns nil unchanged.
//
// This deliberately couples every pooled sender's independent dispatcher
// through one mutex: the contract that the user handler is never called
// concurrently is worth more than per-sender callback parallelism. The
// trade-off is that a slow handler head-of-line-blocks sibling dispatchers
// (inflating their drop counters) — acceptable because the handler is expected
// to be cheap and each dispatcher's bounded inbox absorbs the backpressure.
func serializeErrorHandler(h SenderErrorHandler) SenderErrorHandler {
	if h == nil {
		return nil
	}
	var mu sync.Mutex
	return func(e *SenderError) {
		mu.Lock()
		defer mu.Unlock()
		h(e)
	}
}

// serializeConnectionListener is the SenderConnectionListener counterpart of
// serializeErrorHandler.
func serializeConnectionListener(l SenderConnectionListener) SenderConnectionListener {
	if l == nil {
		return nil
	}
	var mu sync.Mutex
	return func(e SenderConnectionEvent) {
		mu.Lock()
		defer mu.Unlock()
		l(e)
	}
}

// Connect opens a QuestDB facade with default pool sizing. The config must use
// the ws or wss schema; list every cluster node in one addr server list.
func Connect(ctx context.Context, conf string) (*QuestDB, error) {
	return NewQuestDB(ctx, conf)
}

// NewQuestDB opens a QuestDB facade with the given options applied over the
// connect string (an explicit option wins over the matching connect-string key).
func NewQuestDB(ctx context.Context, conf string, opts ...QuestDBOption) (*QuestDB, error) {
	cfg := defaultQuestDBConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	cs, err := parseConfigStr(conf)
	if err != nil {
		return nil, err
	}
	// Accept the qwpws/qwpwss long forms too: both conf parsers the facade
	// invokes below treat them as aliases for ws/wss, so the gate must not
	// reject a string those parsers (and the standalone clients) accept.
	switch cs.Schema {
	case "ws", "wss", "qwpws", "qwpwss":
	default:
		return nil, fmt.Errorf("qwp facade: configuration must use the ws or wss schema, got %q", cs.Schema)
	}
	kv := cs.KeyValuePairs

	// Validate the single cluster config through both parsers up front, so a
	// malformed string fails here even when a pool min is 0 and nothing
	// connects. sanitizeQwpConf adds the cross-field checks newLineSender runs
	// (e.g. auto_flush_bytes > sf_max_bytes); its normalizations are harmless
	// here — senderConf is only read below, the pools re-parse the string per
	// slot.
	senderConf, err := confFromStr(conf)
	if err != nil {
		return nil, err
	}
	if serr := sanitizeQwpConf(senderConf); serr != nil {
		return nil, serr
	}
	if _, err := parseQwpQueryConf(conf); err != nil {
		return nil, err
	}

	// Resolve lazy_connect: tolerate a down server at startup
	// without disabling reads. Explicit option wins over the connect-string key,
	// but the key is still validated so a typo never rides silently.
	lazyConnect, err := poolBool(kv, "lazy_connect", false)
	if err != nil {
		return nil, err
	}
	if cfg.lazyConnectSet {
		lazyConnect = cfg.lazyConnect
	}
	ingestConf := conf
	queryMinDefault := qwpDefaultPoolMin
	if lazyConnect {
		if err := validateLazyConnect(kv, cfg); err != nil {
			return nil, err
		}
		queryMinDefault = 0
		// Inject async unless the user set an (async) initial_connect_retry.
		if _, ok := kv["initial_connect_retry"]; !ok {
			ingestConf = withDefaultAsyncConnect(conf)
		}
	}

	senderMin, err := resolvePoolInt(cfg.senderPoolMinSet, cfg.senderPoolMin, kv, "sender_pool_min", qwpDefaultPoolMin)
	if err != nil {
		return nil, err
	}
	senderMax, err := resolvePoolInt(cfg.senderPoolMaxSet, cfg.senderPoolMax, kv, "sender_pool_max", qwpDefaultPoolMax)
	if err != nil {
		return nil, err
	}
	queryMin, err := resolvePoolInt(cfg.queryPoolMinSet, cfg.queryPoolMin, kv, "query_pool_min", queryMinDefault)
	if err != nil {
		return nil, err
	}
	queryMax, err := resolvePoolInt(cfg.queryPoolMaxSet, cfg.queryPoolMax, kv, "query_pool_max", qwpDefaultPoolMax)
	if err != nil {
		return nil, err
	}
	acquire, err := resolvePoolDur(cfg.acquireTimeoutSet, cfg.acquireTimeout, kv, "acquire_timeout_ms", qwpDefaultAcquireTimeout)
	if err != nil {
		return nil, err
	}
	if acquire <= 0 {
		// Both pools derive the creation-path dial deadline from it, so 0 would
		// pre-expire every borrow that has to build a slot (under lazy_connect
		// the read pool would never connect at all).
		return nil, fmt.Errorf("acquire_timeout_ms must be positive, got %d", acquire/time.Millisecond)
	}
	idle, err := resolvePoolDur(cfg.idleTimeoutSet, cfg.idleTimeout, kv, "idle_timeout_ms", qwpDefaultIdleTimeout)
	if err != nil {
		return nil, err
	}
	lifetime, err := resolvePoolDur(cfg.maxLifetimeSet, cfg.maxLifetime, kv, "max_lifetime_ms", qwpDefaultMaxLifetime)
	if err != nil {
		return nil, err
	}
	hkInterval, err := resolvePoolDur(cfg.housekeeperIntervalSet, cfg.housekeeperInterval, kv, "housekeeper_interval_ms", qwpDefaultHousekeeperInterval)
	if err != nil {
		return nil, err
	}

	// Every pooled sender invokes these callbacks on its own dispatcher goroutine,
	// so serialize them across the pool to keep the single-goroutine contract. When
	// the caller registers none, install the loud default here so the pool emits
	// one serialized event stream instead of an independent default per slot — a
	// standalone sender emits a single stream, and the pool should too.
	logger := qwpEffectiveLogger(cfg.logger)
	errorHandler := serializeErrorHandler(cfg.errorHandler)
	if errorHandler == nil {
		errorHandler = serializeErrorHandler(newDefaultSenderErrorHandler(logger))
	}
	connectionListener := serializeConnectionListener(cfg.connectionListener)
	if connectionListener == nil {
		connectionListener = serializeConnectionListener(newDefaultSenderConnectionListener(logger))
	}

	// Build both pools + the housekeeper, teardown-hardened: on any failure
	// close what was already built, in reverse order (Hazard I at the facade).
	sp, err := newQwpSenderPool(ctx, ingestConf, senderMin, senderMax,
		acquire, idle, lifetime, errorHandler, connectionListener, cfg.drainerListener, logger)
	if err != nil {
		return nil, err
	}
	qp, err := newQwpQueryPool(ctx, conf, queryMin, queryMax, acquire, idle, lifetime, logger)
	if err != nil {
		// Join rather than drop: sp is about to become unreachable, so a
		// retained slot lock (ErrSfCleanupPending) has no other way to reach
		// the caller. See newQwpSenderPool's prewarm unwind.
		if closeErr := sp.close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, err
	}
	hk := newQwpPoolHousekeeper(sp, qp, hkInterval)
	hk.start()
	return &QuestDB{senderPool: sp, queryPool: qp, housekeeper: hk}, nil
}

// validateLazyConnect rejects the two configurations that contradict
// lazy_connect's non-blocking startup.
func validateLazyConnect(kv map[string]string, cfg *questDBConfig) error {
	if mode, ok := kv["initial_connect_retry"]; ok && !strings.EqualFold(mode, "async") {
		return fmt.Errorf("conflicting configuration: lazy_connect=true needs a non-blocking startup, "+
			"but initial_connect_retry=%s makes the initial connect block / fail-fast. Resolve by removing "+
			"initial_connect_retry (lazy_connect implies async) or setting initial_connect_retry=async", mode)
	}
	explicitQueryMin := 0
	if cfg.queryPoolMinSet {
		explicitQueryMin = cfg.queryPoolMin
	} else if v, ok := kv["query_pool_min"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid query_pool_min %q: %v", v, err)
		}
		explicitQueryMin = n
	}
	if explicitQueryMin > 0 {
		return fmt.Errorf("conflicting configuration: lazy_connect=true needs query_pool_min=0 (the read pool "+
			"connects lazily on first use and must not fail-fast at startup), but query_pool_min=%d was set. "+
			"Resolve by removing query_pool_min (lazy_connect defaults it to 0) or setting query_pool_min=0",
			explicitQueryMin)
	}
	return nil
}

// BorrowSender borrows an ingest sender from the pool. Stop using the sender
// before calling its Close method to return it. A healthy connection stays
// open for reuse; the pool disconnects it when removing it or shutting down.
// If the pool is full, BorrowSender waits up to the acquire timeout.
// See [LineSender.Close].
func (db *QuestDB) BorrowSender(ctx context.Context) (LineSender, error) {
	return db.senderPool.borrow(ctx)
}

// BorrowQuery borrows a query session from the pool. Before returning it with
// [Query.Close], stop running queries, reading results, and using slices that
// refer to result-batch memory. With lazy_connect, the first borrow connects
// on demand.
func (db *QuestDB) BorrowQuery(ctx context.Context) (*Query, error) {
	return db.queryPool.borrow(ctx)
}

// Close shuts down QuestDB and its pools. It may run concurrently with other
// Close calls and with borrowing or returning separate handles. Your
// application must stop using each borrowed handle and return it. Close does
// not free result buffers while a borrower may still use them. Callbacks
// should ask the application to stop; starting Close in another goroutine
// does not make concurrent use of a borrowed handle safe.
//
// The first call starts shutdown even if ctx is already cancelled or expired.
// ctx limits how long this call waits, not how long cleanup may continue. It
// also does not change the sender's separate time limits for queueing rows
// and waiting for server acknowledgements. Later calls wait for the same
// shutdown; they do not restart sending or repair cleanup that panicked.
//
// A nil result means both pools, their internal readers and background
// drainers, and the pool maintenance task have finished cleanup. All their
// store-and-forward file locks are released, and no work can later acquire
// or retain another resource. This includes recovered orphan slots outside
// the pool's numbered slots, clients still being created or removed, and
// handles returned during shutdown. No error that Close must report remains.
// Close need not wait for user callbacks: queued notifications may be dropped
// and a callback already running may finish after Close returns. Close does
// not promise empty slot directories or a WebSocket closing handshake.
//
// Check errors.Is(err, ErrCleanupFailed) first. This means cleanup cannot
// safely continue after an internal failure. Close reports it without waiting
// for other unfinished work. Resources that cannot safely be released stay
// held, and affected slots stay reserved, possibly until process restart.
// Corrupt pool state also produces [ErrPoolPoisoned]. Otherwise,
// [ErrCleanupPending] means cleanup is unfinished; unfinished store-and-forward
// cleanup also produces [ErrSfCleanupPending]. If ctx expires while waiting,
// the returned error also wraps ctx.Err() and any recorded errors. Return
// borrowed handles or fix storage faults before waiting again with a fresh
// deadline. Cleanup is not guaranteed to finish.
//
// A later call checks progress again; an earlier timeout or pending result
// is not a permanent failure. Once shutdown finishes, Close returns its saved
// result immediately, even with an expired ctx. A successful cleanup retry
// clears the error it recovered from. Errors from queueing or delivering rows
// (including the acknowledgement timeout), cleanup errors that could not be
// recovered from, and internal failures still appear in the result after
// resources are released. Finished cleanup therefore need not mean nil.
// Once Close returns nil, later calls also return nil.
func (db *QuestDB) Close(ctx context.Context) error {
	db.closeOnce.Do(func() {
		db.senderPool.markClosing()
		db.queryPool.markClosing()
		db.shutdown = newQwpFacadeShutdown(db)
	})
	return db.shutdown.wait(ctx)
}

// closeStep runs one teardown step, converting a panic into an error so a
// faulting step cannot abort the remaining closes.
func closeStep(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: facade teardown step panicked: %v", ErrCleanupFailed, r)
		}
	}()
	return fn()
}

// withDefaultAsyncConnect injects initial_connect_retry=async right after the
// schema separator so a lazy_connect build never blocks on a down server. Only
// used when the user set no initial_connect_retry of their own.
func withDefaultAsyncConnect(conf string) string {
	sep := strings.Index(conf, "::")
	if sep < 0 {
		return conf
	}
	return conf[:sep+2] + "initial_connect_retry=async;" + conf[sep+2:]
}

// poolBool reads a true/false/on/off pool key from the raw KV, defaulting when absent.
func poolBool(kv map[string]string, key string, dflt bool) (bool, error) {
	v, ok := kv[key]
	if !ok {
		return dflt, nil
	}
	switch v {
	case "true", "on":
		return true, nil
	case "false", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid %s %q (expected true/false/on/off)", key, v)
	}
}

// resolvePoolInt resolves a pool size: explicit option (set) > connect-string
// key > default. Rejects a negative value or a non-integer key — the key is
// validated even when the option shadows it, so a config typo never rides
// silently under an option that happens to be set.
func resolvePoolInt(set bool, opt int, kv map[string]string, key string, dflt int) (int, error) {
	resolved := dflt
	if v, ok := kv[key]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid %s %q (expected a non-negative int)", key, v)
		}
		resolved = n
	}
	if set {
		if opt < 0 {
			return 0, fmt.Errorf("%s must be >= 0", key)
		}
		return opt, nil
	}
	return resolved, nil
}

// qwpMaxDurationMillis is the largest millisecond count that still fits a
// time.Duration (int64 nanoseconds); a larger value would wrap to a nonsensical
// duration instead of the magnitude the user asked for.
const qwpMaxDurationMillis = int64(9223372036854775807) / int64(time.Millisecond)

// resolvePoolDur resolves a millisecond pool key into a Duration: explicit
// option (set) > connect-string key > default. Like resolvePoolInt, the key is
// validated even when the option shadows it.
func resolvePoolDur(set bool, opt time.Duration, kv map[string]string, key string, dflt time.Duration) (time.Duration, error) {
	resolved := dflt
	if v, ok := kv[key]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid %s %q (expected a non-negative int, milliseconds)", key, v)
		}
		if int64(n) > qwpMaxDurationMillis {
			return 0, fmt.Errorf("invalid %s %q (milliseconds value is out of range)", key, v)
		}
		resolved = time.Duration(n) * time.Millisecond
	}
	if set {
		if opt < 0 {
			return 0, fmt.Errorf("%s must be >= 0", key)
		}
		return opt, nil
	}
	return resolved, nil
}
