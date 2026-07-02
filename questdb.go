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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Pool defaults — match the Java QuestDBBuilder defaults.
const (
	qwpDefaultPoolMin             = 1
	qwpDefaultPoolMax             = 4
	qwpDefaultAcquireTimeout      = 5 * time.Second
	qwpDefaultIdleTimeout         = 60 * time.Second
	qwpDefaultMaxLifetime         = 30 * time.Minute
	qwpDefaultHousekeeperInterval = 5 * time.Second
)

// QuestDB is a high-level handle to a QuestDB cluster reached over QWP for both
// ingest and query. It owns elastic connection pools for both directions; one
// ws/wss config string (one addr server list) drives the whole cluster.
// Construct once with Connect or NewQuestDB and share across goroutines:
// BorrowSender and BorrowQuery may be called concurrently.
//
// To tolerate the server being down at startup, set lazy_connect=true in the
// config: ingest connects asynchronously (writes buffer until the wire is up)
// and the read pool connects lazily on first borrow. Reads stay enabled.
type QuestDB struct {
	senderPool  *qwpSenderPool
	queryPool   *qwpQueryPool
	housekeeper *qwpPoolHousekeeper
	closeOnce   sync.Once
	closeErr    error
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
}

func defaultQuestDBConfig() *questDBConfig { return &questDBConfig{} }

// WithSenderPoolMin sets the warm/minimum ingest pool size (default 1).
func WithSenderPoolMin(n int) QuestDBOption {
	return func(c *questDBConfig) { c.senderPoolMin = n; c.senderPoolMinSet = true }
}

// WithSenderPoolMax sets the maximum ingest pool size (default 4).
func WithSenderPoolMax(n int) QuestDBOption {
	return func(c *questDBConfig) { c.senderPoolMax = n; c.senderPoolMaxSet = true }
}

// WithQueryPoolMin sets the warm/minimum query pool size (default 1; 0 with
// lazy_connect).
func WithQueryPoolMin(n int) QuestDBOption {
	return func(c *questDBConfig) { c.queryPoolMin = n; c.queryPoolMinSet = true }
}

// WithQueryPoolMax sets the maximum query pool size (default 4).
func WithQueryPoolMax(n int) QuestDBOption {
	return func(c *questDBConfig) { c.queryPoolMax = n; c.queryPoolMaxSet = true }
}

// WithAcquireTimeout bounds how long BorrowSender/BorrowQuery block when the
// pool is exhausted (default 5s).
func WithAcquireTimeout(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.acquireTimeout = d; c.acquireTimeoutSet = true }
}

// WithIdleTimeout sets how long an above-min slot may stay idle before the
// housekeeper reaps it (default 60s; 0 disables idle reaping).
func WithIdleTimeout(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.idleTimeout = d; c.idleTimeoutSet = true }
}

// WithMaxLifetime sets the maximum age of a pooled slot before recycling
// (default 30m; 0 disables age recycling).
func WithMaxLifetime(d time.Duration) QuestDBOption {
	return func(c *questDBConfig) { c.maxLifetime = d; c.maxLifetimeSet = true }
}

// WithHousekeeperInterval sets the reaper sweep interval (default 5s).
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
// sender. See WithErrorHandler.
func WithQuestDBErrorHandler(h SenderErrorHandler) QuestDBOption {
	return func(c *questDBConfig) { c.errorHandler = h }
}

// WithQuestDBConnectionListener applies an ingest SenderConnectionListener to
// every pooled sender. See WithConnectionListener.
func WithQuestDBConnectionListener(l SenderConnectionListener) QuestDBOption {
	return func(c *questDBConfig) { c.connectionListener = l }
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
	if cs.Schema != "ws" && cs.Schema != "wss" {
		return nil, fmt.Errorf("qwp facade: configuration must use the ws or wss schema, got %q", cs.Schema)
	}
	kv := cs.KeyValuePairs

	// Validate the single cluster config through both parsers up front, so a
	// malformed string fails here even when a pool min is 0 and nothing connects.
	senderConf, err := confFromStr(conf)
	if err != nil {
		return nil, err
	}
	if _, err := parseQwpQueryConf(conf); err != nil {
		return nil, err
	}

	// Resolve lazy_connect (Java parity): tolerate a down server at startup
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
	// so serialize them across the pool to keep the single-goroutine contract.
	errorHandler := serializeErrorHandler(cfg.errorHandler)
	connectionListener := serializeConnectionListener(cfg.connectionListener)

	// Build both pools + the housekeeper, teardown-hardened: on any failure
	// close what was already built, in reverse order (Hazard I at the facade).
	sp, err := newQwpSenderPool(ctx, ingestConf, senderMin, senderMax,
		acquire, idle, lifetime, errorHandler, connectionListener)
	if err != nil {
		return nil, err
	}
	qp, err := newQwpQueryPool(ctx, conf, queryMin, queryMax, acquire, idle, lifetime)
	if err != nil {
		_ = sp.close(ctx)
		return nil, err
	}
	// The join budget must cover a reaped slot's close-flush drain, so a reap
	// in flight can never outlive QuestDB.Close.
	closeFlush := qwpSfDefaultCloseFlushTimeout
	if senderConf.closeFlushTimeoutSet {
		closeFlush = max(time.Duration(senderConf.closeFlushTimeoutMillis)*time.Millisecond, 0)
	}
	hk := newQwpPoolHousekeeper(sp, qp, hkInterval, closeFlush+time.Second)
	hk.start()
	return &QuestDB{senderPool: sp, queryPool: qp, housekeeper: hk}, nil
}

// validateLazyConnect rejects the two configurations that contradict
// lazy_connect's non-blocking startup, mirroring Java's resolveLazyConnect.
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

// BorrowSender leases an ingest sender from the pool. Close it (typically via
// defer) to return it; the real disconnect happens at QuestDB.Close. Blocks up
// to the acquire timeout when the pool is exhausted.
func (db *QuestDB) BorrowSender(ctx context.Context) (LineSender, error) {
	return db.senderPool.borrow(ctx)
}

// BorrowQuery leases a query session from the pool. Close it to return it. With
// lazy_connect, the first borrow connects on demand.
func (db *QuestDB) BorrowQuery(ctx context.Context) (*Query, error) {
	return db.queryPool.borrow(ctx)
}

// Close shuts down the housekeeper and both pools, closing every underlying
// sender and query client. Idempotent and safe to call concurrently. Each
// teardown step is panic-guarded so a fault in one cannot skip the others — the
// sender pool (which owns the flocks/mmaps/I/O goroutines) is closed last and
// always runs.
//
// Avoid calling Close from inside a pooled SenderErrorHandler or
// SenderConnectionListener. Pooled callbacks are funnelled through one
// serializing mutex (see serializeErrorHandler), so a Close that blocks on a
// sibling dispatcher mid-delivery head-of-lines the others until each is
// abandoned at its join timeout (qwpSfDispatcherCloseJoinTimeout apiece; every
// pooled sender owns two dispatchers, so the worst case scales with slot
// count). It is bounded — no deadlock or panic — but Close may stall. Hand the
// close off to a separate goroutine instead.
func (db *QuestDB) Close(ctx context.Context) error {
	db.closeOnce.Do(func() {
		// Signal the pools to stop reaping before joining the housekeeper, so a
		// reap cannot start during the join window and outlive Close.
		db.senderPool.markClosing()
		db.queryPool.markClosing()
		hErr := closeStep(func() error { db.housekeeper.stopAndJoin(); return nil })
		qErr := closeStep(func() error { return db.queryPool.close(ctx) })
		sErr := closeStep(func() error { return db.senderPool.close(ctx) })
		// Every step ran; surface the most actionable error.
		db.closeErr = firstCloseErr(sErr, qErr, hErr)
	})
	return db.closeErr
}

// firstCloseErr selects the most actionable teardown error, preferring the
// sender pool (owns flocks/I/O) over the query pool over the housekeeper so a
// recovered panic in any step is not lost. Returns nil only when every step
// succeeded.
func firstCloseErr(sErr, qErr, hErr error) error {
	switch {
	case sErr != nil:
		return sErr
	case qErr != nil:
		return qErr
	case hErr != nil:
		return hErr
	default:
		return nil
	}
}

// closeStep runs one teardown step, converting a panic into an error so a
// faulting step cannot abort the remaining closes.
func closeStep(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("qwp facade: teardown step panicked: %v", r)
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
