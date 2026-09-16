// Copyright (c) 2014-2019 Appsicle
// Copyright (c) 2019-2026 QuestDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Keep failed pools and their clients alive until the process exits. A count
// alone would not keep their resources alive. This list never retries cleanup.
var qwpFailedQueryPools struct {
	sync.Mutex
	pools []*qwpQueryPool
}

// Tests use this hook after the client is recorded for cleanup, but before it
// leaves the live-client list or its cleanup goroutine starts.
var qwpTestQueryTeardownRegistered atomic.Pointer[func(*qwpQueryWorker)]

// Record the client for cleanup before removing it from p.all. The goroutine
// keeps it alive while closing it. If cleanup fails permanently, the pool keeps
// it alive instead.
func (p *qwpQueryPool) startTeardownLocked(w *qwpQueryWorker, before func()) {
	if _, failed := p.failedWorkers[w]; failed {
		return
	}
	if p.teardowns == nil {
		p.teardowns = make(map[*qwpQueryWorker]struct{})
	}
	if _, exists := p.teardowns[w]; exists {
		return
	}
	p.teardowns[w] = struct{}{}
	if hook := qwpTestQueryTeardownRegistered.Load(); hook != nil {
		(*hook)(w)
	}
	p.removeFromAllLocked(w)
	go p.teardownWorker(w, before)
}

func (p *qwpQueryPool) teardownWorker(w *qwpQueryWorker, before func()) {
	failed := false
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				failed = true
				err = &qwpCleanupPanicError{phase: "query pool close", cause: r, stack: debug.Stack()}
			}
		}()
		if before != nil {
			before()
		}
		err = closeQueryClientGuarded(context.Background(), w.client)
		// Catch panics from checking the error too: errors.Is may call methods
		// on a custom error, and those methods can panic.
		if errors.Is(err, ErrCleanupFailed) {
			failed = true
			return errors.Join(ErrCleanupFailed, err)
		}
		return err
	}()
	p.withLock([]*qwpQueryWorker{w}, func() {
		p.closeErr = errors.Join(p.closeErr, err)
		if failed {
			p.failLocked(err, []*qwpQueryWorker{w})
		} else {
			delete(p.teardowns, w)
		}
		p.broadcastLocked()
	})
	// Save the result and wake callers before logging, since a user-provided
	// log handler might block.
	if err != nil {
		qwpEffectiveLogger(p.logger).Warn("qwp query pool: client cleanup failed", "error", err)
	}
}

func (p *qwpQueryPool) closeResult() (<-chan struct{}, error) {
	var result error
	var changed <-chan struct{}
	err := p.withLock(nil, func() {
		result = errors.Join(p.closeErr, p.failedErr)
		if !p.closed || len(p.all)+p.inFlightCreations+len(p.teardowns) != 0 {
			result = errors.Join(result, ErrCleanupPending)
		}
		changed = p.notify
	})
	return changed, errors.Join(result, err)
}

func (p *qwpQueryPool) withLock(workers []*qwpQueryWorker, fn func()) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			if workers == nil {
				workers = p.all
			}
			p.failLocked(&qwpCleanupPanicError{phase: "query pool update", cause: r, stack: debug.Stack()}, workers)
			err = p.failedErr
		}
	}()
	fn()
	return nil
}

func (p *qwpQueryPool) failLocked(err error, workers []*qwpQueryWorker) {
	if p.failedWorkers == nil {
		p.failedWorkers = make(map[*qwpQueryWorker]struct{})
	}
	for _, w := range workers {
		if w != nil {
			p.failedWorkers[w] = struct{}{}
		}
	}
	if p.failedErr == nil {
		p.failedErr = errors.Join(ErrCleanupFailed, ErrPoolPoisoned, err)
		qwpFailedQueryPools.Lock()
		qwpFailedQueryPools.pools = append(qwpFailedQueryPools.pools, p)
		qwpFailedQueryPools.Unlock()
	}
	p.broadcastLocked()
}

func (p *qwpQueryPool) buildAvailable(ctx context.Context) error {
	w, buildErr := p.createWorker(ctx)
	var result error
	err := p.withLock([]*qwpQueryWorker{w}, func() {
		p.inFlightCreations--
		if errors.Is(buildErr, ErrCleanupFailed) {
			p.failLocked(buildErr, nil)
		}
		result = buildErr
		if p.closed || p.closing.Load() || p.failedErr != nil {
			result = errors.Join(result, errPoolClosed, p.failedErr)
		}
		if ctx.Err() != nil {
			result = errors.Join(result, ctx.Err())
		}
		if result != nil {
			if w != nil {
				p.startTeardownLocked(w, nil)
			}
		} else {
			p.all = append(p.all, w)
			p.available = append(p.available, w)
		}
		p.broadcastLocked()
	})
	return errors.Join(result, err)
}
