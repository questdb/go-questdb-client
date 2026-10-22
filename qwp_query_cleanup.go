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
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Tests use this hook after the connection opens, before query I/O starts.
var qwpTestQueryAfterTransportAcquired atomic.Pointer[func(*qwpTransport)]

type qwpQueryBuildError struct {
	cause  error
	client *QwpQueryClient
}

func (e *qwpQueryBuildError) Error() string { return e.cause.Error() }
func (e *qwpQueryBuildError) Unwrap() error { return e.cause }

var qwpFailedQueryGenerations struct {
	sync.Mutex
	items []*qwpConnectResult
}

func (g *qwpConnectResult) retain() {
	qwpFailedQueryGenerations.Lock()
	qwpFailedQueryGenerations.items = append(qwpFailedQueryGenerations.items, g)
	qwpFailedQueryGenerations.Unlock()
}

// If setup panics, keep the connection and any I/O state it created. Do not
// retry setup or cleanup through that partly changed state.
func (g *qwpConnectResult) retainFailure(cause any) {
	g.closeOnce.Do(func() {
		g.closeDone = make(chan struct{})
		g.closeErr = &qwpCleanupPanicError{phase: "query generation acquisition", cause: cause, stack: debug.Stack()}
		g.retain()
		close(g.closeDone)
	})
}

func (g *qwpConnectResult) startClose() {
	g.closeOnce.Do(func() {
		g.closeDone = make(chan struct{})
		go func() {
			defer close(g.closeDone)
			defer func() {
				if r := recover(); r != nil {
					g.closeErr = errors.Join(g.closeErr, &qwpCleanupPanicError{phase: "query generation close", cause: r, stack: debug.Stack()})
					g.retain()
				}
			}()
			stopCtx, cancel := context.WithCancel(context.Background())
			cancel()
			if g.transport != nil {
				_ = g.transport.closeContext(stopCtx)
			}
			if g.io != nil {
				g.closeErr = g.io.shutdown(context.Background())
			}
			if g.transport != nil {
				if err := g.transport.close(); err != nil && !errors.Is(err, net.ErrClosed) {
					g.closeErr = errors.Join(g.closeErr, err)
				}
			}
			if errors.Is(g.closeErr, ErrCleanupFailed) {
				g.retain()
			}
		}()
	})
}

// Record each connection attempt before opening its socket. Save an older
// attempt's cleanup result before removing it from this list. Resources left
// by a permanent failure stay held separately.
func (c *QwpQueryClient) keepGeneration(g *qwpConnectResult) {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	kept := c.generations[:0]
	for _, old := range c.generations {
		select {
		case <-old.closeDone:
			c.generationErr = qwpAppendCloseError(c.generationErr, old.closeErr)
		default:
			kept = append(kept, old)
		}
	}
	c.generations = append(kept, g)
}

func (c *QwpQueryClient) retireBoundGeneration() {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	for _, g := range c.generations {
		if g.transport == c.transport() {
			g.startClose()
			return
		}
	}
}

func (c *QwpQueryClient) recordCloseError(err error) {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	c.closeErr = qwpAppendCloseError(c.closeErr, err)
	if errors.Is(err, ErrCleanupFailed) {
		select {
		case <-c.closeFailed:
		default:
			qwpFailedQueries.Lock()
			qwpFailedQueries.items = append(qwpFailedQueries.items, c)
			qwpFailedQueries.Unlock()
			close(c.closeFailed)
		}
	}
}

func (c *QwpQueryClient) closeResult() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closeErr
}

func (c *QwpQueryClient) closeGenerations() {
	defer func() {
		close(c.closeDone)
		if err := c.closeResult(); err != nil {
			var logger *slog.Logger
			if c.cfg != nil {
				logger = c.cfg.logger
			}
			qwpEffectiveLogger(logger).Error("qwp: query cleanup failed", "error", err)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			c.recordCloseError(&qwpCleanupPanicError{phase: "query close", cause: r, stack: debug.Stack()})
		}
	}()
	// Close set closed under the same lock used to start connection attempts.
	// No new attempts can start. Wait for those already running so we include
	// every connection they opened before reporting successful cleanup.
	c.walks.Wait()
	c.genMu.Lock()
	generations := append([]*qwpConnectResult(nil), c.generations...)
	saved := c.generationErr
	c.genMu.Unlock()
	c.recordCloseError(saved)
	results := make(chan error, len(generations))
	for _, g := range generations {
		g.startClose()
		go func(g *qwpConnectResult) { <-g.closeDone; results <- g.closeErr }(g)
	}
	for range generations {
		c.recordCloseError(<-results)
	}
}
