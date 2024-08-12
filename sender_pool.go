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
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LineSenderPool wraps a mutex-protected slice of [LineSender]. It allows a goroutine to
// Acquire a sender from the pool and Release it back to the pool when it's done being used.
//
// WARNING: This is an experimental API that is designed to work with HTTP senders ONLY.
type LineSenderPool struct {
	maxSenders int // the only option
	numSenders int // number of used and free senders

	// presence of a non-empty conf takes precedence over opts
	conf string
	opts []LineSenderOption

	freeSenders []*pooledSender

	closed bool
	mu     *sync.Mutex
	cond   sync.Cond // used to wake up free sender waiters
}

type pooledSender struct {
	pool    *LineSenderPool
	wrapped LineSender
	dirty   bool   // set to true if any of the sender calls returned an error
	tick    uint64 // even values stand for free sender, odd values mean in-use sender
}

// LineSenderPoolOption defines line sender pool config option.
type LineSenderPoolOption func(*LineSenderPool)

// PoolFromConf instantiates a new LineSenderPool with a QuestDB configuration string.
// Any sender acquired from this pool will be initialized with the same configuration
// string that was passed into the conf argument.
//
// The default maximum number of senders is 64, but can be customized by using the
// [WithMaxSenders] option.
func PoolFromConf(conf string, opts ...LineSenderPoolOption) (*LineSenderPool, error) {
	if strings.HasPrefix(conf, "tcp") {
		return nil, errors.New("tcp/s not supported for pooled senders, use http/s only")
	}

	pool := &LineSenderPool{
		maxSenders:  64,
		conf:        conf,
		freeSenders: make([]*pooledSender, 0, 64),
		mu:          &sync.Mutex{},
	}
	pool.cond = *sync.NewCond(pool.mu)

	for _, opt := range opts {
		opt(pool)
	}

	return pool, nil
}

// PoolFromOptions instantiates a new LineSenderPool using programmatic options.
// Any sender acquired from this pool will be initialized with the same options
// that were passed into the opts argument.
//
// Unlike [PoolFromConf], PoolFromOptions does not have the ability to customize
// the returned LineSenderPool. In this case, to add options (such as [WithMaxSenders]),
// you need manually apply these options after calling this method.
//
//	// Create a PoolFromOptions with LineSender options
//	p, err := PoolFromOptions(
//		WithHttp(),
//		WithAutoFlushRows(1000000),
//	)
//
//	if err != nil {
//		panic(err)
//	}
//
//	// Add Pool-level options manually
//	WithMaxSenders(32)(p)
func PoolFromOptions(opts ...LineSenderOption) (*LineSenderPool, error) {
	pool := &LineSenderPool{
		maxSenders:  64,
		opts:        opts,
		freeSenders: make([]*pooledSender, 0, 64),
		mu:          &sync.Mutex{},
	}
	pool.cond = *sync.NewCond(pool.mu)

	return pool, nil
}

// WithMaxSenders sets the maximum number of senders in the pool.
// The default maximum number of senders is 64.
func WithMaxSenders(count int) LineSenderPoolOption {
	return func(lsp *LineSenderPool) {
		lsp.maxSenders = count
	}
}

// Sender obtains a LineSender from the pool. If the pool is empty, a new
// LineSender will be instantiated using the pool's config string.
// If there is already maximum number of senders obtained from the pool,
// this calls will block until one of the senders is returned back to
// the pool by calling sender.Close().
func (p *LineSenderPool) Sender(ctx context.Context) (LineSender, error) {
	var (
		s   LineSender
		err error
	)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errors.New("cannot Acquire a LineSender from a closed LineSenderPool")
	}

	// We may have to wait for a free sender
	for len(p.freeSenders) == 0 && p.numSenders == p.maxSenders {
		p.cond.Wait()
	}

	if len(p.freeSenders) > 0 {
		// Pop sender off the slice and return it
		s := p.freeSenders[len(p.freeSenders)-1]
		atomic.AddUint64(&s.tick, 1)
		p.freeSenders = p.freeSenders[0 : len(p.freeSenders)-1]
		return s, nil
	}

	if p.conf != "" {
		s, err = LineSenderFromConf(ctx, p.conf)
	} else {
		conf := newLineSenderConfig(httpSenderType)
		for _, opt := range p.opts {
			opt(conf)
			if conf.senderType == tcpSenderType {
				return nil, errors.New("tcp/s not supported for pooled senders, use http/s only")
			}
		}
		s, err = newHttpLineSender(conf)
	}

	if err != nil {
		return nil, err
	}

	p.numSenders++

	ps := &pooledSender{
		pool:    p,
		wrapped: s,
	}
	atomic.AddUint64(&ps.tick, 1)
	return ps, nil
}

func (p *LineSenderPool) free(ctx context.Context, ps *pooledSender) error {
	var flushErr error

	if ps.dirty {
		flushErr = ps.Flush(ctx)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if flushErr != nil {
		// Failed to flush, close and call it a day
		p.numSenders--
		closeErr := ps.wrapped.Close(ctx)
		return fmt.Errorf("%s %w", flushErr, closeErr)
	}

	if ps.dirty || p.closed {
		// Previous error or closed pool, close and call it a day
		p.numSenders--
		return ps.wrapped.Close(ctx)
	}

	p.freeSenders = append(p.freeSenders, ps)
	// Notify free sender waiters, if any
	p.cond.Broadcast()
	return nil
}

// Close sets the pool's status to "closed" and closes all cached LineSenders.
// When LineSenders are released back into a closed pool, they will be closed and discarded.
func (p *LineSenderPool) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		// Already closed
		return nil
	}
	p.closed = true

	var senderErrors []error

	for _, ps := range p.freeSenders {
		senderErr := ps.wrapped.Close(ctx)
		if senderErr != nil {
			senderErrors = append(senderErrors, senderErr)
		}
	}
	p.numSenders -= len(p.freeSenders)
	p.freeSenders = []*pooledSender{}

	if len(senderErrors) == 0 {
		return nil
	}

	err := errors.New("error closing one or more LineSenders in the pool")
	for _, senderErr := range senderErrors {
		err = fmt.Errorf("%s %w", err, senderErr)
	}

	return err
}

// IsClosed will return true if the pool is closed. Once a pool is closed,
// you will not be able to Acquire any new LineSenders from it. When
// LineSenders are released back into a closed pool, they will be closed and
// discarded.
func (p *LineSenderPool) IsClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closed
}

// Len returns the number of LineSenders in the pool.
func (p *LineSenderPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.numSenders
}

func (ps *pooledSender) Table(name string) LineSender {
	ps.wrapped.Table(name)
	return ps
}

func (ps *pooledSender) Symbol(name, val string) LineSender {
	ps.wrapped.Symbol(name, val)
	return ps
}

func (ps *pooledSender) Int64Column(name string, val int64) LineSender {
	ps.wrapped.Int64Column(name, val)
	return ps
}

func (ps *pooledSender) Long256Column(name string, val *big.Int) LineSender {
	ps.wrapped.Long256Column(name, val)
	return ps
}

func (ps *pooledSender) TimestampColumn(name string, ts time.Time) LineSender {
	ps.wrapped.TimestampColumn(name, ts)
	return ps
}

func (ps *pooledSender) Float64Column(name string, val float64) LineSender {
	ps.wrapped.Float64Column(name, val)
	return ps
}

func (ps *pooledSender) StringColumn(name, val string) LineSender {
	ps.wrapped.StringColumn(name, val)
	return ps
}

func (ps *pooledSender) BoolColumn(name string, val bool) LineSender {
	ps.wrapped.BoolColumn(name, val)
	return ps
}

func (ps *pooledSender) AtNow(ctx context.Context) error {
	err := ps.wrapped.AtNow(ctx)
	if err != nil {
		ps.dirty = true
	}
	return err
}

func (ps *pooledSender) At(ctx context.Context, ts time.Time) error {
	err := ps.wrapped.At(ctx, ts)
	if err != nil {
		ps.dirty = true
	}
	return err
}

func (ps *pooledSender) Flush(ctx context.Context) error {
	err := ps.wrapped.Flush(ctx)
	if err != nil {
		ps.dirty = true
	}
	return err
}

func (ps *pooledSender) Close(ctx context.Context) error {
	if atomic.AddUint64(&ps.tick, 1)&1 == 1 {
		return errors.New("double sender close")
	}
	return ps.pool.free(ctx, ps)
}
