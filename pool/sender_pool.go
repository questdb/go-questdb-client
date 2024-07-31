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

// Package pool provides a mechanism to cache previously-used [qdb.LineSender] objects
// in memory so they can be reused without having to allocate and instantiate new senders.
//
// A LineSenderPool is thread-safe and can be used to concurrently Acquire and Release LineSenders
// across multiple goroutines.
//
// Since LineSenders must be used in a single-threaded context, a typical pattern is to Acquire
// a LineSender from a LineSenderPool at the beginning of a goroutine and use a deferred
// execution block to Release the LineSender at the end of the goroutine.
package pool

import (
	"context"
	"fmt"
	"sync"

	qdb "github.com/questdb/go-questdb-client/v3"
)

// LineSenderPool wraps a mutex-protected slice of [qdb.LineSender]. It allows a goroutine to
// Acquire a sender from the pool and Release it back to the pool when it's done being used.
type LineSenderPool struct {
	maxSenders int
	conf       string

	closed bool

	senders []qdb.LineSender
	mu      *sync.Mutex
}

// LineSenderPoolOption defines line sender pool config option.
type LineSenderPoolOption func(*LineSenderPool)

// FromConf instantiates a new LineSenderPool with a QuestDB configuration string.
// Any sender acquired from this pool will be initialized with the same configuration
// string that was passed into the conf argument.
//
// The default maximum number of senders is 64, but can be customized by using the
// [WithMaxSenders] option
func FromConf(conf string, opts ...LineSenderPoolOption) *LineSenderPool {
	pool := &LineSenderPool{
		maxSenders: 64,
		conf:       conf,
		senders:    []qdb.LineSender{},
		mu:         &sync.Mutex{},
	}

	for _, opt := range opts {
		opt(pool)
	}

	return pool
}

// WithMaxSenders sets the maximum number of senders in the pool
func WithMaxSenders(count int) LineSenderPoolOption {
	return func(lsp *LineSenderPool) {
		lsp.maxSenders = count
	}
}

// Acquire returns a LineSender from the pool. If the pool is empty, a new
// LineSender will be instantiated using the pool's config string.
func (p *LineSenderPool) Acquire(ctx context.Context) (qdb.LineSender, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("cannot Acquire a LineSender from a closed LineSenderPool")
	}

	if len(p.senders) > 0 {
		// Pop sender off the slice and return it
		s := p.senders[len(p.senders)-1]
		p.senders = p.senders[0 : len(p.senders)-1]
		return s, nil
	}

	return qdb.LineSenderFromConf(ctx, p.conf)

}

// Release flushes a LineSender and returns it back to the pool. If the pool
// is full, the sender is closed and discarded. In cases where the sender's
// flush fails, it is not added back to the pool.
func (p *LineSenderPool) Release(ctx context.Context, s qdb.LineSender) error {
	// If there is an error on flush, do not add the sender back to the pool
	if err := s.Flush(ctx); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.senders {
		if p.senders[i] == s {
			return fmt.Errorf("LineSender %p is has already been released back to the pool", s)
		}
	}

	if p.closed || len(p.senders) >= p.maxSenders {
		return s.Close(ctx)
	}

	p.senders = append(p.senders, s)

	return nil

}

// Close sets the pool's status to "closed" and closes all cached LineSenders.
func (p *LineSenderPool) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	var senderErrors []error

	for _, s := range p.senders {
		senderErr := s.Close(ctx)
		if senderErr != nil {
			senderErrors = append(senderErrors, senderErr)

		}
	}

	if len(senderErrors) == 0 {
		return nil
	}

	err := fmt.Errorf("error closing one or more LineSenders in the pool")
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

// Len returns the numbers of cached LineSenders in the pool's internal slice
func (p *LineSenderPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.senders)
}
