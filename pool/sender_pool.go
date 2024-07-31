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
package pool

import (
	"context"
	"fmt"
	"sync"

	qdb "github.com/questdb/go-questdb-client/v3"
)

type LineSenderPool struct {
	maxSenders int
	conf       string

	closed bool

	senders []qdb.LineSender
	mu      *sync.Mutex
}

type LineSenderPoolOption func(*LineSenderPool)

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

func WithMaxSenders(count int) LineSenderPoolOption {
	return func(lsp *LineSenderPool) {
		lsp.maxSenders = count
	}
}

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
		err = fmt.Errorf("%w %w", err, senderErr)
	}

	return err
}

func (p *LineSenderPool) IsClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.closed
}
