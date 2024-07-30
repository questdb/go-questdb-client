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

	if len(p.senders) > 0 {
		// Pop sender off the slice and return it
		s := p.senders[len(p.senders)-1]
		p.senders = p.senders[0 : len(p.senders)-1]
		return s, nil
	}

	return qdb.LineSenderFromConf(ctx, p.conf)

}

func (p *LineSenderPool) Release(ctx context.Context, s qdb.LineSender) error {
	err := s.Flush(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.senders {
		if p.senders[i] == s {
			return fmt.Errorf("LineSender %p is has already been released back to the pool", s)
		}
	}

	if len(p.senders) < p.maxSenders {
		p.senders = append(p.senders, s)
	}

	return err

}
