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
	"sync"
	"time"
)

// Track shutdown of the sender pool, query pool, and housekeeper. They do their
// own cleanup; this code only checks the results. Update those results as work
// finishes, without saving a caller's timeout as a permanent cleanup error.
type qwpFacadeShutdown struct {
	db      *QuestDB
	mu      sync.Mutex
	changed chan struct{}
	results [3]error
}

var qwpFailedFacades struct {
	sync.Mutex
	items []*QuestDB
}

func newQwpFacadeShutdown(db *QuestDB) *qwpFacadeShutdown {
	s := &qwpFacadeShutdown{db: db, changed: make(chan struct{}), results: [3]error{ErrCleanupPending, ErrCleanupPending, ErrCleanupPending}}
	if db.senderPool.storeAndForward {
		s.results[0] = errors.Join(ErrCleanupPending, ErrSfCleanupPending)
	}
	var retain sync.Once
	publish := func(i int, err error) {
		if errors.Is(err, ErrCleanupFailed) {
			retain.Do(func() {
				qwpFailedFacades.Lock()
				qwpFailedFacades.items = append(qwpFailedFacades.items, db)
				qwpFailedFacades.Unlock()
			})
		}
		s.mu.Lock()
		s.results[i] = err
		close(s.changed)
		s.changed = make(chan struct{})
		s.mu.Unlock()
	}
	go func() {
		// The sender pool may return before borrowed senders come back or their
		// resources are released. Keep checking that work without restarting it.
		err := closeStep(func() error { return db.senderPool.close(context.Background()) })
		for {
			publish(0, err)
			if !errors.Is(err, ErrCleanupPending) || errors.Is(err, ErrCleanupFailed) {
				return
			}
			timer := time.NewTimer(10 * time.Millisecond)
			<-timer.C
			err = closeStep(db.senderPool.currentCloseResult)
		}
	}()
	go func() { publish(1, closeStep(func() error { return db.queryPool.close(context.Background()) })) }()
	go func() { publish(2, closeStep(func() error { return db.housekeeper.close(context.Background()) })) }()
	return s
}

func (s *qwpFacadeShutdown) snapshot() (<-chan struct{}, error) {
	s.mu.Lock()
	results, changed := s.results, s.changed
	s.mu.Unlock()
	// Report a permanent failure without waiting for other clients to close.
	// Those clients may finish later, so check for their errors on each call.
	// Do not retry the cleanup that failed.
	if s.db != nil {
		if errors.Is(results[0], ErrCleanupFailed) {
			results[0] = errors.Join(results[0], closeStep(s.db.senderPool.currentCloseResult))
		}
		if errors.Is(results[1], ErrCleanupFailed) {
			results[1] = errors.Join(results[1], closeStep(func() error { _, err := s.db.queryPool.closeResult(); return err }))
		}
	}
	return changed, errors.Join(results[:]...)
}

func (s *qwpFacadeShutdown) wait(ctx context.Context) error {
	for {
		changed, result := s.snapshot()
		if !errors.Is(result, ErrCleanupPending) || errors.Is(result, ErrCleanupFailed) {
			return result
		}
		select {
		case <-changed:
		case <-ctx.Done():
			_, result = s.snapshot()
			if errors.Is(result, ErrCleanupPending) && !errors.Is(result, ErrCleanupFailed) {
				return errors.Join(result, ctx.Err())
			}
			return result
		}
	}
}
