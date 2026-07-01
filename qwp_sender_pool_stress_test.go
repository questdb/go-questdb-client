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
	"math/rand"
	"sync"
	"testing"
)

// stressSenderPool hammers a pool with concurrent borrow/write/close and then
// asserts the invariants every Hazard is meant to preserve: no panic / race
// (run with -race), every lease returned, never over max, no leaked SF slots.
func stressSenderPool(t *testing.T, p *qwpSenderPool, goroutines, iters int) {
	t.Helper()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < iters; i++ {
				s, err := p.borrow(context.Background())
				if err != nil {
					if errors.Is(err, errPoolExhausted) || errors.Is(err, errPoolClosed) {
						continue // acceptable under contention
					}
					t.Errorf("borrow: %v", err)
					return
				}
				for w := rng.Intn(3); w >= 0; w-- {
					_ = s.Table("t").Int64Column("v", int64(i)).AtNow(context.Background())
				}
				_ = s.Flush(context.Background())
				if err := s.Close(context.Background()); err != nil {
					t.Errorf("close: %v", err)
				}
			}
		}(int64(g) + 1)
	}
	wg.Wait()

	total, avail, leaked := p.poolSnapshot()
	if total > p.maxSize {
		t.Errorf("total=%d exceeds max=%d", total, p.maxSize)
	}
	if avail != total {
		t.Errorf("available=%d != total=%d — a lease was not returned", avail, total)
	}
	if leaked != 0 {
		t.Errorf("leakedSlots=%d, want 0", leaked)
	}
}

func TestQwpSenderPoolConcurrentStress(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "", 1, 4)
	stressSenderPool(t, p, 8, 60)
}

func TestQwpSenderPoolSfConcurrentStress(t *testing.T) {
	p := newQwpSenderPoolForTest(t, "sf_dir="+t.TempDir()+";", 1, 4)
	stressSenderPool(t, p, 8, 60)
}

func TestQwpQueryPoolConcurrentStress(t *testing.T) {
	p := newQwpQueryPoolForTest(t, 1, 4)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				q, err := p.borrow(context.Background())
				if err != nil {
					if errors.Is(err, errPoolExhausted) || errors.Is(err, errPoolClosed) {
						continue
					}
					t.Errorf("borrow: %v", err)
					return
				}
				if err := q.Close(); err != nil {
					t.Errorf("close: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	total, avail := p.poolSnapshot()
	if total > p.maxSize || avail != total {
		t.Errorf("query pool inconsistent: total=%d available=%d max=%d", total, avail, p.maxSize)
	}
}
