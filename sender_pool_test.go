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
package questdb_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicBehavior(t *testing.T) {
	p, err := qdb.PoolFromConf("http::addr=localhost:1234;protocol_version=2;")
	require.NoError(t, err)
	ctx := context.Background()

	// Start with an empty pool, allocate a new sender
	s1, err := p.Sender(ctx)
	assert.NoError(t, err)

	// Release the sender and add it to the pool
	assert.NoError(t, s1.Close(ctx))

	// Acquiring a sender will return the initial one from the pool
	s2, err := p.Sender(ctx)
	assert.NoError(t, err)
	assert.Same(t, s1, s2)

	// Acquiring another sender will create a new one
	s3, err := p.Sender(ctx)
	assert.NoError(t, err)
	assert.NotSame(t, s1, s3)

	// Releasing the new sender will add it back to the pool
	assert.NoError(t, s3.Close(ctx))

	// Releasing the original sender will add it to the end of the pool slice
	assert.NoError(t, s2.Close(ctx))

	// Acquiring a new sender will pop the original one off the slice
	s4, err := p.Sender(ctx)
	assert.NoError(t, err)
	assert.Same(t, s1, s4)

	// Acquiring another sender will pop the second one off the slice
	s5, err := p.Sender(ctx)
	assert.NoError(t, err)
	assert.Same(t, s3, s5)
}

func TestFlushOnClose(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.Close()

	p, err := qdb.PoolFromOptions(
		qdb.WithHttp(),
		qdb.WithAddress(srv.Addr()),
		qdb.WithAutoFlushDisabled(),
	)
	assert.NoError(t, err)
	defer p.Close(ctx)

	s, err := p.Sender(ctx)
	assert.NoError(t, err)

	err = s.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	assert.NoError(t, err)

	assert.Equal(t, 1, qdb.MsgCount(s))

	assert.NoError(t, s.Close(ctx))

	assert.Equal(t, 0, qdb.MsgCount(s))
}

func TestPooledSenderDoubleClose(t *testing.T) {
	p, err := qdb.PoolFromConf("http::addr=localhost:1234;protocol_version=1;")
	require.NoError(t, err)

	ctx := context.Background()

	// Start with an empty pool, allocate a new sender
	s1, err := p.Sender(ctx)
	assert.NoError(t, err)

	// Release the sender
	assert.NoError(t, s1.Close(ctx))

	// Try to release the sender again. This should fail
	assert.Error(t, s1.Close(ctx))
}

func TestMaxPoolSize(t *testing.T) {
	// Create a pool with 2 max senders
	p, err := qdb.PoolFromConf("http::addr=localhost:1234;protocol_version=1;", qdb.WithMaxSenders(3))
	require.NoError(t, err)

	ctx := context.Background()

	// Allocate 3 senders
	s1, err := p.Sender(ctx)
	assert.NoError(t, err)

	s2, err := p.Sender(ctx)
	assert.NoError(t, err)

	s3, err := p.Sender(ctx)
	assert.NoError(t, err)

	// Release all senders in reverse order
	// Internal slice will look like: [ s3 , s2 ]
	assert.NoError(t, s3.Close(ctx))
	assert.NoError(t, s2.Close(ctx))
	assert.NoError(t, s1.Close(ctx))

	// Acquire 3 more senders.

	// The first one will be s1 (senders get popped off the slice)
	s, err := p.Sender(ctx)
	assert.NoError(t, err)
	assert.Same(t, s, s1)

	// The next will be s2
	s, err = p.Sender(ctx)
	assert.NoError(t, err)
	assert.Same(t, s, s2)

	// The final one will s3
	s, err = p.Sender(ctx)
	assert.NoError(t, err)
	assert.Same(t, s, s3)

	// Now verify the Sender caller gets blocked until a sender is freed
	successFlag := int64(0)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s, err := p.Sender(ctx)
		assert.NoError(t, err)
		atomic.AddInt64(&successFlag, 1)
		assert.Same(t, s, s3)
		assert.NoError(t, s.Close(ctx))
		wg.Done()
	}()

	assert.Equal(t, atomic.LoadInt64(&successFlag), int64(0))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt64(&successFlag), int64(0))

	assert.NoError(t, s3.Close(ctx))
	wg.Wait()
	assert.Equal(t, atomic.LoadInt64(&successFlag), int64(1))

	assert.NoError(t, s2.Close(ctx))
	assert.NoError(t, s1.Close(ctx))
	assert.NoError(t, p.Close(ctx))
}

func TestMultiThreadedPoolWritesOverHttp(t *testing.T) {
	var (
		ctx        = context.Background()
		maxSenders = 2
		numThreads = 5
	)

	srv, err := newTestHttpServer(sendToBackChannel)
	assert.NoError(t, err)
	defer srv.Close()

	wg := &sync.WaitGroup{}

	pool, err := qdb.PoolFromConf(fmt.Sprintf("http::addr=%s", srv.Addr()), qdb.WithMaxSenders(maxSenders))
	require.NoError(t, err)

	for i := 0; i < numThreads; i++ {
		i := i
		wg.Add(1)
		go func() {
			sender, err := pool.Sender(ctx)
			assert.NoError(t, err)

			sender.Table("test").Int64Column("thread", int64(i)).AtNow(ctx)

			assert.NoError(t, sender.Close(ctx))

			wg.Done()
		}()
	}

	wg.Wait()

	assert.NoError(t, pool.Close(ctx))

	lines := []string{}

	go func() {
		for {
			select {
			case msg := <-srv.BackCh:
				lines = append(lines, msg)
			case <-srv.closeCh:
				return
			default:
				continue
			}
		}
	}()

	assert.Eventually(t, func() bool {
		return len(lines) == numThreads
	}, time.Second, 100*time.Millisecond, "expected %d flushed lines but only received %d")
}

func TestTcpNotSupported(t *testing.T) {
	_, err := qdb.PoolFromConf("tcp::addr=localhost:9000")
	assert.ErrorContains(t, err, "tcp/s not supported for pooled senders")

	_, err = qdb.PoolFromConf("tcps::addr=localhost:9000")
	assert.ErrorContains(t, err, "tcp/s not supported for pooled senders")
}
