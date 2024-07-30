package pool_test

import (
	"context"
	"testing"

	"github.com/questdb/go-questdb-client/v3/pool"
	"github.com/stretchr/testify/assert"
)

func TestBasicBehavior(t *testing.T) {
	p := pool.FromConf("http::addr=localhost:1234")
	ctx := context.Background()

	// Start with an empty pool, allocate a new sender
	s1, err := p.Acquire(ctx)
	assert.NoError(t, err)

	// Release the sender and add it to the pool
	assert.NoError(t, p.Release(ctx, s1))

	// Acquiring a sender will return the initial one from the pool
	s2, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.Same(t, s1, s2)

	// Acquiring another sender will create a new one
	s3, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.NotSame(t, s1, s3)

	// Releasing the new sender will add it back to the pool
	assert.NoError(t, p.Release(ctx, s3))

	// Releasing the original sender will add it to the end of the pool slice
	assert.NoError(t, p.Release(ctx, s2))

	// Acquiring a new sender will pop the original one off the slice
	s4, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.Same(t, s1, s4)

	// Acquiring another sender will pop the second one off the slice
	s5, err := p.Acquire(ctx)
	assert.NoError(t, err)
	assert.Same(t, s3, s5)

}

func TestDoubleReleaseShouldFail(t *testing.T) {
	p := pool.FromConf("http::addr=localhost:1234")
	ctx := context.Background()

	// Start with an empty pool, allocate a new sender
	s1, err := p.Acquire(ctx)
	assert.NoError(t, err)

	// Release the sender
	assert.NoError(t, p.Release(ctx, s1))

	// Try to release the sender again. This should fail because it already exists in the slice
	assert.Error(t, p.Release(ctx, s1))

}
