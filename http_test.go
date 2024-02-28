package questdb_test

import (
	"context"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

func TestErrorOnContextDeadlineHttp(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.addr))
	assert.NoError(t, err)
	defer sender.Close()

	// Keep writing until we get an error due to the context deadline.
	for i := 0; i < 100_000; i++ {
		err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
		if err != nil {
			return
		}
		err = sender.Flush(ctx)
		if err != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fail()
}

func TestErrorOnInternalServerErrorHttp(t *testing.T) {
	ctx := context.Background()

	srv, err := newTestHttpServer(returningError)
	assert.NoError(t, err)
	defer srv.close()

	sender, err := qdb.NewHttpLineSender(
		qdb.WithHttpAddress(srv.addr),
		qdb.WithGraceTimeout(10*time.Millisecond),
	)
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.Table(testTable).StringColumn("bar", "baz").AtNow(ctx)
	if err != nil {
		return
	}
	err = sender.Flush(ctx)
	assert.ErrorContains(t, err, "500")

}

func BenchmarkHttpLineSenderBatch1000(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(b, err)
	defer srv.close()

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.addr))
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			sender.
				Table(testTable).
				Symbol("sym_col", "test_ilp1").
				Float64Column("double_col", float64(i)+0.42).
				Int64Column("long_col", int64(i)).
				StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
				BoolColumn("bool_col", true).
				TimestampColumn("timestamp_col", time.UnixMicro(42)).
				At(ctx, time.UnixMicro(int64(1000*i)))
		}
		sender.Flush(ctx)
		sender.Close()
		srv.close()
	}

}

func BenchmarkHttpLineSenderNoFlush(b *testing.B) {
	ctx := context.Background()

	srv, err := newTestHttpServer(readAndDiscard)
	assert.NoError(b, err)

	sender, err := qdb.NewHttpLineSender(qdb.WithHttpAddress(srv.addr))
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sender.
			Table(testTable).
			Symbol("sym_col", "test_ilp1").
			Float64Column("double_col", float64(i)+0.42).
			Int64Column("long_col", int64(i)).
			StringColumn("str_col", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua").
			BoolColumn("bool_col", true).
			TimestampColumn("timestamp_col", time.UnixMicro(42)).
			At(ctx, time.UnixMicro(int64(1000*i)))
	}
	sender.Flush(ctx)
	sender.Close()
	srv.close()

}
