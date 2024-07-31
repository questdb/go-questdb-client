package pool_test

import (
	"context"
	"fmt"

	utils "github.com/questdb/go-questdb-client/v3/internal/testutils"
	"github.com/questdb/go-questdb-client/v3/pool"
)

func Example_lineSenderPool() {
	ctx := context.Background()

	addr := setupMockQuestDBServer()

	pool := pool.FromConf(fmt.Sprintf("http::addr=%s", addr))
	defer func() {
		err := pool.Close(ctx)
		if err != nil {
			panic(err)
		}
	}()

	sender, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}

	sender.Table("prices").
		Symbol("ticker", "AAPL").
		Float64Column("price", 123.45).
		AtNow(ctx)

	if err := pool.Release(ctx, sender); err != nil {
		panic(err)
	}

}

func setupMockQuestDBServer() string {
	srv, err := utils.NewTestHttpServer(utils.ReadAndDiscard)
	if err != nil {
		panic(err)
	}

	return srv.Addr()
}
