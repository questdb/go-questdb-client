package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
)

func main() {
	ctx := context.TODO()
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress("localhost:9000"),
		qdb.WithBearerToken(
			"5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48", // token here
		),
		qdb.WithTls(),
	)
	if err != nil {
		log.Fatal(err)
	}
	// Make sure to close the sender on exit to release resources.
	defer func() {
		err := sender.Close(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Send a few ILP messages.
	tradedTs, err := time.Parse(time.RFC3339, "2022-08-06T15:04:05.123456Z")
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades_go").
		Symbol("pair", "USDGBP").
		Symbol("type", "buy").
		Float64Column("traded_price", 0.83).
		Float64Column("limit_price", 0.84).
		Int64Column("qty", 100).
		At(ctx, tradedTs)
	if err != nil {
		log.Fatal(err)
	}

	tradedTs, err = time.Parse(time.RFC3339, "2022-08-06T15:04:06.987654Z")
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades_go").
		Symbol("pair", "GBPJPY").
		Symbol("type", "sell").
		Float64Column("traded_price", 135.97).
		Float64Column("limit_price", 0.84).
		Int64Column("qty", 400).
		At(ctx, tradedTs)
	if err != nil {
		log.Fatal(err)
	}

	// Make sure that the messages are sent over the network.
	err = sender.Flush(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
