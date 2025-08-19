package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
)

func main() {
	ctx := context.TODO()
	// Connect to QuestDB running on 127.0.0.1:9009
	sender, err := qdb.NewLineSender(ctx, qdb.WithHttp())
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

	// Prepare array data.
	// QuestDB server version 9.0.0 or later is required for array support.
	array, err := qdb.NewNDArray[float64](2, 3, 2)
	if err != nil {
		log.Fatal(err)
	}
	hasMore := true
	val := 100.0
	for hasMore {
		hasMore, err = array.Append(val + 1)
		val = val + 1
		if err != nil {
			log.Fatal(err)
		}
	}

	err = sender.
		Table("trades").
		Symbol("symbol", "ETH-USD").
		Symbol("side", "sell").
		Float64Column("price", 2615.54).
		Float64Column("amount", 0.00044).
		Float64NDArrayColumn("price_history", array).
		At(ctx, tradedTs)
	if err != nil {
		log.Fatal(err)
	}

	// Reuse array
	hasMore = true
	array.ResetAppendIndex()
	val = 200.0
	for hasMore {
		hasMore, err = array.Append(val + 1)
		val = val + 1
		if err != nil {
			log.Fatal(err)
		}
	}
	tradedTs, err = time.Parse(time.RFC3339, "2022-08-06T15:04:06.987654Z")
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("symbol", "BTC-USD").
		Symbol("side", "sell").
		Float64Column("price", 39269.98).
		Float64Column("amount", 0.001).
		Float64NDArrayColumn("price_history", array).
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
