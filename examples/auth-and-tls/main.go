package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client"
)

func main() {
	ctx := context.TODO()
	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithAddress("localhost:9009"),
		qdb.WithAuth(
			"testUser1",
			"5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48"),
		qdb.WithTls(),
	)
	if err != nil {
		log.Fatal(err)
	}
	// Make sure to close the sender on exit to release resources.
	defer sender.Close()
	// Send a few ILP messages.
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Float64Column("value", 12.4).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp2").
		Float64Column("value", 11.4).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	// Make sure that the messages are sent over the network.
	err = sender.Flush(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
