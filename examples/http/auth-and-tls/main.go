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
	bday, err := time.Parse(time.DateOnly, "1856-07-10")
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("inventors").
		Symbol("born", "Austrian Empire").
		TimestampColumn("birthdate", bday). // Epoch in micros.
		Int64Column("id", 0).
		StringColumn("name", "Nicola Tesla").
		At(ctx, time.Now()) // Epoch in nanos.
	if err != nil {
		log.Fatal(err)
	}

	bday, err = time.Parse(time.DateOnly, "1847-02-11")
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("inventors").
		Symbol("born", "USA").
		TimestampColumn("birthdate", bday).
		Int64Column("id", 1).
		StringColumn("name", "Thomas Alva Edison").
		AtNow(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Make sure that the messages are sent over the network.
	err = sender.Flush(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
