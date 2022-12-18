package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client"
)

const(
	sampleData = "0x123a4i"
	f8 = "0xffffffffi"
	f16 = "0xffffffffffffffffi"
	f32 = "0xffffffffffffffffffffffffffffffffi"
	f64 = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffi"
	f128 = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffi"
	f256 = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffi"
)

func main() {
	ctx := context.TODO()
	// Connect to QuestDB running on 127.0.0.1:9009
	sender, err := qdb.NewLineSender(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Make sure to close the sender on exit to release resources.
	defer sender.Close()
	// Send a few ILP messages.
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", sampleData).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", f8).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", f16).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", f32).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", f64).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	/*After this, it throws an error message*/
// io.questdb.cairo.ImplicitCastException: inconvertible value: `0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff` [STRING -> LONG256]
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", f128).
		At(ctx, time.Now().UnixNano())
	if err != nil {
		log.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		Long256Column("value", f256).
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