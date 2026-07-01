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

// Demonstrates the QuestDB facade: one ws/wss config string drives pooled
// ingest and pooled query over the whole cluster. Construct once, share across
// goroutines; borrow a sender or query session per unit of work and Close it to
// return it to the pool. The real disconnect happens only at QuestDB.Close.
//
// Set lazy_connect=true to tolerate the server being down at startup: ingest
// connects asynchronously (writes buffer until the wire is up) and the read
// pool connects lazily on first borrow. Reads stay enabled.
package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	// One cluster config for both directions. List every node in one addr.
	db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			log.Printf("questdb close: %v", err)
		}
	}()

	// Ingest: borrow a sender, write rows, Close to return it to the pool.
	// BorrowSender hands back the LineSender interface. Type-assert it to
	// QwpSender to reach the QWP-only surface — the full type system plus
	// AtNano for a nanosecond-resolution designated timestamp.
	sender, err := db.BorrowSender(ctx)
	if err != nil {
		log.Fatal(err)
	}
	qwpSender, ok := sender.(qdb.QwpSender)
	if !ok {
		log.Fatal("a facade lease is always a QwpSender")
	}
	// Build the row with the fluent API, then close it with AtNano — the
	// nanosecond-resolution designated timestamp only QwpSender exposes (the
	// QWP-only typed columns Int32Column/UuidColumn/arrays/decimals live here
	// too). The inherited fluent methods return LineSender, so call AtNano on
	// the QwpSender handle rather than chaining it.
	qwpSender.
		Table("trades").
		Symbol("symbol", "ETH-USD").
		Float64Column("price", 2615.54)
	if err := qwpSender.AtNano(ctx, time.Now()); err != nil {
		log.Fatal(err)
	}
	if err := qwpSender.Flush(ctx); err != nil {
		log.Fatal(err)
	}
	if err := qwpSender.Close(ctx); err != nil {
		log.Fatal(err)
	}

	// Query: borrow a session, run a SELECT, iterate its result batches.
	query, err := db.BorrowQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := query.Close(); err != nil {
			log.Printf("query close: %v", err)
		}
	}()

	cursor := query.Query(ctx, "select count() from trades")
	defer cursor.Close()
	for batch, err := range cursor.Batches() {
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("rows in batch: %d", batch.RowCount())
	}
}
