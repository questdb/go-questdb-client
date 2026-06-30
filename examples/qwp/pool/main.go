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

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	// One cluster config for both directions. List every node in one addr.
	db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(ctx)

	// Ingest: borrow a sender, write rows, Close to return it to the pool.
	sender, err := db.BorrowSender(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := sender.
		Table("trades").
		Symbol("symbol", "ETH-USD").
		Float64Column("price", 2615.54).
		AtNow(ctx); err != nil {
		log.Fatal(err)
	}
	if err := sender.Flush(ctx); err != nil {
		log.Fatal(err)
	}
	if err := sender.Close(ctx); err != nil {
		log.Fatal(err)
	}

	// Query: borrow a session, run a SELECT, iterate its result batches.
	query, err := db.BorrowQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer query.Close()

	cursor := query.Query(ctx, "select count() from trades")
	defer cursor.Close()
	for batch, err := range cursor.Batches() {
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("rows in batch: %d", batch.RowCount())
	}
}
