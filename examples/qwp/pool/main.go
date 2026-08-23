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
	"errors"
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
		// In SF mode an outstanding lease, construction, or cleanup is a
		// shutdown obligation. Return every lease and retry this "not yet"
		// status; nil is the stable proof that every pooled slot is unlocked.
		for {
			err := db.Close(ctx)
			if errors.Is(err, qdb.ErrSfCleanupPending) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if err != nil {
				log.Printf("questdb close: %v", err)
			}
			return
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
	// This example reads the row straight back with a count query below, so it
	// must confirm the write committed first. Flush publishes the batch to the
	// background send loop but does NOT wait for the server ACK, so a plain
	// Flush here can let the SELECT run before the row lands and print a count
	// of 0. FlushAndGetSequence returns this batch's frame sequence;
	// AwaitAckedFsn blocks until the server acknowledges it. A write-only app
	// does not need the wait — Flush alone is enough.
	fsn, err := qwpSender.FlushAndGetSequence(ctx)
	if err != nil {
		log.Fatal(err)
	}
	ackCtx, cancelAck := context.WithTimeout(ctx, 10*time.Second)
	err = qwpSender.AwaitAckedFsn(ackCtx, fsn)
	cancelAck()
	if err != nil {
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
