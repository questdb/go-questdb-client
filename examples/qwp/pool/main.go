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
// return it to the pool. The pool may close connections when removing unused
// or failed ones, or during shutdown. Returning a borrowed handle does not
// wait for the underlying connection to close.
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
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (resultErr error) {
	ctx := context.Background()

	// One cluster config for both directions. List every node in one addr.
	db, err := qdb.Connect(ctx, "ws::addr=localhost:9000;")
	if err != nil {
		return err
	}
	defer func() {
		// By now, result cursors are closed and borrowed handles are returned.
		// The deadline limits our wait; the pool still handles any cleanup
		// that has not finished.
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := db.Close(closeCtx)
		resultErr = errors.Join(resultErr, err)
		switch {
		case errors.Is(err, qdb.ErrCleanupFailed):
			log.Print("cleanup failed internally; resources may remain held until process exit")
		case errors.Is(err, qdb.ErrCleanupPending):
			log.Print("cleanup is still running; a later db.Close can observe it again")
		}
	}()

	// Ingest: borrow a sender, write rows, Close to return it to the pool.
	// BorrowSender hands back the LineSender interface. Type-assert it to
	// QwpSender to reach the QWP-only surface — the full type system plus
	// AtNano for a nanosecond-resolution designated timestamp.
	sender, err := db.BorrowSender(ctx)
	if err != nil {
		return err
	}
	defer func() {
		returnCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, sender.Close(returnCtx))
	}()
	qwpSender, ok := sender.(qdb.QwpSender)
	if !ok {
		return errors.New("a facade lease is always a QwpSender")
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
		return err
	}
	// The query below reads the row back, so first wait for the server to
	// confirm the write. Flush alone only queues the batch for sending:
	// the SELECT could run too early and count zero rows. FlushAndGetSequence
	// returns a sequence number that AwaitAckedFsn uses to wait for confirmation.
	// Skip that wait only if the application does not need confirmation here.
	fsn, err := qwpSender.FlushAndGetSequence(ctx)
	if err != nil {
		return err
	}
	ackCtx, cancelAck := context.WithTimeout(ctx, 10*time.Second)
	err = qwpSender.AwaitAckedFsn(ackCtx, fsn)
	cancelAck()
	if err != nil {
		return err
	}
	returnCtx, cancelReturn := context.WithTimeout(context.Background(), 10*time.Second)
	err = qwpSender.Close(returnCtx)
	cancelReturn()
	if err != nil {
		return err
	}

	// Query: borrow a session, run a SELECT, iterate its result batches.
	query, err := db.BorrowQuery(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, query.Close()) }()

	cursor := query.Query(ctx, "select count() from trades")
	defer cursor.Close()
	for batch, err := range cursor.Batches() {
		if err != nil {
			return err
		}
		log.Printf("rows in batch: %d", batch.RowCount())
	}
	return nil
}
