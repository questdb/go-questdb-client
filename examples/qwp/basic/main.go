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

// Demonstrates the minimum correct QWP (WebSocket) ingestion idiom for a
// single-host application without failover.
//
// QWP ingestion is asynchronous: the error returned by At/AtNow/Flush is the
// local, latched error (bad value, buffer state, backpressure). Server-side
// rejections (schema mismatch, parse error, ...) arrive out of band on the
// SenderErrorHandler, NOT from the Flush that sent the data. Registering a
// handler is therefore part of the baseline idiom, not an advanced option.
package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	// WithQwp() selects the QWP binary protocol over a plain WebSocket
	// (use qdb.WithTls() for wss). A LineSender is not safe for
	// concurrent use: create one per goroutine.
	sender, err := qdb.NewLineSender(ctx,
		qdb.WithQwp(),
		qdb.WithAddress("localhost:9000"),
		qdb.WithErrorHandler(func(e *qdb.SenderError) {
			// Alert / record metrics here; this runs on a dedicated
			// goroutine, never the producer goroutine. Retriable
			// rejections are informational — the sender reconnects
			// and replays them automatically; terminal ones latch
			// and surface on the next producer call.
			log.Printf("server rejected fsn=[%d,%d] table=%s category=%s: %s",
				e.FromFsn, e.ToFsn, e.TableName, e.Category, e.ServerMessage)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// Close flushes and drains, but a failed close can mean
		// unacked data was not delivered. Always check it.
		if err := sender.Close(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	tradedTs, _ := time.Parse(time.RFC3339, "2022-08-06T15:04:05.123456Z")
	for i := 0; i < 1000; i++ {
		// Call order is fixed: Table, then Symbol(s), then columns,
		// then At/AtNow. A latched fluent error surfaces here.
		err := sender.
			Table("trades").
			Symbol("symbol", "ETH-USD").
			Symbol("side", "sell").
			Float64Column("price", 2615.54).
			Float64Column("amount", 0.00044).
			At(ctx, tradedTs)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Publish everything buffered so far. Flush returns once the batch
	// is published to the cursor engine; it does NOT wait for the
	// server ACK (rejections arrive on the handler above). Batch many
	// rows per Flush rather than flushing per row. For server-ack
	// confirmation, use FlushAndGetSequence paired with AwaitAckedFsn.
	if err := sender.Flush(ctx); err != nil {
		log.Fatal(err)
	}
}
