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
// QWP sends data in the background. A successful Flush queues data for sending;
// it does not confirm that the server accepted it. Calls that write or flush rows
// can report local errors or a saved error that stopped background sending.
// The error handler also reports server rejections, including those the client
// will retry.
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

	// WithQwp() selects the QWP binary protocol over a plain WebSocket
	// (use qdb.WithTls() for wss). A LineSender is not safe for
	// concurrent use: create one per goroutine.
	sender, err := qdb.NewLineSender(ctx,
		qdb.WithQwp(),
		qdb.WithAddress("localhost:9000"),
		qdb.WithErrorHandler(func(e *qdb.SenderError) {
			// Log or record metrics here; this callback runs on a separate
			// goroutine. The sender reconnects and resends after retriable
			// rejections. Errors that stop sending are saved for later calls
			// to report, as described in each method's docs. Notify the code
			// using the sender; do not use or close the sender here.
			log.Printf("server rejected fsn=[%d,%d] table=%s category=%s: %s",
				e.FromFsn, e.ToFsn, e.TableName, e.Category, e.ServerMessage)
		}),
	)
	if err != nil {
		return err
	}
	defer func() {
		// Close queues completed rows, then waits for server confirmation
		// using the configured close-flush timeout. This context limits how
		// long we wait for Close, but cleanup still belongs to the sender.
		// Do not call Close again on this standalone sender after a timeout.
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, sender.Close(closeCtx))
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
			return err
		}
	}

	// Publish everything buffered so far. Flush returns once the batch
	// is published to the cursor engine; it does NOT wait for the
	// server ACK (rejections arrive on the handler above). Batch many
	// rows per Flush rather than flushing per row. For server-ack
	// confirmation, use FlushAndGetSequence paired with AwaitAckedFsn.
	return sender.Flush(ctx)
}
