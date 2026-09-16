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

// Demonstrates the QWP store-and-forward (SF) durability mode.
// Batches queued for sending are kept in files so the client can resend them
// after reconnecting or restarting. Rows not yet queued are not saved there.
// Surviving a process restart does not imply protection against power loss.
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

	// sf_dir is the SF group root — one or more sender instances can
	// share it, each living under <sf_dir>/<sender_id>/.
	//   sender_id          : per-sender slot name (default "default")
	//   sf_max_segment_bytes       : per-segment file size (default 4 MiB)
	//   sf_max_total_bytes : disk cap for THIS sender's slot (default 10 GiB)
	//   close_flush_timeout_millis : how long Close() waits for ACKs
	//                                before proceeding (default 5000;
	//                                0 / -1 skips waiting for confirmation,
	//                                but still queues completed rows)
	//   drain_orphans      : opt in to draining sibling slots left behind
	//                        by other senders that crashed
	conf := "ws::addr=localhost:9000;" +
		"sf_dir=/var/lib/questdb-sf;" +
		"sender_id=trades-feed;" +
		"sf_max_segment_bytes=8388608;" +
		"sf_max_total_bytes=1073741824;" + // 1 GiB
		"close_flush_timeout_millis=5000;" +
		"drain_orphans=on;"
	sender, err := qdb.LineSenderFromConf(ctx, conf)
	if err != nil {
		return err
	}
	defer func() {
		// Close queues completed rows, then waits for server confirmation.
		// Queued data not yet confirmed stays in the SF files for recovery.
		// This context limits our wait, not the cleanup itself. Do not call
		// Close again on this standalone sender if the wait times out.
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := sender.Close(closeCtx)
		resultErr = errors.Join(resultErr, err)
		if errors.Is(err, qdb.ErrCleanupFailed) {
			log.Print("cleanup failed internally; resources may stay held until process exit")
			return
		}
		// The cleanup worker retries ordinary release errors. This check only
		// tells us whether this sender's directory lock has been released.
		// It does not confirm delivery, completion of work on other senders'
		// saved data, or successful cleanup of every resource.
		if qs, ok := sender.(qdb.QwpSender); ok {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for !qs.SlotLockReleased() {
				select {
				case <-closeCtx.Done():
					resultErr = errors.Join(resultErr, qdb.ErrSfCleanupPending, closeCtx.Err())
					return
				case <-ticker.C:
				}
			}
		}
	}()

	tradedTs, _ := time.Parse(time.RFC3339, "2022-08-06T15:04:05.123456Z")
	for i := 0; i < 1000; i++ {
		err := sender.
			Table("trades").
			Symbol("symbol", "ETH-USD").
			Symbol("side", "sell").
			Float64Column("price", 2615.54).
			Float64Column("amount", 0.00044).
			At(ctx, tradedTs)
		if err != nil {
			// Writing can fail because buffers are full, local storage failed,
			// or a previous error stopped the sender. Check before retrying:
			// a failed flush may already have queued some completed rows.
			return err
		}
	}
	return nil
}
