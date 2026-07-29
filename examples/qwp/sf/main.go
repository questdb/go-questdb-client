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
// Outgoing batches are persisted to mmap'd disk segments before they
// leave the wire; the I/O loop replays from disk transparently on
// reconnect or process restart.
package main

import (
	"context"
	"log"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	ctx := context.TODO()

	// sf_dir is the SF group root — one or more sender instances can
	// share it, each living under <sf_dir>/<sender_id>/.
	//   sender_id          : per-sender slot name (default "default")
	//   sf_max_segment_bytes       : per-segment file size (default 4 MiB)
	//   sf_max_total_bytes : disk cap for THIS sender's slot (default 10 GiB)
	//   close_flush_timeout_millis : how long Close() waits for ACKs
	//                                before proceeding (default 5000;
	//                                0 / -1 → fast close, leave on disk)
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
		log.Fatal(err)
	}
	defer func() {
		// Close() drains the engine (waiting up to
		// close_flush_timeout_millis for the server to ACK every
		// frame) and releases the slot lock. Anything still on disk
		// will be replayed by the next process to start with the
		// same sf_dir + sender_id.
		if err := sender.Close(ctx); err != nil {
			log.Fatal(err)
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
			// In SF mode, At() can block briefly on disk-full
			// backpressure when sf_max_total_bytes is reached and
			// the wire path hasn't drained the cap. The error here
			// surfaces the deadline expiry — investigate the wire
			// path (server reachability, server slow, etc.) rather
			// than retrying tighter.
			log.Fatal(err)
		}
	}
}
