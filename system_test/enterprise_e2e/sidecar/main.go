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

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	fmt.Println("READY")

	var sender qdb.LineSender
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		verb := strings.ToUpper(parts[0])

		switch verb {
		case "CONNECT":
			connectString := strings.TrimSpace(line[len(parts[0]):])
			if sender != nil {
				closeSender(sender)
			}
			var err error
			sender, err = qdb.LineSenderFromConf(context.Background(), connectString)
			if err != nil {
				reply("ERR " + err.Error())
				continue
			}
			reply("OK")

		case "SEND":
			if sender == nil {
				reply("ERR no active sender; call CONNECT first")
				continue
			}
			table := parts[1]
			count, _ := strconv.Atoi(parts[2])
			startIndex := 0
			if len(parts) > 3 {
				startIndex, _ = strconv.Atoi(parts[3])
			}
			var lastErr error
			for i := 0; i < count; i++ {
				idx := startIndex + i
				err := sender.
					Table(table).
					Symbol("tag", fmt.Sprintf("test_%d", idx)).
					Int64Column("v", int64(idx)).
					At(context.Background(), time.Now())
				if err != nil {
					lastErr = err
					break
				}
			}
			if lastErr != nil {
				reply("ERR " + lastErr.Error())
			} else {
				reply("OK")
			}

		case "FLUSH":
			if sender == nil {
				reply("ERR no active sender; call CONNECT first")
				continue
			}
			if qwp, ok := sender.(qdb.QwpSender); ok {
				fsn, err := qwp.FlushAndGetSequence(context.Background())
				if err != nil {
					reply("ERR " + err.Error())
				} else {
					reply(fmt.Sprintf("OK %d", fsn))
				}
			} else {
				err := sender.Flush(context.Background())
				if err != nil {
					reply("ERR " + err.Error())
				} else {
					reply("OK -1")
				}
			}

		case "AWAIT_ACKED":
			if sender == nil {
				reply("ERR no active sender; call CONNECT first")
				continue
			}
			fsn, _ := strconv.ParseInt(parts[1], 10, 64)
			timeoutMs, _ := strconv.Atoi(parts[2])
			if qwp, ok := sender.(qdb.QwpSender); ok {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
				err := qwp.AwaitAckedFsn(ctx, fsn)
				cancel()
				if err != nil {
					reply("OK false")
				} else {
					reply("OK true")
				}
			} else {
				reply("OK true")
			}

		case "STATS":
			if sender == nil {
				reply("ERR no active sender; call CONNECT first")
				continue
			}
			if qwp, ok := sender.(qdb.QwpSender); ok {
				// Under request_durable_ack, AckedFsn (and AWAIT_ACKED) is the
				// DURABLE watermark; durableAcks/durableTrim surface that activity.
				reply(fmt.Sprintf("OK acked=%d sent=0 acks=0 reconnAttempts=%d reconnSucc=%d serverErrors=%d durableAcks=%d durableTrim=%d",
					qwp.AckedFsn(),
					qwp.TotalReconnectAttempts(),
					qwp.TotalReconnectsSucceeded(),
					qwp.TotalServerErrors(),
					qwp.TotalDurableAcks(),
					qwp.TotalDurableTrimAdvances()))
			} else {
				reply("OK acked=-1 sent=0 acks=0 reconnAttempts=0 reconnSucc=0 serverErrors=0 durableAcks=0 durableTrim=0")
			}

		case "CLOSE":
			if sender != nil {
				closeSender(sender)
				sender = nil
			}
			reply("OK")

		case "EXIT":
			if sender != nil {
				closeSender(sender)
				sender = nil
			}
			reply("OK")
			return

		default:
			reply("ERR unknown verb: " + verb)
		}
	}

	if sender != nil {
		closeSender(sender)
	}
}

func closeSender(s qdb.LineSender) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Close(ctx)
}

func reply(msg string) {
	fmt.Println(msg)
}
