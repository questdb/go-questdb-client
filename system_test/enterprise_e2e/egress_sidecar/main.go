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

// go-e2e-egress-sidecar drives the read-side QwpQueryClient over a
// line-based stdin/stdout protocol, mirroring the Enterprise Java
// QwpEgressSidecarMain (wrapped by lib/egress_sidecar.py). It is the
// egress counterpart to ../sidecar (which drives the ingress sender), and
// lets the target-role / zone-failover e2e tests exercise the Go egress
// failover loop end-to-end without reimplementing it in Python.
//
// Protocol (one round-trip per command; replies are OK/ERR lines):
//
//	CONNECT <connect-string>  -> OK | ERR <msg>   (eager bind + target= filter)
//	SHOW_ZONE                 -> OK <zone|<unset>> (SQL over the bound wire)
//	SERVER_INFO               -> OK zone=<z> role=<r>  (cached snapshot, no wire)
//	QUERY <sql>               -> OK <rowCount> <latencyMs> | ERR query failed: <msg>
//	CLOSE                     -> OK
//	EXIT                      -> OK (and terminate)
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

func main() {
	fmt.Println("READY")

	var client *qdb.QwpQueryClient
	scanner := bufio.NewScanner(os.Stdin)
	// SQL passed to QUERY can exceed the 64 KiB default token size.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

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
			if client != nil {
				closeClient(client)
				client = nil
			}
			// QwpQueryClientFromConf binds eagerly: it runs the
			// multi-endpoint connect walk and applies the target= role
			// filter. When no endpoint matches, it returns a
			// *QwpRoleMismatchError whose message contains "target=<x>" —
			// which the target-filter tests match against.
			c, err := qdb.QwpQueryClientFromConf(context.Background(), connectString)
			if err != nil {
				reply("ERR " + err.Error())
				continue
			}
			client = c
			reply("OK")

		case "SHOW_ZONE":
			if client == nil {
				reply("ERR no active client; call CONNECT first")
				continue
			}
			// SQL over the currently-bound connection: routes through the
			// per-Execute failover loop, so a dead bound socket triggers a
			// transparent reconnect + rebind before answering. The value
			// column of SHOW PARAMETERS is the zone the *bound* server was
			// started with (an empty value surfaces as <unset>).
			zone, err := showZone(client)
			if err != nil {
				reply("ERR " + err.Error())
				continue
			}
			reply("OK " + zone)

		case "SERVER_INFO":
			if client == nil {
				reply("ERR no active client; call CONNECT first")
				continue
			}
			// In-memory snapshot from the most-recent successful bind. No
			// wire round-trip, so it does not itself drive a reconnect.
			info := client.ServerInfo()
			if info == nil {
				reply("OK zone=<unset> role=-1")
				continue
			}
			zone := info.ZoneId
			if zone == "" {
				zone = "<unset>"
			}
			reply(fmt.Sprintf("OK zone=%s role=%d", zone, int(info.Role)))

		case "QUERY":
			if client == nil {
				reply("ERR no active client; call CONNECT first")
				continue
			}
			sql := strings.TrimSpace(line[len(parts[0]):])
			rows, latencyMs, err := runQuery(client, sql)
			if err != nil {
				reply("ERR query failed: " + err.Error())
				continue
			}
			reply(fmt.Sprintf("OK %d %.3f", rows, latencyMs))

		case "CLOSE":
			if client != nil {
				closeClient(client)
				client = nil
			}
			reply("OK")

		case "EXIT":
			if client != nil {
				closeClient(client)
				client = nil
			}
			reply("OK")
			return

		default:
			reply("ERR unknown verb: " + verb)
		}
	}

	if client != nil {
		closeClient(client)
	}
}

// showZone runs SHOW PARAMETERS for replication.zone on the bound server
// and returns the value column of the first row, or "<unset>" when the
// zone is empty / the row is absent.
func showZone(client *qdb.QwpQueryClient) (string, error) {
	q := client.Query(context.Background(),
		"SHOW PARAMETERS WHERE property_path = 'replication.zone'")
	defer q.Close()

	zone := "<unset>"
	for batch, err := range q.Batches() {
		if err != nil {
			return "", err
		}
		valueCol := -1
		for i := 0; i < batch.ColumnCount(); i++ {
			if strings.EqualFold(batch.ColumnName(i), "value") {
				valueCol = i
				break
			}
		}
		if valueCol < 0 || batch.RowCount() == 0 || batch.IsNull(valueCol, 0) {
			continue
		}
		if v := batch.String(valueCol, 0); v != "" {
			zone = v
		}
	}
	return zone, nil
}

// runQuery executes an arbitrary SELECT and returns (rowCount, latencyMs).
func runQuery(client *qdb.QwpQueryClient, sql string) (int64, float64, error) {
	start := time.Now()
	q := client.Query(context.Background(), sql)
	defer q.Close()

	var rows int64
	for batch, err := range q.Batches() {
		if err != nil {
			return 0, 0, err
		}
		rows += int64(batch.RowCount())
	}
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	return rows, latencyMs, nil
}

func closeClient(c *qdb.QwpQueryClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

func reply(msg string) {
	fmt.Println(msg)
}
