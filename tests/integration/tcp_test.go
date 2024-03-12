/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	qdb "github.com/questdb/go-questdb-client/v3"
)

func TestE2EWriteInBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		n      = 100
		nBatch = 100
	)

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, noAuth)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress))
	assert.NoError(t, err)
	defer sender.Close()

	for i := 0; i < n; i++ {
		for j := 0; j < nBatch; j++ {
			err = sender.
				Table(testTable).
				Int64Column("long_col", int64(j)).
				At(ctx, time.UnixMicro(int64(i*nBatch+j)))
			assert.NoError(t, err)
		}
		err = sender.Flush(ctx)
		assert.NoError(t, err)
	}

	expected := tableData{
		Columns: []column{
			{"long_col", "LONG"},
			{"timestamp", "TIMESTAMP"},
		},
		Dataset: [][]interface{}{},
		Count:   n * nBatch,
	}

	for i := 0; i < n; i++ {
		for j := 0; j < nBatch; j++ {
			expected.Dataset = append(
				expected.Dataset,
				[]interface{}{float64(j), "1970-01-01T00:00:00." + fmt.Sprintf("%06d", i*nBatch+j) + "Z"},
			)
		}
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, testTable, questdbC.httpAddress)
		return reflect.DeepEqual(expected, data)
	}, eventualDataTimeout, 100*time.Millisecond)
}

func TestE2EImplicitFlush(t *testing.T) {
	const bufCap = 100

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, noAuth)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress), qdb.WithBufferCapacity(bufCap))
	assert.NoError(t, err)
	defer sender.Close()

	for i := 0; i < 10*bufCap; i++ {
		err = sender.
			Table(testTable).
			BoolColumn("b", true).
			AtNow(ctx)
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, testTable, questdbC.httpAddress)
		// We didn't call Flush, but we expect the buffer to be flushed at least once.
		return data.Count > 0
	}, eventualDataTimeout, 100*time.Millisecond)
}

func TestE2ESuccessfulAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, authEnabled)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithAddress(questdbC.ilpAddress),
		qdb.WithAuth("testUser1", "5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48"),
	)
	assert.NoError(t, err)

	err = sender.
		Table(testTable).
		StringColumn("str_col", "foobar").
		At(ctx, time.UnixMicro(1))
	assert.NoError(t, err)

	err = sender.
		Table(testTable).
		StringColumn("str_col", "barbaz").
		At(ctx, time.UnixMicro(2))
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	// Close the connection to make sure that ILP messages are written. That's because
	// the server may not write messages that are received immediately after the signed
	// challenge until the connection is closed or more data is received.
	sender.Close()

	expected := tableData{
		Columns: []column{
			{"str_col", "STRING"},
			{"timestamp", "TIMESTAMP"},
		},
		Dataset: [][]interface{}{
			{"foobar", "1970-01-01T00:00:00.000001Z"},
			{"barbaz", "1970-01-01T00:00:00.000002Z"},
		},
		Count: 2,
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, testTable, questdbC.httpAddress)
		return reflect.DeepEqual(expected, data)
	}, eventualDataTimeout, 100*time.Millisecond)
}

func TestE2EFailedAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, authEnabled)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithAddress(questdbC.ilpAddress),
		qdb.WithAuth("wrongKeyId", "1234567890"),
	)
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.
		Table(testTable).
		StringColumn("str_col", "foobar").
		At(ctx, time.UnixMicro(1))
	// If we get an error here or later, it means that the server closed connection.
	if err != nil {
		return
	}

	err = sender.
		Table(testTable).
		StringColumn("str_col", "barbaz").
		At(ctx, time.UnixMicro(2))
	if err != nil {
		return
	}

	err = sender.Flush(ctx)
	if err != nil {
		return
	}

	// Our writes should not get applied.
	time.Sleep(2 * time.Second)
	data := queryTableData(t, testTable, questdbC.httpAddress)
	assert.Equal(t, 0, data.Count)
}

func TestE2EWritesWithTlsProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDBWithProxy(ctx, noAuth)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithAddress(questdbC.proxyIlpTcpAddress), // We're sending data through proxy.
		qdb.WithTlsInsecureSkipVerify(),
	)
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.
		Table(testTable).
		StringColumn("str_col", "foobar").
		At(ctx, time.UnixMicro(1))
	assert.NoError(t, err)

	err = sender.
		Table(testTable).
		StringColumn("str_col", "barbaz").
		At(ctx, time.UnixMicro(2))
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	expected := tableData{
		Columns: []column{
			{"str_col", "STRING"},
			{"timestamp", "TIMESTAMP"},
		},
		Dataset: [][]interface{}{
			{"foobar", "1970-01-01T00:00:00.000001Z"},
			{"barbaz", "1970-01-01T00:00:00.000002Z"},
		},
		Count: 2,
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, testTable, questdbC.httpAddress)
		return reflect.DeepEqual(expected, data)
	}, eventualDataTimeout, 100*time.Millisecond)
}

func TestE2ESuccessfulAuthWithTlsProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDBWithProxy(ctx, authEnabled)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithAddress(questdbC.proxyIlpTcpAddress), // We're sending data through proxy.
		qdb.WithAuth("testUser1", "5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48"),
		qdb.WithTlsInsecureSkipVerify(),
	)
	assert.NoError(t, err)

	err = sender.
		Table(testTable).
		StringColumn("str_col", "foobar").
		At(ctx, time.UnixMicro(1))
	assert.NoError(t, err)

	err = sender.
		Table(testTable).
		StringColumn("str_col", "barbaz").
		At(ctx, time.UnixMicro(2))
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	// Close the connection to make sure that ILP messages are written. That's because
	// the server may not write messages that are received immediately after the signed
	// challenge until the connection is closed or more data is received.
	sender.Close()

	expected := tableData{
		Columns: []column{
			{"str_col", "STRING"},
			{"timestamp", "TIMESTAMP"},
		},
		Dataset: [][]interface{}{
			{"foobar", "1970-01-01T00:00:00.000001Z"},
			{"barbaz", "1970-01-01T00:00:00.000002Z"},
		},
		Count: 2,
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, testTable, questdbC.httpAddress)
		return reflect.DeepEqual(expected, data)
	}, eventualDataTimeout, 100*time.Millisecond)
}

type tableData struct {
	Columns []column        `json:"columns"`
	Dataset [][]interface{} `json:"dataset"`
	Count   int             `json:"count"`
}

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func queryTableData(t *testing.T, tableName, address string) tableData {
	// We always query data using the QuestDB container over http
	address = "http://" + address
	u, err := url.Parse(address)
	assert.NoError(t, err)

	u.Path += "exec"
	params := url.Values{}
	params.Add("query", "'"+tableName+"'")
	u.RawQuery = params.Encode()
	url := fmt.Sprintf("%v", u)

	res, err := http.Get(url)
	assert.NoError(t, err)
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)

	data := tableData{}
	err = json.Unmarshal(body, &data)
	assert.NoError(t, err)

	return data
}
