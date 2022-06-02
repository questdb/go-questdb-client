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

package questdb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	qdb "github.com/questdb/go-questdb-client"
)

const testTable = "my_test_table"

type questdbContainer struct {
	testcontainers.Container
	httpAddress string
	ilpAddress  string
}

func setupQuestDB(ctx context.Context) (*questdbContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "questdb/questdb",
		ExposedPorts: []string{"9000/tcp", "9009/tcp"},
		WaitingFor:   wait.ForHTTP("/").WithPort("9000"),
		// Make sure that ingested rows are committed almost immediately.
		Env: map[string]string{
			"QDB_CAIRO_MAX_UNCOMMITTED_ROWS":        "1",
			"QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL": "100",
			"QDB_PG_ENABLED":                        "false",
			"QDB_HTTP_MIN_ENABLED":                  "false",
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return nil, err
	}
	httpAddress := fmt.Sprintf("http://%s:%s", ip, mappedPort.Port())

	mappedPort, err = container.MappedPort(ctx, "9009")
	if err != nil {
		return nil, err
	}
	ilpAddress := fmt.Sprintf("%s:%s", ip, mappedPort.Port())

	return &questdbContainer{
		Container:   container,
		httpAddress: httpAddress,
		ilpAddress:  ilpAddress,
	}, nil
}

func TestAllFieldTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx)
	assert.NoError(t, err)
	defer questdbC.Terminate(ctx)

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress))
	assert.NoError(t, err)
	defer sender.Close()

	err = sender.
		Table(testTable).
		Symbol("sym_col", "test_ilp1").
		FloatField("double_col", 12.2).
		IntegerField("long_col", 12).
		StringField("str_col", "foobar").
		BooleanField("bool_col", true).
		At(ctx, 1000)
	assert.NoError(t, err)

	err = sender.
		Table(testTable).
		Symbol("sym_col", "test_ilp2").
		FloatField("double_col", 11.2).
		IntegerField("long_col", 11).
		StringField("str_col", "barbaz").
		BooleanField("bool_col", false).
		At(ctx, 2000)
	assert.NoError(t, err)

	err = sender.Flush(ctx)
	assert.NoError(t, err)

	expected := tableData{
		Columns: []column{
			{"sym_col", "SYMBOL"},
			{"double_col", "DOUBLE"},
			{"long_col", "LONG"},
			{"str_col", "STRING"},
			{"bool_col", "BOOLEAN"},
			{"timestamp", "TIMESTAMP"},
		},
		Dataset: [][]interface{}{
			{"test_ilp1", float64(12.2), float64(12), "foobar", true, "1970-01-01T00:00:00.000001Z"},
			{"test_ilp2", float64(11.2), float64(11), "barbaz", false, "1970-01-01T00:00:00.000002Z"},
		},
		Count: 2,
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, questdbC.httpAddress)
		return assert.EqualValues(t, expected, data)
	}, 10*time.Second, 100*time.Millisecond)
}

func TestImplicitFlush(t *testing.T) {
	const bufCap = 100

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx)
	assert.NoError(t, err)
	defer questdbC.Terminate(ctx)

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress), qdb.WithBufferCapacity(bufCap))
	assert.NoError(t, err)
	defer sender.Close()

	for i := 0; i < 10*bufCap; i++ {
		err = sender.
			Table(testTable).
			BooleanField("b", true).
			AtNow(ctx)
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		data := queryTableData(t, questdbC.httpAddress)
		// We didn't call Flush, but we expect the buffer to be flushed at least once.
		return assert.Greater(t, data.Count, bufCap)
	}, 10*time.Second, 100*time.Millisecond)
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

func queryTableData(t *testing.T, address string) tableData {
	u, err := url.Parse(address)
	assert.NoError(t, err)

	u.Path += "exec"
	params := url.Values{}
	params.Add("query", testTable)
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
