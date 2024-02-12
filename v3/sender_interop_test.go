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

package v3_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

// Tests for interoperability between all ILP clients.

type testCases []testCase

type testCase struct {
	Name    string       `json:"testName"`
	Table   string       `json:"table"`
	Symbols []testSymbol `json:"symbols"`
	Columns []testColumn `json:"columns"`
	Result  testResult   `json:"result"`
}

type testSymbol struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type testColumn struct {
	Type  string      `json:"type"`
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type testResult struct {
	Status string `json:"status"`
	Line   string `json:"line"`
}

func TestClientInterop(t *testing.T) {
	ctx := context.Background()

	testCases, err := readTestCases()
	assert.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			srv, err := newTestServer(sendToBackChannel)
			assert.NoError(t, err)

			sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(srv.addr))
			assert.NoError(t, err)

			sender.Table(tc.Table)
			for _, s := range tc.Symbols {
				sender.Symbol(s.Name, s.Value)
			}
			for _, s := range tc.Columns {
				switch s.Type {
				case "LONG":
					sender.Int64Column(s.Name, int64(s.Value.(float64)))
				case "DOUBLE":
					sender.Float64Column(s.Name, s.Value.(float64))
				case "STRING":
					sender.StringColumn(s.Name, s.Value.(string))
				case "BOOLEAN":
					sender.BoolColumn(s.Name, s.Value.(bool))
				default:
					assert.Fail(t, "unexpected column type: "+s.Type)
				}
			}

			err = sender.AtNow(ctx)

			switch tc.Result.Status {
			case "SUCCESS":
				assert.NoError(t, err)
				err = sender.Flush(ctx)
				assert.NoError(t, err)

				expectLines(t, srv.backCh, strings.Split(tc.Result.Line, "\n"))
			case "ERROR":
				assert.Error(t, err)
			default:
				assert.Fail(t, "unexpected test status: "+tc.Result.Status)
			}

			sender.Close()
			srv.close()
		})
	}
}

func readTestCases() (testCases, error) {
	file, err := os.Open("../test/interop/ilp-client-interop-test.json")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var testCases testCases
	err = json.Unmarshal(content, &testCases)
	if err != nil {
		return nil, err
	}

	return testCases, nil
}
