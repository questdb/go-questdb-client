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
	"reflect"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

func (suite *integrationTestSuite) TestE2ESuccessfulHttpBasicAuthWithTlsProxy() {
	if testing.Short() {
		suite.T().Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, httpBasicAuth)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}
	defer questdbC.Stop(ctx)

	sender, err := qdb.NewLineSender(
		ctx,
		qdb.WithHttp(),
		qdb.WithAddress(questdbC.proxyIlpHttpAddress),
		qdb.WithBasicAuth(basicAuthUser, basicAuthPass),
		qdb.WithTlsInsecureSkipVerify(),
	)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}
	defer sender.Close(ctx)

	err = sender.
		Table(testTable).
		StringColumn("str_col", "foobar").
		At(ctx, time.UnixMicro(1))
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}

	err = sender.
		Table(testTable).
		StringColumn("str_col", "barbaz").
		At(ctx, time.UnixMicro(2))
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}

	err = sender.Flush(ctx)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}

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

	assert.Eventually(suite.T(), func() bool {
		data := queryTableData(suite.T(), testTable, questdbC.httpAddress)
		return reflect.DeepEqual(expected, data)
	}, eventualDataTimeout, 100*time.Millisecond)
}

func (suite *integrationTestSuite) TestServerSideError() {
	if testing.Short() {
		suite.T().Skip("skipping integration test")
	}

	ctx := context.Background()

	var (
		sender qdb.LineSender
		err    error
	)

	questdbC, err := setupQuestDB(ctx, noAuth)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}

	sender, err = qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(questdbC.httpAddress))
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}

	err = sender.Table(testTable).Int64Column("long_col", 42).AtNow(ctx)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}
	err = sender.Flush(ctx)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}

	// Now, use wrong type for the long_col.
	err = sender.Table(testTable).StringColumn("long_col", "42").AtNow(ctx)
	if err != nil {
		assert.Fail(suite.T(), err.Error())
		return
	}
	err = sender.Flush(ctx)
	assert.Error(suite.T(), err)
	assert.ErrorContains(suite.T(), err, "my_test_table, column: long_col; cast error from protocol type: STRING to column type")
	assert.ErrorContains(suite.T(), err, "line: 1")

	sender.Close(ctx)
	questdbC.Stop(ctx)
}
