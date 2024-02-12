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
	"fmt"
	"testing"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
)

type configTestCase struct {
	name         string
	config       string
	expectedOpts []qdb.LineSenderOption
	expectedErr  error
}

func TestFromConf(t *testing.T) {
	srv, err := newTestServer(readAndDiscard)
	assert.NoError(t, err)
	defer srv.close()

	testCases := []configTestCase{
		{
			name:   "http and address",
			config: fmt.Sprintf("http::addr=%s;", srv.addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithHttp(),
				qdb.WithAddress(srv.addr),
			},
		},
		{
			name:   "tcp and address",
			config: fmt.Sprintf("tcp::addr=%s;", srv.addr),
			expectedOpts: []qdb.LineSenderOption{
				qdb.WithTcp(),
				qdb.WithAddress(srv.addr),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			actual, actualErr := qdb.FromConf(context.TODO(), tc.config)
			if actualErr != nil {
				assert.ErrorIs(t, actualErr, tc.expectedErr)
				assert.ErrorContains(t, actualErr, tc.expectedErr.Error())
				return
			} else {
				assert.NoError(t, tc.expectedErr)
			}

			defer actual.Close()

			expected, err := qdb.NewLineSender(context.TODO(), tc.expectedOpts...)
			assert.NoError(t, err)
			defer expected.Close()

			assert.EqualValues(t, expected, actual)

		})

	}

}
