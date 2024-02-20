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
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	qdb "github.com/questdb/go-questdb-client/v3"
)

const (
	testTable           = "my_test_table"
	networkName         = "test-network-v3"
	eventualDataTimeout = 60 * time.Second
)

type questdbContainer struct {
	testcontainers.Container
	proxyC                       testcontainers.Container
	network                      testcontainers.Network
	httpAddress                  string
	ilpAddress                   string
	proxyIlpAddress              string
	proxyIlpHttpAddress          string
	proxyIlpHttpBasicAuthAddress string
}

func (c *questdbContainer) Stop(ctx context.Context) error {
	if c.proxyC != nil {
		err := c.proxyC.Terminate(ctx)
		if err != nil {
			return err
		}
	}
	err := c.Terminate(ctx)
	if err != nil {
		return err
	}
	err = c.network.Remove(ctx)
	if err != nil {
		return err
	}
	return nil
}

type ilpAuthType int64

const (
	noAuth         ilpAuthType = 0
	authEnabled    ilpAuthType = 1
	httpBasicAuth  ilpAuthType = 2
	httpBearerAuth ilpAuthType = 3
)

const (
	basicAuthUser = "joe"
	basicAuthPass = "joespassword"
	bearerToken   = "testToken1"
)

func setupQuestDB(ctx context.Context, auth ilpAuthType) (*questdbContainer, error) {
	return setupQuestDB0(ctx, auth, false)
}

func setupQuestDBWithProxy(ctx context.Context, auth ilpAuthType) (*questdbContainer, error) {
	return setupQuestDB0(ctx, auth, true)
}

func setupQuestDB0(ctx context.Context, auth ilpAuthType, setupProxy bool) (*questdbContainer, error) {
	// Make sure that ingested rows are committed almost immediately.
	env := map[string]string{
		"QDB_CAIRO_MAX_UNCOMMITTED_ROWS":        "1",
		"QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL": "100",
		"QDB_PG_ENABLED":                        "true",
		"QDB_HTTP_MIN_ENABLED":                  "false",
		"QDB_LINE_HTTP_ENABLED":                 "true",
	}

	switch auth {
	case authEnabled:
		env["QDB_LINE_TCP_AUTH_DB_PATH"] = "/auth/questdb.auth.txt"
	case httpBasicAuth:
		env["QDB_PG_USER"] = basicAuthUser
		env["QDB_PG_PASSWORD"] = basicAuthPass
	case httpBearerAuth:
		return nil, fmt.Errorf("idk how to set up bearer auth")
	}

	path, err := filepath.Abs("../test")
	if err != nil {
		return nil, err
	}
	req := testcontainers.ContainerRequest{
		Image:          "questdb/questdb:7.3.9",
		ExposedPorts:   []string{"9000/tcp", "9009/tcp"},
		WaitingFor:     wait.ForHTTP("/").WithPort("9000"),
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"questdb"}},
		Env:            env,
		Mounts: testcontainers.Mounts(testcontainers.ContainerMount{
			Source: testcontainers.GenericBindMountSource{
				HostPath: path,
			},
			Target: testcontainers.ContainerMountTarget("/root/.questdb/auth"),
		}),
	}

	newNetwork, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:           networkName,
			CheckDuplicate: true,
		},
	})
	if err != nil {
		return nil, err
	}

	qdbC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		newNetwork.Remove(ctx)
		return nil, err
	}

	ip, err := qdbC.Host(ctx)
	if err != nil {
		newNetwork.Remove(ctx)
		return nil, err
	}

	mappedPort, err := qdbC.MappedPort(ctx, "9000")
	if err != nil {
		newNetwork.Remove(ctx)
		return nil, err
	}
	httpAddress := fmt.Sprintf("http://%s:%s", ip, mappedPort.Port())

	mappedPort, err = qdbC.MappedPort(ctx, "9009")
	if err != nil {
		newNetwork.Remove(ctx)
		return nil, err
	}
	ilpAddress := fmt.Sprintf("%s:%s", ip, mappedPort.Port())

	var (
		haProxyC                  testcontainers.Container
		proxyAddress              string
		proxyHttpAddress          string
		proxyHttpBasicAuthAddress string
	)
	if setupProxy || auth == httpBasicAuth || auth == httpBearerAuth {
		req = testcontainers.ContainerRequest{
			Image:        "haproxy:2.6.0",
			ExposedPorts: []string{"8443/tcp", "8444/tcp", "8445/tcp", "8888/tcp"},
			WaitingFor:   wait.ForHTTP("/").WithPort("8888"),
			Networks:     []string{networkName},
			Mounts: testcontainers.Mounts(testcontainers.ContainerMount{
				Source: testcontainers.GenericBindMountSource{
					HostPath: path,
				},
				Target: testcontainers.ContainerMountTarget("/usr/local/etc/haproxy"),
			}),
		}
		haProxyC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			qdbC.Terminate(ctx)
			newNetwork.Remove(ctx)
			return nil, err
		}

		ip, err := haProxyC.Host(ctx)
		if err != nil {
			qdbC.Terminate(ctx)
			newNetwork.Remove(ctx)
			return nil, err
		}

		mappedPort, err := haProxyC.MappedPort(ctx, "8443")
		if err != nil {
			qdbC.Terminate(ctx)
			newNetwork.Remove(ctx)
			return nil, err
		}
		proxyAddress = fmt.Sprintf("%s:%s", ip, mappedPort.Port())

		mappedPort, err = haProxyC.MappedPort(ctx, "8444")
		if err != nil {
			qdbC.Terminate(ctx)
			newNetwork.Remove(ctx)
			return nil, err
		}
		proxyHttpAddress = fmt.Sprintf("%s:%s", ip, mappedPort.Port())

		mappedPort, err = haProxyC.MappedPort(ctx, "8445")
		if err != nil {
			qdbC.Terminate(ctx)
			newNetwork.Remove(ctx)
			return nil, err
		}
		proxyHttpBasicAuthAddress = fmt.Sprintf("%s:%s", ip, mappedPort.Port())
	}

	return &questdbContainer{
		Container:                    qdbC,
		proxyC:                       haProxyC,
		network:                      newNetwork,
		httpAddress:                  httpAddress,
		ilpAddress:                   ilpAddress,
		proxyIlpAddress:              proxyAddress,
		proxyIlpHttpAddress:          proxyHttpAddress,
		proxyIlpHttpBasicAuthAddress: proxyHttpBasicAuthAddress,
	}, nil
}

func TestE2EValidWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	testCases := []struct {
		name      string
		tableName string
		writerFn  writerFn
		expected  tableData
	}{
		{
			"all column types",
			testTable,
			func(s *qdb.LineSender) error {
				val, _ := big.NewInt(0).SetString("123a4", 16)
				err := s.
					Table(testTable).
					Symbol("sym_col", "test_ilp1").
					Float64Column("double_col", 12.2).
					Int64Column("long_col", 12).
					Long256Column("long256_col", val).
					StringColumn("str_col", "foobar").
					BoolColumn("bool_col", true).
					TimestampColumn("timestamp_col", time.UnixMicro(42)).
					At(ctx, time.UnixMicro(1))
				if err != nil {
					return err
				}

				val, _ = big.NewInt(0).SetString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
				return s.
					Table(testTable).
					Symbol("sym_col", "test_ilp2").
					Float64Column("double_col", 11.2).
					Int64Column("long_col", 11).
					Long256Column("long256_col", val).
					StringColumn("str_col", "barbaz").
					BoolColumn("bool_col", false).
					TimestampColumn("timestamp_col", time.UnixMicro(43)).
					At(ctx, time.UnixMicro(2))
			},
			tableData{
				Columns: []column{
					{"sym_col", "SYMBOL"},
					{"double_col", "DOUBLE"},
					{"long_col", "LONG"},
					{"long256_col", "LONG256"},
					{"str_col", "STRING"},
					{"bool_col", "BOOLEAN"},
					{"timestamp_col", "TIMESTAMP"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{"test_ilp1", float64(12.2), float64(12), "0x0123a4", "foobar", true, "1970-01-01T00:00:00.000042Z", "1970-01-01T00:00:00.000001Z"},
					{"test_ilp2", float64(11.2), float64(11), "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "barbaz", false, "1970-01-01T00:00:00.000043Z", "1970-01-01T00:00:00.000002Z"},
				},
				Count: 2,
			},
		},
		{
			"escaped chars",
			"my-awesome_test 1=2.csv",
			func(s *qdb.LineSender) error {
				return s.
					Table("my-awesome_test 1=2.csv").
					Symbol("sym_name 1=2", "value 1,2=3\n4\r5\"6\\7").
					StringColumn("str_name 1=2", "value 1,2=3\n4\r5\"6\\7").
					At(ctx, time.UnixMicro(1))
			},
			tableData{
				Columns: []column{
					{"sym_name 1=2", "SYMBOL"},
					{"str_name 1=2", "STRING"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{"value 1,2=3\n4\r5\"6\\7", "value 1,2=3\n4\r5\"6\\7", "1970-01-01T00:00:00.000001Z"},
				},
				Count: 1,
			},
		},
		{
			"single symbol",
			testTable,
			func(s *qdb.LineSender) error {
				return s.
					Table(testTable).
					Symbol("foo", "bar").
					At(ctx, time.UnixMicro(42))
			},
			tableData{
				Columns: []column{
					{"foo", "SYMBOL"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{"bar", "1970-01-01T00:00:00.000042Z"},
				},
				Count: 1,
			},
		},
		{
			"single column",
			testTable,
			func(s *qdb.LineSender) error {
				return s.
					Table(testTable).
					Int64Column("foobar", 1_000_042).
					At(ctx, time.UnixMicro(42))
			},
			tableData{
				Columns: []column{
					{"foobar", "LONG"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{float64(1_000_042), "1970-01-01T00:00:00.000042Z"},
				},
				Count: 1,
			},
		},
		{
			"single column long256",
			testTable,
			func(s *qdb.LineSender) error {
				val, _ := big.NewInt(0).SetString("7fffffffffffffff", 16)
				return s.
					Table(testTable).
					Long256Column("foobar", val).
					At(ctx, time.UnixMicro(42))
			},
			tableData{
				Columns: []column{
					{"foobar", "LONG256"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{"0x7fffffffffffffff", "1970-01-01T00:00:00.000042Z"},
				},
				Count: 1,
			},
		},
		{
			"double value with exponent",
			testTable,
			func(s *qdb.LineSender) error {
				return s.
					Table(testTable).
					Float64Column("foobar", 4.2e-100).
					At(ctx, time.UnixMicro(1))
			},
			tableData{
				Columns: []column{
					{"foobar", "DOUBLE"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{4.2e-100, "1970-01-01T00:00:00.000001Z"},
				},
				Count: 1,
			},
		},
	}

	for _, tc := range testCases {
		for _, protocol := range []string{"tcp", "http"} {
			t.Run(fmt.Sprintf("%s: %s", protocol, tc.name), func(t *testing.T) {
				var (
					sender *qdb.LineSender
					u      *url.URL
					err    error
				)
				questdbC, err := setupQuestDB(ctx, noAuth)
				assert.NoError(t, err)

				if protocol == "tcp" {
					sender, err = qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress))
				} else {
					u, err = url.Parse(questdbC.httpAddress)
					assert.NoError(t, err)
					sender, err = qdb.FromConf(ctx, fmt.Sprintf("http::addr=%s", u.Host))
				}
				assert.NoError(t, err)

				err = tc.writerFn(sender)
				assert.NoError(t, err)

				err = sender.Flush(ctx)
				assert.NoError(t, err)

				assert.Eventually(t, func() bool {
					data := queryTableData(t, tc.tableName, questdbC.httpAddress)
					return reflect.DeepEqual(tc.expected, data)
				}, eventualDataTimeout, 100*time.Millisecond)

				sender.Close()
				questdbC.Stop(ctx)
			})
		}

	}
}

func TestE2EWriteInBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		n      = 100
		nBatch = 100
	)

	ctx := context.Background()
	for _, protocol := range []string{"tcp", "http"} {
		t.Run(protocol, func(t *testing.T) {
			var (
				sender *qdb.LineSender
				u      *url.URL
				err    error
			)
			questdbC, err := setupQuestDB(ctx, noAuth)
			assert.NoError(t, err)
			defer questdbC.Stop(ctx)

			if protocol == "tcp" {
				sender, err = qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress))
			} else {
				u, err = url.Parse(questdbC.httpAddress)
				assert.NoError(t, err)
				sender, err = qdb.FromConf(ctx, fmt.Sprintf("http::addr=%s", u.Host))
			}
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
		})
	}

}

func TestE2EImplicitFlush(t *testing.T) {
	const bufCap = 100

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	for _, protocol := range []string{"tcp", "http"} {
		t.Run(protocol, func(t *testing.T) {
			var (
				sender *qdb.LineSender
				u      *url.URL
				err    error
			)

			questdbC, err := setupQuestDB(ctx, noAuth)
			assert.NoError(t, err)
			defer questdbC.Stop(ctx)

			if protocol == "tcp" {
				sender, err = qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress), qdb.WithBufferCapacity(bufCap))
			} else {
				u, err = url.Parse(questdbC.httpAddress)
				assert.NoError(t, err)
				sender, err = qdb.FromConf(ctx, fmt.Sprintf("http::addr=%s;max_buf_size=%d", u.Host, bufCap))
			}
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

		})
	}

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

func TestE2ESuccessfulHttpsBasicAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, httpBasicAuth)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := qdb.FromConf(ctx, fmt.Sprintf("https::addr=%s;user=%s;pass=%s;tls_verify=unsafe_off", questdbC.proxyIlpHttpBasicAuthAddress, basicAuthUser, basicAuthPass))
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
		qdb.WithAddress(questdbC.proxyIlpAddress), // We're sending data through proxy.
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
		qdb.WithAddress(questdbC.proxyIlpAddress), // We're sending data through proxy.
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
