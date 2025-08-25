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
	"fmt"
	"math"
	"math/big"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Common integration tests for ILP/HTTP and ILP/TCP.

const (
	eventualDataTimeout = 60 * time.Second
)

type integrationTestSuite struct {
	suite.Suite
}

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(integrationTestSuite))
}

type writerFn func(b qdb.LineSender) error

type questdbContainer struct {
	testcontainers.Container
	proxyC              testcontainers.Container
	network             testcontainers.Network
	httpAddress         string
	ilpAddress          string
	proxyIlpTcpAddress  string
	proxyIlpHttpAddress string
}

func (c *questdbContainer) Stop(ctx context.Context) error {
	if c.proxyC != nil {
		err := c.proxyC.Terminate(ctx)
		if err != nil {
			return err
		}
	}
	if c.Container != nil {
		err := c.Terminate(ctx)
		if err != nil {
			return err
		}
	}
	if c.network != nil {
		err := c.network.Remove(ctx)
		if err != nil {
			return err
		}
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

	path, err := filepath.Abs("./test")
	if err != nil {
		return nil, err
	}
	req := testcontainers.ContainerRequest{
		Image:          "questdb/questdb:9.0.2",
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
			CheckDuplicate: false,
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
	httpAddress := fmt.Sprintf("%s:%s", ip, mappedPort.Port())

	mappedPort, err = qdbC.MappedPort(ctx, "9009")
	if err != nil {
		newNetwork.Remove(ctx)
		return nil, err
	}
	ilpAddress := fmt.Sprintf("%s:%s", ip, mappedPort.Port())

	var (
		haProxyC            testcontainers.Container
		proxyIlpTcpAddress  string
		proxyIlpHttpAddress string
	)
	if setupProxy || auth == httpBasicAuth || auth == httpBearerAuth {
		req = testcontainers.ContainerRequest{
			Image:        "haproxy:2.6.4",
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
		proxyIlpTcpAddress = fmt.Sprintf("%s:%s", ip, mappedPort.Port())

		mappedPort, err = haProxyC.MappedPort(ctx, "8445")
		if err != nil {
			qdbC.Terminate(ctx)
			newNetwork.Remove(ctx)
			return nil, err
		}
		proxyIlpHttpAddress = fmt.Sprintf("%s:%s", ip, mappedPort.Port())
	}

	return &questdbContainer{
		Container:           qdbC,
		proxyC:              haProxyC,
		network:             newNetwork,
		httpAddress:         httpAddress,
		ilpAddress:          ilpAddress,
		proxyIlpTcpAddress:  proxyIlpTcpAddress,
		proxyIlpHttpAddress: proxyIlpHttpAddress,
	}, nil
}

func (suite *integrationTestSuite) TestE2EValidWrites() {
	if testing.Short() {
		suite.T().Skip("skipping integration test")
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
			func(s qdb.LineSender) error {
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
					{"str_col", "VARCHAR"},
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
			"m y-awesome_test 1=2.csv",
			func(s qdb.LineSender) error {
				return s.
					Table("m y-awesome_test 1=2.csv").
					Symbol("sym_name 1=2", "value 1,2=3\n4\r5\"6\\7").
					StringColumn("str_name 1=2", "value 1,2=3\n4\r5\"6\\7").
					At(ctx, time.UnixMicro(1))
			},
			tableData{
				Columns: []column{
					{"sym_name 1=2", "SYMBOL"},
					{"str_name 1=2", "VARCHAR"},
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
			func(s qdb.LineSender) error {
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
			func(s qdb.LineSender) error {
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
			func(s qdb.LineSender) error {
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
			func(s qdb.LineSender) error {
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
		{
			"double array",
			testTable,
			func(s qdb.LineSender) error {
				values1D := []float64{1.0, 2.0, 3.0, 4.0, 5.0, math.NaN()}
				values2D := [][]float64{{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}, {math.NaN(), math.NaN()}}
				values3D := [][][]float64{{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, math.NaN()}}}
				arrayND, _ := qdb.NewNDArray[float64](2, 2, 1, 2)
				arrayND.Fill(11.0)
				arrayND.Set(math.NaN(), 1, 1, 0, 1)

				err := s.
					Table(testTable).
					Float64Array1DColumn("array_1d", values1D).
					Float64Array2DColumn("array_2d", values2D).
					Float64Array3DColumn("array_3d", values3D).
					Float64ArrayNDColumn("array_nd", arrayND).
					At(ctx, time.UnixMicro(1))
				if err != nil {
					return err
				}
				// empty array
				emptyNdArray, _ := qdb.NewNDArray[float64](2, 2, 0, 2)
				err = s.
					Table(testTable).
					Float64Array1DColumn("array_1d", []float64{}).
					Float64Array2DColumn("array_2d", [][]float64{{}}).
					Float64Array3DColumn("array_3d", [][][]float64{{{}}}).
					Float64ArrayNDColumn("array_nd", emptyNdArray).
					At(ctx, time.UnixMicro(2))
				if err != nil {
					return err
				}
				// null array
				return s.
					Table(testTable).
					Float64Array1DColumn("array_1d", nil).
					Float64Array2DColumn("array_2d", nil).
					Float64Array3DColumn("array_3d", nil).
					Float64ArrayNDColumn("array_nd", nil).
					At(ctx, time.UnixMicro(3))
			},
			tableData{
				Columns: []column{
					{"array_1d", "ARRAY"},
					{"array_2d", "ARRAY"},
					{"array_3d", "ARRAY"},
					{"array_nd", "ARRAY"},
					{"timestamp", "TIMESTAMP"},
				},
				Dataset: [][]interface{}{
					{
						[]interface{}{float64(1), float64(2), float64(3), float64(4), float64(5), nil},
						[]interface{}{[]interface{}{float64(1), float64(2)}, []interface{}{float64(3), float64(4)}, []interface{}{float64(5), float64(6)}, []interface{}{nil, nil}},
						[]interface{}{[]interface{}{[]interface{}{float64(1), float64(2)}, []interface{}{float64(3), float64(4)}}, []interface{}{[]interface{}{float64(5), float64(6)}, []interface{}{float64(7), nil}}},
						[]interface{}{[]interface{}{[]interface{}{[]interface{}{float64(11), float64(11)}}, []interface{}{[]interface{}{float64(11), float64(11)}}}, []interface{}{[]interface{}{[]interface{}{float64(11), float64(11)}}, []interface{}{[]interface{}{float64(11), nil}}}},
						"1970-01-01T00:00:00.000001Z"},
					{
						[]interface{}{},
						[]interface{}{},
						[]interface{}{},
						[]interface{}{},
						"1970-01-01T00:00:00.000002Z"},
					{
						nil,
						nil,
						nil,
						nil,
						"1970-01-01T00:00:00.000003Z"},
				},
				Count: 3,
			},
		},
	}

	for _, tc := range testCases {
		for _, protocol := range []string{"tcp", "http"} {
			for _, pVersion := range []int{0, 1, 2} {
				suite.T().Run(fmt.Sprintf("%s: %s", tc.name, protocol), func(t *testing.T) {
					var (
						sender qdb.LineSender
						err    error
					)

					ignoreArray := false
					questdbC, err := setupQuestDB(ctx, noAuth)
					assert.NoError(t, err)

					switch protocol {
					case "tcp":
						if pVersion == 0 {
							sender, err = qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(questdbC.ilpAddress))
							ignoreArray = true
						} else if pVersion == 1 {
							sender, err = qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(questdbC.ilpAddress), qdb.WithProtocolVersion(qdb.ProtocolVersion1))
							ignoreArray = true
						} else if pVersion == 2 {
							sender, err = qdb.NewLineSender(ctx, qdb.WithTcp(), qdb.WithAddress(questdbC.ilpAddress), qdb.WithProtocolVersion(qdb.ProtocolVersion2))
						}
						assert.NoError(t, err)
					case "http":
						if pVersion == 0 {
							sender, err = qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(questdbC.httpAddress))
						} else if pVersion == 1 {
							sender, err = qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(questdbC.httpAddress), qdb.WithProtocolVersion(qdb.ProtocolVersion1))
							ignoreArray = true
						} else if pVersion == 2 {
							sender, err = qdb.NewLineSender(ctx, qdb.WithHttp(), qdb.WithAddress(questdbC.httpAddress), qdb.WithProtocolVersion(qdb.ProtocolVersion2))
						}
						assert.NoError(t, err)
					default:
						panic(protocol)
					}
					if ignoreArray && tc.name == "double array" {
						return
					}

					dropTable(t, tc.tableName, questdbC.httpAddress)
					err = tc.writerFn(sender)
					assert.NoError(t, err)

					err = sender.Flush(ctx)
					assert.NoError(t, err)

					assert.Eventually(t, func() bool {
						data := queryTableData(t, tc.tableName, questdbC.httpAddress)
						return reflect.DeepEqual(tc.expected, data)
					}, eventualDataTimeout, 100*time.Millisecond)

					sender.Close(ctx)
					questdbC.Stop(ctx)
				})
			}
		}
	}
}
