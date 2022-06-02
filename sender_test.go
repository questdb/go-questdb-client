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
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	qdb "github.com/questdb/go-questdb-client"
)

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
	fmt.Println(httpAddress)

	mappedPort, err = container.MappedPort(ctx, "9009")
	if err != nil {
		return nil, err
	}
	ilpAddress := fmt.Sprintf("%s:%s", ip, mappedPort.Port())
	fmt.Println(ilpAddress)

	return &questdbContainer{
		Container:   container,
		httpAddress: httpAddress,
		ilpAddress:  ilpAddress,
	}, nil
}

func TestWriteAllFieldTypes(t *testing.T) {
	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer questdbC.Terminate(ctx)

	sender, err := qdb.NewLineSender(ctx, qdb.WithAddress(questdbC.ilpAddress))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		FloatField("double_col", 12.4).
		IntegerField("long_col", 12).
		StringField("str_col", "foobar").
		BooleanField("bool_col", true).
		At(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp2").
		FloatField("double_col", 11.4).
		IntegerField("long_col", 11).
		StringField("str_col", "barbaz").
		BooleanField("bool_col", false).
		AtNow(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = sender.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
