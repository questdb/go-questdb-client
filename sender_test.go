package questdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	questdb "github.com/questdb/go-questdb-client"
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

func TestBasicWrite(t *testing.T) {
	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer questdbC.Terminate(ctx)

	sender, err := questdb.NewLineSenderWithConfig(ctx, questdb.Config{
		Address:   questdbC.ilpAddress,
		BufferCap: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	err = sender.
		Table("trades").
		Symbol("name", "test_ilp1").
		FloatField("value", 12.4).
		AtNow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = sender.
		Table("trades").
		Symbol("name", "test_ilp2").
		FloatField("value", 11.4).
		AtNow(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = sender.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
