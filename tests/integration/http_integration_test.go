package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/questdb/go-questdb-client/v3/pkg/http"
	"github.com/stretchr/testify/assert"
)

func TestE2ESuccessfulHttpBasicAuthWithTlsProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// todo: either this needs to run last, all tests need to use tls, or we use a pluggable client
	// because using tls modifies the global http context

	ctx := context.Background()

	questdbC, err := setupQuestDB(ctx, httpBasicAuth)
	assert.NoError(t, err)
	defer questdbC.Stop(ctx)

	sender, err := http.NewLineSender(
		http.WithAddress(questdbC.proxyIlpHttpAddress),
		http.WithBasicAuth(basicAuthUser, basicAuthPass),
		http.WithTlsInsecureSkipVerify(),
	)
	assert.NoError(t, err)
	defer sender.Close(ctx)

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
