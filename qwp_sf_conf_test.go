/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package questdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSfConfParseAcceptsAllKnobs(t *testing.T) {
	conf, err := confFromStr(strings.Join([]string{
		"ws::addr=localhost:9000",
		"sf_dir=/tmp/sf",
		"sender_id=my-sender",
		"sf_max_bytes=8388608",
		"sf_max_total_bytes=21474836480",
		"sf_durability=memory",
		"sf_append_deadline_millis=20000",
		"reconnect_max_duration_millis=120000",
		"reconnect_initial_backoff_millis=200",
		"reconnect_max_backoff_millis=10000",
		"initial_connect_retry=on",
		"close_flush_timeout_millis=2500",
		"drain_orphans=on",
		"max_background_drainers=2;",
	}, ";"))
	require.NoError(t, err)
	assert.Equal(t, "/tmp/sf", conf.sfDir)
	assert.Equal(t, "my-sender", conf.senderId)
	assert.Equal(t, int64(8388608), conf.sfMaxBytes)
	assert.Equal(t, int64(21474836480), conf.sfMaxTotalBytes)
	assert.Equal(t, "memory", conf.sfDurability)
	assert.Equal(t, 20000, conf.sfAppendDeadlineMillis)
	assert.Equal(t, 120000, conf.reconnectMaxDurationMillis)
	assert.Equal(t, 200, conf.reconnectInitialBackoffMillis)
	assert.Equal(t, 10000, conf.reconnectMaxBackoffMillis)
	assert.True(t, conf.initialConnectRetry)
	assert.Equal(t, 2500, conf.closeFlushTimeoutMillis)
	assert.True(t, conf.closeFlushTimeoutSet)
	assert.True(t, conf.drainOrphans)
	assert.Equal(t, 2, conf.maxBackgroundDrainers)
}

func TestSfConfRejectsNonQwpSchema(t *testing.T) {
	for _, schema := range []string{"http", "https", "tcp", "tcps"} {
		t.Run(schema, func(t *testing.T) {
			_, err := confFromStr(schema + "::addr=localhost:9000;sf_dir=/tmp/sf;")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "QWP")
		})
	}
}

func TestSfConfRejectsBadSenderId(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sender_id=bad/id;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid character")
}

func TestSfConfRejectsBadDurability(t *testing.T) {
	_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sf_durability=bogus;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "memory")
}

func TestSfConfRejectsDeferredDurabilityModes(t *testing.T) {
	for _, v := range []string{"flush", "append"} {
		_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;sf_durability=" + v + ";")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "deferred")
	}
}

func TestSfConfRejectsNegativeNumbers(t *testing.T) {
	cases := []string{
		"sf_max_bytes=-1",
		"sf_max_total_bytes=-1",
		"sf_append_deadline_millis=0",
		"reconnect_initial_backoff_millis=0",
		"reconnect_max_backoff_millis=0",
		"max_background_drainers=-1",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := confFromStr("ws::addr=localhost:9000;sf_dir=/tmp/sf;" + c + ";")
			require.Error(t, err)
		})
	}
}

func TestSanitizeQwpConfRejectsSfKeysWithoutSfDir(t *testing.T) {
	cases := []func(c *lineSenderConfig){
		func(c *lineSenderConfig) { c.senderId = "x" },
		func(c *lineSenderConfig) { c.sfMaxBytes = 1 << 20 },
		func(c *lineSenderConfig) { c.sfMaxTotalBytes = 1 << 30 },
		func(c *lineSenderConfig) { c.sfDurability = "memory" },
		func(c *lineSenderConfig) { c.sfAppendDeadlineMillis = 5000 },
		func(c *lineSenderConfig) { c.drainOrphans = true },
		func(c *lineSenderConfig) { c.maxBackgroundDrainers = 4 },
	}
	for i, mut := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			conf := newLineSenderConfig(qwpSenderType)
			conf.address = "localhost:9000"
			mut(conf)
			err := sanitizeQwpConf(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "sf_dir")
		})
	}
}

func TestSanitizeQwpConfRejectsTotalLessThanSegment(t *testing.T) {
	conf := newLineSenderConfig(qwpSenderType)
	conf.address = "localhost:9000"
	conf.sfDir = "/tmp/sf"
	conf.sfMaxBytes = 1 << 20
	conf.sfMaxTotalBytes = 1 << 18
	err := sanitizeQwpConf(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sf_max_total_bytes")
}

// TestSfConfEndToEnd builds a sender from a connect string with
// sf_dir set, sends rows through it, closes, and confirms the
// fake server saw the frames AND the slot dir was created on disk.
func TestSfConfEndToEnd(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()

	tmp := t.TempDir()
	addr := strings.TrimPrefix(srv.URL, "http://")
	confStr := strings.Join([]string{
		"ws::addr=" + addr,
		"sf_dir=" + tmp,
		"sender_id=test-slot",
		"sf_max_bytes=4096",
		"sf_max_total_bytes=" + fmt.Sprintf("%d", int64(64*1024)),
		"close_flush_timeout_millis=5000;",
	}, ";")

	ls, err := LineSenderFromConf(context.Background(), confStr)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, ls.Table("t").Int64Column("v", int64(i)).AtNow(context.Background()))
	}
	require.NoError(t, ls.Close(context.Background()))

	// The slot dir must have been created.
	st, err := os.Stat(filepath.Join(tmp, "test-slot"))
	require.NoError(t, err)
	assert.True(t, st.IsDir())
	// On clean drain, residual .sfa files are unlinked. The .lock
	// file may remain (it's not unlinked on close).
	entries, err := os.ReadDir(filepath.Join(tmp, "test-slot"))
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotEqual(t, ".sfa", filepath.Ext(e.Name()),
			"unexpected leftover segment file %s", e.Name())
	}
	// Server received at least one frame.
	assert.GreaterOrEqual(t, srv.totalFramesReceived.Load(), int64(1))
}

func TestSfConfPicksDefaultSenderIdWhenUnset(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	tmp := t.TempDir()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := LineSenderFromConf(context.Background(),
		"ws::addr="+addr+";sf_dir="+tmp+";close_flush_timeout_millis=2000;")
	require.NoError(t, err)
	require.NoError(t, ls.Close(context.Background()))
	// Default sender_id is "default".
	st, err := os.Stat(filepath.Join(tmp, "default"))
	require.NoError(t, err)
	assert.True(t, st.IsDir())
}

func TestSfConfWithSfDirOptionBuilder(t *testing.T) {
	srv := newQwpSfTestServer(t, qwpSfTestServerOpts{})
	defer srv.Close()
	tmp := t.TempDir()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ls, err := NewLineSender(context.Background(),
		WithQwp(),
		WithAddress(addr),
		WithSfDir(tmp),
		WithSenderId("opt-builder"),
		WithCloseFlushTimeout(2*time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, ls.Table("t").Int64Column("v", 1).AtNow(context.Background()))
	require.NoError(t, ls.Close(context.Background()))
	st, err := os.Stat(filepath.Join(tmp, "opt-builder"))
	require.NoError(t, err)
	assert.True(t, st.IsDir())
}
