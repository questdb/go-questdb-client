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
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// qwpTestDecodeSingleInt64Table reads the values sent by this test. Each test
// message contains one table named t. Its first column, v, contains 64-bit
// whole numbers and has no missing values. Checking the values proves that a
// retry sent the original rows, not just the same number of messages.
func qwpTestDecodeSingleInt64Table(frame []byte) ([]int64, error) {
	if len(frame) < qwpHeaderSize {
		return nil, fmt.Errorf("short QWP frame: %d", len(frame))
	}
	if binary.LittleEndian.Uint32(frame[:4]) != qwpMagic {
		return nil, fmt.Errorf("bad QWP magic")
	}
	if got := binary.LittleEndian.Uint16(frame[6:8]); got != 1 {
		return nil, fmt.Errorf("table count = %d, want 1", got)
	}

	r := qwpByteReader{buf: frame, pos: qwpHeaderSize}
	if _, err := r.readVarint(); err != nil { // Skip where the symbol list starts.
		return nil, err
	}
	deltaCount, err := r.readVarint()
	if err != nil {
		return nil, err
	}
	readString := func() (string, error) {
		n, err := r.readVarint()
		if err != nil {
			return "", err
		}
		if n > uint64(r.remaining()) {
			return "", fmt.Errorf("string length %d exceeds %d remaining bytes", n, r.remaining())
		}
		value := string(r.buf[r.pos : r.pos+int(n)])
		r.pos += int(n)
		return value, nil
	}
	for i := uint64(0); i < deltaCount; i++ {
		if _, err := readString(); err != nil {
			return nil, err
		}
	}

	table, err := readString()
	if err != nil {
		return nil, err
	}
	if table != "t" {
		return nil, fmt.Errorf("table = %q, want t", table)
	}
	rowCount, err := r.readVarint()
	if err != nil {
		return nil, err
	}
	columnCount, err := r.readVarint()
	if err != nil {
		return nil, err
	}
	if columnCount == 0 {
		return nil, fmt.Errorf("table has no columns")
	}
	for col := uint64(0); col < columnCount; col++ {
		name, err := readString()
		if err != nil {
			return nil, err
		}
		typeCode, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if col == 0 && (name != "v" || qwpTypeCode(typeCode) != qwpTypeLong) {
			return nil, fmt.Errorf("first column = %q/0x%02x, want v/LONG", name, typeCode)
		}
	}

	nullFlag, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if nullFlag != 0 {
		return nil, fmt.Errorf("v column unexpectedly has nulls")
	}
	values := make([]int64, int(rowCount))
	for i := range values {
		values[i], err = r.readInt64LE()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// TestQwpSfMaintenanceFailureReachesTheProducer checks what an application
// sees when the client cannot update its files for more than a brief moment.
// The write call must return ErrSfDurability, unsent rows must remain queued,
// and retrying after storage recovers must send every row exactly once.
//
// The test allows room for only two data files. It then makes the file update
// required before deleting old data fail. The old file cannot be deleted, no
// new file can be created, and writes eventually have to wait. This is similar
// to what happens when a disk is full or read-only.
func TestQwpSfMaintenanceFailureReachesTheProducer(t *testing.T) {
	for _, tc := range []struct {
		name string
		conf string
		// publish adds one row and calls the API that should report the error.
		publish func(ctx context.Context, s LineSender, i int) error
	}{
		{
			// At stores the row in memory; Flush tries to publish it.
			name: "Flush",
			conf: "auto_flush=off;",
			publish: func(ctx context.Context, s LineSender, i int) error {
				if err := s.Table("t").Int64Column("v", int64(i)).At(ctx, time.Unix(0, int64(i+1)*1000)); err != nil {
					return err
				}
				return s.Flush(ctx)
			},
		},
		{
			// With automatic flushing after one row, At publishes the row and
			// must return the storage error itself.
			name: "At",
			conf: "auto_flush_rows=1;",
			publish: func(ctx context.Context, s LineSender, i int) error {
				return s.Table("t").Int64Column("v", int64(i)).At(ctx, time.Unix(0, int64(i+1)*1000))
			},
		},
		{
			name: "AtNow",
			conf: "auto_flush_rows=1;",
			publish: func(ctx context.Context, s LineSender, i int) error {
				return s.Table("t").Int64Column("v", int64(i)).AtNow(ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var (
				receivedMu sync.Mutex
				received   []int64
				decodeErr  error
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(qwpHeaderVersion, "1")
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				var seq int64
				for {
					_, frame, err := conn.Read(ctx)
					if err != nil {
						return
					}
					values, err := qwpTestDecodeSingleInt64Table(frame)
					receivedMu.Lock()
					if err != nil && decodeErr == nil {
						decodeErr = err
					}
					received = append(received, values...)
					receivedMu.Unlock()
					_ = conn.Write(ctx, websocket.MessageBinary, buildAckOK(seq))
					seq++
				}
			}))
			t.Cleanup(srv.Close)

			// Allow only two data files. Once both are in use, an old file must
			// be deleted before another one can be created.
			conf := "ws::addr=" + strings.TrimPrefix(srv.URL, "http://") +
				";sf_dir=" + t.TempDir() +
				";sf_max_segment_bytes=65536;sf_max_total_bytes=131072" +
				";sf_append_deadline_millis=50;close_flush_timeout_millis=200;" + tc.conf
			ls, err := LineSenderFromConf(ctx, conf)
			require.NoError(t, err)
			sender := ls.(*qwpLineSender)
			t.Cleanup(func() { _ = ls.Close(ctx) })

			// From this point on, fail the file update required before old data
			// can be deleted. Install the failure after startup so this tests a
			// disk problem during use, not a failure to open the sender.
			injected := errors.New("injected ack-watermark fsync failure")
			var failing atomic.Bool
			hook := func(*os.File) error {
				if failing.Load() {
					return injected
				}
				return nil
			}
			qwpSfAckWatermarkSync.Store(&hook)
			t.Cleanup(func() {
				failing.Store(false)
				qwpSfAckWatermarkSync.Store(nil)
			})
			failing.Store(true)

			// Keep writing until the storage error reaches the caller. A short
			// wait-for-space error is expected before the storage failure has
			// lasted long enough to be reported.
			deadline := time.Now().Add(30 * time.Second)
			var durabilityErr error
			attemptedRows := 0
			for durabilityErr == nil {
				err := tc.publish(ctx, ls, attemptedRows)
				attemptedRows++
				switch {
				case err == nil:
				case errors.Is(err, ErrSfDurability):
					durabilityErr = err
				default:
					require.ErrorIs(t, err, ErrBackpressureTimeout,
						"a slot whose maintenance is failing must report that or backpressure, nothing else")
				}
				if err != nil {
					// Avoid queuing too many rows while waiting for the client to
					// classify the repeated failure as a storage error.
					time.Sleep(10 * time.Millisecond)
				}
				require.False(t, time.Now().After(deadline),
					"a trim that can never commit must surface as ErrSfDurability from %s", tc.name)
			}
			require.ErrorIs(t, durabilityErr, injected,
				"the error must name the storage failure that caused it")
			require.ErrorIs(t, sender.cursorEngine.managerEntry.entryMaintenanceError(), ErrSfDurability,
				"the producer error must come from real manager state, not a one-off local check")

			// The error must leave the unsent rows queued and the sender open.
			require.Greater(t, sender.pendingRowCount, 0,
				"unpublished rows must remain pending after a non-terminal storage error")
			require.False(t, sender.closed.Load(), "a failing disk must not close the sender")

			// Remove the failure and flush the rows that are already queued.
			// Do not add another row during the retry.
			failing.Store(false)
			retryDeadline := time.Now().Add(30 * time.Second)
			var publishedFsn int64
			for {
				publishedFsn, err = ls.(QwpSender).FlushAndGetSequence(ctx)
				if err == nil {
					break
				}
				// A retry may still see the old error until the background work
				// succeeds. Both errors mean the caller may retry.
				require.True(t, errors.Is(err, ErrBackpressureTimeout) || errors.Is(err, ErrSfDurability),
					"once storage recovers, only the non-terminal errors may remain, got: %v", err)
				require.False(t, time.Now().After(retryDeadline),
					"a retry after the disk recovered must succeed")
				time.Sleep(10 * time.Millisecond)
			}
			require.Zero(t, sender.pendingRowCount,
				"a successful retry must publish the rows it retained")

			ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			require.NoError(t, ls.(QwpSender).AwaitAckedFsn(ackCtx, publishedFsn))
			cancel()
			receivedMu.Lock()
			gotDecodeErr := decodeErr
			gotValues := append([]int64(nil), received...)
			receivedMu.Unlock()
			require.NoError(t, gotDecodeErr)
			expectedValues := make([]int64, attemptedRows)
			for i := range expectedValues {
				expectedValues[i] = int64(i)
			}
			require.Equal(t, expectedValues, gotValues,
				"recovery must deliver every retained row exactly once, without replacement")
			require.NoError(t, sender.cursorEngine.managerEntry.entryMaintenanceError(),
				"maintenance that works again must clear the latched error")
		})
	}
}
