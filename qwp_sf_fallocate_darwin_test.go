//go:build darwin

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
	"os"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

func TestQwpSfDarwinRejectsReservationFailure(t *testing.T) {
	for _, cause := range []error{unix.ENOTSUP, unix.EOPNOTSUPP, unix.EINVAL, unix.ENOSPC} {
		t.Run(cause.Error(), func(t *testing.T) {
			orig := qwpSfFcntlFstoreFn.load()
			t.Cleanup(func() { qwpSfFcntlFstoreFn.store(orig) })
			calls := 0
			qwpSfFcntlFstoreFn.store(func(fd uintptr, cmd int, store *unix.Fstore_t) error {
				calls++
				require.Equal(t, unix.F_PREALLOCATE, cmd)
				require.Equal(t, int64(64*1024), store.Length)
				if calls%2 == 1 {
					require.Equal(t, uint32(unix.F_ALLOCATECONTIG|unix.F_ALLOCATEALL), store.Flags)
					return unix.ENOSPC
				}
				require.Equal(t, uint32(unix.F_ALLOCATEALL), store.Flags)
				return cause
			})
			requireSfReservationRejected(t, cause)
			require.Equal(t, 4, calls, "two reservation attempts per failed allocation")
		})
	}
}

func TestQwpSfDarwinReservationRetriesWithoutContiguity(t *testing.T) {
	orig := qwpSfFcntlFstoreFn.load()
	t.Cleanup(func() { qwpSfFcntlFstoreFn.store(orig) })
	calls := 0
	qwpSfFcntlFstoreFn.store(func(fd uintptr, cmd int, store *unix.Fstore_t) error {
		calls++
		if calls == 1 {
			require.Equal(t, uint32(unix.F_ALLOCATECONTIG|unix.F_ALLOCATEALL), store.Flags)
			store.Bytesalloc = 4096
			return unix.ENOSPC
		}
		require.Equal(t, uint32(unix.F_ALLOCATEALL), store.Flags)
		require.Zero(t, store.Bytesalloc)
		return nil
	})
	f, err := os.CreateTemp(t.TempDir(), "reserve-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	// The successful syscall is mocked, so do not map or write through this file.
	require.NoError(t, qwpSfAllocate(f, 64*1024))
	require.Equal(t, 2, calls)
	st, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(64*1024), st.Size())
}
