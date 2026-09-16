//go:build linux

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
	"testing"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

func TestQwpSfLinuxRejectsReservationFailure(t *testing.T) {
	for _, cause := range []error{unix.EOPNOTSUPP, unix.EINVAL, unix.ENOSYS, unix.ENOSPC} {
		t.Run(cause.Error(), func(t *testing.T) {
			orig := qwpSfFallocateFn.load()
			t.Cleanup(func() { qwpSfFallocateFn.store(orig) })
			calls := 0
			qwpSfFallocateFn.store(func(fd int, mode uint32, off, length int64) error {
				calls++
				require.Zero(t, mode)
				require.Zero(t, off)
				require.Equal(t, int64(64*1024), length)
				return cause
			})
			requireSfReservationRejected(t, cause)
			require.Equal(t, 2, calls, "one syscall per failed allocation")
		})
	}
}
