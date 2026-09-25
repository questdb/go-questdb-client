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
	"errors"
	"fmt"
)

// ErrSfDurability identifies a store-and-forward operation that could not save
// required local state durably. Wrapped causes remain available through
// errors.Is.
var ErrSfDurability = errors.New("qwp/sf: could not durably commit store-and-forward state")

// qwpSfErrRecoveryFailClosed identifies on-disk data that cannot be replayed
// safely. Filesystem failures use qwpSfDurabilityError.
//
//lint:ignore ST1012 The qwpSf prefix groups internal store-and-forward errors.
var qwpSfErrRecoveryFailClosed = errors.New("qwp/sf: recovery failed closed")

// qwpSfDurabilityError adds an operation, an optional path, and an optional
// cause to ErrSfDurability. errors.Is matches both the sentinel and the cause.
func qwpSfDurabilityError(op, path string, cause error) error {
	if cause == nil {
		if path == "" {
			return fmt.Errorf("%w: %s", ErrSfDurability, op)
		}
		return fmt.Errorf("%w: %s %s", ErrSfDurability, op, path)
	}
	if errors.Is(cause, ErrSfDurability) {
		if path == "" {
			return fmt.Errorf("%s: %w", op, cause)
		}
		return fmt.Errorf("%s %s: %w", op, path, cause)
	}
	if path == "" {
		return fmt.Errorf("%w: %s: %w", ErrSfDurability, op, cause)
	}
	return fmt.Errorf("%w: %s %s: %w", ErrSfDurability, op, path, cause)
}

// qwpSfFailClosed reports an on-disk inconsistency. Filesystem errors use
// qwpSfDurabilityError instead.
func qwpSfFailClosed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", qwpSfErrRecoveryFailClosed, fmt.Sprintf(format, args...))
}
