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

package questdb

import (
	"fmt"
	"time"
)

// HttpError is a server-sent error message.
type HttpError struct {
	httpStatus int

	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line,omitempty"`
	ErrorId string `json:"errorId"`
}

// Error returns full error message string.
func (e *HttpError) Error() string {
	return fmt.Sprintf("%d %s id: %s, code: %s, line: %d",
		e.httpStatus,
		e.Message,
		e.ErrorId,
		e.Code,
		e.Line,
	)
}

// HttpStatus returns error HTTP status code.
func (e *HttpError) HttpStatus() int {
	return e.httpStatus
}

// RetryTimeoutError is error indicating failed flush retry attempt.
type RetryTimeoutError struct {
	LastErr error
	Timeout time.Duration
}

// NewRetryTimeoutError returns a new RetryTimeoutError error.
func NewRetryTimeoutError(timeout time.Duration, lastError error) *RetryTimeoutError {
	return &RetryTimeoutError{
		LastErr: lastError,
		Timeout: timeout,
	}
}

// Error returns full error message string.
func (e *RetryTimeoutError) Error() string {
	msg := fmt.Sprintf("retry timeout reached: %s.", e.Timeout)
	if e.LastErr != nil {
		msg += " " + e.LastErr.Error()
	}
	return msg
}
