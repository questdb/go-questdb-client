package http

import (
	"fmt"
	"time"
)

type HttpError struct {
	httpStatus int

	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line"`
	ErrorId string `json:"errorId"`
}

func (e *HttpError) Error() string {
	return fmt.Sprintf("%d %s id: %s, code: %s, line: %d",
		e.httpStatus,
		e.Message,
		e.ErrorId,
		e.Code,
		e.Line,
	)

}

type RetryTimeoutError struct {
	LastErr error
	Timeout time.Duration
}

func NewRetryTimeoutError(timeout time.Duration, lastError error) *RetryTimeoutError {
	return &RetryTimeoutError{
		LastErr: lastError,
		Timeout: timeout,
	}
}

func (e *RetryTimeoutError) Error() string {
	msg := fmt.Sprintf("retry timeout reached: %s.", e.Timeout)
	if e.LastErr != nil {
		msg += " " + e.LastErr.Error()
	}
	return msg

}
