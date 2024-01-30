package questdb

import (
	"io"
	"time"
)

type transport interface {
	io.Writer

	Close()
	SetWriteDeadline(time.Time)
	SetDeadline(time.Time)
}

type httpTransport struct {
	minThroughputBytesPerSecond int
	graceTimeout                time.Duration
	retryTimeout                time.Duration
	transactional               bool
	initBufSizeBytes            int
	maxBufSizeBytes             int
	// tlsVerify                   TlsVerify
	// tlsRoots                    string
}
