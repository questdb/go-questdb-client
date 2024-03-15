package questdb

import "github.com/questdb/go-questdb-client/v3/pkg/tcp"

var (
	NewLineSender             = tcp.NewLineSender
	WithAddress               = tcp.WithAddress
	WithAuth                  = tcp.WithAuth
	WithBufferCapacity        = tcp.WithBufferCapacity
	WithFileNameLimit         = tcp.WithFileNameLimit
	WithTls                   = tcp.WithTls
	WithTlsInsecureSkipVerify = tcp.WithTlsInsecureSkipVerify
)

type LineSender tcp.LineSender
type LineSenderOption tcp.LineSenderOption
