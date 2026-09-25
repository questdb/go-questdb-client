// Copyright (c) 2014-2019 Appsicle
// Copyright (c) 2019-2026 QuestDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
)

// Tests can interrupt setup after connecting, before the send loop uses it.
var qwpTestAfterIngestTransportConnect atomic.Pointer[func(*qwpTransport)]

type qwpSfTransportBuildPanic struct {
	cause     any
	transport *qwpTransport
}

func (p qwpSfTransportBuildPanic) String() string {
	return fmt.Sprintf("qwp: ingest transport setup panicked: %v", p.cause)
}

func qwpSfConnectTransport(ctx context.Context, address string, opts qwpTransportOpts, dump io.Writer) (*qwpTransport, error) {
	t := &qwpTransport{dumpWriter: dump}
	defer func() {
		if r := recover(); r != nil {
			// The panic may have left the transport partly set up. Keep it;
			// do not repeat setup or try to close it in this state.
			t.retainFailure()
			panic(qwpSfTransportBuildPanic{cause: r, transport: t})
		}
	}()
	err := t.connect(ctx, address, opts)
	if err == nil {
		if hook := qwpTestAfterIngestTransportConnect.Load(); hook != nil {
			(*hook)(t)
		}
	}
	if err != nil && t.conn == nil && t.dumpConn == nil {
		return nil, err
	}
	// Setup can fail while a WebSocket or dump pipe is still open. Return
	// the transport with the connection error so the caller can track its
	// cleanup, not use it to send data.
	return t, err
}

// Construction code calls this factory first; the send loop takes over later.
// They do not call it at the same time. The engine's cleanup worker reads the
// saved connections only after these calls have stopped. Save only failed
// attempts here; the send loop already tracks successful connections.
func (e *qwpSfCursorEngine) trackConnectCleanup(factory qwpSfReconnectFactory) qwpSfReconnectFactory {
	if factory == nil {
		return nil
	}
	return func(ctx context.Context, idx int) (tr *qwpTransport, err error) {
		defer func() {
			if r := recover(); r != nil {
				if held, ok := r.(qwpSfTransportBuildPanic); ok {
					e.keepRejectedTransport(held.transport)
				}
				panic(r)
			}
			if err != nil && tr != nil {
				e.keepRejectedTransport(tr)
			}
		}()
		return factory(ctx, idx)
	}
}

func (e *qwpSfCursorEngine) keepRejectedTransport(t *qwpTransport) {
	kept := e.rejectedTransports[:0]
	for _, old := range e.rejectedTransports {
		select {
		case <-old.closeDone:
			e.rejectedTransportErr = qwpAppendCloseError(e.rejectedTransportErr, qwpTransportReleaseError(old.closeErr))
		default:
			kept = append(kept, old)
		}
	}
	clear(e.rejectedTransports[len(kept):])
	e.rejectedTransports = append(kept, t)
}

// Wait for all failed connection attempts to finish cleanup. If one is blocked,
// still collect errors from the others. Each transport already has a close
// operation; these goroutines only wait for it and never start another one.
func (e *qwpSfCursorEngine) closeRejectedTransports(cause error) error {
	result := errors.Join(cause, e.rejectedTransportErr)
	if len(e.rejectedTransports) == 0 {
		return result
	}
	results := make(chan error, len(e.rejectedTransports))
	for _, tr := range e.rejectedTransports {
		go func(tr *qwpTransport) {
			<-tr.closeDone
			results <- tr.closeErr
		}(tr)
	}
	if result != nil {
		e.cleanup.publish(result, false, false)
	}
	for range e.rejectedTransports {
		result = errors.Join(result, qwpTransportReleaseError(<-results))
		if result != nil {
			e.cleanup.publish(result, false, false)
		}
	}
	return result
}

func qwpTransportReleaseError(err error) error {
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
