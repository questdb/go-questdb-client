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
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

// qwpWritePath is the WebSocket endpoint for QWP ingestion.
const qwpWritePath = "/write/v4"

// qwpSubprotocol is the WebSocket subprotocol for QWP.
const qwpSubprotocol = "qwp"

// qwpAckMinSize is the minimum ACK response size:
// 1 byte status + 8 bytes sequence number.
const qwpAckMinSize = 9

// qwpTransportOpts configures a WebSocket transport connection.
type qwpTransportOpts struct {
	// tlsMode controls certificate verification.
	// When true, certificate verification is skipped.
	tlsInsecureSkipVerify bool

	// authorization is the value for the Authorization HTTP
	// header, e.g. "Bearer <token>" or "Basic <base64>".
	// Empty string means no auth.
	authorization string
}

// qwpTransport wraps a WebSocket connection for sending QWP
// messages and receiving ACK responses. It is not safe for
// concurrent use; in sync mode the caller goroutine owns it,
// in async mode the I/O goroutine owns it.
type qwpTransport struct {
	conn *websocket.Conn

	// recvBuf is a reusable buffer for reading ACK responses,
	// sized to avoid allocations on the steady-state path.
	recvBuf []byte
}

// connect establishes a WebSocket connection to the QWP endpoint.
// The url should be a ws:// or wss:// URL without the path; the
// /write/v4 path is appended automatically.
func (t *qwpTransport) connect(ctx context.Context, url string, opts qwpTransportOpts) error {
	wsURL := url + qwpWritePath

	dialOpts := &websocket.DialOptions{
		Subprotocols: []string{qwpSubprotocol},
	}

	// Auth header.
	if opts.authorization != "" {
		dialOpts.HTTPHeader = http.Header{
			"Authorization": []string{opts.authorization},
		}
	}

	// TLS configuration for wss:// connections.
	if opts.tlsInsecureSkipVerify {
		dialOpts.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	conn, _, err := websocket.Dial(ctx, wsURL, dialOpts)
	if err != nil {
		return fmt.Errorf("qwp: websocket dial: %w", err)
	}

	// Remove the default read limit — QWP ACKs are small but
	// error payloads can vary.
	conn.SetReadLimit(-1)

	t.conn = conn
	if t.recvBuf == nil {
		t.recvBuf = make([]byte, 0, qwpDefaultInitRecvBufSize)
	}
	return nil
}

// sendMessage sends a QWP message as a WebSocket binary frame.
func (t *qwpTransport) sendMessage(ctx context.Context, data []byte) error {
	if t.conn == nil {
		return fmt.Errorf("qwp: not connected")
	}
	return t.conn.Write(ctx, websocket.MessageBinary, data)
}

// readAck reads and parses the server's ACK response. It returns
// the status code and the full response payload (including the
// status byte).
//
// ACK format:
//
//	[statusCode: uint8]
//	[errorLength: uint16 LE] (optional, present when status != OK)
//	[errorMessage: UTF-8]    (errorLength bytes)
func (t *qwpTransport) readAck(ctx context.Context) (qwpStatusCode, []byte, error) {
	if t.conn == nil {
		return 0, nil, fmt.Errorf("qwp: not connected")
	}

	msgType, data, err := t.conn.Read(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("qwp: read ack: %w", err)
	}
	if msgType != websocket.MessageBinary {
		return 0, nil, fmt.Errorf("qwp: expected binary message, got %v", msgType)
	}
	if len(data) < qwpAckMinSize {
		return 0, nil, fmt.Errorf("qwp: ack too short: %d bytes", len(data))
	}

	statusCode := qwpStatusCode(data[0])
	return statusCode, data, nil
}

// parseAckError extracts an error message from an ACK response
// payload. The layout is:
//
//	[statusCode: uint8] [sequence: int64 LE] [errorLength: uint16 LE] [errorMessage: UTF-8]
//
// The error length and message are only present for non-OK statuses.
// Returns empty string if no error message is present.
func parseAckError(data []byte) string {
	// data[0] = status, data[1:9] = sequence, data[9:11] = errLen.
	const errLenOffset = 9   // 1 (status) + 8 (sequence)
	const errMsgOffset = 11  // errLenOffset + 2 (uint16)
	if len(data) < errMsgOffset {
		return ""
	}
	errLen := int(binary.LittleEndian.Uint16(data[errLenOffset:errMsgOffset]))
	if len(data) < errMsgOffset+errLen {
		return ""
	}
	return string(data[errMsgOffset : errMsgOffset+errLen])
}

// parseAckSequence extracts the sequence number from an ACK
// response. Returns 0 if the response is too short.
func parseAckSequence(data []byte) int64 {
	if len(data) < 9 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data[1:9]))
}

// close sends a graceful WebSocket close frame and cleans up.
func (t *qwpTransport) close(ctx context.Context) error {
	if t.conn == nil {
		return nil
	}
	err := t.conn.Close(websocket.StatusNormalClosure, "")
	t.conn = nil
	return err
}
