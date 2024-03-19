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
	"bufio"
	"context"
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

type tcpLineSender struct {
	buf     buffer
	address string
	conn    net.Conn
}

func newTcpLineSender(ctx context.Context, conf *lineSenderConfig) (*tcpLineSender, error) {
	var (
		d    net.Dialer
		key  *ecdsa.PrivateKey
		conn net.Conn
		err  error
	)

	s := &tcpLineSender{
		address: conf.address,
		// TCP sender doesn't limit max buffer size, hence 0
		buf: newBuffer(conf.initBufSize, 0, conf.fileNameLimit),
	}

	// Process tcp args in the same exact way that we do in v2
	if conf.tcpKeyId != "" && conf.tcpKey != "" {
		rawKey, err := base64.RawURLEncoding.DecodeString(conf.tcpKey)
		if err != nil {
			return nil, fmt.Errorf("failed to decode auth key: %v", err)
		}
		// elliptic.P256().ScalarBaseMult is deprecated, so we use ecdh key
		// and convert it to the ecdsa one.
		ecdhKey, err := ecdh.P256().NewPrivateKey(rawKey)
		if err != nil {
			return nil, fmt.Errorf("invalid auth key: %v", err)
		}
		ecdhPubKey := ecdhKey.PublicKey().Bytes()
		key = &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{
				Curve: elliptic.P256(),
				X:     big.NewInt(0).SetBytes(ecdhPubKey[1:33]),
				Y:     big.NewInt(0).SetBytes(ecdhPubKey[33:]),
			},
			D: big.NewInt(0).SetBytes(ecdhKey.Bytes()),
		}
	}

	if conf.tlsMode == tlsDisabled {
		conn, err = d.DialContext(ctx, "tcp", s.address)
	} else {
		config := &tls.Config{}
		if conf.tlsMode == tlsInsecureSkipVerify {
			config.InsecureSkipVerify = true
		}
		conn, err = tls.DialWithDialer(&d, "tcp", s.address, config)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	if key != nil {
		if deadline, ok := ctx.Deadline(); ok {
			conn.SetDeadline(deadline)
		}

		_, err = conn.Write([]byte(conf.tcpKeyId + "\n"))
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to write key id: %v", err)
		}

		reader := bufio.NewReader(conn)
		raw, err := reader.ReadBytes('\n')
		if len(raw) < 2 {
			conn.Close()
			return nil, fmt.Errorf("empty challenge response from server: %v", err)
		}
		// Remove the `\n` in the last position.
		raw = raw[:len(raw)-1]
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to read challenge response from server: %v", err)
		}

		// Hash the challenge with sha256.
		hash := crypto.SHA256.New()
		hash.Write(raw)
		hashed := hash.Sum(nil)

		stdSig, err := ecdsa.SignASN1(rand.Reader, key, hashed)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to sign challenge using auth key: %v", err)
		}
		_, err = conn.Write([]byte(base64.StdEncoding.EncodeToString(stdSig) + "\n"))
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to write signed challenge: %v", err)
		}

		// Reset the deadline.
		conn.SetDeadline(time.Time{})
	}

	s.conn = conn

	return s, nil
}

func (s *tcpLineSender) Close(_ context.Context) error {
	if s.conn != nil {
		conn := s.conn
		s.conn = nil
		return conn.Close()
	}
	return nil
}

func (s *tcpLineSender) Table(name string) LineSender {
	s.buf.Table(name)
	return s
}

func (s *tcpLineSender) Symbol(name, val string) LineSender {
	s.buf.Symbol(name, val)
	return s
}

func (s *tcpLineSender) Int64Column(name string, val int64) LineSender {
	s.buf.Int64Column(name, val)
	return s
}

func (s *tcpLineSender) Long256Column(name string, val *big.Int) LineSender {
	s.buf.Long256Column(name, val)
	return s
}

func (s *tcpLineSender) TimestampColumn(name string, ts time.Time) LineSender {
	s.buf.TimestampColumn(name, ts)
	return s
}

func (s *tcpLineSender) Float64Column(name string, val float64) LineSender {
	s.buf.Float64Column(name, val)
	return s
}

func (s *tcpLineSender) StringColumn(name, val string) LineSender {
	s.buf.StringColumn(name, val)
	return s
}

func (s *tcpLineSender) BoolColumn(name string, val bool) LineSender {
	s.buf.BoolColumn(name, val)
	return s
}

func (s *tcpLineSender) Flush(ctx context.Context) error {
	err := s.buf.LastErr()
	s.buf.ClearLastErr()
	if err != nil {
		s.buf.DiscardPendingMsg()
		return err
	}
	if s.buf.HasTable() {
		s.buf.DiscardPendingMsg()
		return errors.New("pending ILP message must be finalized with At or AtNow before calling Flush")
	}

	if err = ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		s.conn.SetWriteDeadline(deadline)
	} else {
		s.conn.SetWriteDeadline(time.Time{})
	}

	if _, err := s.buf.WriteTo(s.conn); err != nil {
		return err
	}

	// bytes.Buffer grows as 2*cap+n, so we use 3x as the threshold.
	if s.buf.Cap() > 3*s.buf.initBufSize {
		// Shrink the buffer back to desired capacity.
		s.buf.ResetSize()
	}
	return nil
}

func (s *tcpLineSender) AtNow(ctx context.Context) error {
	return s.At(ctx, time.Time{})
}

func (s *tcpLineSender) At(ctx context.Context, ts time.Time) error {
	sendTs := true
	if ts.IsZero() {
		sendTs = false
	}

	err := s.buf.At(ts, sendTs)
	if err != nil {
		return err
	}

	if s.buf.Len() > s.buf.initBufSize {
		return s.Flush(ctx)
	}
	return nil
}

// Messages returns a copy of accumulated ILP messages that are not
// flushed to the TCP connection yet. Useful for debugging purposes.
func (s *tcpLineSender) Messages() string {
	return s.buf.Messages()
}

// MsgCount returns the number of buffered messages
func (s *tcpLineSender) MsgCount() int {
	return s.buf.msgCount
}

// BufLen returns the number of bytes written to the buffer.
func (s *tcpLineSender) BufLen() int {
	return s.buf.Len()
}
