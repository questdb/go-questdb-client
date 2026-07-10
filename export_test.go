/*+*****************************************************************************
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

import "github.com/coder/websocket"

type (
	Buffer           = buffer
	ConfigData       = configData
	TcpLineSender    = tcpLineSender
	LineSenderConfig = lineSenderConfig
	SenderType       = senderType
)

// QwpSfClassify exposes the internal status-byte → Category mapping
// for cross-language regression tests in the questdb_test package.
func QwpSfClassify(status QwpStatusCode) Category { return qwpSfClassify(status) }

// QwpSfDefaultPolicyFor exposes the spec-default Category → Policy
// mapping for unit tests.
func QwpSfDefaultPolicyFor(c Category) Policy { return qwpSfDefaultPolicyFor(c) }

// QwpSfIsTerminalCloseCode exposes the WS terminal close-code
// classifier for unit tests.
func QwpSfIsTerminalCloseCode(code websocket.StatusCode) bool {
	return qwpSfIsTerminalCloseCode(code)
}

var (
	GlobalTransport                     = globalTransport
	NoSenderType             SenderType = noSenderType
	HttpSenderType           SenderType = httpSenderType
	TcpSenderType            SenderType = tcpSenderType
	QwpSenderType            SenderType = qwpSenderType
	DefaultAutoFlushInterval            = defaultAutoFlushInterval
	DefaultAutoFlushRows                = defaultAutoFlushRows
)

func NewBuffer(initBufSize int, maxBufSize int, fileNameLimit int) Buffer {
	return newBuffer(initBufSize, maxBufSize, fileNameLimit)
}

func ParseConfigStr(conf string) (configData, error) {
	return parseConfigStr(conf)
}

// ConfFromStr parses a connect string into a *LineSenderConfig. The
// returned config has NOT been sanitized — call SanitizeConf for the
// post-sanitize shape (defaults applied, endpoints back-filled from
// address, transport-specific validation run).
func ConfFromStr(conf string) (*LineSenderConfig, error) {
	return confFromStr(conf)
}

// SanitizeConf dispatches to the per-transport sanitizer. Exposed for
// tests that need to apply post-parse defaults (e.g. authTimeoutMs,
// QWP endpoints) without going through the full newLineSender path
// (which would attempt a dial).
func SanitizeConf(c *LineSenderConfig) error {
	switch c.senderType {
	case tcpSenderType:
		return sanitizeTcpConf(c)
	case httpSenderType:
		return sanitizeHttpConf(c)
	case qwpSenderType:
		return sanitizeQwpConf(c)
	}
	return nil
}

// unwrapPooledQwp reaches the delegate behind a facade pool lease so the
// switch helpers below can dispatch on the concrete sender type. A stale lease
// no longer owns its slot, so inspecting the delegate would read a re-borrowed
// slot's state — panic with a clear message instead (test-helper contract,
// mirroring the "unexpected struct" arms).
func unwrapPooledQwp(s LineSender) LineSender {
	ps, ok := s.(*qwpPooledSender)
	if !ok {
		return s
	}
	if !ps.live() {
		panic("stale qwpPooledSender lease: slot returned or re-borrowed")
	}
	return ps.slot.delegate
}

func Messages(s LineSender) []byte {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	s = unwrapPooledQwp(s)
	if hs, ok := s.(*httpLineSender); ok {
		return hs.Messages()
	}
	if hs, ok := s.(*httpLineSenderV2); ok {
		return hs.Messages()
	}
	if hs, ok := s.(*httpLineSenderV3); ok {
		return hs.Messages()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.Messages()
	}
	if ts, ok := s.(*tcpLineSenderV2); ok {
		return ts.Messages()
	}
	if ts, ok := s.(*tcpLineSenderV3); ok {
		return ts.Messages()
	}
	if _, ok := s.(*qwpLineSender); ok {
		panic("Messages() is not applicable to QWP senders: QWP has no ILP message buffer")
	}
	panic("unexpected struct")
}

func MsgCount(s LineSender) int {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	s = unwrapPooledQwp(s)
	if hs, ok := s.(*httpLineSender); ok {
		return hs.MsgCount()
	}
	if hs, ok := s.(*httpLineSenderV2); ok {
		return hs.MsgCount()
	}
	if hs, ok := s.(*httpLineSenderV3); ok {
		return hs.MsgCount()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.MsgCount()
	}
	if ts, ok := s.(*tcpLineSenderV2); ok {
		return ts.MsgCount()
	}
	if ts, ok := s.(*tcpLineSenderV3); ok {
		return ts.MsgCount()
	}
	if qs, ok := s.(*qwpLineSender); ok {
		return qs.pendingRowCount
	}
	panic("unexpected struct")
}

func BufLen(s LineSender) int {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	s = unwrapPooledQwp(s)
	if hs, ok := s.(*httpLineSender); ok {
		return hs.BufLen()
	}
	if hs, ok := s.(*httpLineSenderV2); ok {
		return hs.BufLen()
	}
	if hs, ok := s.(*httpLineSenderV3); ok {
		return hs.BufLen()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.BufLen()
	}
	if ts, ok := s.(*tcpLineSenderV2); ok {
		return ts.BufLen()
	}
	if ts, ok := s.(*tcpLineSenderV3); ok {
		return ts.BufLen()
	}
	if qs, ok := s.(*qwpLineSender); ok {
		total := 0
		for _, tb := range qs.tableBuffers {
			total += tb.approxDataSize()
		}
		return total
	}
	panic("unexpected struct")
}

func ProtocolVersion(s LineSender) protocolVersion {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	s = unwrapPooledQwp(s)
	if _, ok := s.(*httpLineSender); ok {
		return ProtocolVersion1
	}
	if _, ok := s.(*httpLineSenderV2); ok {
		return ProtocolVersion2
	}
	if _, ok := s.(*httpLineSenderV3); ok {
		return ProtocolVersion3
	}
	if _, ok := s.(*tcpLineSender); ok {
		return ProtocolVersion1
	}
	if _, ok := s.(*tcpLineSenderV2); ok {
		return ProtocolVersion2
	}
	if _, ok := s.(*tcpLineSenderV3); ok {
		return ProtocolVersion3
	}
	if _, ok := s.(*qwpLineSender); ok {
		panic("ProtocolVersion() is not applicable to QWP senders: QWP is not an ILP protocol")
	}
	panic("unexpected struct")
}

func NewLineSenderConfig(t SenderType) *LineSenderConfig {
	return newLineSenderConfig(t)
}

// ConfigEndpoints returns the multi-host failover list parsed by
// sanitizeQwpConf. Each entry is rendered as host:port (IPv6 hosts
// are bracketed) so tests can compare against literals. Returns nil
// for non-QWP senders.
func ConfigEndpoints(c *LineSenderConfig) []string {
	if c == nil || len(c.endpoints) == 0 {
		return nil
	}
	out := make([]string, len(c.endpoints))
	for i, e := range c.endpoints {
		out[i] = e.String()
	}
	return out
}

// ConfigAuthTimeoutMs returns the effective auth_timeout_ms after
// sanitization (default 15000).
func ConfigAuthTimeoutMs(c *LineSenderConfig) int { return c.authTimeoutMs }

// ConfigZone returns the parsed zone= value (silently stored but
// unused on SF ingress).
func ConfigZone(c *LineSenderConfig) string { return c.zone }

// ConfigTarget returns the parsed target= value as a string
// (any/primary/replica).
func ConfigTarget(c *LineSenderConfig) string { return c.target.String() }

func SetLittleEndian(littleEndian bool) {
	isLittleEndian = littleEndian
}
