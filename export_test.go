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

type (
	Buffer           = buffer
	ConfigData       = configData
	TcpLineSender    = tcpLineSender
	LineSenderConfig = lineSenderConfig
	SenderType       = senderType
)

var (
	GlobalTransport                     = globalTransport
	NoSenderType             SenderType = noSenderType
	HttpSenderType           SenderType = httpSenderType
	TcpSenderType            SenderType = tcpSenderType
	DefaultAutoFlushInterval            = defaultAutoFlushInterval
	DefaultAutoFlushRows                = defaultAutoFlushRows
)

func NewBuffer(initBufSize int, maxBufSize int, fileNameLimit int) Buffer {
	return newBuffer(initBufSize, maxBufSize, fileNameLimit)
}

func ParseConfigStr(conf string) (configData, error) {
	return parseConfigStr(conf)
}

func ConfFromStr(conf string) (*LineSenderConfig, error) {
	return confFromStr(conf)
}

func Messages(s LineSender) []byte {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	if hs, ok := s.(*httpLineSender); ok {
		return hs.Messages()
	}
	if hs, ok := s.(*httpLineSenderV2); ok {
		return hs.Messages()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.Messages()
	}
	if ts, ok := s.(*tcpLineSenderV2); ok {
		return ts.Messages()
	}
	panic("unexpected struct")
}

func MsgCount(s LineSender) int {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	if hs, ok := s.(*httpLineSender); ok {
		return hs.MsgCount()
	}
	if hs, ok := s.(*httpLineSenderV2); ok {
		return hs.MsgCount()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.MsgCount()
	}
	if ts, ok := s.(*tcpLineSenderV2); ok {
		return ts.MsgCount()
	}
	panic("unexpected struct")
}

func BufLen(s LineSender) int {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	if hs, ok := s.(*httpLineSender); ok {
		return hs.BufLen()
	}
	if hs, ok := s.(*httpLineSenderV2); ok {
		return hs.BufLen()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.BufLen()
	}
	if ts, ok := s.(*tcpLineSenderV2); ok {
		return ts.BufLen()
	}
	panic("unexpected struct")
}

func ProtocolVersion(s LineSender) protocolVersion {
	if ps, ok := s.(*pooledSender); ok {
		s = ps.wrapped
	}
	if _, ok := s.(*httpLineSender); ok {
		return ProtocolVersion1
	}
	if _, ok := s.(*httpLineSenderV2); ok {
		return ProtocolVersion2
	}
	if _, ok := s.(*tcpLineSender); ok {
		return ProtocolVersion1
	}
	if _, ok := s.(*tcpLineSenderV2); ok {
		return ProtocolVersion2
	}
	panic("unexpected struct")
}

func NewLineSenderConfig(t SenderType) *LineSenderConfig {
	return newLineSenderConfig(t)
}

func SetLittleEndian(littleEndian bool) {
	isLittleEndian = littleEndian
}
