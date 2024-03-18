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

import "sync/atomic"

type (
	Buffer           = buffer
	ConfigData       = configData
	TcpLineSender    = tcpLineSender
	LineSenderConfig = lineSenderConfig
)

var (
	ClientCt *atomic.Int64 = &clientCt
)

func NewBuffer(initBufSize int, fileNameLimit int) Buffer {
	return newBuffer(initBufSize, fileNameLimit)
}

func ParseConfigStr(conf string) (configData, error) {
	return parseConfigStr(conf)
}

func ConfFromStr(conf string) (*LineSenderConfig, error) {
	return confFromStr(conf)
}

func Messages(s LineSender) string {
	if hs, ok := s.(*httpLineSender); ok {
		return hs.Messages()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.Messages()
	}
	panic("unexpected struct")
}

func MsgCount(s LineSender) int {
	if hs, ok := s.(*httpLineSender); ok {
		return hs.MsgCount()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.MsgCount()
	}
	panic("unexpected struct")
}

func BufLen(s LineSender) int {
	if hs, ok := s.(*httpLineSender); ok {
		return hs.BufLen()
	}
	if ts, ok := s.(*tcpLineSender); ok {
		return ts.BufLen()
	}
	panic("unexpected struct")
}
