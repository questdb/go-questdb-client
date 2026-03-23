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
	"encoding/binary"
	"math"
)

// qwpWireBuffer is a low-level byte buffer for building QWP binary
// messages. It wraps a []byte and provides typed put methods for all
// QWP wire types. The buffer grows as needed but reuses its backing
// array on reset.
type qwpWireBuffer struct {
	buf []byte
}

// ensure grows the buffer so that at least n more bytes can be written
// without further allocation. This should be called before writing a
// known number of bytes to avoid repeated slice growth.
func (w *qwpWireBuffer) ensure(n int) {
	if cap(w.buf)-len(w.buf) >= n {
		return
	}
	newCap := 2 * cap(w.buf)
	needed := len(w.buf) + n
	if newCap < needed {
		newCap = needed
	}
	newBuf := make([]byte, len(w.buf), newCap)
	copy(newBuf, w.buf)
	w.buf = newBuf
}

// reset discards all written data but retains the backing array.
func (w *qwpWireBuffer) reset() {
	w.buf = w.buf[:0]
}

// len returns the number of bytes written.
func (w *qwpWireBuffer) len() int {
	return len(w.buf)
}

// bytes returns the written bytes. The returned slice is valid until
// the next mutating call.
func (w *qwpWireBuffer) bytes() []byte {
	return w.buf
}

// putByte appends a single byte.
func (w *qwpWireBuffer) putByte(v byte) {
	w.buf = append(w.buf, v)
}

// putUint16LE appends a uint16 in little-endian byte order.
func (w *qwpWireBuffer) putUint16LE(v uint16) {
	w.ensure(2)
	w.buf = w.buf[:len(w.buf)+2]
	binary.LittleEndian.PutUint16(w.buf[len(w.buf)-2:], v)
}

// putUint32LE appends a uint32 in little-endian byte order.
func (w *qwpWireBuffer) putUint32LE(v uint32) {
	w.ensure(4)
	w.buf = w.buf[:len(w.buf)+4]
	binary.LittleEndian.PutUint32(w.buf[len(w.buf)-4:], v)
}

// putInt32LE appends an int32 in little-endian byte order.
func (w *qwpWireBuffer) putInt32LE(v int32) {
	w.putUint32LE(uint32(v))
}

// putUint64LE appends a uint64 in little-endian byte order.
func (w *qwpWireBuffer) putUint64LE(v uint64) {
	w.ensure(8)
	w.buf = w.buf[:len(w.buf)+8]
	binary.LittleEndian.PutUint64(w.buf[len(w.buf)-8:], v)
}

// putInt64LE appends an int64 in little-endian byte order.
func (w *qwpWireBuffer) putInt64LE(v int64) {
	w.putUint64LE(uint64(v))
}

// putFloat32LE appends a float32 in IEEE 754 little-endian byte order.
func (w *qwpWireBuffer) putFloat32LE(v float32) {
	w.putUint32LE(math.Float32bits(v))
}

// putFloat64LE appends a float64 in IEEE 754 little-endian byte order.
func (w *qwpWireBuffer) putFloat64LE(v float64) {
	w.putUint64LE(math.Float64bits(v))
}

// putInt64BE appends an int64 in big-endian byte order. Used for
// decimal unscaled values.
func (w *qwpWireBuffer) putInt64BE(v int64) {
	w.ensure(8)
	w.buf = w.buf[:len(w.buf)+8]
	binary.BigEndian.PutUint64(w.buf[len(w.buf)-8:], uint64(v))
}

// putVarint appends v encoded as an unsigned LEB128 varint.
func (w *qwpWireBuffer) putVarint(v uint64) {
	w.ensure(qwpMaxVarintLen)
	n := qwpPutVarint(w.buf[len(w.buf):len(w.buf)+qwpMaxVarintLen], v)
	w.buf = w.buf[:len(w.buf)+n]
}

// putString appends a varint-prefixed UTF-8 string (varint length +
// raw string bytes).
func (w *qwpWireBuffer) putString(s string) {
	w.putVarint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

// putBytes appends raw bytes.
func (w *qwpWireBuffer) putBytes(data []byte) {
	w.buf = append(w.buf, data...)
}

// patchUint32LE writes a uint32 in little-endian at the given offset,
// overwriting the existing bytes. Used for patching header fields like
// payload length after the full message has been written.
func (w *qwpWireBuffer) patchUint32LE(offset int, v uint32) {
	binary.LittleEndian.PutUint32(w.buf[offset:], v)
}
