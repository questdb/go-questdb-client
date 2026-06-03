/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
	"errors"
	"math"
	"math/bits"
)

// qwpMaxVarintLen is the maximum number of bytes a varint-encoded
// uint64 can occupy (ceil(64/7) = 10).
const qwpMaxVarintLen = 10

// qwpWireBuffer is a low-level byte buffer for building QWP binary
// messages. It wraps a []byte and provides typed put methods for all
// QWP wire types. The buffer grows as needed but reuses its backing
// array on reset.
type qwpWireBuffer struct {
	buf []byte
}

// preallocate sets the initial backing-array capacity. Safe to call
// on a zero-value buffer; a no-op if the existing capacity already
// meets or exceeds initCap.
func (w *qwpWireBuffer) preallocate(initCap int) {
	if initCap <= 0 || cap(w.buf) >= initCap {
		return
	}
	w.buf = make([]byte, 0, initCap)
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

// putZeros appends n zero bytes. Since reset() preserves the backing
// array, the memory beyond len(buf) may hold stale data from earlier
// encodes, so we explicitly clear the new tail (compiled to memclr).
func (w *qwpWireBuffer) putZeros(n int) {
	if n <= 0 {
		return
	}
	w.ensure(n)
	end := len(w.buf)
	w.buf = w.buf[:end+n]
	clear(w.buf[end : end+n])
}

// patchUint32LE writes a uint32 in little-endian at the given offset,
// overwriting the existing bytes. Used for patching header fields like
// payload length after the full message has been written.
func (w *qwpWireBuffer) patchUint32LE(offset int, v uint32) {
	binary.LittleEndian.PutUint32(w.buf[offset:], v)
}

// qwpPutVarint encodes v as an unsigned LEB128 varint into buf and
// returns the number of bytes written. buf must have at least
// qwpMaxVarintLen bytes of space.
func qwpPutVarint(buf []byte, v uint64) int {
	i := 0
	for v > 0x7F {
		buf[i] = byte(v&0x7F) | 0x80
		v >>= 7
		i++
	}
	buf[i] = byte(v)
	return i + 1
}

// qwpReadVarint decodes an unsigned LEB128 varint from buf. It returns
// the decoded value and the number of bytes consumed, or an error if
// the varint is malformed or truncated.
//
// The byte-10 guard rejects payloads where a 10th byte contributes data
// bits beyond bit 63 of the result. Without it, a hostile server varint
// whose final byte sets any of bits 1..6 would silently overflow uint64
// via the shift below, producing a wildly wrong value the caller cannot
// distinguish from a legitimate one. Mirrors the Java reference decoder
// guard in QwpResultBatchDecoder.decodeVarint.
func qwpReadVarint(buf []byte) (uint64, int, error) {
	var v uint64
	var shift uint
	for i, b := range buf {
		if i >= qwpMaxVarintLen {
			return 0, 0, errors.New("qwp: varint overflow")
		}
		if shift == 63 && b&0x7E != 0 {
			return 0, 0, errors.New("qwp: varint overflow")
		}
		v |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return v, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, errors.New("qwp: varint truncated")
}

// qwpVarintSize returns the number of bytes needed to encode v as an
// unsigned LEB128 varint.
func qwpVarintSize(v uint64) int {
	if v == 0 {
		return 1
	}
	// Number of bits needed, divided by 7, rounded up.
	return (bits.Len64(v) + 6) / 7
}

// qwpStringSize returns the number of bytes needed to encode s as a
// varint-prefixed UTF-8 string (varint length + string bytes).
func qwpStringSize(s string) int {
	return qwpVarintSize(uint64(len(s))) + len(s)
}

// qwpDecodeError is the sentinel error type returned by decode paths
// (qwpByteReader, Gorilla decoder, RESULT_BATCH decoder). Dedicated type
// so callers can distinguish decode failures from transport / framing
// errors via errors.As, without regex-ing the message. The optional
// `cause` field carries the underlying error (if any) so errors.Is /
// errors.As can reach through to its identity.
type qwpDecodeError struct {
	msg   string
	cause error
}

func (e *qwpDecodeError) Error() string {
	return "qwp: decode: " + e.msg
}

func (e *qwpDecodeError) Unwrap() error {
	return e.cause
}

func newQwpDecodeError(msg string) *qwpDecodeError {
	return &qwpDecodeError{msg: msg}
}

func wrapQwpDecodeError(msg string, cause error) *qwpDecodeError {
	return &qwpDecodeError{msg: msg, cause: cause}
}

// qwpByteReader is a position-tracking reader over a QWP frame payload.
// Produced typed-value errors are always *qwpDecodeError; truncation,
// overflow, and out-of-range inputs all bubble up as a single error
// class so the hot path can stay branch-light.
//
// The reader aliases its input: slice(n) returns a sub-slice of buf, so
// the caller must not retain returned slices past the frame's lifetime.
// In the QWP egress model the WebSocket recv buffer stays pinned while
// the user's range iteration runs; once it returns, slices derived from
// this reader are no longer valid.
type qwpByteReader struct {
	buf []byte
	pos int
}

// reset rebinds the reader to a new buffer and rewinds pos to zero.
func (r *qwpByteReader) reset(buf []byte) {
	r.buf = buf
	r.pos = 0
}

// remaining returns the count of unread bytes.
func (r *qwpByteReader) remaining() int { return len(r.buf) - r.pos }

// atEnd reports whether the reader has consumed every byte.
func (r *qwpByteReader) atEnd() bool { return r.pos >= len(r.buf) }

// readByte reads one byte.
func (r *qwpByteReader) readByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, newQwpDecodeError("unexpected end of buffer reading uint8")
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// readUint16LE reads a little-endian uint16.
func (r *qwpByteReader) readUint16LE() (uint16, error) {
	if r.pos+2 > len(r.buf) {
		return 0, newQwpDecodeError("unexpected end of buffer reading uint16")
	}
	v := binary.LittleEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

// readUint32LE reads a little-endian uint32.
func (r *qwpByteReader) readUint32LE() (uint32, error) {
	if r.pos+4 > len(r.buf) {
		return 0, newQwpDecodeError("unexpected end of buffer reading uint32")
	}
	v := binary.LittleEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// readInt32LE reads a little-endian int32.
func (r *qwpByteReader) readInt32LE() (int32, error) {
	u, err := r.readUint32LE()
	return int32(u), err
}

// readUint64LE reads a little-endian uint64.
func (r *qwpByteReader) readUint64LE() (uint64, error) {
	if r.pos+8 > len(r.buf) {
		return 0, newQwpDecodeError("unexpected end of buffer reading uint64")
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

// readInt64LE reads a little-endian int64.
func (r *qwpByteReader) readInt64LE() (int64, error) {
	u, err := r.readUint64LE()
	return int64(u), err
}

// readFloat64LE reads an IEEE 754 little-endian float64.
func (r *qwpByteReader) readFloat64LE() (float64, error) {
	u, err := r.readUint64LE()
	return math.Float64frombits(u), err
}

// readVarint reads an unsigned LEB128 varint, surfacing the existing
// overflow / truncation errors from qwpReadVarint as *qwpDecodeError
// while preserving the underlying error via Unwrap.
func (r *qwpByteReader) readVarint() (uint64, error) {
	v, n, err := qwpReadVarint(r.buf[r.pos:])
	if err != nil {
		return 0, wrapQwpDecodeError(err.Error(), err)
	}
	r.pos += n
	return v, nil
}

// readVarintInt63 reads an unsigned varint and rejects values where the
// uint64→int64 cast would flip the sign. Used for varint-encoded fields
// that the wire spec treats as non-negative int63 (row count, column
// count, name lengths, etc.). Without this check, a hostile varint can
// drive a length past the bound check via two's-complement arithmetic
// — see QwpResultBatchDecoder.java around row_count and col_count.
func (r *qwpByteReader) readVarintInt63() (int64, error) {
	v, err := r.readVarint()
	if err != nil {
		return 0, err
	}
	if v > uint64(1<<63-1) {
		return 0, newQwpDecodeError("varint overflow: value exceeds int63")
	}
	return int64(v), nil
}

// advance skips n bytes. Errors when fewer than n bytes remain.
func (r *qwpByteReader) advance(n int) error {
	if n < 0 || r.pos+n > len(r.buf) {
		return newQwpDecodeError("unexpected end of buffer while advancing")
	}
	r.pos += n
	return nil
}

// slice returns a sub-slice of the underlying buffer covering the next
// n bytes and advances pos. The returned slice aliases the input — do
// not retain it past the frame's lifetime. Errors when fewer than n
// bytes remain.
func (r *qwpByteReader) slice(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, newQwpDecodeError("unexpected end of buffer while slicing")
	}
	s := r.buf[r.pos : r.pos+n]
	r.pos += n
	return s, nil
}
