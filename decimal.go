/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
	"fmt"
	"math/big"
)

const (
	decimalBinaryTypeCode byte   = 0x17
	maxDecimalScale       uint32 = 76
)

// ScaledDecimal represents a decimal value as a two's complement big-endian byte slice and a scale.
// NULL decimals are represented by an offset of 32.
type ScaledDecimal struct {
	scale    uint32
	unscaled [32]byte
	offset   uint8
}

type ShopspringDecimal interface {
	Coefficient() *big.Int
	Exponent() int32
}

// NewScaledDecimal constructs a decimal from a two's complement big-endian unscaled value and a scale.
// A nil/empty unscaled slice produces a NULL decimal.
func NewScaledDecimal(unscaled []byte, scale uint32) (ScaledDecimal, error) {
	if len(unscaled) == 0 {
		return ScaledDecimal{
			offset: 32,
		}, nil
	}
	normalized, offset, err := normalizeTwosComplement(unscaled)
	if err != nil {
		return ScaledDecimal{}, err
	}
	return ScaledDecimal{
		scale:    scale,
		unscaled: normalized,
		offset:   offset,
	}, nil
}

// NewDecimal constructs a decimal from an arbitrary-precision integer and a scale.
// Providing a nil unscaled value produces a NULL decimal.
func NewDecimal(unscaled *big.Int, scale uint32) (ScaledDecimal, error) {
	if unscaled == nil {
		return ScaledDecimal{
			offset: 32,
		}, nil
	}
	unscaledRaw, offset, err := bigIntToTwosComplement(unscaled)
	if err != nil {
		return ScaledDecimal{}, err
	}
	return ScaledDecimal{
		scale:    scale,
		unscaled: unscaledRaw,
		offset:   offset,
	}, nil
}

// NewDecimalFromInt64 constructs a decimal from a 64-bit integer and a scale.
func NewDecimalFromInt64(unscaled int64, scale uint32) ScaledDecimal {
	var be [8]byte
	binary.BigEndian.PutUint64(be[:], uint64(unscaled))
	offset := trimTwosComplement(be[:])
	payload := [32]byte{}
	copy(payload[32-(8-offset):], be[offset:])
	return ScaledDecimal{
		scale:    scale,
		unscaled: payload,
		offset:   uint8(32 - (8 - offset)),
	}
}

// IsNull reports whether the decimal represents NULL.
func (d ScaledDecimal) IsNull() bool {
	return d.offset >= 32
}

// Scale returns the decimal scale.
func (d ScaledDecimal) Scale() uint32 {
	return d.scale
}

// UnscaledValue returns a copy of the unscaled integer value.
// For NULL decimals it returns nil.
func (d ScaledDecimal) UnscaledValue() *big.Int {
	if d.IsNull() {
		return nil
	}
	return twosComplementToBigInt(d.unscaled[d.offset:])
}

func (d ScaledDecimal) ensureValidScale() error {
	if d.IsNull() {
		return nil
	}
	if d.scale > maxDecimalScale {
		return fmt.Errorf("decimal scale %d exceeds maximum %d", d.scale, maxDecimalScale)
	}
	return nil
}

func convertShopspringDecimal(value ShopspringDecimal) (ScaledDecimal, error) {
	coeff := value.Coefficient()
	if coeff == nil {
		return ScaledDecimal{
			offset: 32,
		}, nil
	}

	exp := value.Exponent()
	var scale uint32
	var unscaled *big.Int
	if exp >= 0 {
		unscaled = new(big.Int).Set(coeff)
		unscaled.Mul(unscaled, bigPow10(int(exp)))
		scale = 0
	} else {
		scale = uint32(-exp)
		unscaled = new(big.Int).Set(coeff)
	}
	return NewDecimal(unscaled, scale)
}

func bigPow10(exponent int) *big.Int {
	if exponent <= 0 {
		return big.NewInt(1)
	}
	result := big.NewInt(1)
	ten := big.NewInt(10)
	for i := 0; i < exponent; i++ {
		result.Mul(result, ten)
	}
	return result
}

func bigIntToTwosComplement(value *big.Int) ([32]byte, uint8, error) {
	if value.Sign() == 0 {
		return [32]byte{0}, 31, nil
	}
	if value.Sign() > 0 {
		bytes := value.Bytes()
		if bytes[0]&0x80 != 0 {
			bytes = append([]byte{0x00}, bytes...)
		}
		return normalizeTwosComplement(bytes)
	}

	bitLen := value.BitLen()
	byteLen := (bitLen + 8) / 8
	if byteLen == 0 {
		byteLen = 1
	}

	tmp := new(big.Int).Lsh(big.NewInt(1), uint(byteLen*8))
	tmp.Add(tmp, value) // value is negative, so this subtracts magnitude
	bytes := tmp.Bytes()
	if len(bytes) < int(byteLen) {
		padding := make([]byte, int(byteLen)-len(bytes))
		bytes = append(padding, bytes...)
	}

	if bytes[0]&0x80 == 0 {
		bytes = append([]byte{0xFF}, bytes...)
	}
	return normalizeTwosComplement(bytes)
}

// normalizeTwosComplement normalizes a two's complement big-endian byte slice to fit within 32 bytes and returns the normalized value along with the offset to the first significant byte.
func normalizeTwosComplement(src []byte) ([32]byte, uint8, error) {
	if len(src) == 0 {
		return [32]byte{0}, 32, nil
	}
	offset := trimTwosComplement(src)
	if len(src)-offset > 32 {
		return [32]byte{}, 0, fmt.Errorf("decimal unscaled value exceeds 32 bytes")
	}
	var trimmed [32]byte
	copy(trimmed[32-(len(src)-offset):], src[offset:])
	return trimmed, uint8(32 - (len(src) - offset)), nil
}

// trimTwosComplement removes redundant sign bytes from a two's complement big-endian byte slice and returns the offset to the first significant byte.
func trimTwosComplement(bytes []byte) int {
	if len(bytes) <= 1 {
		return 0
	}
	signBit := bytes[0] & 0x80
	i := 0
	for i < len(bytes)-1 {
		if signBit == 0 {
			if bytes[i] == 0x00 && bytes[i+1]&0x80 == 0 {
				i++
				continue
			}
		} else {
			if bytes[i] == 0xFF && bytes[i+1]&0x80 != 0 {
				i++
				continue
			}
		}
		break
	}
	return i
}

func twosComplementToBigInt(bytes []byte) *big.Int {
	if len(bytes) == 0 {
		return big.NewInt(0)
	}
	if bytes[0]&0x80 == 0 {
		return new(big.Int).SetBytes(bytes)
	}

	inverted := make([]byte, len(bytes))
	for i := range bytes {
		inverted[i] = ^bytes[i]
	}

	magnitude := new(big.Int).SetBytes(inverted)
	magnitude.Add(magnitude, big.NewInt(1))
	magnitude.Neg(magnitude)
	return magnitude
}

// validateDecimalText checks that the provided string is a valid decimal representation.
// It accepts numeric digits, optional sign, decimal point, exponent (e/E) and NaN/Infinity tokens.
func validateDecimalText(text string) error {
	if text == "" {
		return fmt.Errorf("decimal literal cannot be empty")
	}

	switch text {
	case "NaN", "Infinity", "+Infinity", "-Infinity":
		return nil
	}

	i := 0
	length := len(text)
	if text[0] == '+' || text[0] == '-' {
		if length == 1 {
			return fmt.Errorf("decimal literal contains sign without digits")
		}
		i++
	}

	digits := 0
	seenDot := false
	for i < length {
		ch := text[i]
		switch {
		case ch >= '0' && ch <= '9':
			digits++
			i++
		case ch == '.':
			if seenDot {
				return fmt.Errorf("decimal literal has multiple decimal points")
			}
			seenDot = true
			i++
		case ch == 'e' || ch == 'E':
			if digits == 0 {
				return fmt.Errorf("decimal literal exponent without mantissa")
			}
			i++
			if i >= length {
				return fmt.Errorf("decimal literal has incomplete exponent")
			}
			if text[i] == '+' || text[i] == '-' {
				i++
				if i >= length {
					return fmt.Errorf("decimal literal has incomplete exponent")
				}
			}
			expDigits := 0
			for i < length && text[i] >= '0' && text[i] <= '9' {
				i++
				expDigits++
			}
			if expDigits == 0 {
				return fmt.Errorf("decimal literal exponent has no digits")
			}
			if i != length {
				return fmt.Errorf("decimal literal has trailing characters")
			}
			return nil
		default:
			return fmt.Errorf("decimal literal contains invalid character %q", ch)
		}
	}

	if digits == 0 {
		return fmt.Errorf("decimal literal must contain at least one digit")
	}
	return nil
}
