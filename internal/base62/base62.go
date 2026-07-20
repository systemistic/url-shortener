// Package base62 implements base62 encoding of unsigned 64-bit integers
// using the alphabet [0-9a-zA-Z]. It is the canonical short-code encoding
// for the URL shortener: 62^7 ≈ 3.5 trillion codes at 7 characters, and a
// full 64-bit ID never exceeds 11 characters.
package base62

import (
	"errors"
	"fmt"
	"math"
)

const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Base is the radix of the encoding.
const Base = uint64(len(alphabet))

// MaxLen is the maximum length of an encoded 64-bit value.
const MaxLen = 11 // ceil(64 / log2(62))

// ErrEmpty is returned by Decode for the empty string.
var ErrEmpty = errors.New("base62: empty string")

// ErrOverflow is returned by Decode when the value exceeds 64 bits.
var ErrOverflow = errors.New("base62: value overflows uint64")

// reverse maps an ASCII byte to its digit value, or -1 if invalid.
var reverse = func() (t [256]int8) {
	for i := range t {
		t[i] = -1
	}
	for i := 0; i < len(alphabet); i++ {
		t[alphabet[i]] = int8(i)
	}
	return t
}()

// Encode returns the base62 representation of n. Encode(0) == "0".
func Encode(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [MaxLen]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = alphabet[n%Base]
		n /= Base
	}
	return string(buf[i:])
}

// Decode parses a base62 string produced by Encode. It rejects empty
// strings, characters outside [0-9a-zA-Z], and values that overflow uint64.
func Decode(s string) (uint64, error) {
	if s == "" {
		return 0, ErrEmpty
	}
	var n uint64
	for i := 0; i < len(s); i++ {
		d := reverse[s[i]]
		if d < 0 {
			return 0, fmt.Errorf("base62: invalid character %q at index %d", s[i], i)
		}
		if n > (math.MaxUint64-uint64(d))/Base {
			return 0, ErrOverflow
		}
		n = n*Base + uint64(d)
	}
	return n, nil
}
