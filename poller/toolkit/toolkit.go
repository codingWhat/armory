package toolkit

import (
	"reflect"
	"unsafe"
)

const (
	bitsize       = 32 << (^uint(0) >> 63)
	maxintHeadBit = 1 << (bitsize - 2)
)

// IsPowerOfTwo reports whether given integer is a power of two.
func IsPowerOfTwo(n int) bool {
	return n&(n-1) == 0
}

// CeilToPowerOfTwo returns the least power of two integer value greater than
// or equal to n.
func CeilToPowerOfTwo(n int) int {
	if n&maxintHeadBit != 0 && n > maxintHeadBit {
		panic("argument is too large")
	}
	if n <= 2 {
		return 2
	}
	n--
	n = fillBits(n)
	n++
	return n
}

// FloorToPowerOfTwo returns the greatest power of two integer value less than
// or equal to n.
func FloorToPowerOfTwo(n int) int {
	if n <= 2 {
		return 2
	}
	n = fillBits(n)
	n >>= 1
	n++
	return n
}

func fillBits(n int) int {
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n
}

// BytesToString converts byte slice to a string without memory allocation.
//
// Note it may break if the implementation of string or slice header changes in the future go versions.
func BytesToString(b []byte) string {
	/* #nosec G103 */
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to a byte slice without memory allocation.
//
// Note it may break if the implementation of string or slice header changes in the future go versions.
func StringToBytes(s string) (b []byte) {
	/* #nosec G103 */
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	/* #nosec G103 */
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	bh.Data, bh.Len, bh.Cap = sh.Data, sh.Len, sh.Len
	return b
}
