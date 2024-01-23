package main

import (
	"math/rand"
	"reflect"
	"unsafe"
)

/*
https://github.com/bruceshao/lockfree

*/

var sli = make([]uint8, 16)
var p = byteArrayPointer(16)

func main() {
	idx := rand.Intn(16)
	setSlice(idx, 10)
	idx = rand.Intn(16)
	setValByPointer(idx, 10)
}

func setSlice(k int, v uint8) {
	sli[k] = v
}

func setValByPointer(pos int, val uint8) {
	*(*uint8)(unsafe.Pointer(uintptr(pos) + uintptr(p))) = val
}

func byteArrayPointer(cap int) unsafe.Pointer {
	bytes := make([]uint8, cap)
	rs := *(*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	return unsafe.Pointer(&rs.Data)
	//unsafe.Pointer(&bytes)

}
