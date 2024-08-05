package hash

import (
	"github.com/cespare/xxhash/v2"
	"testing"
)

// var text = "abcdefg" // fnv 比 xxxhash快
var text = "abcdefg312dsaasa" // xxxhash 比 fnv快
//var text = "abcdefg312dsaasdasdqsads" // xxxhash 比 fnv快

func BenchmarkFNV(b *testing.B) {
	h := newDefaultHasher()
	for i := 0; i < b.N; i++ {
		h.Sum64(text)
	}
}

func BenchmarkXXHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		xxhash.Sum64String(text)
	}
}
