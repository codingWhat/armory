package simplecache

import (
	"strconv"
	"sync"
	"testing"
)

func Benchmark_test(b *testing.B) {
	c := &cache{
		mu:   &sync.RWMutex{},
		data: map[string]interface{}{},
	}

	b.ResetTimer()
	size := 1000
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			c.set(strconv.Itoa(i), i)
			i = (i + 1) % size
		}
	})
}
