package sre

import (
	"fmt"
	"testing"
	"time"
)

func TestNewWindow(t *testing.T) {
	counter := NewRollingCounter(5*time.Second, 100)

	for i := 0; i < 20; i++ {
		counter.Add(1)
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(6 * time.Second)
	for i := 0; i < 10; i++ {
		counter.Add(0)
		time.Sleep(200 * time.Millisecond)
	}

	var (
		total   int64
		success int64
	)
	counter.Reduce(func(iterator *BucketIterator) float64 {
		for iterator.IsValid() {
			bucket, err := iterator.GetAndNext()
			if err == nil {
				total += bucket.Total
				success += bucket.Success
				continue
			}
			return 0
		}
		return 0
	})

	fmt.Println(total, "----", success)

}
