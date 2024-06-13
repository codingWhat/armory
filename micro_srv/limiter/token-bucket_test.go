package limiter

import (
	"fmt"
	"testing"
	"time"
)

func TestTokenBucket_Allow(t *testing.T) {

	tb := NewTokenBucket(2.0, 3.0)
	for i := 0; i <= 15; i++ {
		now := time.Now().Format("15:04:05")
		if tb.Allow() {
			fmt.Println(i, now+"请求通过")
		} else {
			fmt.Println(i, now+"请求不通过")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
