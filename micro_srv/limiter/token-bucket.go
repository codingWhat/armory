package limiter

import (
	"sync"
	"time"
)

type TokenBucket struct {
	Rate float64

	Token    float64 //当前存在的token
	Capacity float64

	lastRefill time.Time

	sync.Mutex
}

func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{Rate: rate, Capacity: capacity, Token: capacity, lastRefill: time.Now()}
}

func (tb *TokenBucket) Allow() bool {
	tb.Lock()
	defer tb.Unlock()
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.Token += elapsed * tb.Rate
	if tb.Token > tb.Capacity {
		tb.Token = tb.Capacity
	}

	if tb.Token >= 1.0 {
		tb.Token--
		tb.lastRefill = now
		return true
	}

	return false
}
