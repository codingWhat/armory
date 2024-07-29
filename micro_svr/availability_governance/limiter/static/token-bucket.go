package static

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
	//fmt.Println(elapsed, tb.Rate, elapsed*tb.Rate)
	//两个float64相乘会有精度损失
	tb.Token += elapsed * tb.Rate
	//tb.Token += multiply(now.Sub(tb.lastRefill), tb.Rate)

	if tb.Token > tb.Capacity {
		tb.Token = tb.Capacity
	}

	//fmt.Println("--->", tb.Token)
	if tb.Token >= 1.0 {
		tb.Token--
		tb.lastRefill = now
		return true
	}

	return false
}

func multiply(d time.Duration, limit float64) float64 {
	//sec := float64(d/time.Second) * limit
	//nsec := float64(d%time.Second) * limit
	//
	////fmt.Println(d.Seconds()*limit, "multiply---->", sec, nsec/1e9, sec+nsec/1e9)
	//return sec + nsec/1e9

	sec := float64(d/time.Second) * limit
	nsec := float64(d%time.Second) * limit
	return sec + nsec/1e9
}
