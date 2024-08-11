package static

import (
	"sync"
	"time"
)

type TokenBucketSTD struct {
	rate       float64    // 令牌添加到桶中的速率。
	burst      float64    // 桶的最大容量。
	tokens     float64    // 当前桶中的令牌数量。
	lastUpdate time.Time  // 上次更新令牌数量的时间。
	mu         sync.Mutex // 互斥锁，确保线程安全。
}

func (tb *TokenBucketSTD) tokensFromDuration(d time.Duration) float64 {
	// Split the integer and fractional parts ourself to minimize rounding errors.
	// See golang.org/issues/34861.
	sec := float64(d/time.Second) * tb.rate
	nsec := float64(d%time.Second) * tb.rate
	return sec + nsec/1e9
}

// NewTokenBucketSTD 创建一个新的令牌桶，给定令牌添加速率和桶的容量。
func NewTokenBucketSTD(rate float64, b float64) *TokenBucketSTD {
	return &TokenBucketSTD{
		rate:   rate,
		burst:  b,
		tokens: 0,
	}
}
func (tb *TokenBucketSTD) durationFromTokens(tokens float64) time.Duration {
	seconds := tokens / tb.rate
	return time.Nanosecond * time.Duration(1e9*seconds)
}

// Allow 检查是否可以从桶中取出一个令牌。如果可以，它取出一个令牌并返回 true。
// 如果不可以，它返回 false。
func (tb *TokenBucketSTD) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	// 计算（可生成令牌数)所需要的时间，burst令牌桶容量，tokens: 当前存在的令牌个数
	maxElapsed := tb.durationFromTokens(float64(tb.burst) - tb.tokens)
	elapsed := now.Sub(tb.lastUpdate)
	if elapsed > maxElapsed {
		elapsed = maxElapsed
	}

	// 计算生成的令牌
	delta := tb.tokensFromDuration(elapsed)
	tokens := tb.tokens + delta
	if tokens > tb.burst {
		tokens = tb.burst
	}
	tokens--
	var waitDuration time.Duration
	if tokens < 0 {
		//说明取不到1个token, 那就计算取到1个token所需要的等待时间
		waitDuration = tb.durationFromTokens(-tokens)
	}
	ok := 1 <= tb.burst && waitDuration <= 0
	if ok {
		tb.lastUpdate = now
		tb.tokens = tokens
	}
	return ok
}
