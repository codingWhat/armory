package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
	"sync/atomic"
	"time"
)

type TokenBucket struct {
	client *redis.Client
	key    string

	rate     float64
	capacity float64
}

func NewTokenBucket() *TokenBucket {
	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // 如果有密码，请提供密码
		DB:       0,  // 选择默认数据库
	})

	return &TokenBucket{
		client: client,
	}
}

func (tb *TokenBucket) Allow(key string, rate float64, capacity float64) (bool, error) {
	// 调用 Lua 脚本
	script := `
		local key = KEYS[1]
		local rate = tonumber(ARGV[1])
		local capacity = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])
		local token = tonumber(redis.call('get', key) or capacity)
		local lastRefill = tonumber(redis.call('get', key .. ':lastRefill') or now)

		local elapsed = now - lastRefill
		local refill = elapsed * rate + token
		if refill > capacity then
			refill = capacity
		end

		if refill >= 1 then
			redis.call('set', key, refill - 1)
			redis.call('set', key .. ':lastRefill', now)
			return 1
		else
			return 0
		end
	`

	// 传入 Lua 脚本参数
	now := time.Now().Unix()
	// 执行 Lua 脚本
	result, err := tb.client.Do(context.Background(), "eval", script, 1, key, rate, capacity, now).Result()
	if err != nil {
		return false, err
	}

	return result == int64(1), nil
}

func main() {
	tb := NewTokenBucket()
	gw := sync.WaitGroup{}
	gw.Add(120)
	count := atomic.Int64{}
	for i := 0; i < 120; i++ {
		go func(i int) {
			defer gw.Done()
			status, err := tb.Allow("bucket", 10.0, 10.0)
			if status {
				count.Add(1)
			}
			fmt.Printf("go %d status:%v error: %v\n", i, status, err)
		}(i)
	}
	gw.Wait()
	fmt.Printf("allow %d\n\n", count.Load())
}
