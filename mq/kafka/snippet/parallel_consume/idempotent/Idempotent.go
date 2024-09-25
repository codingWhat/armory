package idempotent

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/willf/bloom"
	"strconv"
	"time"
)

type Srv struct {
	bf *bloom.BloomFilter
	//redisBf *RedisBF
	redis *redis.Client
}

func NewIdempotentSrv() *Srv {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis 服务器地址
		Password: "",               // Redis 服务器密码，如果没有密码则为空字符串
	})

	return &Srv{
		bf:    bloom.NewWithEstimates(50000000, 0.00001),
		redis: client,
	}
}

func (i *Srv) RebuildBF(ctx context.Context, hour int) {
	now := time.Now()

	// 计算距离下一个凌晨四点的时间间隔
	next := time.Date(now.Year(), now.Month(), now.Day()+1, hour, 0, 0, 0, now.Location())
	duration := next.Sub(now)

	// 创建定时器，等待时间间隔后执行任务
	timer := time.NewTimer(duration)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// 重新计算下一个凌晨四点的时间间隔
			next = next.Add(24 * time.Hour)
			duration = next.Sub(time.Now())
			timer.Reset(duration)
		}
	}
}

// ttl 取决于业务，保证每次重复消费就能取到，
func (i *Srv) Add(ctx context.Context, data []byte, ttl int) {
	i.bf.Add(data)

	//解决假阳
	i.redis.SetNX(ctx, string(data), 1, time.Duration(ttl)*time.Second) //存储容量问题:
}

func (i *Srv) Contain(ctx context.Context, data []byte) bool {

	if !i.bf.Test(data) {
		return false
	}

	v, err := i.redis.Get(ctx, string(data)).Result()
	if err == redis.Nil {
		return false
	}
	exist, _ := strconv.Atoi(v)
	return exist == 1
}
