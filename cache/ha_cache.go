package cache

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"time"
)

var (
	FoModOneTier = 1
	FoModTwoTier = 2
)

type HACache struct {
	client redis.Client
	lc     LocalCache
	conf   *Config
}

type CustomLoadFunc func(ctx context.Context) (interface{}, error)

func (h *HACache) GetWithLoadFn(ctx context.Context, key string, fn CustomLoadFunc) (string, error) {
	switch h.conf.foMod {
	case FoModOneTier:
		return h.getWithLoadFn(ctx, key, fn)
	case FoModTwoTier:
		val, isExists := h.lc.Get([]byte(key))
		if isExists {
			return val.(string), nil
		}
		ret, err := h.getWithLoadFn(ctx, key, fn)
		if err == nil {
			ttl := time.Duration(h.conf.lcExpire+rand.Int()%h.conf.expireRandFactor) * time.Second
			_ = h.lc.Set([]byte(key), []byte(ret), ttl)
			return ret, err
		}
		return "", err
	}
	return "", errors.New("invalid fail over mod")
}

func (h *HACache) getWithLoadFn(ctx context.Context, key string, fn CustomLoadFunc) (string, error) {
	result, err := h.client.Get(ctx, key).Result()
	if err == nil {
		return result, nil
	}
	data, err := fn(ctx)
	if err == nil {
		// 注意: 缓存雪崩和缓存穿透的处理
		// 这里可能返回空，为了防止缓存穿透，空也得缓存
		ttl := time.Duration(h.conf.rcExpire+rand.Int()%h.conf.expireRandFactor) * time.Second
		_, _ = h.client.Set(ctx, key, data, ttl).Result()
		return data.(string), nil
	}
	return "", err
}

func NewHaCache(client redis.Client, ops ...Opts) (*HACache, error) {
	defaultConf := CreateDefaultConf()
	for _, op := range ops {
		op(defaultConf)
	}

	return &HACache{
		client: client,
		lc:     defaultConf.lc,
		conf:   defaultConf,
	}, nil
}
