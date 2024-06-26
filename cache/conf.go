package cache

import (
	"github.com/redis/go-redis/v9"
)

func CreateDefaultConf() *Config {
	return &Config{
		foMod: FoModOneTier,
	}
}

type Config struct {
	foMod  int //降级模式
	lc     LocalCache
	client redis.Client

	lcExpire         int
	rcExpire         int
	expireRandFactor int
}

type Opts func(c *Config)

func WithLocalCache(lc LocalCache, lcExpire int, erf int) Opts {
	return func(c *Config) {
		c.lc = lc
		c.lcExpire = lcExpire
		c.expireRandFactor = erf
		c.foMod = FoModTwoTier
	}
}
