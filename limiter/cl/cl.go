package cl

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

type ClusterLimiter struct {
	pool *redis.Pool
}

func NewClusterLimiter() *ClusterLimiter {
	var (
		readTimeout  = time.Second
		writeTimeout = time.Second
	)
	p := &redis.Pool{
		MaxIdle:     20,
		MaxActive:   500,
		IdleTimeout: time.Duration(120),
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialPassword(""), redis.DialReadTimeout(readTimeout), redis.DialWriteTimeout(writeTimeout))
			if err != nil {
				fmt.Printf("redisClient dial host: %s, auth: %s err: %s", "127.0.0.1:6379", "xxxxxx", err.Error())
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			if err != nil {
				fmt.Printf("redisClient ping err: %s", err.Error())
			}
			return err
		},
	}
	return &ClusterLimiter{pool: p}
}

func (cl *ClusterLimiter) Allow(ctx context.Context, key string, limit int, duration ...time.Duration) (bool, error) {
	var (
		expireTime time.Duration
		freezeTime time.Duration
	)
	if len(duration) == 1 {
		expireTime = duration[0]
	} else if len(duration) > 1 {
		expireTime = duration[0]
		freezeTime = duration[1]
	}

	script := `
        local key = KEYS[1]
        local max_count = tonumber(ARGV[1])
        local expire_time = tonumber(ARGV[2])
        local freeze_time = tonumber(ARGV[3])
        
        local current = tonumber(redis.call('get', key) or "0")
        
        if current >= max_count then
          if freeze_time > 0 then
              redis.call('setex', key, freeze_time, current) 
          end 
          return 0
        end
        
        if current < max_count then
          if current == 0 then
            if expire_time > 0 then
              redis.call('setex', key, expire_time, 1)
            else
               return 0   
            end
          else
            redis.call('incr', key)
          end
          return 1
        else
          return 0
        end
	`
	scr := redis.NewScript(1, script)
	result, err := scr.DoContext(ctx, cl.pool.Get(), key, limit, expireTime, freezeTime)
	if err != nil {
		return false, err
	}

	return result == int64(1), nil
}
