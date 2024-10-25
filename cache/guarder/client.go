package guarder

import (
	"context"
	"golang.org/x/sync/singleflight"
	"hash/crc32"
	"math/rand"
	"strconv"
	"strings"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

// Client redis请求接口
type RedisClient interface {
	// Do sends a command to the server and returns the received reply. The Redis command reference
	// https://pkg.go.dev/github.com/gomodule/redigo/redis#hdr-Executing_Commands
	Do(ctx context.Context, cmd string, args ...interface{}) (reply interface{}, err error)
	Pipeline(ctx context.Context) (redigo.Conn, error)
}

type MemoryStore interface {
	Set(key string, val interface{}, ttl time.Duration) error
	Get(key string) (interface{}, error)
}

type CustomLoadFunc func(ctx context.Context, client interface{}, key ...string) (map[string]interface{}, error)

type Client struct {
	redisClient RedisClient
	memStore    MemoryStore
	ops         Option

	sg             *singleflight.Group
	localCacheTTL  time.Duration
	remoteCacheTTL time.Duration
	randFactor     int
}

func New(redis RedisClient, store MemoryStore) *Client {

	return &Client{
		redisClient: redis,
		memStore:    store,
	}
}
func (c *Client) SetCacheTTL(ttl time.Duration) *Client {
	c.remoteCacheTTL = ttl
	return c
}

func (c *Client) SetLocalCacheTTL(ttl time.Duration) *Client {
	c.localCacheTTL = ttl
	return c
}

// 随机化过期时间，应对缓存雪崩
func (c *Client) calculateRandTime(baseTime time.Duration) time.Duration {
	return baseTime + time.Duration(rand.Int()%c.randFactor)
}

func (c *Client) mergeReq(ctx context.Context, keys []string, loadFn CustomLoadFunc) (interface{}, error) {
	//应对缓存击穿，合并请求
	sgKey := crc32.ChecksumIEEE([]byte(strings.Join(keys, ",")))
	sgData, err, _ := c.sg.Do(strconv.Itoa(int(sgKey)), func() (interface{}, error) {
		return loadFn(ctx, keys)
	})
	return sgData, err
}

func (c *Client) loadFromLocalCacheWithKeys(keys ...string) (map[string]interface{}, []string, error) {
	missedKeys := make([]string, 0, len(keys))
	ret := make(map[string]interface{}, len(keys))
	for _, key := range keys {
		lcCacheData, err := c.memStore.Get(key)
		if err != nil {
			return nil, nil, err
		}
		if lcCacheData == nil {
			missedKeys = append(missedKeys, key)
		} else {
			ret[key] = lcCacheData
		}
	}
	return ret, missedKeys[:], nil
}

func (c *Client) save2LocalCache(k string, v interface{}) {
	_ = c.memStore.Set(k, v, c.calculateRandTime(c.localCacheTTL))
}

func (c *Client) batchSave2LocalCache(data map[string]interface{}) {
	for k, v := range data {
		c.save2LocalCache(k, v)
	}
}

func mergeInterfaceMap(a map[string]interface{}, b map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for k, v := range b {
		ret[k] = v
	}
	for k, v := range a {
		ret[k] = v
	}
	return ret
}
