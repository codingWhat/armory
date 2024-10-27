package guarder

import (
	"context"
	"github.com/codingWhat/armory/cache/guarder/redis"
	"hash/crc32"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type MemoryStore interface {
	Set(key string, val interface{}, ttl time.Duration) error
	Get(key string) (interface{}, error)
}

type CustomLoadFunc func(ctx context.Context, client interface{}, key ...string) (map[string]interface{}, error)

type Client struct {
	remoteClient redis.RedisClient
	memStore     MemoryStore

	sg             *singleflight.Group
	localCacheTTL  atomic.Int32
	remoteCacheTTL atomic.Int32
	randFactor     int

	taskManger *TaskManger

	ops *Options
}

func New(redis redis.RedisClient, options ...Option) *Client {

	currOpts := &Options{}
	for _, op := range options {
		op(currOpts)
	}

	client := &Client{
		remoteClient: redis,
		ops:          currOpts,
		randFactor:   30,
	}

	if currOpts.EnableLocalCache {
		client.memStore = NewSimpleMemoryStore()
	}
	if currOpts.LocalCache != nil {
		client.memStore = currOpts.LocalCache
	}

	if currOpts.LocalCacheTTL.Seconds() > 0 {
		client.localCacheTTL.Store(int32(currOpts.LocalCacheTTL.Seconds()))
	}

	client.remoteCacheTTL.Store(int32(15))
	if currOpts.RemoteCacheTTL.Seconds() > 0 {
		client.remoteCacheTTL.Store(int32(currOpts.RemoteCacheTTL.Seconds()))
	}

	client.taskManger = NewTaskManager()

	return client
}

// AddCronTask 设置定期更新任务
func (c *Client) AddCronTask(ctx context.Context, taskName string, interval time.Duration, fn func()) {
	c.taskManger.AddTask(ctx, NewCronTask(taskName, interval, fn, c.taskManger))
}

func (c *Client) RemoveCronTask(ctx context.Context, taskName string) {
	c.taskManger.RemoveTask(ctx, taskName)
}

func (c *Client) SetCacheTTL(ttl int32) *Client {
	c.remoteCacheTTL.Store(ttl)
	return c
}

func (c *Client) SetLocalCacheTTL(ttl int32) *Client {
	c.localCacheTTL.Store(ttl)
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
	ttl := c.localCacheTTL.Load()
	_ = c.memStore.Set(k, v, c.calculateRandTime(time.Duration(ttl)*time.Second))
}

func (c *Client) batchSave2LocalCache(data map[string]interface{}) {
	for k, v := range data {
		c.save2LocalCache(k, v)
	}
}

func (c *Client) Close() {
	if c.taskManger != nil {
		c.taskManger.Close()
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
