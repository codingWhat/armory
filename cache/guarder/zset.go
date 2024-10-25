package guarder

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type Lease struct {
}

// Grant 获取租约
func (c *Client) Grant(ctx context.Context, key string, ttl time.Duration, loadFn CustomLoadFunc) {

}

func (c *Client) ZRevRangeWithLoad(ctx context.Context, key string, start, stop int, loadFn CustomLoadFunc) ([]string, error) {
	lcData, err := c.memStore.Get(key)
	if err != nil {
		return nil, err
	}
	v, ok := lcData.([]string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("[ZRevRangeWithLoad] data's format is not invalid. data: %v", lcData))
	}
	if v != nil {
		return v, nil
	}

	reply, err := c.redisClient.Do(ctx, "ZREVRANGE", key, start, stop)
	if err != nil {
		return nil, err
	}
	redisData, ok := reply.([]string)
	if ok {
		c.save2LocalCache(key, reply)
		return redisData, nil
	}
	//应对缓存击穿，合并请求
	sgData, err := c.mergeReq(ctx, []string{key, strconv.Itoa(start), strconv.Itoa(stop)}, loadFn)
	dbData := sgData.(map[string]interface{})
	//异步更新到Redis，同步更新本地缓存
	Async(func() {
		ctx := context.Background()
		args := make([]interface{}, len(dbData)*2+1)
		args[0] = key
		idx := 1
		for k, v := range dbData {
			args[idx] = k
			idx++
			args[idx] = v
			idx++
		}
		if _, err := c.redisClient.Do(ctx, "ZADD", args); err != nil {
			fmt.Println("---> zadd failed, err:", err.Error())
		}
	})

	c.save2LocalCache(key, dbData[key].([]string))
	return dbData[key].([]string), nil
}
