package guarder

import (
	"context"
	"time"
)

func (c *Client) MGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	paramInterfaceList := make([]interface{}, len(keys))
	for i, ele := range keys {
		paramInterfaceList[i] = ele
	}
	redisResult, err := c.remoteClient.Do(ctx, "MGET", paramInterfaceList)
	if err != nil {
		return nil, err
	}
	interfaceValList, _ := redisResult.([]interface{})
	ret := make(map[string]interface{})
	for idx, inter := range interfaceValList {
		ret[keys[idx]] = inter
	}
	return ret, nil
}

func (c *Client) MGetWithLoadFn(ctx context.Context, keys []string, loadFn CustomLoadFunc) (map[string]interface{}, error) {
	keysNum := len(keys)
	lcData, missedKeys, err := c.loadFromLocalCacheWithKeys(keys...)
	if err != nil {
		return nil, err
	}
	if len(lcData) == keysNum {
		return lcData, nil
	}

	keys = missedKeys
	redisData, err := c.MGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	//写入本地缓存
	c.batchSave2LocalCache(redisData)

	ret := mergeInterfaceMap(redisData, lcData)
	if len(ret) == keysNum {
		return ret, nil
	}
	missedKeys = []string{}
	for _, k := range keys {
		_, ok := ret[k]
		if !ok {
			missedKeys = append(missedKeys, k)
		}
	}
	//应对缓存击穿，合并请求
	sgData, err := c.mergeReq(ctx, missedKeys, loadFn)
	dbData := sgData.(map[string]interface{})
	//异步更新到Redis，
	go WithRecover(
		func() {
			ctx := context.Background()
			ttl := time.Duration(c.remoteCacheTTL.Load())
			conn, _ := c.remoteClient.Pipeline(ctx)
			for k, v := range dbData {
				_ = conn.Send("set", k, v, "ex", c.calculateRandTime(ttl).Seconds())
			}
			_ = conn.Flush()
			num := len(dbData)
			for i := 0; i < num; i++ {
				_, _ = conn.Receive()
			}
		},
	)
	//同步更新本地缓存
	c.batchSave2LocalCache(dbData)

	//合并结果
	return mergeInterfaceMap(ret, dbData), nil
}
