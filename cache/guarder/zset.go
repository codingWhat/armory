package guarder

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
)

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	return c.zrevrange(ctx, key, start, stop)
}

var ErrInvalidDataFormat = errors.New(" data's format is not invalid. data")

func (c *Client) ReloadZrevRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	reply, err := c.remoteClient.Do(ctx, "ZREVRANGE", key, start, stop)
	if err != nil {
		return nil, err
	}
	values, ok := reply.([]interface{})
	if ok {
		ret := make([]string, 0, len(values))
		for _, val := range values {
			v := val.([]byte)
			ret = append(ret, string(v))
		}
		c.save2LocalCache(key, ret)
		return ret, nil
	}
	return nil, errors.WithMessage(ErrInvalidDataFormat, fmt.Sprintf("zrevrange, data:%+v", values))
}

func (c *Client) zrevrange(ctx context.Context, key string, start, stop int) ([]string, error) {
	lcData, err := c.memStore.Get(key)
	if err == nil {
		v, ok := lcData.([]string)
		if ok {
			fmt.Println("get from local cache")
			return v, nil
		}
	}
	fmt.Println("get from redis")
	return c.ReloadZrevRange(ctx, key, start, stop)
}

func (c *Client) ZRevRangeWithLoad(ctx context.Context, key string, start, stop int, loadFn CustomLoadFunc) ([]string, error) {
	ret, err := c.zrevrange(ctx, key, start, stop)
	if err != nil && !errors.Is(err, ErrInvalidDataFormat) {
		return nil, err
	}

	if len(ret) != 0 {
		return ret, nil
	}

	//应对缓存击穿，合并请求
	sgData, err := c.mergeReq(ctx, []string{key, strconv.Itoa(start), strconv.Itoa(stop)}, loadFn)
	dbData := sgData.(map[string]interface{})
	//异步更新到Redis，同步更新本地缓存
	go WithRecover(func() {
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
		if _, err := c.remoteClient.Do(ctx, "ZADD", args); err != nil {
			fmt.Println("---> zadd failed, err:", err.Error())
		}
	})

	c.save2LocalCache(key, dbData[key].([]string))
	return dbData[key].([]string), nil
}
