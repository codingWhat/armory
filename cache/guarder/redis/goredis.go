package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type GoRedisClient struct {
	client *redis.Client
}

func NewGoRedisClient(client *redis.Client) *GoRedisClient {
	return &GoRedisClient{client: client}
}

func (c *GoRedisClient) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return c.client.Do(ctx, cmd, args).Result()
}

func (c *GoRedisClient) Pipeline(ctx context.Context) (RedisPipeline, error) {
	return NewGoRedisPipeline(c.client.Pipeline()), nil
}

type GoRedisPipeline struct {
	pipeline redis.Pipeliner
}

func NewGoRedisPipeline(pipeline redis.Pipeliner) *GoRedisPipeline {
	return &GoRedisPipeline{pipeline: pipeline}
}

func (p *GoRedisPipeline) Send(commandName string, args ...interface{}) error {
	// go-redis的Send方法和redigo的Send方法参数略有不同，需要转换一下
	return p.pipeline.Process(context.Background(), redis.NewCmd(context.Background(), append([]interface{}{commandName}, args...)...))
}

func (p *GoRedisPipeline) Flush() error {
	_, err := p.pipeline.Exec(context.Background())
	return err
}

func (p *GoRedisPipeline) Receive() (reply interface{}, err error) {
	// go-redis的pipeline操作在Flush方法时已经接收所有响应，所以Receive方法可以直接返回nil
	return nil, nil
}
