package redis

import (
	"context"
	"github.com/gomodule/redigo/redis"
)

type RedigoClient struct {
	pool *redis.Pool
}

func NewRedigoClient(pool *redis.Pool) *RedigoClient {
	return &RedigoClient{pool: pool}
}

func (c *RedigoClient) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return conn.Do(cmd, args...)
}

func (c *RedigoClient) Pipeline(ctx context.Context) (RedisPipeline, error) {
	return NewRedigoPipeline(c.pool.Get()), nil
}

type RedigoPipeline struct {
	conn redis.Conn
}

func NewRedigoPipeline(conn redis.Conn) *RedigoPipeline {
	return &RedigoPipeline{conn: conn}
}

func (p *RedigoPipeline) Send(commandName string, args ...interface{}) error {
	return p.conn.Send(commandName, args...)
}

func (p *RedigoPipeline) Flush() error {
	return p.conn.Flush()
}

func (p *RedigoPipeline) Receive() (reply interface{}, err error) {
	return p.conn.Receive()
}
