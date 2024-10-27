package redis

import "context"

type RedisClient interface {
	Do(ctx context.Context, cmd string, args ...interface{}) (reply interface{}, err error)
	Pipeline(ctx context.Context) (RedisPipeline, error)
}

type RedisPipeline interface {
	Send(commandName string, args ...interface{}) error
	Flush() error
	Receive() (reply interface{}, err error)
}
