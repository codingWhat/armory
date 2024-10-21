package redis

import (
	"context"
	"fmt"
	"github.com/codingWhat/armory/distributedlock"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type lock struct {
	locks  sync.Map
	client *redis.Client
}

func New(client *redis.Client) distributedlock.Locker {
	return &lock{
		locks:  sync.Map{},
		client: client,
	}
}

func (l *lock) lockWith(k string) {
	val, ok := l.locks.Load(k)
	if !ok {
		mu := &sync.Mutex{}
		l.locks.Store(k, mu)
		mu.Lock()
		return
	}
	mu := val.(*sync.Mutex)
	mu.Lock()
}

func (l *lock) unLockWith(k string) {
	val, ok := l.locks.Load(k)
	if !ok {
		return
	}
	mu := val.(*sync.Mutex)
	mu.Unlock()
}
func (l *lock) getKey(rkey, owner string) string {
	return fmt.Sprintf("%s%s", rkey, owner)
}

func (l *lock) Lock(ctx context.Context, key string, owner string, ttl time.Duration) error {
	l.lockWith(l.getKey(key, owner))
	defer l.unLockWith(l.getKey(key, owner))

	script := `
    if redis.call("exists", KEYS[1]) == 0 then
        redis.call("hset", KEYS[1], ARGV[1], 1)
        redis.call("pexpire", KEYS[1], ARGV[2])
        return 1
    elseif redis.call("hexists", KEYS[1], ARGV[1]) >= 1 then
        redis.call("hincrby", KEYS[1], ARGV[1], 1)
        redis.call("pexpire", KEYS[1], ARGV[2])
        return 1
    else
        return 0
    end
`
	result, err := l.client.Eval(ctx, script, []string{key}, owner, ttl/time.Millisecond).Result()
	if err != nil {
		return errors.WithMessage(err, "lock failed")
	}

	if result.(int64) == 1 {
		return nil
	}
	return errors.New("lock failed")
}

func (l *lock) UnLock(ctx context.Context, key string, owner string) error {
	script := `
    if redis.call("hexists", KEYS[1], ARGV[1]) == 0 then
		redis.call("hdel", KEYS[1], ARGV[1])
		return 1
    elseif redis.call("hincrby", KEYS[1], ARGV[1], -1) > 0 then
        return 1
    else
        redis.call("del", KEYS[1])
        return 1
    end
    `
	_, err := l.client.Eval(ctx, script, []string{key}, owner).Result()
	if err != nil {
		return errors.WithMessage(err, "unlock failed")
	}
	return nil
}
