package guarder

import (
	"context"
	"fmt"
	redis2 "github.com/codingWhat/armory/cache/guarder/redis"
	"github.com/gomodule/redigo/redis"
	"testing"
	"time"
)

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}
func TestClient(t *testing.T) {

	ctx := context.Background()
	redisClient := redis2.NewRedigoClient(newPool())
	client := New(redisClient,
		WithEnableLocalCache(), //使用默认的本地缓存， 可以用	WithLocalCache()替换
		WithLocalCacheTTL(5*time.Second),
	)
	client.AddCronTask(ctx, "reload-cache-actively", 2*time.Second, func() {
		fmt.Println("reload-cache-actively", time.Now().Unix())
		_, _ = client.ReloadZrevRange(ctx, "aaa", 0, 1)
	})
	defer func() {
		client.RemoveCronTask(ctx, "reload-cache-actively")
		client.Close()
	}()

	for i := 0; i < 10; i++ {
		revRange, err := client.ZRevRange(ctx, "aaa", 0, 1)
		fmt.Println(revRange, err)
		time.Sleep(5 * time.Second)
	}

	//if err != nil {
	//	return
	//}
	//// local cache + redis
	//client.MGet(ctx, []string{"1"})
	//// local cache + redis + db
	//client.MGetWithLoadFn(ctx, []string{"1"}, nil)
}
