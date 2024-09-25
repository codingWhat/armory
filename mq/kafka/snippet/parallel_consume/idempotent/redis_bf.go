package idempotent

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/spaolacci/murmur3"
	"math"
	"strconv"
)

var maps = 14

type RedisBF struct {
	redis *redis.Client
	bits  uint
	key   string
}

func NewRedisBF(client *redis.Client, key string) *RedisBF {
	return &RedisBF{
		redis: client,
		bits:  math.MaxInt,
		key:   key,
	}
}
func (r *RedisBF) getLocations(data []byte) []uint {
	// 创建指定容量的切片
	locations := make([]uint, maps)
	// maps表示k值,作者定义为了常量:14
	for i := uint(0); i < uint(maps); i++ {
		// 哈希计算,使用的是"MurmurHash3"算法,并每次追加一个固定的i字节进行计算
		hashValue := murmur3.Sum64(append(data, byte(i)))
		// 取下标offset
		locations[i] = uint(hashValue % uint64(r.bits))
	}

	return locations
}

func (r *RedisBF) buildArgs(offsets []uint) ([]string, error) {
	var args []string
	for _, offset := range offsets {
		if offset >= r.bits {
			return nil, errors.New("offset is so large")
		}
		args = append(args, strconv.FormatUint(uint64(offset), 10))
	}
	return args, nil
}

func (r *RedisBF) Add(ctx context.Context, data []byte) error {
	setScript := `
	for _, offset in ipairs(ARGV) do
	   redis.call("setbit", KEYS[1], offset, 1)
	end
	`
	locations := r.getLocations(data)
	args, err := r.buildArgs(locations)
	if err != nil {
		return err
	}

	_, err = r.redis.Eval(ctx, setScript, []string{r.key}, args).Result()
	if err == redis.Nil {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (r *RedisBF) Contain(ctx context.Context, data []byte) (bool, error) {
	testScript := `
    for _, offset in ipairs(ARGV) do
        if tonumber(redis.call("getbit", KEYS[1], offset)) == 0 then
            return false
        end
    end
    return true`
	locations := r.getLocations(data)
	args, err := r.buildArgs(locations)
	if err != nil {
		return false, err
	}

	exist, err := r.redis.Eval(ctx, testScript, []string{}, args).Result()
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return exist.(bool), nil
}
