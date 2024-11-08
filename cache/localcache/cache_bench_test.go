package localcache

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/dgraph-io/ristretto"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)
import (
	"github.com/coocood/freecache"
)

const maxEntrySize = 256
const maxEntryCount = 10000

type myStruct struct {
	Id int `json:"id"`
}

type constructor[T any] interface {
	Get(int) T
	Parse([]byte) (T, error)
	ToBytes(T) ([]byte, error)
}

func key(i int) string {
	return fmt.Sprintf("key-%010d", i)
}

// 泛型函数，用于初始化一个 Ristretto 缓存
func initRistretto[Key string, Val any]() *ristretto.Cache[Key, Val] {
	cache, _ := ristretto.NewCache[Key, Val](&ristretto.Config[Key, Val]{
		NumCounters: int64(100000 * 10), // 10倍的最大元素数量，用于计数器
		MaxCost:     1e6,                // 最大成本，可以根据实际情况调整
		BufferItems: 64,                 // 缓冲区大小
	})
	return cache
}

func initBigCache(entriesInWindow int) *bigcache.BigCache {
	cache, _ := bigcache.New(context.Background(), bigcache.Config{
		Shards:             256,
		LifeWindow:         10 * time.Minute,
		MaxEntriesInWindow: entriesInWindow,
		MaxEntrySize:       maxEntrySize,
		Verbose:            false,
	})

	return cache
}

func parallelKey(threadID int, counter int) string {
	return fmt.Sprintf("key-%04d-%06d", threadID, counter)
}

func SyncMapSetParallel[T any](cs constructor[T], b *testing.B) {
	var m sync.Map
	b.RunParallel(func(pb *testing.PB) {
		thread := rand.Intn(1000)
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			m.Store(parallelKey(thread, id), cs.Get(id))
		}
	})
}

type structConstructor struct {
}

func (sc structConstructor) Get(n int) myStruct {
	return myStruct{Id: n}
}

func (sc structConstructor) Parse(data []byte) (myStruct, error) {
	var s myStruct
	err := json.Unmarshal(data, &s)
	return s, err
}

func (sc structConstructor) ToBytes(v myStruct) ([]byte, error) {
	return json.Marshal(v)
}

func BenchmarkSyncMapSetParallelForStruct(b *testing.B) {
	SyncMapSetParallel[myStruct](structConstructor{}, b)
}

func FreeCacheSetParallel[T any](cs constructor[T], b *testing.B) {
	cache := freecache.NewCache(maxEntryCount * maxEntrySize)

	b.RunParallel(func(pb *testing.PB) {
		thread := rand.Intn(1000)
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			data, _ := cs.ToBytes(cs.Get(id))
			cache.Set([]byte(parallelKey(thread, id)), data, 0)
		}
	})
}
func BenchmarkRistrettoSetParallelForStruct(b *testing.B) {
	RistrettoSetParallel[myStruct](structConstructor{}, b)
}

func RistrettoSetParallel[T any](cs constructor[T], b *testing.B) {
	cache := initRistretto[string, myStruct]()
	b.RunParallel(func(pb *testing.PB) {
		thread := rand.Intn(1000)
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			cache.Set(parallelKey(thread, id), myStruct{Id: id}, 10)
			cache.Wait()
		}
	})
}

func BenchmarkFreeCacheSetParallelForStruct(b *testing.B) {
	FreeCacheSetParallel[myStruct](structConstructor{}, b)
}

func BigCacheSetParallel[T any](cs constructor[T], b *testing.B) {
	cache := initBigCache(maxEntryCount)

	b.RunParallel(func(pb *testing.PB) {
		thread := rand.Intn(1000)
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			data, _ := cs.ToBytes(cs.Get(id))
			cache.Set(parallelKey(thread, id), data)
		}
	})
}

func BenchmarkBigCacheSetParallelForStruct(b *testing.B) {
	BigCacheSetParallel[myStruct](structConstructor{}, b)
}

func LCSetParallel[T any](cs constructor[T], b *testing.B) {
	cache := New(WithCapacity(10000), WithSyncMode())
	b.RunParallel(func(pb *testing.PB) {
		thread := rand.Intn(1000)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			//data, _ := cs.ToBytes(cs.Get(id))
			cache.Set(ctx, parallelKey(thread, id), cs.Get(id), 2*time.Second)

		}
		cancel()
	})
}
func BenchmarkLCSetParallelForStruct(b *testing.B) {
	LCSetParallel[myStruct](structConstructor{}, b)
}

func SyncMapGetParallel[T any](cs constructor[T], b *testing.B) {
	b.StopTimer()
	var m sync.Map
	for i := 0; i < maxEntryCount; i++ {
		m.Store(key(i), cs.Get(i))
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			e, ok := m.Load(key(id))
			if ok {
				_ = (T)(e.(T))
			}
		}
	})
}

func FreeCacheGetParallel[T any](cs constructor[T], b *testing.B) {
	b.StopTimer()
	cache := freecache.NewCache(maxEntryCount * maxEntrySize)
	for i := 0; i < maxEntryCount; i++ {
		data, _ := cs.ToBytes(cs.Get(i))
		cache.Set([]byte(key(i)), data, 0)
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			data, _ := cache.Get([]byte(key(id)))
			v, _ := cs.Parse(data)
			_ = (T)(v)
		}
	})
}

func BigCacheGetParallel[T any](cs constructor[T], b *testing.B) {
	b.StopTimer()
	cache := initBigCache(maxEntryCount)
	for i := 0; i < maxEntryCount; i++ {
		data, _ := cs.ToBytes(cs.Get(i))
		cache.Set(key(i), data)
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			data, _ := cache.Get(key(id))
			v, _ := cs.Parse(data)
			_ = (T)(v)
		}
	})
}

func BenchmarkSyncMapGetParallelForStruct(b *testing.B) {
	SyncMapGetParallel[myStruct](structConstructor{}, b)
}

func BenchmarkFreeCacheGetParallelForStruct(b *testing.B) {
	FreeCacheGetParallel[myStruct](structConstructor{}, b)
}

func BenchmarkBigCacheGetParallelForStruct(b *testing.B) {
	BigCacheGetParallel[myStruct](structConstructor{}, b)
}

func BenchmarkRistrettoGetParallelForStruct(b *testing.B) {
	RistrettoGetParallel[myStruct](structConstructor{}, b)
}

func RistrettoGetParallel[T any](cs constructor[T], b *testing.B) {
	b.StopTimer()
	cache := initRistretto[string, myStruct]()
	for i := 0; i < maxEntryCount; i++ {
		cache.Set(strconv.Itoa(i), myStruct{Id: i}, 10)
		cache.Wait()
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			data, _ := cache.Get(key(id))
			_ = data
		}
	})
}

func LCGetParallel[T any](cs constructor[T], b *testing.B) {
	b.StopTimer()
	cache := New(WithCapacity(10000), WithSyncMode())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	for i := 0; i < maxEntryCount; i++ {
		cache.Set(ctx, key(i), cs.Get(i), 2*time.Second)
	}
	cancel()
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := rand.Intn(maxEntryCount)
			_, _ = cache.Get(key(id))
			//v, _ := cs.Parse(T(data))
			//_ = (T)(v)
		}
	})
}
func BenchmarkLCGetParallelForStruct(b *testing.B) {
	LCGetParallel[myStruct](structConstructor{}, b)
}
