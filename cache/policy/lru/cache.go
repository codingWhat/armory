package lru

import (
	"container/list"
	cache2 "github.com/codingWhat/armory/cache"
	"sync"
)

var (
	lc          cache2.Cache
	DefaultSize = 1000
)

func InitLocalCache(size int) {
	lc = New(size)
}

var GetLocalCache = func() cache2.Cache {
	sync.OnceFunc(func() {
		if lc != nil {
			lc = New(DefaultSize)
		}
	})
	return lc
}

func New(size int) cache2.Cache {
	return &cache{
		capacity: size,
		items:    make(map[string]*list.Element),
		ll:       list.New(),
	}
}

type cache struct {
	capacity int
	ll       *list.List
	items    map[string]*list.Element
	mu       sync.RWMutex
}

func (c *cache) removeOldest() {
	oldestE := c.ll.Back()
	if oldestE != nil {
		e := oldestE.Value.(*entry)
		delete(c.items, e.key)
		c.ll.Remove(oldestE)
	}
}

func (c *cache) Get(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value
	}

	return nil
}

func (c *cache) Set(key string, val any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//如果存在，移到对头，更新value，currSize
	if ele, ok := c.items[key]; ok {
		ent := ele.Value.(*entry)
		ent.key = key
		ent.value = val
		c.ll.MoveToFront(ele)
	} else {
		if c.ll.Len() >= c.capacity {
			c.removeOldest()
		}
		// 如果不存在，新建节点，移到对头，更新currSize
		ent := &entry{key: key, value: val}
		c.items[key] = c.ll.PushFront(ent)
	}
}

type entry struct {
	key   string
	value any
}
