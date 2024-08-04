package lru

import (
	"container/list"
	"errors"
	"sync"
)

type Cache struct {
	size     int64
	currSize int64
	ll       *list.List
	items    map[string]*entry
	sync.RWMutex
}

func (c *Cache) removeOldest() {
	oldestE := c.ll.Back()
	if oldestE != nil {
		e := oldestE.Value.(*entry)
		delete(c.items, e.key)
		c.ll.Remove(oldestE)
		c.currSize -= int64(len(e.key)) + e.value.Len()
	}
}

func (c *Cache) Get(key string) (Value, error) {
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()
	if node, ok := c.items[key]; ok {
		c.ll.PushFront(node)
		return node.value, nil
	}

	return nil, errors.New("no exists")
}

func (c *Cache) Set(key string, val Value) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	//如果存在，移到对头，更新value，currSize
	if e, ok := c.items[key]; ok {
		c.currSize += val.Len() - e.value.Len()
		e.value = val
		c.ll.PushFront(e)
	} else {
		if c.currSize > c.size {
			c.removeOldest()
		}
		// 如果不存在，新建节点，移到对头，更新currSize
		e := &entry{key: key, value: val}
		c.currSize += int64(len(key)) + val.Len()
		c.ll.PushFront(e)
		c.items[key] = e
	}
}

type Value interface {
	Len() int64
}

type entry struct {
	key   string
	value Value
}
