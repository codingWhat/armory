package lfu

import (
	"container/list"
	"errors"
)

type Value interface {
	Len() int64
}

type entry struct {
	Prev *entry
	Next *entry
	Key  string
	Val  Value
	Freq int
}

type Cache struct {
	Capacity int
	items    map[string]*list.Element
	ll       *list.List
}

func (c *Cache) Put(k string, v Value) {
	ele, ok := c.items[k]
	if !ok {
		// remove the least frequency node
		ele = &list.Element{Value: &entry{Key: k, Freq: 1, Val: v}}
	} else {
		ele.Value.(*entry).Freq++
	}
	c.items[k] = ele
}

func (c *Cache) adjust(ele *list.Element) {

}

func (c *Cache) Get(k string) (Value, error) {
	ele, ok := c.items[k]
	if !ok {
		return nil, errors.New("no exists")
	}

	ele.Value.(*entry).Freq++
	c.adjust(ele)
	return ele.Value.(*entry).Val, nil
}

func NewCache(capacity int) *Cache {
	return &Cache{Capacity: capacity}
}
