package lfu

import (
	"container/list"
	cache2 "github.com/codingWhat/armory/cache"
)

func New(size int) cache2.Cache {
	return &cache{
		capacity: size,
		items:    make(map[string]*list.Element),
		freq:     make(map[int]*list.List),
	}
}

type entry struct {
	Key  string
	Val  any
	Freq int
}

type cache struct {
	capacity int
	items    map[string]*list.Element
	freq     map[int]*list.List
}

func (c *cache) Set(k string, v any) {
}

func (c *cache) adjust(ele *list.Element) {

}

func (c *cache) Get(k string) any {
	ele, ok := c.items[k]
	if !ok {
		return nil
	}
	ele.Value.(*entry).Freq++
	c.adjust(ele)
	return ele.Value.(*entry).Val
}
