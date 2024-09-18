package lfu

import (
	"container/list"
	"fmt"
	cache2 "github.com/codingWhat/armory/cache"
)

func New(size int) cache2.Cache {
	return &cache{
		capacity: size,
		keyElems: make(map[string]*list.Element), //key => val, 注意这里的ele对应指定freq里的element, 切记，当更新元素频次时也更新这里
		freqKeys: make(map[int]*list.List),       //freqKeys => 相同freq的keys
		minFreq:  1,
	}
}

type entry struct {
	Key  string
	Val  any
	Freq int
}

type cache struct {
	capacity int
	keyElems map[string]*list.Element //list+map 支持O(1)删除/查找链表元素
	freqKeys map[int]*list.List

	minFreq int
}

func (c *cache) Set(k string, v any) {
	if ele, ok := c.keyElems[k]; ok {
		ent := c.getEntry(ele)
		fmt.Println("--->before", ent)
		ent.Val = v
		//增加计数
		c.keyElems[k] = c.increaseFreq(ele)
	} else {
		//容量检测
		if len(c.keyElems) >= c.capacity {
			c.removeOldestKey()
		}

		ent := &entry{Key: k, Val: v, Freq: 1}
		if _, ok := c.freqKeys[ent.Freq]; !ok {
			c.freqKeys[ent.Freq] = list.New()
		}

		ele = c.freqKeys[ent.Freq].PushBack(ent)
		c.keyElems[k] = ele //写入hash

		c.minFreq = ent.Freq
	}
}

func (c *cache) removeOldestKey() {
	ele := c.freqKeys[c.minFreq].Front()
	if ele == nil {
		return
	}
	c.freqKeys[c.minFreq].Remove(ele)
	ent := c.getEntry(ele)
	delete(c.keyElems, ent.Key)
}

func (c *cache) increaseFreq(ele *list.Element) *list.Element {
	//从旧的freq链表中移除。添加到心的freq链表中
	ent := ele.Value.(*entry)
	oldFreq := ent.Freq
	c.freqKeys[oldFreq].Remove(ele)

	if c.freqKeys[oldFreq].Len() == 0 {
		delete(c.freqKeys, oldFreq)
		if oldFreq == c.minFreq {
			c.minFreq++
		}
	}
	ent.Freq++
	newFreq := ent.Freq
	if _, ok := c.freqKeys[newFreq]; !ok {
		c.freqKeys[newFreq] = list.New()
	}

	return c.freqKeys[newFreq].PushBack(ent)
}

func (c *cache) getEntry(ele *list.Element) *entry {
	return ele.Value.(*entry)
}

func (c *cache) Get(k string) any {
	ele, ok := c.keyElems[k]
	if !ok {
		return nil
	}

	defer func() {
		c.keyElems[k] = c.increaseFreq(ele)
	}()
	return c.getEntry(ele).Val
}
