package localcache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key      string
	val      any
	expireAt time.Time

	mu sync.RWMutex
}

type Policy interface {
	isFull() bool
	add(*entry) (*list.Element, *list.Element) // 返回新增，淘汰的entry
	remove(*list.Element)
	update(*entry, *list.Element)
	renew(*list.Element)
	batchRenew([]*list.Element)
}

type LRU struct {
	ll    *list.List
	store store

	size int
}

func (l *LRU) isFull() bool {
	return l.ll.Len() >= l.size
}

func (l *LRU) add(e *entry) (*list.Element, *list.Element) {
	if l.ll.Len() < l.size {
		return l.ll.PushFront(e), nil
	}
	lastEle := l.ll.Back()
	if lastEle == nil {
		return nil, nil
	}
	l.ll.Remove(lastEle)

	return lastEle, l.ll.PushFront(e)
}

func getEntry(ele *list.Element) *entry {
	return ele.Value.(*entry)
}

func (l *LRU) update(e *entry, ele *list.Element) {

	old := getEntry(ele)
	old.val = e.val
	old.expireAt = e.expireAt
	l.ll.MoveToFront(ele)

}
func (l *LRU) remove(element *list.Element) {
	l.ll.Remove(element)
}

func (l *LRU) renew(element *list.Element) {
	l.ll.MoveToFront(element)
}

func (l *LRU) batchRenew(elements []*list.Element) {
	for _, elem := range elements {
		l.ll.MoveToFront(elem)
	}
}
