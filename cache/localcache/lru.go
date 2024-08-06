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

type policy interface {
	isFull() bool
	add(*entry) (*list.Element, *list.Element) // 返回新增，淘汰的entry
	remove(*list.Element)
	update(*entry, *list.Element)
	renew(*list.Element)
	batchRenew([]*list.Element)
}

func newPolicy(size int) policy {
	return newLRU(size)
}

func newLRU(size int) *lru {
	return &lru{
		ll:   list.New(),
		size: size,
	}
}

type lru struct {
	ll *list.List

	size int
}

func (l *lru) isFull() bool {
	return l.ll.Len() >= l.size
}

func (l *lru) add(e *entry) (*list.Element, *list.Element) {
	if l.ll.Len() < l.size {
		return l.ll.PushFront(e), nil
	}
	victim := l.ll.Back()
	if victim == nil {
		return nil, nil
	}
	l.ll.Remove(victim)
	return l.ll.PushFront(e), victim
}

func getEntry(ele *list.Element) *entry {
	return ele.Value.(*entry)
}

func (l *lru) update(e *entry, ele *list.Element) {

	old := getEntry(ele)
	old.val = e.val
	old.expireAt = e.expireAt
	l.ll.MoveToFront(ele)

}
func (l *lru) remove(element *list.Element) {
	l.ll.Remove(element)
}

func (l *lru) renew(element *list.Element) {
	l.ll.MoveToFront(element)
}

func (l *lru) batchRenew(elements []*list.Element) {
	for _, elem := range elements {
		l.ll.MoveToFront(elem)
	}
}
