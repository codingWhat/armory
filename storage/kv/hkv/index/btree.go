package index

import (
	"bytes"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/google/btree"
	"sync"
)

// Item btree的元素对象
type Item struct {
	Key     []byte //btree中存储的是key和磁盘位置信息
	PosInfo *data.LogRecordPos
}

func (i *Item) Less(b btree.Item) bool {
	return bytes.Compare(i.Key, b.(*Item).Key) == 1
}

func NewBtree() *Btree {
	return &Btree{
		btree: btree.New(32),
		lock:  &sync.RWMutex{},
	}
}

func (b *Btree) Size() int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.btree.Len()
}

func (b *Btree) Iterator(ops IteratorOptions) Iterator {
	return newBtreeIterator(b, ops)
}

type Btree struct {
	btree *btree.BTree
	lock  *sync.RWMutex
}

func (b *Btree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	if len(key) == 0 {
		return nil
	}

	b.lock.Lock()
	defer b.lock.Unlock()
	item := &Item{
		Key:     key,
		PosInfo: pos,
	}
	old := b.btree.ReplaceOrInsert(item)
	if old == nil {
		return nil
	}

	return old.(*Item).PosInfo
}

func (b *Btree) Get(key []byte) *data.LogRecordPos {
	if len(key) == 0 {
		return nil
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	item := &Item{Key: key}
	val := b.btree.Get(item)
	if val == nil {
		return nil
	}

	return val.(*Item).PosInfo
}

func (b *Btree) Delete(key []byte) (*data.LogRecordPos, bool) {
	if len(key) == 0 {
		return nil, false
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	item := &Item{Key: key}
	old := b.btree.Get(item)
	it := b.btree.Delete(item)
	if it == nil {
		return nil, false
	}
	if old == nil {
		return nil, true
	}
	return old.(*Item).PosInfo, true
}
