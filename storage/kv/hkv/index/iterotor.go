package index

import (
	"bytes"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/google/btree"
	"sort"
)

type Iterator interface {
	Rewind()
	Valid() bool
	Next()
	Key() []byte
	Value() *data.LogRecordPos
	Seek(key []byte)
	Close()
}

type BtreeIterator struct {
	tree    *Btree
	currIdx int
	values  []*Item

	reverse bool
}

type IteratorOptions struct {
	Reverse bool

	Prefix []byte
}

func newBtreeIterator(tree *Btree, ops IteratorOptions) *BtreeIterator {

	values := make([]*Item, tree.Size())

	var idx int
	getValuesFunc := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	if ops.Reverse {
		tree.btree.Descend(getValuesFunc)
	} else {
		tree.btree.Ascend(getValuesFunc)
	}

	return &BtreeIterator{tree: tree, currIdx: 0, values: values, reverse: ops.Reverse}
}

func (b *BtreeIterator) Rewind() {
	b.currIdx = 0
}

func (b *BtreeIterator) Valid() bool {
	return b.currIdx < len(b.values)
}

func (b *BtreeIterator) Next() {
	b.currIdx++
}

func (b *BtreeIterator) Key() []byte {
	return b.values[b.currIdx].Key
}

func (b *BtreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIdx].PosInfo
}
func (b *BtreeIterator) Seek(key []byte) {
	if b.reverse {
		b.currIdx = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].Key, key) <= 0
		})
	} else {
		b.currIdx = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].Key, key) >= 0
		})
	}
}

func (b *BtreeIterator) Close() {
	b.values = nil
}
