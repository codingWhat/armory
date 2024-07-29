package index

import (
	"bytes"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	lock *sync.RWMutex
	tree goart.Tree
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		lock: new(sync.RWMutex),
		tree: goart.New(),
	}
}
func (a *AdaptiveRadixTree) Put(k []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	a.lock.Lock()
	old, _ := a.tree.Insert(k, pos)
	a.lock.Unlock()
	if old == nil {
		return nil
	}
	return old.(*data.LogRecordPos)
}

func (a *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	a.lock.RLock()
	pos, ok := a.tree.Search(key)
	a.lock.RUnlock()
	if ok {
		return pos.(*data.LogRecordPos)
	}
	return nil
}

func (a *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	a.lock.Lock()
	old, ok := a.tree.Delete(key)
	a.lock.Unlock()
	if old == nil {
		return nil, false
	}
	return old.(*data.LogRecordPos), ok
}

func (a *AdaptiveRadixTree) Size() int {
	a.lock.RLock()
	size := a.tree.Size()
	a.lock.RUnlock()
	return size
}

func (a *AdaptiveRadixTree) Iterator(options IteratorOptions) Iterator {
	return newARTIterator(a.tree, options.Reverse)
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {

	values := make([]*Item, tree.Size())

	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	tree.ForEach(func(node goart.Node) (cont bool) {
		values[idx] = &Item{
			Key:     node.Key(),
			PosInfo: node.Value().(*data.LogRecordPos),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	})

	return &artIterator{
		tree:    tree,
		currIdx: 0,
		reverse: reverse,
		values:  values,
	}
}

type artIterator struct {
	tree    goart.Tree
	currIdx int
	values  []*Item

	reverse bool
}

func (a *artIterator) Rewind() {
	a.currIdx = 0
}

func (a *artIterator) Valid() bool {
	return a.currIdx < len(a.values)
}

func (a *artIterator) Next() {
	a.currIdx++
}

func (a *artIterator) Key() []byte {
	return a.values[a.currIdx].Key
}

func (a *artIterator) Value() *data.LogRecordPos {
	return a.values[a.currIdx].PosInfo
}

func (a *artIterator) Seek(key []byte) {
	if a.reverse {
		a.currIdx = sort.Search(len(a.values), func(i int) bool {
			return bytes.Compare(a.values[i].Key, key) <= 0
		})
	} else {
		a.currIdx = sort.Search(len(a.values), func(i int) bool {
			return bytes.Compare(a.values[i].Key, key) >= 0
		})
	}
}

func (a *artIterator) Close() {
	a.values = nil
}
