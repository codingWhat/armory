package hkv

import (
	"bytes"
	"github.com/codingWhat/armory/storage/kv/hkv/index"
)

type Iterator interface {
	Rewind()
	Valid() bool
	Next()
	Key() []byte
	Value() ([]byte, error)
	Seek(key []byte)
	Close()
}

type Iter struct {
	indexer index.Iterator
	db      *DB
	ops     IterOptions
}

func newIter(db *DB, ops IterOptions) *Iter {
	return &Iter{
		ops:     ops,
		db:      db,
		indexer: db.index.Iterator(index.IteratorOptions(ops)),
	}
}

func (iterator *Iter) Rewind() {
	iterator.indexer.Rewind()
	iterator.skipToNext()
}

func (iterator *Iter) Valid() bool {
	return iterator.indexer.Valid()
}

func (iterator *Iter) Seek(key []byte) {
	iterator.indexer.Seek(key)
	iterator.skipToNext()
}

func (iterator *Iter) Next() {
	iterator.indexer.Next()
	iterator.skipToNext()
}

func (iterator *Iter) Key() []byte {
	return iterator.indexer.Key()
}

func (iterator *Iter) Value() ([]byte, error) {
	pos := iterator.indexer.Value()
	return iterator.db.getValueByPos(pos)
}
func (iterator *Iter) Close() {
	iterator.indexer.Close()
}

// 如果有迭代器有前缀过滤
func (iterator *Iter) skipToNext() {
	if iterator.ops.Prefix == nil {
		return
	}

	prefixLen := len(iterator.ops.Prefix)
	for ; iterator.Valid(); iterator.Next() {
		if bytes.Compare(iterator.ops.Prefix, iterator.Key()[:prefixLen]) == 0 {
			break
		}
	}
}
