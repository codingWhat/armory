package index

import (
	"github.com/codingWhat/armory/storage/kv/hkv/data"
)

// Indexer 内存索引的kv结构
type Indexer interface {
	Put([]byte, *data.LogRecordPos) *data.LogRecordPos
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos, bool)

	Size() int

	Iterator(IteratorOptions) Iterator
}

type IndexType int

const (
	MBtree IndexType = iota + 1
	ART
	BPlustree
)

func NewIndexer(indexType IndexType, path string, syncWrites bool) Indexer {

	switch indexType {
	case MBtree:
		return NewBtree()
	case ART:
		return NewART()
	case BPlustree:
		return newBPlushTree(path, syncWrites)
	default:
		panic("unsupported index type")
	}
}
