package index

import (
	"fmt"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()

	p := &data.LogRecordPos{FileID: 1, Offset: 22}
	ok := art.Put([]byte("aa"), p)
	assert.Equal(t, true, ok)

	pos := art.Get([]byte("aa"))
	assert.NotNil(t, pos)
	assert.Equal(t, p.FileID, pos.FileID)
	assert.Equal(t, p.Offset, pos.Offset)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	p := &data.LogRecordPos{FileID: 1, Offset: 22}
	ok := art.Put([]byte("aa"), p)
	assert.Equal(t, true, ok)

	p1 := &data.LogRecordPos{FileID: 1, Offset: 23}
	ok = art.Put([]byte("ab"), p1)
	assert.Equal(t, true, ok)

	p2 := &data.LogRecordPos{FileID: 1, Offset: 24}
	ok = art.Put([]byte("ac"), p2)
	assert.Equal(t, true, ok)

	p3 := &data.LogRecordPos{FileID: 1, Offset: 25}
	ok = art.Put([]byte("ad"), p3)
	assert.Equal(t, true, ok)

	p4 := &data.LogRecordPos{FileID: 1, Offset: 26}
	ok = art.Put([]byte("ae"), p4)
	assert.Equal(t, true, ok)

	it := art.Iterator(IteratorOptions{
		//Reverse: true,
		Reverse: false,
		Prefix:  nil,
	})

	for it.Rewind(); it.Valid(); it.Next() {
		fmt.Println(string(it.Key()), "---->", it.Value())
	}
}
