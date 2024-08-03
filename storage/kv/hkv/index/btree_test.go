package index

import (
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {

	btree := NewBtree()
	ret := btree.Put([]byte("key1"), &data.LogRecordPos{FileID: 1, Offset: 2})
	assert.Equal(t, true, ret)

	ret = btree.Put(nil, &data.LogRecordPos{FileID: 1, Offset: 2})
	assert.Equal(t, false, ret)
}

func TestBtree_Get(t *testing.T) {

	btree := NewBtree()
	ret := btree.Put([]byte("key1"), &data.LogRecordPos{FileID: 1, Offset: 2})
	assert.Equal(t, true, ret)

	v := btree.Get([]byte("key1"))
	assert.Equal(t, int64(2), v.Offset)

	ret = btree.Put(nil, &data.LogRecordPos{FileID: 1, Offset: 2})
	assert.Equal(t, false, ret)

	v = btree.Get(nil)
	assert.Nil(t, v)
}
