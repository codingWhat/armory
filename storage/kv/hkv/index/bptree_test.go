package index

import (
	"fmt"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_newBPTreeIterator(t *testing.T) {
	dir := os.TempDir()
	defer func() {
		os.RemoveAll(dir)
	}()
	bp := newBPlushTree(dir, false)

	ret := bp.Put([]byte("k1"), &data.LogRecordPos{
		FileID: 1,
		Offset: 22,
	})
	assert.Equal(t, true, ret)

	ret = bp.Put([]byte("k2"), &data.LogRecordPos{
		FileID: 2,
		Offset: 23,
	})
	assert.Equal(t, true, ret)

	ret = bp.Put([]byte("k3"), &data.LogRecordPos{
		FileID: 3,
		Offset: 24,
	})
	assert.Equal(t, true, ret)

	ret = bp.Put([]byte("k4"), &data.LogRecordPos{
		FileID: 4,
		Offset: 25,
	})
	assert.Equal(t, true, ret)

	it := bp.Iterator(IteratorOptions{Reverse: false})

	for it.Rewind(); it.Valid(); it.Next() {
		fmt.Println(string(it.Key()), "<---->", it.Value().FileID, it.Value().Offset)
	}
}
