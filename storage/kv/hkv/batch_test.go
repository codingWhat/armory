package hkv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_encodeTxnKey(t *testing.T) {
	encKey := encodeTxnKey(1, []byte("key1"))
	seqNo, key := decodeTxnKey(encKey)
	assert.Equal(t, uint64(1), seqNo)
	assert.Equal(t, []byte("key1"), key)
}

func Test_WriteBatch(t *testing.T) {
	//db, err := Open(DefaultOptions)
	//tmp, _ := os.MkdirTemp("", "bc-WriteBatch")
	//db.options.DirPath = tmp
	//fmt.Println("--->", db.options.DirPath)
	//
	////defer destroyDB(db)
	//
	//wb := NewWriteBatch(db, WriteBatchOptions{
	//	IsWritesSync: true,
	//})
	//assert.Nil(t, err)
	//err = wb.Put([]byte("k1"), []byte("v1"))
	//assert.Nil(t, err)
	//err = wb.Put([]byte("k2"), []byte("v2"))
	//assert.Nil(t, err)
	//err = wb.Put([]byte("k3"), []byte("v3"))
	//
	//assert.Nil(t, err)
	//
	//err = wb.Commit()
	//assert.Nil(t, err)
	//get, err := db.Get([]byte("k1"))
	//assert.Nil(t, err)
	//assert.Equal(t, []byte("v1"), get)
	//fmt.Println("---->done")
	ops := DefaultOptions
	ops.DirPath = "/var/folders/j0/zg6yts991rdbm78_ggnzbpqw0000gn/T/bc-WriteBatch1841187235/"
	db, err := Open(ops)
	assert.Nil(t, err)

	get, err := db.Get([]byte("k1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("v1"), get)
}
