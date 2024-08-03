package hkv

import (
	"github.com/codingWhat/armory/storage/kv/hkv/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDB(db *DB) {
	if db.activeFile == nil {
		return
	}

	db.Close()
	os.RemoveAll(db.options.DirPath)
}

func TestDB_Put(t *testing.T) {

	db, err := Open(DefaultOptions)
	if err != nil {
		panic(err)
	}
	defer destroyDB(db)

	err = db.Put([]byte("aa"), []byte("v2"))
	assert.Nil(t, err)

	err = db.Put([]byte("ab"), []byte("v1"))
	assert.Nil(t, err)

	get, err := db.Get([]byte("aa"))
	assert.Nil(t, err)
	assert.Equal(t, get, []byte("v2"))

	get, err = db.Get([]byte("ab"))
	assert.Nil(t, err)
	assert.Equal(t, get, []byte("v1"))

}
func TestDB_Flock(t *testing.T) {
	db, err := Open(DefaultOptions)
	if err != nil {
		panic(err)
	}
	defer destroyDB(db)

	err = db.Put([]byte("aa"), []byte("vv"))
	assert.Nil(t, err)

	get, err := db.Get([]byte("aa"))
	assert.Nil(t, err)
	assert.Equal(t, get, []byte("vv"))

	db, err = Open(DefaultOptions)
	assert.Equal(t, err, ErrDBPathHasUsed)
}

func BenchmarkDB_Put(b *testing.B) {
	db, err := Open(DefaultOptions)
	if err != nil {
		panic(err)
	}
	defer destroyDB(db)

	b.ResetTimer()
	b.ReportAllocs()

	val := utils.RandomValue(20)
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), val)
		if err != nil {
			b.Log(err)
		}
	}
}

func BenchmarkDB_Get(b *testing.B) {
	db, err := Open(DefaultOptions)
	if err != nil {
		panic(err)
	}
	defer destroyDB(db)

	val := utils.RandomValue(40)
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), val)
		if err != nil {
			b.Log(err)
		}
	}
	//BenchmarkDB_Get-10    	     236	   5063220 ns/op	     455 B/op	       9 allocs/op
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), val)
		if err != nil {
			b.Log(err)
		}
	}
}
