package hkv

import (
	"github.com/codingWhat/armory/storage/kv/hkv/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_Merge(t *testing.T) {
	ops := DefaultOptions
	//ops.DirPath = ""
	ops.DataFileSize = 1024
	ops.MergeRatio = 0.1

	db, err := Open(ops)
	if err != nil {
		return
	}

	assert.Equal(t, int64(0), db.reclaimableSize)
	defer func() {
		os.RemoveAll(ops.DirPath)
		os.RemoveAll(db.getMergePath())
	}()

	val := utils.RandomValue(20)
	for i := 0; i < 3; i++ {
		for i := 0; i < 10; i++ {
			err := db.Put(utils.GetTestKey(i), val)
			if err != nil {
				t.Error(err)
			}
		}
	}
	assert.Greater(t, db.reclaimableSize, int64(0))
	assert.Equal(t, int64(1380), db.reclaimableSize)
	err = db.Merge()
	if err != nil {
		t.Error(err)
	}
	db.Close()

	db, err = Open(ops)
	if err != nil {
		return
	}

	assert.Equal(t, int64(0), db.reclaimableSize)
	db.Close()
}
