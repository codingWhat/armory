package main

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"io/ioutil"
	"os"
	"time"
)

func main() {

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := badger.Open(badger.DefaultOptions(tmpDir))
	if err != nil {
		return
	}

	err = db.Update(func(txn *badger.Txn) error {
		// 后续可以前缀扫描，对不同的partition 分治处理。再通过targetid哈希到对应的merge 线程中
		key := fmt.Sprintf("partition:%dtargetid:%d", 1, 1000010235)
		err := txn.Set([]byte(key), []byte("BatchHashMsg"))
		return err
	})
	if err != nil {
		panic(err)
	}

	go func(partitionID int) {
		db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			prefix := []byte(fmt.Sprintf("partition:%d", partitionID))
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				_ = item.Key()
				//var dbhm BatchHashMsg
				dbhm := make(map[string]interface{})
				_ = item.Value(func(val []byte) error {
					return json.Unmarshal(val, &dbhm)
				})

				//retry ...
			}
			return nil
		})
	}(1)

}
