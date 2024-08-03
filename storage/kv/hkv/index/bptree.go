package index

import (
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"go.etcd.io/bbolt"
	"path"
)

const (
	BPIndexFileName = "bptree-index"
)

var indexBucketName = []byte("bp-index")

type BPlusTree struct {
	bptree *bbolt.DB
}

func newBPlushTree(dir string, syncWrites bool) *BPlusTree {

	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites

	fileName := path.Join(dir, BPIndexFileName)
	bptree, err := bbolt.Open(fileName, 0644, opts)
	if err != nil {
		panic("bptree cant open,err:" + err.Error())
	}

	err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte(indexBucketName))
		return err
	})
	if err != nil {
		panic("bptree failed to create bucket")
	}

	return &BPlusTree{
		bptree: bptree,
	}
}

func (b *BPlusTree) Put(k []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var old []byte
	if err := b.bptree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		old = bucket.Get(k)
		val := data.EncodeLogRecordPos(pos)
		return bucket.Put(k, val)
	}); err != nil {
		panic("bptree Put panic, err:" + err.Error())
	}
	if len(old) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(old)
}

func (b *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var val *data.LogRecordPos
	if err := b.bptree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		bytes := bucket.Get(key)
		if len(bytes) == 0 {
			return nil
		}
		val = data.DecodeLogRecordPos(bytes)
		return nil
	}); err != nil {
		panic("bptree Get panic, err:" + err.Error())
	}

	return val
}

func (b *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var old []byte
	if err := b.bptree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		old = bucket.Get(key)
		return bucket.Delete(key)
	}); err != nil {
		panic("bptree Delete panic, err:" + err.Error())
	}
	if len(old) == 0 {
		return nil, false
	}

	return data.DecodeLogRecordPos(old), true
}

func (b *BPlusTree) Size() int {
	var size int
	if err := b.bptree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("bptree Size panic, err:" + err.Error())
	}
	return size
}

func (b *BPlusTree) Iterator(options IteratorOptions) Iterator {
	return newBPTreeIterator(b.bptree, options.Reverse)
}

func newBPTreeIterator(bolt *bbolt.DB, reverse bool) *bpTreeIterator {
	tx, err := bolt.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bpTreeIterator{
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

type bpTreeIterator struct {
	cursor *bbolt.Cursor

	currKey []byte
	currVal []byte

	reverse bool
}

func (bp *bpTreeIterator) Rewind() {

	if bp.reverse {
		bp.currKey, bp.currVal = bp.cursor.Last()
	} else {
		bp.currKey, bp.currVal = bp.cursor.First()
	}
}

func (bp *bpTreeIterator) Valid() bool {
	return len(bp.currKey) != 0
}

func (bp *bpTreeIterator) Next() {
	if bp.reverse {
		bp.currKey, bp.currVal = bp.cursor.Prev()
	} else {
		bp.currKey, bp.currVal = bp.cursor.Next()
	}
}

func (bp *bpTreeIterator) Key() []byte {
	return bp.currKey
}

func (bp *bpTreeIterator) Value() *data.LogRecordPos {
	if len(bp.currVal) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(bp.currVal)
}

func (bp *bpTreeIterator) Seek(key []byte) {
	bp.currKey, bp.currVal = bp.cursor.Seek(key)
}

func (bp *bpTreeIterator) Close() {
	return
}
