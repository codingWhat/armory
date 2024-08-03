package hkv

import (
	"encoding/binary"
	"fmt"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"sync"
	"sync/atomic"
)

const (
	TxnFinished = "txn-finished"
)

type WriteBatch struct {
	ops           WriteBatchOptions
	mu            *sync.RWMutex
	db            *DB
	seqNo         uint64
	pendingWrites map[string]*data.LogRecord
}

func NewWriteBatch(db *DB, options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.isInitial {
		panic("bplusTree doesnt support batch write")
	}
	wb := &WriteBatch{
		ops:           options,
		mu:            new(sync.RWMutex),
		db:            db,
		seqNo:         db.getNextSeq(),
		pendingWrites: make(map[string]*data.LogRecord),
	}
	return wb
}

func (wb *WriteBatch) Put(k, v []byte) error {

	wb.mu.Lock()

	defer wb.mu.Unlock()

	lr := &data.LogRecord{Type: data.LogTypeNormal, Value: v, Key: encodeTxnKey(wb.seqNo, k)}
	wb.pendingWrites[string(k)] = lr
	return nil
}

func (wb *WriteBatch) Delete(k []byte) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if pos := wb.db.index.Get(k); pos == nil {
		if _, ok := wb.pendingWrites[string(k)]; ok {
			delete(wb.pendingWrites, string(k))
		}
		return
	}

	lr := &data.LogRecord{Type: data.LogTypeDelete, Value: encodeTxnKey(wb.seqNo, k), Key: k}
	wb.pendingWrites[string(k)] = lr
}

func (wb *WriteBatch) Commit() error {
	if len(wb.pendingWrites) == 0 || wb.pendingWrites == nil {
		return nil
	}

	//先批量写入文件
	successWrites := make(map[string]*data.LogRecordPos)
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	seqNo := atomic.LoadUint64(&wb.db.seqNo)
	for k, record := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Type:  record.Type,
			Key:   encodeTxnKey(seqNo, record.Key),
			Value: record.Value,
		})
		if err != nil {
			return err
		}
		successWrites[k] = pos
	}
	finished := &data.LogRecord{Type: data.LogTypeTxnFinished, Key: encodeTxnKey(wb.seqNo, []byte(TxnFinished))}
	_, err := wb.db.appendLogRecord(finished)
	if err != nil {
		return err
	}

	if wb.ops.IsWritesSync && wb.db.activeFile != nil {
		if err = wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	fmt.Println("---->successWrites", successWrites)
	//再更新索引
	for k, record := range wb.pendingWrites {
		pos := successWrites[k]

		if record.Type == data.LogTypeNormal {
			if pos = wb.db.index.Put(record.Key, pos); pos != nil {
				wb.db.reclaimableSize += int64(pos.Size)
				return ErrTxnIndexUpdateFailed
			}
		} else if record.Type == data.LogTypeDelete {
			if pos, ok := wb.db.index.Delete(record.Key); ok {
				wb.db.reclaimableSize += int64(pos.Size)
				return ErrTxnIndexUpdateFailed
			}
		}
	}

	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

func encodeTxnKey(seqNo uint64, key []byte) []byte {
	//这里注意下, 磁盘格式keySize是变长的5字节
	seqByte := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seqByte, seqNo)
	seqNoKey := make([]byte, n+len(key))
	copy(seqNoKey[:n], seqByte[:n])
	copy(seqNoKey[n:], key)
	return seqNoKey
}

func decodeTxnKey(data []byte) (seqNo uint64, key []byte) {
	seqNo, idx := binary.Uvarint(data)
	key = data[idx:]
	return seqNo, key
}

type LogRecordKeyPos struct {
	record *data.LogRecord
	pos    *data.LogRecordPos
}
