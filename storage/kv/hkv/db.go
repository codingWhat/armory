package hkv

import (
	"errors"
	"fmt"
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/codingWhat/armory/storage/kv/hkv/fio"
	"github.com/codingWhat/armory/storage/kv/hkv/index"
	"github.com/codingWhat/armory/storage/kv/hkv/utils"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// NormalWriteSeq 普通写的事务id为0
const (
	NormalWriteSeq = 0
)

var SeqNoName = []byte("seqNo")

type DB struct {
	options Options
	flocker *flock.Flock
	index   index.Indexer
	lock    *sync.RWMutex

	closed    bool
	iter      Iterator
	seqNo     uint64
	isInitial bool
	isMerging bool

	fileIds    []int                     // 仅用作加载索引
	activeFile *data.DataFile            //活跃文件
	olderFiles map[uint32]*data.DataFile //归档文件

	reclaimableSize int64 //可以回收空间
}

func (db *DB) Iterator(options IterOptions) Iterator {
	db.iter = newIter(db, options)
	return db.iter
}

func (db *DB) ListKeys() [][]byte {
	keys := make([][]byte, db.index.Size())
	var idx int
	for db.iter.Rewind(); db.iter.Valid(); db.iter.Next() {
		keys[idx] = db.iter.Key()
	}

	return keys
}

func (db *DB) Fold(fn func(k, v []byte) bool) error {
	for db.iter.Rewind(); db.iter.Valid(); db.iter.Next() {
		val, err := db.iter.Value()
		if err != nil {
			return err
		}
		if !fn(db.iter.Key(), val) {
			break
		}
	}
	return nil
}

func (db *DB) loadIndexHintFiles() error {

	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//如果索引hint文件不存在直接返回
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var offset int64
	for {
		record, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)

		offset += size
	}
	return nil
}

func (db *DB) loadDataFiles() error {
	//遍历目录
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	//获取所有文件id
	var fileIds []int
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {

			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataFileDamaged
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//排序所有文件id, 设置活跃和归档文件
	sort.Ints(fileIds)
	db.fileIds = fileIds
	for i, fileId := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		f, err := data.OpenDataFile(db.options.DirPath, uint32(fileId), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = f
		} else {
			db.olderFiles[uint32(fileId)] = f
		}
	}

	return nil
}

//func (db *DB) loadIndexes() error {
//
//	//说明数据库为空
//	if len(db.fileIds) == 0 {
//		return nil
//	}
//
//	var txnOpsAggr = make(map[uint64][]*LogRecordKeyPos)
//	var seqNo uint64
//	//按顺序遍历文件
//	for _, fileId := range db.fileIds {
//
//		var dataFile *data.DataFile
//		if uint32(fileId) == db.activeFile.FileId {
//			dataFile = db.activeFile
//		} else {
//			dataFile = db.olderFiles[uint32(fileId)]
//		}
//
//		var offset int64 = 0
//		for {
//			record, size, err := dataFile.ReadLogRecord(offset)
//			if err != nil {
//				if err == io.EOF {
//					break
//				}
//				return err
//			}
//			seqNo, realKey := decodeTxnKey(record.Key)
//			pos := &data.LogRecordPos{Offset: offset, FileID: uint32(fileId), Size: uint32(size)}
//
//			if seqNo == NormalWriteSeq {
//				db.updateIndex(realKey, record.Type, pos)
//			} else {
//				if record.Type == data.LogTypeTxnFinished {
//					for _, keyPos := range txnOpsAggr[seqNo] {
//						db.updateIndex(keyPos.record.Key, record.Type, pos)
//					}
//				} else {
//					record.Key = realKey
//					txnOpsAggr[seqNo] = append(txnOpsAggr[seqNo], &LogRecordKeyPos{
//						pos:    pos,
//						record: record,
//					})
//				}
//			}
//
//			offset += size
//		}
//
//		//更新数据库最新的序列号
//		if seqNo > db.seqNo {
//			db.seqNo = seqNo
//		}
//
//		//更新活跃文件的writeOffset
//		if uint32(fileId) == db.activeFile.FileId {
//			db.activeFile.WriteOffset = offset
//		}
//
//	}
//
//	return nil
//}

// key []byte, tp data.LogType, fileId uint32, offset int64)
func (db *DB) updateIndex(realKey []byte, tp data.LogType, pos *data.LogRecordPos) {
	var oldPos *data.LogRecordPos
	if tp == data.LogTypeDelete {
		oldPos, _ = db.index.Delete(realKey)
		db.reclaimableSize += int64(pos.Size)
	} else {
		oldPos = db.index.Put(realKey, &data.LogRecordPos{FileID: pos.FileID, Offset: pos.Offset, Size: pos.Size})
	}

	if oldPos != nil {
		db.reclaimableSize += int64(oldPos.Size)
	}

}

func Open(options Options) (*DB, error) {
	//检查配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//判断目录是否存在，不存在创建
	var isInitial bool = false
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	lockPath := path.Join(options.DirPath, "db.lock")
	locker := flock.New(lockPath)
	hasLocked, err := locker.TryLock()
	if err != nil {
		fmt.Println("----->err", err)
		return nil, err
	}
	if !hasLocked {
		return nil, ErrDBPathHasUsed
	}
	//实例化db对象
	db := &DB{
		flocker:    locker,
		lock:       new(sync.RWMutex),
		options:    options,
		index:      index.NewIndexer(index.IndexType(options.IndexType), options.DirPath, options.SyncWrites),
		olderFiles: make(map[uint32]*data.DataFile),
		isInitial:  isInitial,
	}

	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if options.IndexType != BPlusTree {
		//加载hint文件，写入到index中，加速加载流程
		if err := db.loadIndexHintFiles(); err != nil {
			return nil, err
		}
		//加载索引-v1版本
		//if err := db.loadIndexes(); err != nil {
		//	return nil, err
		//}
		//加载索引-v2版本- 只加载未merge的文件到index中
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		if db.options.MMapAtStartup {
			if err := db.resetToFileIO(); err != nil {
				return nil, err
			}
		}
	}

	// 取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	}

	return db, nil
}
func (db *DB) resetToFileIO() error {
	if db.activeFile != nil {
		if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}

	for _, f := range db.olderFiles {
		if err := f.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}

	return nil
}
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, _ := strconv.ParseInt(string(record.Value), 0, 10)
	db.seqNo = uint64(seqNo)

	return os.Remove(fileName)
}

func (db *DB) loadIndexFromDataFiles() error {

	if len(db.fileIds) == 0 {
		return nil
	}

	unMergedFileIdFile := path.Join(db.options.DirPath, data.MergeFinishedFileName)
	var unMergeFileId int = 0
	if _, err := os.Stat(unMergedFileIdFile); err == nil {
		file, err := data.OpenMergeFinishedFile(db.options.DirPath)
		if err != nil {
			return err
		}

		record, _, err := file.ReadLogRecord(0)
		if err != nil {
			return err
		}
		fid, err := strconv.Atoi(string(record.Value))
		if err != nil {
			return err
		}

		unMergeFileId = fid
	}

	var txnOpsAggr = make(map[uint64][]*LogRecordKeyPos)
	var currSeq uint64
	for _, fileId := range db.fileIds {
		if fileId < unMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if db.activeFile.FileId == uint32(fileId) {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[uint32(fileId)]
		}
		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			seqNo, realKey := decodeTxnKey(record.Key)
			pos := &data.LogRecordPos{Offset: offset, FileID: uint32(fileId), Size: uint32(size)}
			if seqNo == NormalWriteSeq {
				db.updateIndex(realKey, record.Type, pos)
			} else {
				if record.Type == data.LogTypeTxnFinished {
					for _, keyPos := range txnOpsAggr[seqNo] {
						db.updateIndex(keyPos.record.Key, keyPos.record.Type, keyPos.pos)
					}
				} else {
					record.Key = realKey
					txnOpsAggr[seqNo] = append(txnOpsAggr[seqNo], &LogRecordKeyPos{
						pos:    pos,
						record: record,
					})
				}
			}
			//更新数据库最新的序列号
			if seqNo > currSeq {
				currSeq = seqNo
			}

			offset += size
		}

		//更新活跃文件的writeOffset
		if uint32(fileId) == db.activeFile.FileId {
			db.activeFile.WriteOffset = offset
		}
	}

	db.seqNo = currSeq
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("data dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("data file size must greater than 0")
	}

	if options.MergeRatio < 0 || options.MergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return ErrKeyIsNotExists
	}

	record := &data.LogRecord{Key: encodeTxnKey(NormalWriteSeq, key), Type: data.LogTypeDelete}
	//先写磁盘, 判断当前写入文件大小是否会溢出
	pos, err := db.appendLogRecord(record)
	db.reclaimableSize += int64(pos.Size)

	old, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if old != nil {
		db.reclaimableSize += int64(old.Size)
	}
	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyIsNotExists
	}
	return db.getValueByPos(pos)
}

func (db *DB) getValueByPos(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if pos.FileID == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.FileID]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotExists
	}

	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}
func (db *DB) Put(key, val []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	record := &data.LogRecord{Key: encodeTxnKey(NormalWriteSeq, key), Value: val, Type: data.LogTypeNormal}
	//先写磁盘, 判断当前写入文件大小是否会溢出
	pos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}
	//后写索引
	old := db.index.Put(key, pos)
	if old != nil {
		db.reclaimableSize += int64(old.Size)
	}
	return nil
}

func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	enRecord, size := data.EncodeLogRecord(record)
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		err := db.setActiveFile()
		if err != nil {
			return nil, err
		}
	}

	offset := db.activeFile.WriteOffset
	err := db.activeFile.Write(enRecord)
	if err != nil {
		return nil, err
	}
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	return &data.LogRecordPos{
		FileID: db.activeFile.FileId,
		Offset: offset,
		Size:   uint32(size),
	}, err
}

func (db *DB) setActiveFile() error {
	var initFileId uint32 = 0
	if db.activeFile != nil {
		initFileId = db.activeFile.FileId + 1
	}

	f, err := data.OpenDataFile(db.options.DirPath, initFileId, fio.StandardFIO)
	if err != nil {
		return err
	}

	db.activeFile = f
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	return db.activeFile.Sync()
}

func (db *DB) getNextSeq() uint64 {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.seqNo++
	return db.seqNo
}
func (db *DB) Close() error {

	defer func() {
		err := db.flocker.Unlock()
		if err != nil {
			fmt.Println("db flock unlock failed. err:", err.Error())
			return
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	if db.closed {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	db.closed = true

	if db.options.IndexType == BPlusTree {
		seqFile, err := data.OpenSeqNoFile(db.options.DirPath)
		if err != nil {
			return err
		}

		lr := &data.LogRecord{
			Key:   encodeTxnKey(NormalWriteSeq, SeqNoName),
			Value: []byte(strconv.FormatUint(db.seqNo, 10)),
		}
		enRecord, _ := data.EncodeLogRecord(lr)
		if err := seqFile.Write(enRecord); err != nil {
			return err
		}
		if err := seqFile.Sync(); err != nil {
			return err
		}
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, f := range db.olderFiles {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
func (db *DB) Stat() *Stat {

	db.lock.RLock()
	rSize := db.reclaimableSize
	keyNum := db.index.Size()
	db.lock.RUnlock()

	dataSize, _ := utils.DirSize(db.options.DirPath)
	return &Stat{
		KeyNum:          keyNum,
		FilesNum:        len(db.olderFiles) + 1,
		ReclaimableSize: rSize,
		DataFileSize:    dataSize,
	}
}

type Stat struct {
	KeyNum          int
	FilesNum        int
	ReclaimableSize int64
	DataFileSize    int64
}
