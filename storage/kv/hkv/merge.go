package hkv

import (
	"github.com/codingWhat/armory/storage/kv/hkv/data"
	"github.com/codingWhat/armory/storage/kv/hkv/utils"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
)

const MergeFinished = "mergeFinished"

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//判断合并目录文件是否存在，不存在直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	var (
		hasMerged   bool
		mergedFiles []string
	)
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			hasMerged = true
		}

		mergedFiles = append(mergedFiles, entry.Name())
	}

	if !hasMerged {
		return nil
	}

	//获取未merge文件id
	unMergedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	unMergedFileId, err := unMergedFile.ReadUnMergedFileId()
	if err != nil {
		return err
	}

	//清理文件
	for i := 0; i < unMergedFileId; i++ {
		fileName := data.GetDataFileName(db.options.DirPath, uint32(i))
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	//迁移文件
	for _, fileName := range mergedFiles {
		old := path.Join(mergePath, fileName)
		newf := path.Join(db.options.DirPath, fileName)
		if err := os.Rename(old, newf); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.lock.Lock()
	// 如果 merge 正在进行当中，则直接返回
	if db.isMerging {
		db.lock.Unlock()
		return ErrMergeIsProgress
	}

	dbDataSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.lock.Unlock()
		return err
	}

	//如果可merge的数据比例小于用户指定的阈值，则不执行merge
	if float32(db.reclaimableSize)/float32(dbDataSize) <= db.options.MergeRatio {
		db.lock.Unlock()
		return ErrMergeIsNotReachRatio
	}

	//如果数据文件使用空间-可回收空间 大于 当前磁盘可用空间，拒绝merge
	availableSize, err := utils.AvailableDiskSpace()
	if err != nil {
		db.lock.Unlock()
		return err
	}

	if dbDataSize-db.reclaimableSize >= availableSize {
		db.lock.Unlock()
		return ErrDiskSpaceIsNotEnoughForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	if err = db.activeFile.Sync(); err != nil {
		db.lock.Unlock()
		return err
	}

	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err = db.setActiveFile(); err != nil {
		db.lock.Unlock()
		return err
	}
	// 记录最近没有参与 merge 的文件 id
	unMergedFileId := db.activeFile.FileId
	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	db.lock.Unlock()

	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	//获取merge路径。判断是否存在，存在删除
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err = os.RemoveAll(db.getMergePath()); err != nil {
			return err
		}
	}

	ops := db.options
	ops.SyncWrites = false
	ops.DirPath = mergePath
	mergeDB, err := Open(ops)
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()
	hintFile, err := data.OpenHintFile(ops.DirPath)
	if err != nil {
		return err
	}
	for _, mergeFile := range mergeFiles {
		var offset int64
		for {
			record, size, err := mergeFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			_, realKey := decodeTxnKey(record.Key)
			pos := db.index.Get(realKey)
			if pos != nil && pos.FileID == mergeFile.FileId && pos.Offset == offset {
				record.Key = encodeTxnKey(NormalWriteSeq, realKey)
				pos, err := mergeDB.appendLogRecord(record)
				if err != nil {
					return err
				}

				err = hintFile.WriteHintRecord(realKey, pos)
				if err != nil {
					return err
				}

			}
			offset += size
		}
	}
	//持久化 active file | hint file
	err = hintFile.Sync()
	if err != nil {
		return err
	}
	err = mergeDB.Sync()
	if err != nil {
		return err
	}

	//写入merge完成标识
	mergeFinishedFile, err := data.OpenMergeFinishedFile(ops.DirPath)
	if err != nil {
		return err
	}
	err = mergeFinishedFile.WriteMergeFinishedRecord([]byte(MergeFinished), []byte(strconv.Itoa(int(unMergedFileId))))
	if err != nil {
		return err
	}

	return mergeFinishedFile.Sync()
}

func (db *DB) getMergePath() string {
	p := path.Clean(db.options.DirPath)
	dir := path.Dir(p)

	return path.Join(dir, path.Base(p)+"-merge")
}
