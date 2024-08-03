package hkv

import (
	"os"
)

type Options struct {
	DirPath string //数据目录

	DataFileSize int64 //每个数据文件的大小

	SyncWrites bool //是否每次写入Sync

	IndexType IndexType

	MergeRatio float32 // merge比例

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 10 * 1024,
	SyncWrites:   true,
	IndexType:    Btree,
}

type IterOptions struct {
	Reverse bool
	Prefix  []byte
}

var DefaultIterOptions = IterOptions{
	Reverse: false,
	Prefix:  nil,
}

type IndexType int

const (
	Btree IndexType = iota + 1
	ART
	BPlusTree
)

type WriteBatchOptions struct {
	IsWritesSync bool
}
