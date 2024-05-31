package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

// AvailableDiskSpace 获取当前磁盘的可用空间
func AvailableDiskSpace() (int64, error) {
	currentPath, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t
	err = syscall.Statfs(currentPath, &stat)
	if err != nil {
		return 0, err
	}

	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// DirSize 获取指定目录的磁盘使用大小
func DirSize(path string) (int64, error) {

	var size int64
	err := filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		size += info.Size()
		return nil
	})

	return size, err
}
