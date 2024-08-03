package fio

import (
	"os"
)

type FileIO struct {
	f *os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &FileIO{f: file}, nil
}

//func NewFileIO(path string) (*FileIO, error) {
//	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
//	if err != nil {
//		return nil, err
//	}
//
//	return &FileIO{f: file}, nil
//}

func (f *FileIO) Write(bytes []byte) (int, error) {
	return f.f.Write(bytes)
}

func (f *FileIO) Read(data []byte, offset int64) (int, error) {
	return f.f.ReadAt(data, offset)
}

func (f *FileIO) Sync() error {
	return f.f.Sync()
}

func (f *FileIO) Close() error {
	return f.f.Close()
}

func (f *FileIO) Size() (int64, error) {
	stat, err := f.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
