package wal

import "os"

type WAL struct {
	f *os.File
}

func (w *WAL) ReadAt(offset int64) {
	w.f.Seek(offset, 0)

}

func New(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil

}

func (w *WAL) Put(e *Entry) error {
	return Encode(w.f, e)
}

func (w *WAL) Iterate() (*Entry, error) {
	return Decode(w.f)
}
