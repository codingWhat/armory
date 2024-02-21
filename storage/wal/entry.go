package wal

type Entry struct {
	Type      int32
	KeySize   int32
	ValueSize int32
	Key       string
	Value     string
}
