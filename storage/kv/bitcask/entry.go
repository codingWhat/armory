package bitcask

import (
	"hash/crc32"
	"time"
)

type Entry struct {
	Type      int8
	CRC       uint32
	TS        int32
	KeySize   int32
	ValueSize int32
	Key       []byte
	Value     []byte
}

var (
	EntryTypePut = 1
	EntryTypeDel = 2
)

func NewPutEntry(key []byte, value []byte) *Entry {
	e := NewEntry(key, value)
	e.Type = int8(EntryTypePut)
	return e
}

func NewDelEntry(key []byte) *Entry {
	e := NewEntry(key, []byte{})
	e.Type = int8(EntryTypeDel)
	return e
}

func NewEntry(key []byte, value []byte) *Entry {
	crc := crc32.ChecksumIEEE(value)
	return &Entry{
		CRC:       crc,
		TS:        int32(time.Now().Unix()),
		KeySize:   int32(len(key)),
		ValueSize: int32(len(value)),
		Key:       key,
		Value:     value}

}

func (e *Entry) Size() int32 {
	return int32(headerSize + len(e.Key) + len(e.Value))
}
