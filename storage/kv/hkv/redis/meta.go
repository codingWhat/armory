package redis

import (
	"encoding/binary"
)

var (
	metadataHeaderSize int64 = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraHeaderSize    int64 = binary.MaxVarintLen64 * 2
)

type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32

	head uint64 //list
	tail uint64 // list
}

func encodeMetadata(mt *metadata) []byte {
	size := metadataHeaderSize
	if redisDataType(mt.dataType) == List {
		size += extraHeaderSize
	}

	buf := make([]byte, size)
	buf[0] = mt.dataType
	index := 1
	index += binary.PutVarint(buf[index:], mt.expire)
	index += binary.PutVarint(buf[index:], mt.version)
	index += binary.PutUvarint(buf[index:], uint64(mt.size))
	if redisDataType(mt.dataType) == List {
		index += binary.PutUvarint(buf[index:], mt.head)
		index += binary.PutUvarint(buf[index:], mt.tail)
	}

	return buf[:index]
}

func decodeMetaData(buf []byte) *metadata {
	mt := &metadata{}
	mt.dataType = buf[0]
	index := 1
	expire, size := binary.Varint(buf[index:])

	index += size
	mt.expire = expire

	version, size := binary.Varint(buf[index:])
	index += size
	mt.version = version

	s, size := binary.Uvarint(buf[index:])
	index += size
	mt.size = uint32(s)

	if redisDataType(mt.dataType) == List {
		head, size := binary.Uvarint(buf[index:])
		index += size
		mt.head = head

		tail, size := binary.Uvarint(buf[index:])
		index += size
		mt.tail = tail
	}

	return mt
}
