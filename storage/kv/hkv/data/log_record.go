package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordPos struct {
	FileID uint32
	Offset int64
	Size   uint32
}

func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	n := binary.PutUvarint(buf[0:], uint64(pos.FileID))
	n += binary.PutVarint(buf[n:], pos.Offset)
	n += binary.PutUvarint(buf[n:], uint64(pos.Size))
	return buf[:n]
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	fid, n := binary.Uvarint(buf)
	index := n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Uvarint(buf[index:])

	l := &LogRecordPos{}
	l.FileID = uint32(fid)
	l.Offset = offset
	l.Size = uint32(size)
	return l
}

type LogType byte

// keySize 有seq(64) + key(32) 组成
// var MaxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen64 + binary.MaxVarintLen32
var MaxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

const (
	// LogTypeNormal 磁盘日志类型-正常(添加)
	LogTypeNormal LogType = iota + 1
	// LogTypeDelete 磁盘日志类型-删除
	LogTypeDelete
	LogTypeTxnFinished
)

// LogRecord 磁盘日志格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogType
}

type LogRecordHeader struct {
	CRC       uint32
	LogType   LogType
	KeySize   uint32
	ValueSize uint32
}

func EncodeLogRecord(log *LogRecord) ([]byte, int64) {
	header := make([]byte, MaxLogRecordHeaderSize)
	header[4] = byte(log.Type)
	index := 5

	kSize := len(log.Key)
	vSize := len(log.Value)
	index += binary.PutUvarint(header[index:], uint64(kSize))
	index += binary.PutUvarint(header[index:], uint64(vSize))

	size := index + kSize + vSize
	bytes := make([]byte, size)
	copy(bytes[:index], header[:index])
	copy(bytes[index:], log.Key)
	copy(bytes[index+kSize:], log.Value)

	crc := crc32.ChecksumIEEE(bytes[4:])
	binary.LittleEndian.PutUint32(bytes[:4], crc)

	return bytes, int64(size)
}

func DecodeLogRecordHeader(data []byte) (*LogRecordHeader, int64) {
	if len(data) < 4 {
		return nil, 0
	}
	header := &LogRecordHeader{}
	header.CRC = binary.LittleEndian.Uint32(data[:4])
	header.LogType = LogType(data[4])
	index := 5
	kSize, n := binary.Uvarint(data[index:])
	index += n
	vSize, n := binary.Uvarint(data[index:])
	index += n
	header.KeySize = uint32(kSize)
	header.ValueSize = uint32(vSize)

	return header, int64(index)
}

func getCRC(log *LogRecord, header []byte) uint32 {

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, log.Key)
	crc = crc32.Update(crc, crc32.IEEETable, log.Value)
	return crc
}
