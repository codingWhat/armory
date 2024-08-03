package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeLogRecordHeader(t *testing.T) {
	ret, _ := EncodeLogRecord(&LogRecord{Key: []byte("aa"), Value: []byte("bb"), Type: LogTypeNormal})

	header, _ := DecodeLogRecordHeader(ret)
	assert.Equal(t, header.KeySize, uint32(2))
}

func TestEncodeLogRecordPos(t *testing.T) {
	l := &LogRecordPos{FileID: 1, Offset: 222, Size: uint32(323)}
	pos := EncodeLogRecordPos(l)
	recordPos := DecodeLogRecordPos(pos)

	assert.Equal(t, l.FileID, recordPos.FileID)
	assert.Equal(t, l.Offset, recordPos.Offset)
	assert.Equal(t, l.Size, recordPos.Size)
}
