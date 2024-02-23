package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
)

var headerSize = 17 //1+4 + 4 + 4 + 4

func Decode(reader io.Reader) (*Entry, error) {
	e := &Entry{}
	header := make([]byte, headerSize)
	_, err := reader.Read(header)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(header)
	err = binary.Read(buffer, binary.LittleEndian, &e.Type)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &e.CRC)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &e.TS)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &e.KeySize)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buffer, binary.LittleEndian, &e.ValueSize)
	if err != nil {
		return nil, err
	}

	kv := make([]byte, e.KeySize+e.ValueSize)
	_, err = reader.Read(kv)
	if err != nil {
		return nil, err
	}

	e.Key = kv[:e.KeySize]
	e.Value = kv[e.KeySize:]
	return e, err
}

func encode(e *Entry) *bytes.Buffer {
	size := headerSize + len(e.Key) + len(e.Value)
	buffer := bytes.NewBuffer(make([]byte, 0, size))
	crc := crc32.ChecksumIEEE(e.Value)
	err := binary.Write(buffer, binary.LittleEndian, e.Type)
	if err != nil {
		panic(err)
	}
	err = binary.Write(buffer, binary.LittleEndian, crc)
	if err != nil {
		panic(err)
	}
	err = binary.Write(buffer, binary.LittleEndian, e.TS)
	if err != nil {
		panic(err)

	}
	err = binary.Write(buffer, binary.LittleEndian, int32(len(e.Key)))
	if err != nil {
		panic(err)
	}
	err = binary.Write(buffer, binary.LittleEndian, int32(len(e.Value)))
	if err != nil {
		panic(err)
	}
	buffer.Write(e.Key)
	buffer.Write(e.Value)

	return buffer
}
