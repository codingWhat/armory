package wal

import (
	"encoding/binary"
	"io"
)

func Decode(reader io.Reader) (*Entry, error) {
	return nil, nil
}

func Encode(w io.Writer, e *Entry) error {

	err := binary.Write(w, binary.LittleEndian, e.Type)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, e.KeySize)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, e.ValueSize)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(e.Key))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(e.Value))
	return err
}
