package mdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type MDB struct {
	f    *os.File
	data map[string]string
	sync.RWMutex
}

func (m *MDB) Del(k string) error {

	return nil
}

func NewMDB(path string) (*MDB, error) {

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := f.Stat()
	db := &MDB{f: f, data: make(map[string]string)}
	if fileInfo.Size() > 0 {
		err = db.load()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (m *MDB) load() error {
	m.f.Seek(0, 0)

	for {

		e, err := decode(m.f)
		fmt.Println("---->22", e, err)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if e.isPut() {
			m.data[e.Key] = e.Value
		} else {
			delete(m.data, e.Key)
		}
	}

	return nil
}

func (m *MDB) sync() {
	for k, v := range m.data {
		err := encode(m.f, &entry{
			LogType:   1,
			KeySize:   int32(len(k)),
			ValueSize: int32(len(v)),
			Value:     v,
			Key:       k,
		})
		if err != nil {
			panic(err)
		}
	}
	_ = m.f.Sync()
}

func (m *MDB) Get(k string) (string, bool) {
	m.RLock()
	defer m.RUnlock()
	ret, ok := m.data[k]
	return ret, ok
}

func (m *MDB) Sync() {
	m.Lock()
	_ = m.f.Sync()
	m.Unlock()
}

func (m *MDB) Put(k, v string) error {
	m.Lock()
	defer m.Unlock()
	if len(m.data) > 3 {
		m.sync()
	}
	m.data[k] = v
	return nil
}

type entry struct {
	LogType   int32
	KeySize   int32
	ValueSize int32
	Key       string
	Value     string
}

func (e *entry) isPut() bool {
	return e.LogType == 1
}

func encode(writer io.Writer, e *entry) error {
	err := binary.Write(writer, binary.LittleEndian, e.LogType)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.LittleEndian, e.KeySize)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.LittleEndian, e.ValueSize)
	if err != nil {
		return err
	}
	_, err = writer.Write([]byte(e.Key))
	if err != nil {
		return err
	}
	_, err = writer.Write([]byte(e.Value))
	return err
}

func decode(reader io.Reader) (*entry, error) {
	e := &entry{}
	err := binary.Read(reader, binary.LittleEndian, &e.LogType)
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, binary.LittleEndian, &e.KeySize)
	if err != nil {
		return nil, err
	}
	err = binary.Read(reader, binary.LittleEndian, &e.ValueSize)
	if err != nil {
		return nil, err
	}

	k := make([]byte, e.KeySize)
	v := make([]byte, e.ValueSize)
	_, err = reader.Read(k)
	if err != nil {
		return nil, err
	}
	_, err = reader.Read(v)
	if err != nil {
		return nil, err
	}
	fmt.Println("---->decode", e.LogType, e.KeySize, e.ValueSize, string(k), string(v))
	e.Key = string(k)
	e.Value = string(v)
	return e, nil
}
