package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var maxPerFile = 30

type PosInfo struct {
	FileID string
	Offset int64
}

type DB struct {
	active     *os.File
	activeSize int64
	archives   map[string]*os.File
	index      map[string]*PosInfo
	sync.RWMutex
	dir string
}

func (db *DB) activeFileName() string {
	stat, _ := db.active.Stat()
	return stat.Name()
}

func (db *DB) archivePath() string {
	return db.dir
}

func (db *DB) archive() {
	db.archives[db.active.Name()] = db.active
	_ = os.Rename(db.active.Name(), db.archivePath()+db.activeFileName())

	fmt.Println("archive----->", db.active.Name(), db.archivePath()+db.activeFileName())
}
func isDirOrCreate(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(path, os.ModePerm)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}

	}

	return fileInfo.IsDir()
}

func Open(dir string) *DB {
	if !isDirOrCreate(dir) {
		panic("dir is not invalid ")
	}

	db := &DB{dir: dir}

	db.index = make(map[string]*PosInfo)
	db.archives = make(map[string]*os.File)

	db.loadActive()
	if db.active == nil {
		db.openActive()
	}

	return db
}

func (db *DB) loadActive() {
	err := filepath.Walk(db.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("遍历目录失败:", err)
			return err
		}
		if strings.HasPrefix(info.Name(), "log") {

			f, err := os.OpenFile(info.Name(), os.O_RDWR, 0600)
			if err != nil {
				panic(err)
			}
			db.active = f
			var offset int64 = 0
			for {
				e, err := decode(f)
				if err == io.EOF {
					break
				}
				db.index[string(e.Key)] = &PosInfo{
					FileID: info.Name(), Offset: offset,
				}
				offset += int64(e.Size())
			}
		}
		return nil
	})

	if err != nil {
		panic(err)
	}
}

func (db *DB) openActive() {
	newFileName := fmt.Sprintf("log_%d.log", len(db.archives)+1)
	file, err := os.OpenFile(db.dir+newFileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	db.activeSize = 0
	db.active = file
}

func (db *DB) Put(k string, v []byte) error {
	db.Lock()
	defer db.Unlock()

	entry := NewEntry([]byte(k), v)
	record := encode(entry).Bytes()
	//如果超出当前活跃文件大小，则归档当前活跃文件
	offset := db.activeSize

	fileId := db.active.Name()
	if db.activeSize+int64(entry.Size()) > int64(maxPerFile) {
		db.archive()
		db.openActive()
		fileId = db.active.Name()
		offset = 0
	}

	_, err := db.active.Write(record)
	if err != nil {
		return err
	}
	db.index[k] = &PosInfo{FileID: fileId, Offset: offset}
	db.activeSize += int64(entry.Size())
	return nil
}

func (db *DB) Get(k string) ([]byte, error) {
	db.RLock()
	defer db.RUnlock()
	posInfo, ok := db.index[k]
	if !ok {
		return nil, errors.New("key not exist")
	}

	/*
	 读取key的所属的文件id
	 从对应的文件中读取数据
	*/
	var f *os.File
	fName := posInfo.FileID
	if fName == db.active.Name() {
		f = db.active
		_, err := f.Seek(posInfo.Offset, 0)
		if err != nil {
			return nil, err
		}
	} else {
		if len(db.archives) == 0 {
			msg := fmt.Sprintf("get data error, key:%s, pos: %+v", k, posInfo)
			panic(msg)
		}
		f = db.archives[posInfo.FileID]
		_, err := f.Seek(posInfo.Offset, 0)
		if err != nil {
			return nil, err
		}
	}

	e, err := decode(f)
	if err == io.EOF {
		return nil, errors.New("key is not in disk")
	}
	return e.Value, nil
}

func decode(reader io.Reader) (*Entry, error) {
	e := &Entry{}
	header := make([]byte, 16)
	_, err := reader.Read(header)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(header)
	err = binary.Read(buffer, binary.LittleEndian, &e.CRC)
	if err != nil {
		return nil, err
	}
	binary.Read(buffer, binary.LittleEndian, &e.TS)
	binary.Read(buffer, binary.LittleEndian, &e.KeySize)
	binary.Read(buffer, binary.LittleEndian, &e.ValueSize)

	kv := make([]byte, e.KeySize+e.ValueSize)
	_, err = reader.Read(kv)
	if err != nil {
		return nil, err
	}

	e.Key = kv[:e.KeySize]
	e.Value = kv[e.KeySize:]
	return e, err
}

var headerSize = 16 //4 + 4 + 4 + 4

func encode(e *Entry) *bytes.Buffer {
	size := headerSize + len(e.Key) + len(e.Value)
	buffer := bytes.NewBuffer(make([]byte, 0, size))
	crc := crc32.ChecksumIEEE(e.Value)

	binary.Write(buffer, binary.LittleEndian, crc)
	binary.Write(buffer, binary.LittleEndian, e.TS)
	binary.Write(buffer, binary.LittleEndian, int32(len(e.Key)))
	binary.Write(buffer, binary.LittleEndian, int32(len(e.Value)))
	buffer.Write(e.Key)
	buffer.Write(e.Value)

	return buffer
}

type Entry struct {
	CRC       uint32
	TS        int32
	KeySize   int32
	ValueSize int32
	Key       []byte
	Value     []byte
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
