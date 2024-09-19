package bitcask

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	maxPerFile      = 4 * 100 // 4K
	ErrKeyNoExist   = errors.New("key not exists")
	ErrKeyNotInDisk = errors.New("key is not in disk")
)

type PosInfo struct {
	FileID int
	Offset int64
}

type DB struct {
	active     *os.File
	activeSize int64
	Archives   map[int]*os.File
	Index      map[string]*PosInfo
	sync.RWMutex
	dir string
}

func (db *DB) merge() {
	err := filepath.Walk(db.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("megre-遍历目录失败:", err)
			//return err
			return nil
		}
		if info.IsDir() {
			return nil
		}

		if strings.HasPrefix(info.Name(), "archive") {
			f, err := os.OpenFile(db.dir+info.Name(), os.O_RDWR|os.O_APPEND, 0600)
			if err != nil {
				panic(err)
			}
			fileId := pluckFileID(info.Name())
			newBakName := db.dir + info.Name() + "bak"
			f1, err := os.OpenFile(newBakName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
			if err != nil {
				panic(err)
			}

			var (
				offset    int64 = 0
				buff      bytes.Buffer
				indexes         = make(map[string]*PosInfo)
				newOffset int64 = 0
			)
			for {
				e, err := Decode(f)
				if err == io.EOF {
					fmt.Println("---->Decode EOf", offset, f.Name())
					break
				}

				//被删除
				if e.Type == int8(EntryTypeDel) {
					offset += int64(e.Size())
					continue
				}

				pos, ok := db.Index[string(e.Key)]
				fmt.Println("---->fileID", fileId, f.Name(), string(e.Key), e.Type, pos, ok, offset)
				if !ok {
					offset += int64(e.Size())
					continue
				}

				if pos.FileID != fileId || pos.Offset != offset {
					offset += int64(e.Size())
					continue
				}

				indexes[string(e.Key)] = &PosInfo{
					FileID: fileId,
					Offset: newOffset,
				}
				newOffset += int64(e.Size())
				n, err := buff.Write(encode(e).Bytes())
				fmt.Println("---->buff.Write", f.Name(), n, err, buff.Len())
				offset += int64(e.Size())
			}
			if buff.Len() == 0 {
				//fmt.Println("MarkConsumed---->", fileId, f.Name(), buff.Len(), db.dir+info.Name(), newBakName)
				os.Remove(db.dir + info.Name())
				os.Remove(newBakName)
				os.Remove(db.dir + "hint_" + strconv.Itoa(fileId))
			} else {
				hintName := db.dir + "hint_" + strconv.Itoa(fileId)
				hintFile, err := os.OpenFile(hintName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
				if err != nil {
					panic(err)
				}
				//简单处理，hint数据json存储
				hintData, _ := json.Marshal(indexes)
				_, _ = hintFile.Write(hintData)
				//数据写入新文件
				_, _ = f1.Write(buff.Bytes())
				os.Rename(db.dir+info.Name(), db.dir+info.Name()+"dep")
				if err = os.Rename(newBakName, db.dir+info.Name()); err == nil {
					os.Remove(db.dir + info.Name() + "dep")
				}
				os.Rename(newBakName, db.dir+info.Name())
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (db *DB) activeFileName() string {
	stat, _ := db.active.Stat()
	return stat.Name()
}

func (db *DB) archivePath() string {
	return db.dir
}

func (db *DB) archive() {
	fileID := pluckFileID(db.active.Name())
	name := fmt.Sprintf("archive_%d", fileID)
	_ = os.Rename(db.active.Name(), db.archivePath()+name)
	file, err := os.OpenFile(db.archivePath()+name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	db.Archives[fileID] = file
}
func isDirOrCreate(path string) bool {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(path, os.ModePerm)
			if err != nil {
				panic(err)
			}
			return true
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

	db.Index = make(map[string]*PosInfo)
	db.Archives = make(map[int]*os.File)

	db.loadArchive()
	db.loadActive()
	if db.active == nil {
		db.openActive()
	}

	return db
}

func (db *DB) loadArchive() {

	var (
		fileNames     []string
		hintFileNames []string
	)
	err := filepath.Walk(db.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("遍历目录失败:", err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		//optimize: load the hint file to speed up the start-up
		if strings.HasPrefix(info.Name(), "hint") {
			hintFileNames = append(hintFileNames, info.Name())
			//直接加载archive文件
		} else if strings.HasPrefix(info.Name(), "archive") {
			fileNames = append(fileNames, info.Name())
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	if len(hintFileNames) > 0 {
		sort.Strings(hintFileNames)

		for _, fileName := range hintFileNames {
			f, err := os.OpenFile(db.dir+fileName, os.O_RDWR|os.O_APPEND, 0600)
			if err != nil {
				panic(err)
			}
			all, err := ioutil.ReadAll(f)
			if err != nil {
				panic(err)
			}
			indexes := make(map[string]*PosInfo)
			_ = json.Unmarshal(all, &indexes)
			for k, posInfo := range indexes {
				db.Index[k] = posInfo
			}
		}
	}

	sort.Strings(fileNames)
	for _, fileName := range fileNames {
		f, err := os.OpenFile(db.dir+fileName, os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			panic(err)
		}
		db.Archives[pluckFileID(fileName)] = f
		var offset int64 = 0
		for {
			e, err := Decode(f)
			if err == io.EOF {
				break
			}
			db.Index[string(e.Key)] = &PosInfo{
				FileID: pluckFileID(fileName), Offset: offset,
			}
			offset += int64(e.Size())
		}
	}
}

func (db *DB) loadActive() {
	err := filepath.Walk(db.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("遍历目录失败:", err)
			return err
		}
		if strings.HasPrefix(info.Name(), "active") {

			f, err := os.OpenFile(db.dir+info.Name(), os.O_RDWR|os.O_APPEND, 0600)
			if err != nil {
				panic(err)
			}
			db.active = f
			var offset int64 = 0
			for {
				e, err := Decode(f)
				if err == io.EOF {
					break
				}
				db.Index[string(e.Key)] = &PosInfo{
					FileID: pluckFileID(info.Name()), Offset: offset,
				}
				offset += int64(e.Size())
			}
			db.activeSize = offset

		}
		return nil
	})

	if err != nil {
		panic(err)
	}
}

func pluckFileID(name string) int {
	info := strings.Split(name, "_")
	ret, _ := strconv.Atoi(info[1])
	return ret
}

func (db *DB) openActive() {
	newFileName := fmt.Sprintf("active_%d", len(db.Archives)+1)
	file, err := os.OpenFile(db.dir+newFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	currOffset, _ := file.Seek(0, 1)
	db.activeSize = currOffset
	db.active = file
}

func (db *DB) Put(k string, v []byte) error {
	db.Lock()
	defer db.Unlock()

	entry := NewPutEntry([]byte(k), v)
	record := encode(entry).Bytes()
	//如果超出当前活跃文件大小，则归档当前活跃文件
	offset := db.activeSize

	fileId := pluckFileID(db.active.Name())
	if db.activeSize+int64(entry.Size()) > int64(maxPerFile) {
		db.archive()
		db.openActive()
		fileId = pluckFileID(db.active.Name())
		offset = 0
	}

	_, err := db.active.Write(record)
	if err != nil {
		return err
	}
	db.Index[k] = &PosInfo{FileID: fileId, Offset: offset}
	db.activeSize += int64(entry.Size())
	return nil
}

func (db *DB) Del(k string) error {
	db.Lock()
	defer db.Unlock()
	_, ok := db.Index[k]
	if !ok {
		return ErrKeyNoExist
	}
	entry := NewDelEntry([]byte(k))
	record := encode(entry).Bytes()
	//如果超出当前活跃文件大小，则归档当前活跃文件
	if db.activeSize+int64(entry.Size()) > int64(maxPerFile) {
		db.archive()
		db.openActive()
	}
	_, err := db.active.Write(record)
	if err != nil {
		return err
	}

	delete(db.Index, k)
	db.activeSize += int64(entry.Size())
	return nil
}

func (db *DB) Get(k string) ([]byte, error) {
	db.RLock()
	defer db.RUnlock()
	posInfo, ok := db.Index[k]
	if !ok {
		return nil, ErrKeyNoExist
	}

	/*
	 读取key的所属的文件id
	 从对应的文件中读取数据
	*/
	var f *os.File
	fName := posInfo.FileID
	if fName == pluckFileID(db.active.Name()) {
		f = db.active
		//_, err := f.Seek(0, 0)
		//all, err := io.ReadAll(f)
		//if err != nil {
		//	return nil, err
		//}

		//fmt.Println(f.Name(), "---->active, offset", k,
		//	all, posInfo.FileID, posInfo.offsets)
		_, err := f.Seek(posInfo.Offset, 0)
		if err != nil {
			return nil, err
		}
	} else {

		if len(db.Archives) == 0 {
			msg := fmt.Sprintf("get data error, key:%s, pos: %+v", k, posInfo)
			panic(msg)
		}
		f = db.Archives[posInfo.FileID]
		//_, err := f.Seek(0, 0)
		//stat, _ := f.Stat()
		//all, err := io.ReadAll(f)
		//if err != nil {
		//	return nil, err
		//}
		//fmt.Println(stat.Name(), stat.Size(), "---->archive, offset", k, all, posInfo.FileID, posInfo.offsets)

		_, err := f.Seek(posInfo.Offset, 0)

		if err != nil {
			return nil, err
		}
	}
	//fmt.Println("----> ready to Decode, ", db.Archives, curOffset)
	e, err := Decode(f)
	if err == io.EOF {
		return nil, ErrKeyNotInDisk
	}
	if err != nil {
		return nil, err
	}
	return e.Value, nil

	//return nil, nil
}
