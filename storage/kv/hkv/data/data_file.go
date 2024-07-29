package data

import (
	"errors"
	"fmt"
	"github.com/codingWhat/armory/storage/kv/hkv/fio"
	"io"
	"path/filepath"
	"strconv"
)

var (
	ErrDataIsDamaged = errors.New("data is damaged")
)

const (
	DataFileSuffix        = ".data"
	HintFileName          = "index-hint"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IoManager   fio.IOManager
}

func OpenSeqNoFile(path string) (*DataFile, error) {
	filePath := filepath.Join(path, SeqNoFileName)
	return newDataFile(filePath, 0, fio.StandardFIO)
}

func OpenMergeFinishedFile(path string) (*DataFile, error) {
	filePath := filepath.Join(path, MergeFinishedFileName)
	return newDataFile(filePath, 0, fio.StandardFIO)
}

func OpenHintFile(path string) (*DataFile, error) {
	filePath := filepath.Join(path, HintFileName)
	return newDataFile(filePath, 0, fio.StandardFIO)
}

func newDataFile(filePath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	f, err := fio.NewIOManager(filePath, ioType)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{FileId: fileId, WriteOffset: 0, IoManager: f}
	return dataFile, nil
}

func OpenDataFile(path string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	//filePath := filepath.Join(path, fmt.Sprintf("%09d", fileId)+DataFileSuffix)
	filePath := GetDataFileName(path, fileId)
	return newDataFile(filePath, fileId, ioType)
}

func GetDataFileName(path string, fileId uint32) string {
	return filepath.Join(path, fmt.Sprintf("%09d", fileId)+DataFileSuffix)
}

func (d *DataFile) ReadUnMergedFileId() (int, error) {
	record, _, err := d.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	fid, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return fid, nil
}

func (d *DataFile) Write(data []byte) error {
	n, err := d.IoManager.Write(data)
	if err != nil {
		return err
	}
	d.WriteOffset += int64(n)
	return nil
}

func (d *DataFile) WriteMergeFinishedRecord(k []byte, v []byte) error {
	lr := &LogRecord{
		Key:   k,
		Value: v,
	}
	buf, _ := EncodeLogRecord(lr)

	return d.Write(buf)
}

func (d *DataFile) WriteHintRecord(k []byte, pos *LogRecordPos) error {
	recordPos := EncodeLogRecordPos(pos)
	lr := &LogRecord{Key: k, Value: recordPos}
	record, _ := EncodeLogRecord(lr)

	return d.Write(record)
}

func (d *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	//读取头部数据
	//解码头部数据
	//读取kv数据
	//解码kv
	//校验CRC
	header, err := d.ReadBytes(MaxLogRecordHeaderSize, offset)
	if err != nil {
		if err != io.EOF {
			return nil, 0, err
		}
	}

	logHeader, realSize := DecodeLogRecordHeader(header)
	if logHeader.KeySize == 0 && logHeader.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	kvSize := int(logHeader.KeySize) + int(logHeader.ValueSize)

	kv, err := d.ReadBytes(kvSize, realSize+offset)
	if err != nil {
		if err != io.EOF {
			return nil, 0, err
		}
	}

	lr := &LogRecord{
		Key:   kv[:logHeader.KeySize],
		Value: kv[logHeader.KeySize:],
		Type:  logHeader.LogType,
	}

	crc := getCRC(lr, header[4:realSize])
	if crc != logHeader.CRC {
		return nil, 0, ErrDataIsDamaged
	}

	size := realSize + int64(kvSize)
	return lr, size, nil
}

func (d *DataFile) SetIOManager(filePath string, ioType fio.FileIOType) error {
	if err := d.IoManager.Close(); err != nil {
		return err
	}

	manager, err := fio.NewIOManager(GetDataFileName(filePath, d.FileId), ioType)
	if err != nil {
		return err
	}

	d.IoManager = manager
	return nil
}

func (d *DataFile) ReadBytes(n int, offset int64) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := d.IoManager.Read(bytes, offset)
	return bytes, err
}
func (d *DataFile) Close() error {
	return d.IoManager.Close()
}

func (d *DataFile) Sync() error {
	return d.IoManager.Sync()
}
