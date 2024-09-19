package main

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"hash/crc32"
)

var _db *gorm.DB

func GetDBConn() *gorm.DB {
	if _db != nil {
		return _db
	}

	dsn := "root:@tcp(localhost:3306)/db_dashboard?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	sqldb, _ := db.DB()
	sqldb.SetMaxOpenConns(10)
	sqldb.SetMaxIdleConns(5)

	_db = db
	return _db
}

type BatchInsertDBStage struct {
	BaseProcessor

	// worker pool
	wp *WorkerPool

	pph  *PartitionParallelHandler
	next Processor
}

func NewBatchInsertDBStage(pph *PartitionParallelHandler) Processor {
	return &BatchInsertDBStage{
		pph: pph,
		wp:  NewWorkerPool(10, 200), //2000 最大2000
	}
}

func (s *BatchInsertDBStage) Input(msgs HashMsg) {
	ieee := crc32.ChecksumIEEE(msgs.RawKey())
	s.wp.Input(int(ieee)%s.wp.workerSize, msgs)
}
func (s *BatchInsertDBStage) Process(hm HashMsg) HashMsg {
	dbhm := hm.(*DBHashMsg)

	for i := 0; i < 3; i++ {
		if err := TXNBatchInsertAndUpdate(dbhm.creates, dbhm.updates); err != nil {
			fmt.Println("---->TXNBatchInsertAndUpdate, err:", err.Error())
		} else {
			break
		}
	}

	if s.Next() == nil {
		for _, offset := range dbhm.offsets {
			_ = s.pph.poc.MarkConsumed(offset)
		}
	}

	return nil
}
