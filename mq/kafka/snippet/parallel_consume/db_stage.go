package main

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func GetDBConn() *gorm.DB {
	dsn := "root:@tcp(localhost:3306)/db_dashboard?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return db
}

type BatchInsertDBStage struct {
	// worker pool
	pph        *PartitionParallelHandler
	workerSize int
	next       *Processor
}

func NewBatchInsertDBStage(pph *PartitionParallelHandler) Processor {
	return &BatchInsertDBStage{pph: pph}
}

func (s *BatchInsertDBStage) Input(msgs HashMsg) {
	//todo buz logic
	//不同阶段实现不同的msg struct
}
func (s *BatchInsertDBStage) Process() {
	//todo buz logic

	if s.Next() == nil {
		s.pph.poc.MarkConsumed(1)
	}
}
func (s *BatchInsertDBStage) Next() Processor {
	panic(nil)
}
func (s *BatchInsertDBStage) SetNext(processor Processor) {
	//todo buz logic
}
