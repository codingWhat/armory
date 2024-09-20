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
		wp:  NewWorkerPool(1, 2000, true),
	}
}

func (s *BatchInsertDBStage) Input(msgs HashMsg) {
	ieee := crc32.ChecksumIEEE(msgs.RawKey())
	s.wp.Input(int(ieee)%s.wp.workerSize, msgs)
}

type DBHashMsg struct {
	updates []*Comments
	creates []*Comments
	offsets []int64 //sort & commit
}

func (d *DBHashMsg) RawKey() []byte {
	return nil
}
func (s *BatchInsertDBStage) Process(hm HashMsg) HashMsg {
	bhm := hm.(BatchHashMsg)

	// 遍聚合边计算，
	dbhm := &DBHashMsg{
		creates: make([]*Comments, 0, len(bhm)),
		updates: make([]*Comments, 0, len(bhm)),
		offsets: make([]int64, 0, len(bhm)),
	}
	parentReplyCount := make(map[uint64]int)
	for _, msg := range bhm {
		comment := msg.(*Comments)
		dbhm.offsets = append(dbhm.offsets, comment.Offset)
		dbhm.creates = append(dbhm.creates, comment)
		if comment.Parentid != 0 {
			if _, ok := parentReplyCount[comment.Parentid]; ok {
				parentReplyCount[comment.Parentid] = 0
			}
			parentReplyCount[comment.Parentid]++
		}
	}
	for pid, reply := range parentReplyCount {
		c := newComment()
		c.Reply = reply
		c.ID = pid
		dbhm.updates = append(dbhm.updates, c)
	}

	// 此处异步执行，不阻塞
	go func() {
		for i := 0; i < 3; i++ {
			if err := TXNBatchInsertAndUpdate(dbhm.creates, dbhm.updates); err != nil {
				fmt.Println("---->TXNBatchInsertAndUpdate, err:", err.Error())
			} else {
				break
			}
		}
	}()

	if s.Next() == nil {
		for _, offset := range dbhm.offsets {
			_ = s.pph.poc.MarkConsumed(offset)
		}
	}

	return nil
}
