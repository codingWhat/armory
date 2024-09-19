package main

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"hash/crc32"
	"time"
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

type IOStage struct {
	BaseProcessor

	// worker pool
	wp *WorkerPool

	pph  *PartitionParallelHandler
	next Processor
}

func NewBatchInsertDBStage(pph *PartitionParallelHandler) Processor {
	return &IOStage{
		pph: pph,
		wp:  NewWorkerPoolWithBatch(1, 2000, 1000, 1*time.Second), //2000 最大2000
	}
}

func (s *IOStage) Input(msgs HashMsg) {
	ieee := crc32.ChecksumIEEE(msgs.RawKey())
	s.wp.Input(int(ieee)%s.wp.workerSize, msgs)
}
func (s *IOStage) Process(hm HashMsg) HashMsg {
	bhm := hm.(*BatchHashMsg)

	dbhm := &DBHashMsg{
		creates: make([]*Comments, 0, len(bhm.hm)),
		updates: make([]*Comments, 0, len(bhm.hm)),
		offsets: make([]int64, 0, len(bhm.hm)),
	}
	parentReplyCount := make(map[uint64]int)
	for _, msg := range bhm.hm {
		m := msg.(*Msg)
		comment := newComment()
		comment.ID = m.ID
		comment.Parentid = uint64(m.Parentid)
		comment.Content = m.Content
		comment.Targetid = m.Targetid
		comment.Parentid = uint64(m.Parentid)
		dbhm.offsets = append(dbhm.offsets, m.Offset)
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

	for i := 0; i < 3; i++ {
		if err := TXNBatchInsertAndUpdate(dbhm.creates, dbhm.updates); err != nil {
			fmt.Println("---->TXNBatchInsertAndUpdate, err:", err.Error())
		} else {
			break
		}
	}

	// 提交
	for _, offset := range dbhm.offsets {
		_ = s.pph.poc.MarkConsumed(offset)
	}

	return nil
}
