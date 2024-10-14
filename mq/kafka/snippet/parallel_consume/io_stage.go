package parallel_consume

import (
	"errors"
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
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
	})

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
	p := &BatchInsertDBStage{
		pph: pph,
	}
	conf := DefaultConfig()
	conf.WorkerSize = 10
	conf.BatchMode = true
	conf.BatchInterval = 1 * time.Second
	conf.BatchSize = 10000
	p.wp = NewWorkerPool(p, conf)

	return p
}

func (s *BatchInsertDBStage) Close() {
	s.wp.Close()
}

func (s *BatchInsertDBStage) Input(msg HashMsg) {
	ieee := crc32.ChecksumIEEE(msg.RawKey())
	s.wp.Input(int(ieee)%s.wp.conf.WorkerSize, msg)
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
		m := msg.(*Msg)
		comment := newComment()
		comment.ID = m.ID
		comment.Parentid = m.Parentid
		comment.Targetid = m.Targetid
		comment.Content = m.Content
		dbhm.offsets = append(dbhm.offsets, m.Offset)
		dbhm.creates = append(dbhm.creates, comment)

		if comment.Parentid != 0 {
			if _, ok := parentReplyCount[comment.Parentid]; !ok {
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

	// 由于涉及批量更新操作，此处不能异步必须串性，否则会引起mysql死锁，
	// 比如: G1和G2;
	// time1: G1 -> A; G2 -> B
	// time2: G1-> B'; G2 -> A'
	for i := 0; i < 3; i++ {
		err := TXNBatchInsertAndUpdate(dbhm.creates, dbhm.updates)
		if err == nil || errors.Is(err, gorm.ErrDuplicatedKey) {
			break
		} else if err != nil {
			fmt.Println("---->TXNBatchInsertAndUpdate, err:", err.Error())
		}
	}

	//fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "==TXNBatchInsertAndUpdate, cost:", time.Now().Sub(start).Milliseconds(), ", num:", len(dbhm.creates), len(dbhm.updates))
	if s.Next() == nil {
		for _, offset := range dbhm.offsets {
			_ = s.pph.poc.MarkConsumed(offset)
		}
	}

	return nil
}
