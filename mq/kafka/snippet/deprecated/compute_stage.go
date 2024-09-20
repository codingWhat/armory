package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"hash/crc32"
	"runtime"
)

type BatchDataComputeStage struct {
	BaseProcessor

	pph *PartitionParallelHandler

	next Processor

	// worker pool
	workerPool *WorkerPool
}

func NewBatchDataGroupStage(pph *PartitionParallelHandler) Processor {
	return &BatchDataComputeStage{
		pph:        pph,
		workerPool: NewWorkerPool(runtime.NumCPU(), 1000),
	}
}

func (s *BatchDataComputeStage) Input(msgs HashMsg) {
	ieee := crc32.ChecksumIEEE(msgs.RawKey())
	s.workerPool.Input(int(ieee)%s.workerPool.workerSize, msgs)
}

func (s *BatchDataComputeStage) Process(hm HashMsg) HashMsg {
	bem := hm.(*BatchExtMsg)
	dbhm := &DBHashMsg{
		creates: make([]*Comments, 0, len(bem.msgs)),
		updates: make([]*Comments, 0, len(bem.msgs)),
		offsets: make([]int64, 0, len(bem.msgs)),
	}
	parentReplyCount := make(map[uint64]int)
	for _, extmsg := range bem.msgs {
		comment, err := unSerialize(extmsg.msg)
		if err != nil {
			fmt.Println("---->unSerialize failed. err:", err.Error())
			continue
		}
		dbhm.offsets = append(dbhm.offsets, extmsg.msg.Offset)
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

	return dbhm
}

func unSerialize(msg *sarama.ConsumerMessage) (*Comments, error) {
	c := &Comments{}
	err := json.Unmarshal(msg.Value, c)
	return c, err
}

type DBHashMsg struct {
	updates []*Comments
	creates []*Comments
	offsets []int64 //sort & commit
}

func (d *DBHashMsg) RawKey() []byte {
	return nil
}
