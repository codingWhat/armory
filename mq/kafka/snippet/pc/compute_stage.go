package main

import (
	"encoding/json"
	"hash/crc32"
	"runtime"
	"strconv"
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
	em := hm.(*ExtMsg)
	smsg := em.msg

	msg, err := unmarshall(smsg.Value)
	if err != nil {
		return nil
	}
	msg.Offset = smsg.Offset

	return msg

}

type Msg struct {
	ID       uint64 `json:"commentid"`
	Content  string `json:"content"`
	Targetid int    `json:"targetid"`
	Parentid int64  ` json:"parentid"`

	Offset int64
}

func (m *Msg) RawKey() []byte {
	return []byte(strconv.Itoa(m.Targetid))
}

func unmarshall(data []byte) (*Msg, error) {
	m := &Msg{}
	err := json.Unmarshal(data, m)
	return m, err
}

type DBHashMsg struct {
	updates []*Comments
	creates []*Comments
	offsets []int64 //sort & commit
}

func (d *DBHashMsg) RawKey() []byte {
	return nil
}
