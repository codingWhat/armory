package main

import (
	"hash/crc32"
	"runtime"
)

type BatchDataComputeStage struct {
	BaseProcessor

	pph *PartitionParallelHandler

	next Processor

	workerPool *WorkerPool
}

func NewBatchDataGroupStage(pph *PartitionParallelHandler) Processor {

	return &BatchDataComputeStage{
		pph: pph,
		// 按 tps 2000处理，8个c计算，每个c平摊，250+， channel size 为300
		//当满足250+, 传递给下游io线程
		workerPool: NewWorkerPool(runtime.NumCPU(), 300, false),
	}
}

func (s *BatchDataComputeStage) Input(msgs HashMsg) {
	ieee := crc32.ChecksumIEEE(msgs.RawKey())
	s.workerPool.Input(int(ieee)%runtime.NumCPU(), msgs)
}

func (s *BatchDataComputeStage) Process(hm HashMsg) HashMsg {

	em := hm.(*ExtMsg)
	msg := em.msg
	m, _ := unSerialize(msg)

	comment := newComment()
	comment.ID = m.ID
	comment.Parentid = m.Parentid
	comment.Targetid = m.Targetid
	comment.Content = m.Content
	return comment
}
