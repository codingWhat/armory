package parallel_consume

import (
	"math"
)

type BatchDataComputeStage struct {
	BaseProcessor

	pph *PartitionParallelHandler

	next Processor

	workerPool *WorkerPool

	idx int
}

func NewBatchDataGroupStage(pph *PartitionParallelHandler) Processor {

	p := &BatchDataComputeStage{
		pph: pph,
		// 按 tps 2000处理，8个c计算，每个c平摊，250+， channel size 为300
		//当满足250+, 传递给下游io线程
	}
	conf := DefaultConfig()
	//conf.ChSize = 200
	p.workerPool = NewWorkerPool(p, conf)

	return p
}

func (s *BatchDataComputeStage) Close() {
	s.workerPool.Close()
}

func (s *BatchDataComputeStage) Input(msg HashMsg) {
	if s.idx >= math.MaxInt-1 {
		s.idx = 0
	}
	s.workerPool.Input(s.idx%s.workerPool.conf.WorkerSize, msg)
	s.idx++
}

func (s *BatchDataComputeStage) Process(hm HashMsg) HashMsg {

	em := hm.(*ExtMsg)
	msg := em.msg
	m, err := unSerialize(msg)
	if err != nil {
		panic(err)
	}
	m.Offset = msg.Offset
	return m
}
