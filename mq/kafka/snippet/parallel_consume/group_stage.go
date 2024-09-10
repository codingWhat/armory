package main

type BatchDataGroupStage struct {
	pph *PartitionParallelHandler
	// worker pool
	workerSize int
	next       Processor
}

func NewBatchDataGroupStage(pph *PartitionParallelHandler) Processor {
	return &BatchDataGroupStage{pph: pph}
}
func (s *BatchDataGroupStage) Input(msgs HashMsg) {
	//todo buz logic
	//不同阶段实现不同的msg struct
}
func (s *BatchDataGroupStage) Process() {
	//todo buz logic
}
func (s *BatchDataGroupStage) Next() Processor {
	panic(nil)
}
func (s *BatchDataGroupStage) SetNext(processor Processor) {
	//todo buz logic
}
