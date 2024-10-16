package parallel_consume

import (
	"github.com/IBM/sarama"
	"runtime"
	"time"
)

type HashMsg interface {
	RawKey() []byte
}

type Processor interface {
	Input(HashMsg)
	Next() Processor
	SetNext(processor Processor)
	Process(hm HashMsg) HashMsg
	Close()
}

type ProcessEngine interface {
	Start()
	AddProcessor(processor Processor)
	Close()
}

type BaseProcessor struct {
	next Processor
}

func (bp *BaseProcessor) Next() Processor {
	return bp.next
}

func (bp *BaseProcessor) SetNext(processor Processor) {
	bp.next = processor
}

func NewPartitionParallelHandler(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) *PartitionParallelHandler {
	poc := NewPartitionOffsetCommitter(claim.Partition(), sess)
	return &PartitionParallelHandler{
		poc:           poc,
		batchSize:     100,
		flushInterval: 1 * time.Second,
	}
}

type PartitionParallelHandler struct {
	poc *PartitionOffsetCommitter

	dispatchCh chan Processor
	processors Processor

	batchSize     int
	flushInterval time.Duration
}

func (pph *PartitionParallelHandler) Input(msg *sarama.ConsumerMessage) {
	// msg dispatch
	for pph.poc.Add(msg) != nil {
		runtime.Gosched()
	}

	pph.processors.Input(newExtMsg(msg))
}

func (pph *PartitionParallelHandler) Close() {
	close(pph.poc.addCh)
}

func (pph *PartitionParallelHandler) AddProcessor(processor Processor) {
	if pph.processors == nil {
		pph.processors = processor
		return
	}

	p := pph.processors
	for p.Next() != nil {
		p = p.Next()
	}
	p.SetNext(processor)
}

func newExtMsg(m *sarama.ConsumerMessage) *ExtMsg {
	return &ExtMsg{
		msg: m,
	}
}
