package main

import (
	"errors"
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
		input:         make(chan *sarama.ConsumerMessage),
		batchSize:     100,
		flushInterval: 1 * time.Second,
	}
}

type PartitionParallelHandler struct {
	poc *PartitionOffsetCommitter

	input chan *sarama.ConsumerMessage

	dispatchCh chan Processor
	processors Processor

	batchSize     int
	flushInterval time.Duration
}

func (pph *PartitionParallelHandler) Input() chan *sarama.ConsumerMessage {
	return pph.input
}
func (pph *PartitionParallelHandler) Close() {
	close(pph.input)
	close(pph.poc.input)
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

func (pph *PartitionParallelHandler) Start() error {

	msgBatch := make([]*ExtMsg, 0, pph.batchSize) // tps 2000, 8c对应8个goroutine,  每个goroutine获得 250+, batchSize: 300
	idx := 0
	ticker := time.NewTicker(pph.flushInterval) //1s
	defer ticker.Stop()

	var processBatchExtMsgs = func(msgBatch []*ExtMsg) {

		handleBatchMsg := make([]*ExtMsg, len(msgBatch))
		//深拷贝，防止下游修改
		copy(handleBatchMsg, msgBatch)
		for _, msg := range handleBatchMsg {
			pph.processors.Input(msg)
		}
		msgBatch = msgBatch[:0]
		resetTicker(ticker, pph.flushInterval)
		idx = 0
	}

	for {
		select {
		case <-ticker.C:
			if idx > 0 {
				processBatchExtMsgs(msgBatch)
			}
		case msg, ok := <-pph.input:
			if !ok {
				return nil
			}

			if msg.Key == nil {
				return errors.New("msg does not have key")
			}

			/*------------------- 计算:【消息批量 & 消息分组】 & 【 计算：【构建model & 计算计数】 &  io: 批量插入】 & 并行有序提交-------------------------*/
			// msg dispatch
			for pph.poc.Offer(msg) != nil {
				runtime.Gosched()
			}

			msgBatch[idx] = newExtMsg(msg)
			idx++
			if idx >= pph.batchSize { // 数据已经达到缓存最大值
				processBatchExtMsgs(msgBatch)
			}
		}
	}
}

func newExtMsg(m *sarama.ConsumerMessage) *ExtMsg {
	return &ExtMsg{
		msg: m,
	}
}

type BatchExtMsg struct {
	msgs []*ExtMsg
	key  []byte
}

func NewBatchExtMsg(key []byte, msgs []*ExtMsg) *BatchExtMsg {
	return &BatchExtMsg{
		msgs: msgs,
		key:  key,
	}
}

func (bem *BatchExtMsg) RawKey() []byte {
	return bem.key
}
