package main

import (
	"errors"
	"github.com/IBM/sarama"
	"time"
)

type HashMsg interface {
	RawKey() []byte
}

type Processor interface {
	Input(HashMsg)
	Process()
	Next() Processor
	SetNext(processor Processor)
}

type ProcessEngine interface {
	Start()
	AddProcessor(processor Processor)
}

func NewPartitionParallelHandler(poc *PartitionOffsetCommitter) *PartitionParallelHandler {
	return &PartitionParallelHandler{
		poc:           poc,
		input:         make(chan *sarama.ConsumerMessage),
		batchSize:     100,
		flushInterval: 1 * time.Second,
	}
}

type PartitionParallelHandler struct {
	poc *PartitionOffsetCommitter

	input      chan *sarama.ConsumerMessage
	processors Processor

	batchSize     int
	flushInterval time.Duration
}

func (pph *PartitionParallelHandler) Input() chan *sarama.ConsumerMessage {
	return pph.input
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

	msgBatch := make(map[string][]*ExtMsg, pph.batchSize)
	idx := 0
	ticker := time.NewTicker(pph.flushInterval)
	defer ticker.Stop()

	var processBatchExtMsgs = func(msgBatch map[string][]*ExtMsg) {
		for rawKey, msgs := range msgBatch {
			if len(msgs) == 0 {
				delete(msgBatch, rawKey)
				continue
			}

			handleBatchMsg := make([]*ExtMsg, len(msgs))
			//深拷贝，防止下游修改
			copy(handleBatchMsg, msgs)
			pph.processors.Input(NewBatchExtMsg([]byte(rawKey), msgs))
			msgBatch[rawKey] = msgs[:0]
		}
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
				return errors.New("claim message channel close")
			}
			if msg.Key == nil {
				return errors.New("msg does not have key")
			}

			/*-------------------消息分发 & 批量插入 & 并行有序提交-------------------------*/
			// msg dispatch
			key := string(msg.Key)
			if _, ok := msgBatch[key]; !ok {
				msgBatch[key] = make([]*ExtMsg, 0)
			}
			msgBatch[key] = append(msgBatch[key], newExtMsg(msg))
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
