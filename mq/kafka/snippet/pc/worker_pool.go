package main

import "time"

type WorkerPool struct {
	workerPool []chan HashMsg
	workerSize int
	p          Processor

	batchSize     int
	batchInterval time.Duration
}

func NewWorkerPool(workerSize int, chSize int) *WorkerPool {
	wp := &WorkerPool{workerPool: make([]chan HashMsg, workerSize), workerSize: workerSize}

	for i := 0; i < wp.workerSize; i++ {
		ch := make(chan HashMsg, chSize)
		wp.workerPool = append(wp.workerPool, ch)
		go wp.Run(ch)
	}

	return wp
}

func NewWorkerPoolWithBatch(workerSize int, chSize int, batchSize int, batchInterval time.Duration) *WorkerPool {
	wp := &WorkerPool{
		workerPool: make([]chan HashMsg, workerSize),
		workerSize: workerSize, batchSize: batchSize, batchInterval: batchInterval,
	}

	for i := 0; i < wp.workerSize; i++ {
		ch := make(chan HashMsg, chSize)
		wp.workerPool = append(wp.workerPool, ch)
		go wp.RunWithBatch(ch)
	}

	return wp
}

func (wp *WorkerPool) Input(idx int, msgs HashMsg) {
	wp.workerPool[idx] <- msgs
}

type BatchHashMsg struct {
	hm []HashMsg
}

func (b *BatchHashMsg) RawKey() []byte {
	return b.hm[0].RawKey()
}

func (wp *WorkerPool) RunWithBatch(msgCh chan HashMsg) {

	ticker := time.NewTicker(wp.batchInterval) //1s
	defer ticker.Stop()

	idx := 0
	msgBatch := &BatchHashMsg{hm: make([]HashMsg, wp.batchSize)}
	var processBatchExtMsgs = func(msgBatch *BatchHashMsg) {

		handleBatchMsg := &BatchHashMsg{hm: make([]HashMsg, len(msgBatch.hm))}
		//深拷贝，防止下游修改
		copy(handleBatchMsg.hm, msgBatch.hm)

		out := wp.p.Process(handleBatchMsg)
		if wp.p.Next() != nil && out != nil {
			wp.p.Next().Input(out)
		}

		msgBatch.hm = msgBatch.hm[:0]
		resetTicker(ticker, wp.batchInterval)
		idx = 0
	}

	for {
		select {
		case <-ticker.C:
			if idx > 0 {
				processBatchExtMsgs(msgBatch)
			}
		case hm, ok := <-msgCh:
			if !ok {
				return
			}

			msgBatch.hm[idx] = hm
			idx++
			if idx >= wp.batchSize { // 数据已经达到缓存最大值
				processBatchExtMsgs(msgBatch)
			}
		}
	}
}

func (wp *WorkerPool) Run(msgCh chan HashMsg) {

	for {
		select {
		case hm, ok := <-msgCh:
			if !ok {
				return
			}
			out := wp.p.Process(hm)
			if wp.p.Next() != nil && out != nil {
				wp.p.Next().Input(out)
			}
		}
	}
}
