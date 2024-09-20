package main

type WorkerPool struct {
	workerPool []chan HashMsg
	workerSize int
	p          Processor
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

func (wp *WorkerPool) Input(idx int, msgs HashMsg) {
	wp.workerPool[idx] <- msgs
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
