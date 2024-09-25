package parallel_consume

import (
	"math"
	"runtime"
	"sync"
	"time"
)

type WorkerPool struct {
	workerPool []chan HashMsg
	p          Processor

	mu   sync.RWMutex
	conf *Config
}

type Config struct {
	WorkerSize    int
	ChSize        int
	BatchMode     bool
	BatchSize     int
	BatchInterval time.Duration
}

var DefaultConfig = func() *Config {
	return &Config{
		WorkerSize: runtime.NumCPU(),
		BatchMode:  false,
	}
}

func NewWorkerPool(p Processor, conf *Config) *WorkerPool {
	wp := &WorkerPool{
		p:          p,
		conf:       conf,
		workerPool: make([]chan HashMsg, 0, conf.WorkerSize),
	}

	wp.addWorker(conf.BatchMode, conf.WorkerSize, conf.ChSize)

	return wp
}

func (wp *WorkerPool) addWorker(batchMode bool, workerNum int, chSize int) {
	for i := 0; i < workerNum; i++ {
		ch := make(chan HashMsg, chSize)
		if batchMode {
			go wp.RunWithBatch(ch)
		} else {
			go wp.Run(ch)
		}
		wp.mu.Lock()
		wp.workerPool = append(wp.workerPool, ch)
		wp.conf.WorkerSize = len(wp.workerPool)
		wp.mu.Unlock()
	}
}

// UpdaterWorker 每个Stage 可以监听配置，更新worker配置
func (wp *WorkerPool) UpdaterWorker(batchMode bool, workerNum int, chSize int) {
	if workerNum <= 0 {
		return
	}
	//必须保证一个, 不运行应该关闭退出
	diff := workerNum - wp.conf.WorkerSize
	if diff < 0 {
		diff = int(math.Abs(float64(diff)))
		del := wp.workerPool[:diff]

		// 先移除，在关闭channel
		wp.mu.Lock()
		wp.workerPool = wp.workerPool[:diff]
		wp.conf.WorkerSize = len(wp.workerPool)
		wp.mu.Unlock()
		for _, ch := range del {
			close(ch)
		}

	} else {
		wp.addWorker(batchMode, workerNum, chSize)
	}
}

func (wp *WorkerPool) Close() {
	wp.mu.Lock()
	for _, ch := range wp.workerPool {
		close(ch)
	}
	wp.mu.Unlock()
}

func (wp *WorkerPool) Input(idx int, msgs HashMsg) {
	wp.mu.RLock()
	wp.workerPool[idx] <- msgs
	wp.mu.RUnlock()
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

type BatchHashMsg []HashMsg

func (b BatchHashMsg) RawKey() []byte {
	c := b
	return c[0].RawKey()
}

func (wp *WorkerPool) RunWithBatch(msgCh chan HashMsg) {

	msgBatch := make([]HashMsg, wp.conf.BatchSize) // tps 2000, 8c对应8个goroutine,  每个goroutine获得 250+, batchSize: 300
	idx := 0
	ticker := time.NewTicker(wp.conf.BatchInterval) //1s
	defer ticker.Stop()

	var processBatchExtMsgs = func(msgBatch BatchHashMsg) {
		handleBatchMsg := make(BatchHashMsg, len(msgBatch))
		//深拷贝，防止下游修改
		copy(handleBatchMsg, msgBatch)

		out := wp.p.Process(handleBatchMsg)
		if wp.p.Next() != nil && out != nil {
			wp.p.Next().Input(out)
		}

		resetTicker(ticker, wp.conf.BatchInterval)
		idx = 0
	}

	for {
		select {
		case hm, ok := <-msgCh:
			if !ok {
				return
			}
			msgBatch[idx] = hm
			idx++
			if idx >= wp.conf.BatchSize { // 数据已经达到缓存最大值
				processBatchExtMsgs(msgBatch)
			}

		case <-ticker.C:
			if idx > 0 {
				processBatchExtMsgs(msgBatch[:idx])
			}
		}
	}
}
