package entity

import (
	"fmt"
)

type BatchGroupProcessor struct {
	workerPool []chan interface{}
	workerSize int

	handler func(interface{} /*自定义参数*/)
}

func PanicHandler(err any) {
	fmt.Println("PanicHandler--->", err)
}

func withRecoverV2(fn func(interface{}), params interface{}) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn(params)
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()
	fn()
}

func NewBatchGroupProcessor(size int) *BatchGroupProcessor {
	bgp := &BatchGroupProcessor{}
	for i := 0; i < size; i++ {
		bgp.workerPool = append(bgp.workerPool, make(chan interface{}))
	}

	bgp.workerSize = size
	for _, ch := range bgp.workerPool {
		go bgp.run(ch)
	}
	return bgp
}

func (bgp *BatchGroupProcessor) Input(x int, y bool) {
	bgp.workerPool[x%bgp.workerSize] <- y
}

func (bgp *BatchGroupProcessor) run(ch chan interface{}) {

	for {
		select {
		case msg := <-ch:
			// todo buz logic
			go withRecoverV2(bgp.handler, msg)
		}
	}
}
