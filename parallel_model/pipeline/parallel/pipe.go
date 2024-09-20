package parallel

import (
	"github.com/codingWhat/armory/parallel_model/pipeline/serial"
	"sync"
)

type Pipe struct {
	serial.BasePipe

	parallel int

	subFunc   func([]interface{}) interface{}
	mergeFunc func([]interface{}) interface{}
}

func NewPipe(size int, subFunc func([]interface{}) interface{}, mergeFunc func([]interface{}) interface{}) *Pipe {
	return &Pipe{
		parallel:  size,
		subFunc:   subFunc,
		mergeFunc: mergeFunc,
	}
}

func (p *Pipe) DoProcess(input interface{}) interface{} {

	batch := input.([]interface{})
	unit := len(batch) / p.parallel

	wg := &sync.WaitGroup{}
	result := make([]interface{}, p.parallel)
	for i := 0; i < p.parallel; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			result[idx] = p.subFunc(batch[idx*unit : (idx+1)*unit])
		}()
	}
	wg.Wait()
	return p.mergeFunc(result)
}
