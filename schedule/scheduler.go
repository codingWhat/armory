package main

import (
	"context"
	"fmt"
	"github.com/codingWhat/armory/schedule/data"
	"github.com/codingWhat/armory/schedule/models"
	"runtime"
	"time"
)

func main() {
	scheduler := NewScheduler()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler.RegHandler(ctx, "face", &models.FaceModel{}, 100)

	go scheduler.Schedule()

	for i := 0; i < 1000; i++ {
		i := i
		for !scheduler.Input(i) {
		}
	}

	time.Sleep(10 * time.Second)
}

type Handler interface {
	Name() string
	Quota() int
	Request(context.Context, *data.ProcessUnit) error
	RespBatchHandle(context.Context, []map[string]interface{}) error
}

type Processor struct {
	ID      int
	ms      *Scheduler
	handler Handler
	taskQ   chan *data.ProcessUnit
	next    *Processor
}

func NewProcessor(id int, ms *Scheduler, handler Handler) *Processor {
	return &Processor{
		ID:      id,
		ms:      ms,
		handler: handler,
		taskQ:   make(chan *data.ProcessUnit, handler.Quota()),
	}
}

func (a *Processor) Input() chan *data.ProcessUnit {
	return a.taskQ
}
func (a *Processor) isFree() bool {
	load := len(a.taskQ)
	sum := a.handler.Quota()
	return float32(load)/float32(sum) < 0.8
}

func (a *Processor) calculateFreeAndSum() (int, int) {
	pp := a
	cnt := 0
	load := 0
	for pp != nil {
		load += len(pp.taskQ)
		pp = pp.next
		cnt++
	}
	sum := len(a.taskQ) * cnt
	return sum - load, sum
}

func (a *Processor) Run(ctx context.Context) {

	timer := time.NewTicker(5 * time.Second)
	var batchPU []*data.ProcessUnit
	fmt.Println("start up processor:", a.ID)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if len(batchPU) > 0 {
				fmt.Println("---->timer.C", a.handler.Name(), batchPU[len(batchPU)-1].Data)
				batchData := make([]map[string]interface{}, 10)
				if err := a.handler.RespBatchHandle(ctx, batchData); err == nil {
					a.ms.recordWriteLog("finish")
				} else {
					//todo log
					//定期扫描程序，去重新队列
				}
			}
		case pu := <-a.taskQ:
			fmt.Println(a.ID, "----->FaceModel request handle, data:", pu.Data)
			if err := a.handler.Request(ctx, pu); err != nil {
				if pu.IsCanRetry() {
					fmt.Println("---->retry", pu)
					a.ms.retryQ <- pu
				}
			} else {
				batchPU = append(batchPU, pu)
			}

		}
	}
}

type Scheduler struct {
	processors map[string]*Processor
	batchSize  int

	wakeCh chan struct{}
	inpCh  chan *data.ProcessUnit
	retryQ chan *data.ProcessUnit
}

func (ms *Scheduler) Input(inpt interface{}) bool {
	if ms.isBusy() {
		return false
	}
	pu := data.NewDefaultProcessUnit(inpt)
	select {
	case ms.inpCh <- pu:
		if len(ms.inpCh) >= ms.batchSize {
			ms.wakeCh <- struct{}{}
		}
		return true
	default:
		return false
	}
}

func NewScheduler() *Scheduler {

	return &Scheduler{
		batchSize:  10,
		wakeCh:     make(chan struct{}),
		processors: make(map[string]*Processor),
		inpCh:      make(chan *data.ProcessUnit, 1000),
	}
}
func (ms *Scheduler) recordWriteLog(status string) error {
	//todo 记录操作流水
	fmt.Println("---->recordWriteLog:", status)
	return nil
}

func (ms *Scheduler) getExceptLogs() []string {
	//todo 从流水表中取出异常日志
	//需要二次判断任务是否已经写入，只是更新操作流水表失败了
	return nil
}

func (ms *Scheduler) Schedule() {

	timer := time.NewTicker(15 * time.Second)
	for {
		select {
		case <-timer.C:
			logs := ms.getExceptLogs()
			if len(logs) == 0 {
				continue
			}
			//todo
			//判断是否已经写到db，如果有更新记录的状态为finished.
			//如果没写，重新调度任务

			//for _, log := range logs {
			//	_ = log
			//	img := newDefaultProcessUnit(log)
			//	img.isExceptRetry = true
			//	ms.pushPU2Q(img)
			//}

		case pu := <-ms.retryQ:
			if pu.IsReachMaxRetry() {
				//todo log...
				continue
			}
			//塞入队尾
			ms.pushPU2Q(pu)
			pu.IncrRetryNum()
		case <-ms.wakeCh:
			//若没有新来的数据就休眠
			pus := ms.batchGetImagesFromQ(ms.batchSize)
			if len(pus) == 0 {
				runtime.Gosched()
				continue
			}

			var nextRound []*data.ProcessUnit
			for _, pu := range pus {
				modelP := ms.chooseProcessorPBy(pu)
				if modelP == nil {
					nextRound = append(nextRound, pu)
					continue
				}
				//fmt.Println("---->chosed p:", modelP.ID)
				//记录写入流水日志
				if err := ms.recordWriteLog("prepare"); err == nil {
					modelP.Input() <- pu
				}
			}
			//说明模型都很忙,休眠等待
			if len(nextRound) > 0 && (len(nextRound) == len(pus) || len(nextRound) > len(pus)/2) {
				time.Sleep(1 * time.Second)
			}
			for _, img := range nextRound {
				ms.pushPU2Q(img)
			}
		}
	}
}

func (ms *Scheduler) isBusy() bool {
	free := 0
	sum := 0
	for _, processor := range ms.processors {
		f, s := processor.calculateFreeAndSum()
		free += f
		sum += s
	}

	return float32(free)/float32(sum) < 0.2
}

func (ms *Scheduler) batchGetImagesFromQ(size int) []*data.ProcessUnit {

	var pus []*data.ProcessUnit
	for len(pus) < size {
		pu := <-ms.inpCh
		pus = append(pus, pu)
	}
	return pus
}
func (ms *Scheduler) pushPU2Q(pu *data.ProcessUnit) {
	ms.inpCh <- pu
}

func (ms *Scheduler) chooseProcessorPBy(pu *data.ProcessUnit) *Processor {
	//map 遍历负载均衡
	for name, processor := range ms.processors {
		if pu.IsProcessedBy(name) {
			continue
		}

		p := processor
		for p.next != nil {
			if !p.isFree() {
				p = p.next
			} else {
				break
			}
		}
		if !p.isFree() {
			continue
		}

		pu.RecordProcess(name)
		return p
	}

	return nil
}

func (ms *Scheduler) RegHandler(ctx context.Context, name string, handler Handler, ops ...int) {
	num := 1
	if len(ops) > 0 {
		num = ops[0]
	}
	for i := 0; i < num; i++ {
		newP := NewProcessor(i, ms, handler)
		p, ok := ms.processors[name]
		if !ok {
			ms.processors[name] = newP
		} else {
			for p.next != nil {
				p = p.next
			}
			p.next = newP
		}
		p = newP
		go p.Run(ctx)
	}
}
