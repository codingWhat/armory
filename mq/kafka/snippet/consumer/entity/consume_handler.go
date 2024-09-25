package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/codingWhat/armory/mq/kafka/snippet/consumer/pq"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// newLimiter get a *rate.Limiter
func newLimiter(conf *ConsumerConf) *rate.Limiter {
	if conf == nil {
		return rate.NewLimiter(rate.Inf, 0)
	}
	return rate.NewLimiter(rate.Limit(conf.Rate), conf.Burst)
}

type ConsumerGroupHandle struct {
	ctx     context.Context
	limiter *rate.Limiter
}

func NewConsumerGroupHandle(ctx context.Context, conf *ConsumerConf) *ConsumerGroupHandle {
	return &ConsumerGroupHandle{
		ctx:     ctx,
		limiter: newLimiter(conf),
	}
}

func (c *ConsumerGroupHandle) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("Setup kafka session，member_id：%v，generation_id：%v，claims：%v",
		session.MemberID(), session.GenerationID(), session.Claims())
	return nil
}

func (c *ConsumerGroupHandle) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("Cleanup kafka session，member_id：%v，generation_id：%v，claims：%v",
		session.MemberID(), session.GenerationID(), session.Claims())
	return nil
}

func (c *ConsumerGroupHandle) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	limiter := rate.NewLimiter(rate.Every(1*time.Second), 2000)
	for {
		select {
		case <-c.ctx.Done():
			return errors.New("server shut down")
		case <-sess.Context().Done():
			return errors.New("session close")
		case msg, ok := <-claim.Messages():
			if !ok {
				return errors.New("get msg failed")
			}
			if err := limiter.Wait(c.ctx); err != nil {
				return err
			}
			poc := GetPOC(msg.Partition, sess)
			//todo msg dispatch

			/*-------------------模拟消息有序提交-------------------------*/
			//写入优先级队列。 注意必须是手动提交模式
			if err := poc.Add(msg); err != nil {
				panic(err) //for test
			}
			//模拟消息处理
			go func(m *sarama.ConsumerMessage) {
				time.Sleep(time.Duration(rand.Int()%4) * time.Second) //模拟随机耗时

				if err := poc.MarkConsumed(m.Offset); err != nil {
					panic(err) // for test
				}
			}(msg)

		}
	}
}

type ParallelHandler struct {
	pph map[int32]*PartitionParallelHandler
}

type HashMsg interface {
	RawKey() []byte
}

type Processor interface {
	Input(HashMsg)
	Process()
	Next() Processor
}

type Stage struct {
	// worker pool
	workerSize int
	next       *Stage
}

func (s *Stage) Input(msgs HashMsg) {
	//todo buz logic
	//不同阶段实现不同的msg struct
	// idx := msg.RawKey() % s.workerSize
	//s.workerPool(idx).Input(s.Process)
}

func (s *Stage) Process() {
	//todo buz logic
}

type PartitionParallelHandler struct {
	poc *PartitionOffsetCommitter

	mu         sync.RWMutex
	processors map[string]Processor
}

func (pph *PartitionParallelHandler) AddStageProcessor(name string, p Processor) {
	pph.mu.Lock()
	defer pph.mu.Unlock()
	pph.processors[name] = p
}

var (
	pocs map[int32]*PartitionOffsetCommitter
	mu   sync.RWMutex
)

func init() {
	pocs = make(map[int32]*PartitionOffsetCommitter)
}

func onceInitPOC(partitionID int32, sess sarama.ConsumerGroupSession) {
	mu.Lock()
	defer mu.Unlock()
	_, ok := pocs[partitionID]
	if !ok {
		pocs[partitionID] = NewPartitionOffsetCommitter(partitionID, sess)
	}
}

func GetPOC(partitionID int32, sess sarama.ConsumerGroupSession) *PartitionOffsetCommitter {
	mu.Lock()
	defer mu.Unlock()
	_, ok := pocs[partitionID]
	if !ok {
		pocs[partitionID] = NewPartitionOffsetCommitter(partitionID, sess)
	}
	return pocs[partitionID]
}

func NewPartitionOffsetCommitter(partitionID int32, sess sarama.ConsumerGroupSession) *PartitionOffsetCommitter {
	poc := &PartitionOffsetCommitter{
		Partition:      partitionID,
		pq:             pq.New(),
		commitInterval: 1 * time.Second,
		sess:           sess,
		addCh:          make(chan *event, 20),
		markCh:         make(chan *event, 20),
	}

	go withRecover(poc.Run)

	return poc
}

type EvtType int

const (
	AddEvt EvtType = iota + 1
	DelEvt
)

type event struct {
	t      EvtType
	offset int64
	msg    *sarama.ConsumerMessage

	errChan chan error
}

type PartitionOffsetCommitter struct {
	pq *pq.PriorityQueue

	Partition int32
	addCh     chan *event
	markCh    chan *event

	commitInterval time.Duration
	sess           sarama.ConsumerGroupSession
}

func (poc *PartitionOffsetCommitter) Add(msg *sarama.ConsumerMessage) error {

	ch := make(chan error)
	poc.addCh <- &event{
		t:       AddEvt,
		msg:     msg,
		errChan: ch,
	}
	return <-ch
}

func (poc *PartitionOffsetCommitter) MarkConsumed(offset int64) error {

	ch := make(chan error)
	poc.markCh <- &event{
		t:       DelEvt,
		offset:  offset,
		errChan: ch,
	}
	return <-ch
}

func (poc *PartitionOffsetCommitter) Run() {

	ticker := time.NewTicker(poc.commitInterval)
	for {
		select {
		case evt := <-poc.addCh:
			poc.processEvt(evt)
		case evt := <-poc.markCh:
			poc.processEvt(evt)
		case <-ticker.C:
			poc.commit()
		}
	}
}
func (poc *PartitionOffsetCommitter) commit() {
	minItem := poc.pq.Peek()
	if minItem == nil {
		return
	}

	for {
		item, err := poc.pq.Pop()
		if errors.Is(err, pq.ErrEmpty) {
			break
		}

		qv := item.Value.(*ExtMsg)
		if !qv.mark {
			_ = poc.pq.Push(item)
			break
		}
		fmt.Println("【Commit Goroutine】 ", time.Now().Format("2006-01-02 15:04:05"), ", partitionID:", poc.Partition, ", offset:", qv.msg.Offset, ", val:", string(qv.msg.Value))
		poc.sess.MarkMessage(qv.msg, "")
	}

	poc.sess.Commit()
}

type ExtMsg struct {
	msg  *sarama.ConsumerMessage
	mark bool
}

func (poc *PartitionOffsetCommitter) processEvt(evt *event) {
	offset := evt.offset
	key := strconv.FormatInt(offset, 10)
	var err error

	defer func() {
		if evt.errChan != nil {
			evt.errChan <- err
		}
	}()

	if evt.t == AddEvt {
		err = poc.pq.Push(&pq.Item{
			Key:      key,
			Priority: offset,
			Value:    &ExtMsg{msg: evt.msg},
		})
		if err == nil {
			//fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "---->", "Recv Msg---->", poc.Partition, offset)
		}
	} else {
		item := poc.pq.PopByKey(key)
		if item == nil {
			return
		}
		qv := item.Value.(*ExtMsg) // 标记 被消费成功
		qv.mark = true
		err = poc.pq.Push(item)
	}
}
