package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/codingWhat/armory/mq/kafka/snippet/consumer/pq"
	"golang.org/x/time/rate"
	"log"
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
			if ok {
				if limiter.Allow() {
					//todo msg dispatch
					fmt.Println("---->", msg)
				}
			}
		}
	}
}

var (
	pocs map[int32]*PartitionOffsetCommitter
	mu   sync.RWMutex
)

func init() {
	pocs = make(map[int32]*PartitionOffsetCommitter)
}

func GetPOC(partitionID int32) *PartitionOffsetCommitter {
	mu.Lock()
	defer mu.Unlock()
	_, ok := pocs[partitionID]
	if !ok {
		pocs[partitionID] = NewPartitionOffsetCommitter()
	}
	return pocs[partitionID]
}

func NewPartitionOffsetCommitter() *PartitionOffsetCommitter {
	return &PartitionOffsetCommitter{
		pq: pq.New(),
	}
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

	input chan *event

	sess sarama.ConsumerGroupSession
}

func (poc *PartitionOffsetCommitter) Offer(msg *sarama.ConsumerMessage) error {

	ch := make(chan error)
	poc.input <- &event{
		t:       AddEvt,
		msg:     msg,
		errChan: ch,
	}
	return <-ch
}

func (poc *PartitionOffsetCommitter) Remove(offset int64) error {

	ch := make(chan error)
	poc.input <- &event{
		t:       DelEvt,
		offset:  offset,
		errChan: ch,
	}
	return <-ch
}

func (poc *PartitionOffsetCommitter) Run() error {

	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case evt := <-poc.input:
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

		qv := item.Value.(*QVal)
		if !qv.mark {
			_ = poc.pq.Push(item)
			break
		}
		poc.sess.MarkMessage(qv.msg, "")

	}
	poc.sess.Commit()
}

type QVal struct {
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
			Value:    &QVal{msg: evt.msg},
		})
	} else {
		item := poc.pq.PopByKey(key)
		if item == nil {
			return
		}
		qv := item.Value.(*QVal) // 标记 被消费成功
		qv.mark = true
		err = poc.pq.Push(item)
	}
}
