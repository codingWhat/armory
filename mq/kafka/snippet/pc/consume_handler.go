package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/codingWhat/armory/mq/kafka/snippet/consumer/pq"
	"golang.org/x/time/rate"
	"log"
	"runtime/debug"
	"strconv"
	"time"
)

type ConsumerGroupHandle struct {
	ctx context.Context
}

func NewConsumerGroupHandle(ctx context.Context) *ConsumerGroupHandle {
	return &ConsumerGroupHandle{
		ctx: ctx,
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

	//资源回收！！！
	pph := NewPartitionParallelHandler(sess, claim)
	pph.AddProcessor(NewBatchDataGroupStage(pph))
	pph.AddProcessor(NewBatchInsertDBStage(pph))

	defer pph.Close() //如果消费协程退出，则pph也退出。

	for {
		select {
		case <-c.ctx.Done():
			return errors.New("server shut down")
		case <-sess.Context().Done():
			return errors.New("session close")
		case msg, ok := <-claim.Messages():
			if !ok {
				return errors.New("claim message channel close")
			}
			if err := limiter.Wait(c.ctx); err != nil {
				return err
			}
			pph.Input(msg)
		}
	}
}

func resetTicker(ticker *time.Ticker, d time.Duration) {
	ticker.Stop()
	select {
	case <-ticker.C:
	default:
	}
	ticker.Reset(d)
}

func NewPartitionOffsetCommitter(partitionID int32, sess sarama.ConsumerGroupSession) *PartitionOffsetCommitter {
	poc := &PartitionOffsetCommitter{
		Partition:      partitionID,
		pq:             pq.New(),
		commitInterval: 1 * time.Second,
		sess:           sess,
		input:          make(chan *event, 20),
	}

	go poc.Run()
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
	input     chan *event

	commitInterval time.Duration
	sess           sarama.ConsumerGroupSession
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

func (poc *PartitionOffsetCommitter) MarkConsumed(offset int64) error {

	ch := make(chan error)
	poc.input <- &event{
		t:       DelEvt,
		offset:  offset,
		errChan: ch,
	}
	return <-ch
}

func (poc *PartitionOffsetCommitter) Run() {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			fmt.Println("----> PartitionOffsetCommitter Run recover:", err)
		}
	}()

	ticker := time.NewTicker(poc.commitInterval)
	for {
		select {
		case evt, ok := <-poc.input:
			if !ok {
				return
			}

			poc.processEvt(evt)
		case <-ticker.C:
			poc.commit()
		}
	}
}
func (poc *PartitionOffsetCommitter) commit() {
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

func (e *ExtMsg) RawKey() []byte {
	return e.msg.Key
}

func (poc *PartitionOffsetCommitter) processEvt(evt *event) {
	offset := evt.offset
	if evt.t == AddEvt {
		offset = evt.msg.Offset
	}
	key := strconv.FormatInt(offset, 10)
	var err error

	defer func() {
		if evt.errChan != nil {
			evt.errChan <- err
		}
	}()

	if evt.t == AddEvt {
		em := &ExtMsg{msg: evt.msg}
		err = poc.pq.Push(&pq.Item{
			Key:      key,
			Priority: offset,
			Value:    em,
		})
		if err == nil {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "---->", "Recv Msg---->", poc.Partition, offset)
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
