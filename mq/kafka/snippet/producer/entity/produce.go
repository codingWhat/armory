package entity

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"log"
)

type Producer struct {
	isSync bool
	ap     sarama.AsyncProducer
	sp     sarama.SyncProducer
}

// SendMessage 生产消息
func (p *Producer) SendMessage(key, msg, topic string) error {
	if !p.isSync {
		return errors.New(" only be called by sync-producer")
	}

	msgX := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	if key != "" {
		msgX.Key = sarama.StringEncoder(key)
	}
	_, _, err := p.sp.SendMessage(msgX)
	return err
}

// BatchSendMessages 批量生产消息
func (p *Producer) BatchSendMessages(ctx context.Context, topic string, msgs []string) error {
	if p.isSync {
		return errors.New(" only be called by async-producer")
	}

	go func() {
		msgsLen := len(msgs)
		delMsgLen := 0
		// [!important] 异步生产者发送后必须把返回值从 Errors 或者 Successes 中读出来 不然会阻塞 sarama 内部处理逻辑 导致只能发出去一条消息
		for {
			select {
			case s := <-p.ap.Successes():
				delMsgLen += 1
				if s != nil {
					log.Printf("[BatchSendMessages] Success: topic:%v, partition:%d key:%v msg:%+v \n", s.Topic, s.Partition, s.Key, s.Value)
				}
			case e := <-p.ap.Errors():
				delMsgLen += 1
				if e != nil {
					log.Printf("[BatchSendMessages] Errors：err:%v msg:%+v \n", e.Msg, e.Err)
				}
			}
			if delMsgLen >= msgsLen {
				break
			}
		}
	}()

	// 批量生产消息
	for _, msg := range msgs {
		msgX := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		p.ap.Input() <- msgX
	}
	return nil
}
