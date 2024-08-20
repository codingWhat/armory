package producer

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"time"
)

// SendMessage 生产消息
func SendMessage(ctx context.Context, key, msg, topic string) error {
	producer, err := NewSyncProducer()
	if err != nil {
		log.Printf("new kafka produce err : %v", err)
		return err
	}
	msgX := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	if key != "" {
		msgX.Key = sarama.StringEncoder(key)
	}
	partition, offset, err := producer.SendMessage(msgX)
	if err != nil {
		log.Printf("send msg to kafka err : %v", err)
		return err
	}
	log.Printf("send msg(%s) to topic(%s) partition(%d) offset(%d) success", msg, topic, partition, offset)
	return producer.Close()
}

var (
	producerPool Pool
	err          error
)

func InitProducerPool(initSize, maxSize int, connMaxAge time.Duration) {
	producerPool, err = NewChannelPool(initSize, maxSize, connMaxAge, func() (Conn, error) {
		return NewASyncProducer()
	})
	if err != nil {
		panic(err)
	}
}

// SendMessages 批量生产消息
func SendMessages(ctx context.Context, topic string, msgs []string) error {
	// 这里可以考虑池化producer, 不过需要考虑连接保活
	conn, err := producerPool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	producer := conn.(*PoolConn).Conn.(sarama.AsyncProducer)
	go func() {
		msgsLen := len(msgs)
		delMsgLen := 0
		// [!important] 异步生产者发送后必须把返回值从 Errors 或者 Successes 中读出来 不然会阻塞 sarama 内部处理逻辑 导致只能发出去一条消息
		for {
			select {
			case s := <-producer.Successes():
				delMsgLen += 1
				if s != nil {
					log.Printf("[SendMessages] Success: topic:%v, partition:%d key:%v msg:%+v \n", s.Topic, s.Partition, s.Key, s.Value)
				}
			case e := <-producer.Errors():
				delMsgLen += 1
				if e != nil {
					log.Printf("[SendMessages] Errors：err:%v msg:%+v \n", e.Msg, e.Err)
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
		producer.Input() <- msgX
	}
	return nil
}
