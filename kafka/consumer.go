package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"runtime"
	"strconv"
	"time"
)

func main() {

	config := sarama.NewConfig()
	//config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	//config.Consumer.Offsets.AutoCommit.Enable = false // 手动提交
	config.Consumer.Return.Errors = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_8_1_0
	config.Consumer.MaxProcessingTime = 10 * time.Second

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	consumeGrp, err := sarama.NewConsumerGroup([]string{"21.6.115.7:9092"}, "test_consume1", config)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc, _ := context.WithTimeout(context.Background(), 600*time.Millisecond)
	cc.Deadline()
	pauseCh := make(chan struct{})
	h := ConcurrentConsume{}
	for {

		select {
		case <-pauseCh:
			consumeGrp.PauseAll()
		default:

		}

		err := consumeGrp.Consume(ctx, []string{"test_topic"}, h)
		if err != nil {
			fmt.Println("consume err:", err.Error())
			return
		}

	}

}

// ConcurrentConsume 并行消费(！！！无顺序要求)
type ConcurrentConsume struct {
}

func (m ConcurrentConsume) Setup(session sarama.ConsumerGroupSession) error {

	return nil
}

func (m ConcurrentConsume) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (m ConcurrentConsume) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	//通过channel 来限流
	var limit chan byte
	limit = make(chan byte, 100) // channel 控制并发，每个partition 并发消费(！！！无顺序要求)
	gid := GetGoroutineID()
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			log.Printf("Message claimed: gid: %d, partition: %d, value = %s, timestamp = %v, topic = %s, offSet: %d", gid, message.Partition, string(message.Value), message.Timestamp, message.Topic, message.Offset)
			limit <- 1
		case <-session.Context().Done():
			return nil
		}
	}
}

func businessLogic(msg *sarama.ConsumerMessage) {

}

// GetGoroutineID 获取当前goroutine的ID
func GetGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	idField := bytes.Fields(b)[1]
	id, err := strconv.ParseUint(string(idField), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
