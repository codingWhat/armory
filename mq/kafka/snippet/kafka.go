package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"time"
)

func main() {
	// 验证consume group handler 一个session共用一个
	// 验证maxProcessingTime
	config := sarama.NewConfig()
	config.ChannelBufferSize = 10

	config.Version = sarama.V2_8_1_0

	config.Consumer.Fetch.Max = 500
	config.Consumer.Retry.Backoff = 2 * time.Second
	config.Consumer.MaxProcessingTime = 3 * time.Second //单条消息处理时间 * channel size + buffer(重试所需); 单条消息处理时间=fetch max/(单条处理速度 * [线程数])

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	cg, err := sarama.NewConsumerGroup([]string{"21.6.115.7:9092"}, "test_handler", config)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		err = cg.Consume(ctx, []string{"test_topic"}, &ConsumeHandler{})
		if err != nil {
			panic(err)
		}
	}

}

type ConsumeHandler struct {
	id int
}

func (c *ConsumeHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Setup--->", session.MemberID())
	return nil
}

func (c *ConsumeHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Cleanup--->", session.MemberID())
	return nil
}

func (c *ConsumeHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	fmt.Println(claim.Partition(), "ConsumeClaim--->")
	fmt.Printf("ConsumeClaim---> %p, %p \n", c, claim)
	for msg := range claim.Messages() {
		fmt.Println("Consume--->", msg.Partition, msg.Offset)
		session.MarkMessage(msg, "")
	}
	return nil
}
