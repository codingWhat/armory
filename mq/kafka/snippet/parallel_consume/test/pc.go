package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/codingWhat/armory/mq/kafka/snippet/parallel_consume"
	"time"
)

func main() {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_8_1_0

	//conf.ChannelBufferSize = 10000
	//broker侧，每次取多少，失败处理
	conf.Consumer.Fetch.Max = 50000
	conf.Consumer.Retry.Backoff = 1 * time.Second

	//consumer侧
	conf.Consumer.MaxProcessingTime = 1 * time.Second
	conf.Consumer.Offsets.AutoCommit.Enable = false
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest

	//消费者组
	conf.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	cg, err := sarama.NewConsumerGroup([]string{""}, "pc_million", conf)
	if err != nil {
		return
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	for {
		err := cg.Consume(ctx, []string{"million"}, parallel_consume.NewConsumerGroupHandle(ctx))
		if err != nil {
			fmt.Println("----> Consume failed. err:", err.Error())
			continue
		}
	}

}
