package entity

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

func AddConsumer2ConsumerGroup(topics []string, conf *ConsumerConf) error {

	client, err := NewConsumerClient(
		WithConsumerGroupRebalanceStrategies(sarama.NewBalanceStrategyRoundRobin()),
		WithChannelSize(conf.ChannelSize))
	if err != nil {
		return err
	}

	cg, err := sarama.NewConsumerGroupFromClient(conf.Group, client)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer func() {
			done <- struct{}{}
		}()

		for {
			if err := cg.Consume(ctx, topics, NewConsumerGroupHandle(ctx, conf)); err != nil {
				msg := fmt.Sprintf("consume failed, err: %+v, topic: %+v", err, topics)
				log.Println(msg)
				return
			}
			if ctx.Err() != nil {
				log.Println(ctx.Err())
				return
			}
		}
	}()

	<-done
	cancel()
	return cg.Close()
}
