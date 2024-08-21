package entity

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

func AddConsumer2ConsumerGroup(topics []string, group string) error {

	client, err := NewConsumerClient(
		WithConsumerGroupRebalanceStrategies(sarama.NewBalanceStrategyRoundRobin()),
		WithChannelSize(1000))
	if err != nil {
		return err
	}

	cg, err := sarama.NewConsumerGroupFromClient(group, client)
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
			if err := cg.Consume(ctx, topics, &ConsumerGroupHandle{}); err != nil {
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

type ConsumerGroupHandle struct {
}

func (c *ConsumerGroupHandle) Setup(session sarama.ConsumerGroupSession) error {
	//TODO implement me
	panic("implement me")
}

func (c *ConsumerGroupHandle) Cleanup(session sarama.ConsumerGroupSession) error {
	//TODO implement me
	panic("implement me")
}

func (c *ConsumerGroupHandle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// todo buz logic
		session.MarkMessage(msg, "")
		session.Commit()
	}
	return nil
}
