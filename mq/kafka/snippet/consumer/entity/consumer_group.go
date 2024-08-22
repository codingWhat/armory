package entity

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"golang.org/x/time/rate"
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
	for {
		if err := c.limiter.Wait(c.ctx); err != nil {
			if errors.Is(err, c.ctx.Err()) {
				return errors.New("server shut down")
			}
		}

		select {
		case <-c.ctx.Done():
			return errors.New("server shut down")
		case <-sess.Context().Done():
			return errors.New("session close")
		case msg, ok := <-claim.Messages():
			if ok {
				// todo buz logic
				sess.MarkMessage(msg, "")
				sess.Commit()
			}
		}
	}
}
