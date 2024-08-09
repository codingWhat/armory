package mq

import "context"

type Broker struct {
}

type Produce interface {
	AsyncBatchSend(context.Context)
}

type Producer struct {
}

type Consumer struct {
}
