package entity

import (
	"github.com/IBM/sarama"
)

// NewConsumerClient 新建kafka consumer client
func NewConsumerClient(ops ...Option) (newClient sarama.Client, err error) {
	//consumerConfig := NewConsumerConfig(AssignorRoundRobin, 1000)
	consumerConfig := DefaultConsumerConfig()
	for _, op := range ops {
		op(consumerConfig)
	}

	return sarama.NewClient(KafkaConf.Brokers, consumerConfig)
}
