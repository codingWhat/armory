package entity

import (
	"github.com/IBM/sarama"
	"log"
)

// NewConsumerClient 新建kafka consumer client
func NewConsumerClient(ops ...Option) (newClient sarama.Client, err error) {
	//consumerConfig := NewConsumerConfig(AssignorRoundRobin, 1000)
	consumerConfig := DefaultConsumerConfig()
	for _, op := range ops {
		op(consumerConfig)
	}
	newClient, err = sarama.NewClient(KafkaConf.Brokers, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	return newClient, err
}
