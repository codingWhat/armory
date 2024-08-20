package entity

import (
	"github.com/IBM/sarama"
	"log"
)

// NewConsumerClient 新建kafka consumer client
func NewConsumerClient() (newClient sarama.Client, err error) {
	consumerConfig := NewConsumerConfig(AssignorRoundRobin, 1000)
	newClient, err = sarama.NewClient(KafkaConf.Brokers, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	return newClient, err
}
