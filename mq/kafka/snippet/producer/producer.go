package producer

import "github.com/IBM/sarama"

func NewASyncProducer() (sarama.AsyncProducer, error) {
	return sarama.NewAsyncProducer(KafkaConf.Brokers, NewProducerConfig())
}
func NewSyncProducer() (sarama.SyncProducer, error) {
	return sarama.NewSyncProducer(KafkaConf.Brokers, NewProducerConfig())
}
