package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"strconv"
	"time"
)

func main() {
	config := sarama.NewConfig()
	//config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	//config.Consumer.Offsets.AutoCommit.Enable = false // 手动提交
	config.Version = sarama.V2_8_1_0
	config.Net.MaxOpenRequests = 1
	//config.Producer.Idempotent = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 4
	config.Producer.Retry.Backoff = 200 * time.Millisecond
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Messages = 100
	config.Producer.Partitioner = sarama.NewHashPartitioner
	metrics.UseNilMetrics = true
	//config.Producer.Transaction.ID = "12"
	config.Metadata.Full = false

	producer, err := sarama.NewAsyncProducer([]string{"21.6.115.7:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		producer.Close()
	}()

	for i := 0; i < 10; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "test_topic",
			Key:   sarama.StringEncoder(strconv.Itoa(i)),
			Value: sarama.StringEncoder("new add partition, " + strconv.Itoa(i)),
		}
	}

	for {
		select {
		case s := <-producer.Successes():
			fmt.Println("---> success", s.Topic, s.Partition, s.Offset, s.Value)
		}
	}
}
