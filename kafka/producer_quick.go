package main

import (
	"encoding/json"
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
	metrics.UseNilMetrics = true
	config.Producer.Retry.Backoff = 200 * time.Millisecond
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewHashPartitioner
	metrics.UseNilMetrics = true
	//config.Producer.Transaction.ID = "12"
	config.Metadata.Full = false

	config.Metadata.Retry.Max = 60

	metrics.NewRegistry()

	producer, err := sarama.NewAsyncProducer([]string{"21.6.115.7:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		producer.Close()
	}()

	for i := 0; i < 100; i++ {
		buzid := i + 1
		msg := map[string]interface{}{
			"info":  "sadas",
			"buzid": buzid,
		}
		v, _ := json.Marshal(msg)
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "quickstart-events",
			Key:   sarama.StringEncoder(strconv.Itoa(buzid)),
			Value: sarama.ByteEncoder(v),
		}
	}

	for {

		select {
		case s := <-producer.Successes():
			fmt.Println("---> success", s.Topic, s.Partition, s.Offset, s.Value)
		case err := <-producer.Errors():
			fmt.Println("----> error:", err.Error())
		}
	}
}
