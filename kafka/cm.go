package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// CKafkaConfig Ckafka配置项
type CKafkaConfig struct {
	Topic           []string   `json:"topic"`
	SASL            CKafkaSASL `json:"sasl"`
	Servers         []string   `json:"bootstrapServers"`
	ConsumerGroupId string     `json:"consumerGroupId"`
}

// CKafkaSASL sasl配置
type CKafkaSASL struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	InstanceId string `json:"instanceId"`
}

// ParseConfig 配置解析结构
func ParseConfig(configPath string) (*CKafkaConfig, error) {
	fileContent, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(fileContent)
	c := &CKafkaConfig{}
	decodeError := decoder.Decode(c)
	if decodeError != nil {
		return nil, decodeError
	}
	return c, nil
}

func main() {

	cfg, err := ParseConfig("./kafka.json")
	if err != nil {
		log.Fatal(err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		// 设置接入点，请通过控制台获取对应Topic的接入点。
		"bootstrap.servers": strings.Join(cfg.Servers, ","),
		// 设置的消息消费组
		"group.id":          cfg.ConsumerGroupId,
		"auto.offset.reset": "earliest",

		// 使用 Kafka 消费分组机制时，消费者超时时间。当 Broker 在该时间内没有收到消费者的心跳时，认为该消费者故障失败，Broker
		// 发起重新 Rebalance 过程。目前该值的配置必须在 Broker 配置group.min.session.timeout.ms=6000和group.max.session.timeout.ms=300000 之间
		"session.timeout.ms": 10000,
	})

	if err != nil {
		log.Fatal(err)
	}
	// 订阅的消息topic 列表
	err = c.SubscribeTopics(cfg.Topic, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// 客户端将自动尝试恢复所有的 error
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
