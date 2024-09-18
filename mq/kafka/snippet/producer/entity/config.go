package entity

import (
	"github.com/IBM/sarama"
	"log"
)

type KafkaBaseConf struct {
	Brokers []string `json:"brokers"`
	User    string   `json:"user"`
	Pwd     string   `json:"pwd"`
	Version string   `json:"version"`
}

var KafkaConf *KafkaBaseConf

func InitKafkaConfigInfo() {
	// get 'key info' from local config
	// get config info from remote config center by 'key'
	KafkaConf = &KafkaBaseConf{
		Brokers: []string{""},
		User:    "",
		Pwd:     "",
	}
}
func LoadKafkaConfigInfo() *KafkaBaseConf {
	return KafkaConf
}

type Option func(config *sarama.Config)

func WithProducerRetryMax(maxRetry int) Option {
	return func(config *sarama.Config) {
		config.Producer.Retry.Max = maxRetry
	}
}

func WithProducerCompression(c sarama.CompressionCodec) Option {
	return func(config *sarama.Config) {
		config.Producer.Compression = c
	}
}

// DefaultProducerConfig 生产者默认配置
var DefaultProducerConfig = NewProducerConfig

// NewProducerConfig 生成生产者sarama config
func NewProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.MetricRegistry = nil
	version, err := sarama.ParseKafkaVersion(KafkaConf.Version)
	if err != nil {
		log.Fatalf("Error parsing Kafka version: %v", err)
	}
	config.Version = version
	config.Metadata.Full = true
	// 生产者相关设置
	config.Producer.Retry.Max = 3                    // 失败发送重试次数
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follow都确认, min.ins
	//config.Producer.Partitioner = sarama.NewRandomPartitioner // 默认走hash, 随机分配分区partition
	config.Producer.Return.Successes = true // 成功交付的消息将在success channel返回

	config.Producer.Compression = sarama.CompressionSnappy //消息压缩
	// 鉴权相关设置
	config.Net.SASL.Enable = true
	config.Producer.Idempotent = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	config.Net.SASL.User = KafkaConf.User
	config.Net.SASL.Password = KafkaConf.Pwd
	return config
}
