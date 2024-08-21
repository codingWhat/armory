package entity

import (
	"github.com/IBM/sarama"
	"sync/atomic"
	"time"
)

var TopicConsumeConfig atomic.Value // topic => config

func InitKafkaConsumeConfig() {
	// get 'key info' from local config
	// get config info from remote config center by 'key'

	// handle the add or delete ops, then set `TopicConsumeConfig`
	TopicConsumeConfig.Store(make(map[string][]string))

}

func LoadTopicConsumeConfig() map[string][]string {
	return TopicConsumeConfig.Load().(map[string][]string)
}

func SetTopicConsumeConfig(val map[string][]string) {
	TopicConsumeConfig.Store(val)
}

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

const (
	// 消费者分区分配策略
	AssignorSticky     = "sticky"     // 分区分配策略 sticky
	AssignorRoundRobin = "roundrobin" // 分区分配策略 roundrobin
	AssignorRange      = "range"      // 分区分配策略 range
)

type Option func(config *sarama.Config)

func WithConsumerGroupRebalanceStrategies(strategy sarama.BalanceStrategy) Option {
	return func(config *sarama.Config) {
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{strategy}
	}
}

func WithProducerCompression(c sarama.CompressionCodec) Option {
	return func(config *sarama.Config) {
		config.Producer.Compression = c
	}
}

func WithMaxProcessTime(t time.Duration) Option {
	return func(config *sarama.Config) {
		config.Consumer.MaxProcessingTime = t
	}
}

func WithKafkaVersion(version sarama.KafkaVersion) Option {
	return func(config *sarama.Config) {
		config.Version = version
	}
}

func WithSASL(user, pwd string) Option {
	return func(config *sarama.Config) {
		// 认证信息
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.User = KafkaConf.User
		config.Net.SASL.Password = KafkaConf.Pwd
	}
}

func WithChannelSize(size int) Option {
	return func(config *sarama.Config) {
		config.ChannelBufferSize = size
	}
}

func DefaultConsumerConfig() *sarama.Config {

	//config.Consumer.Return.Errors = false // 单独消费者时，需要读取
	/*
	  partitionConsumer, err := consumer.ConsumePartition() -> for err := range partitionConsumer.Errors(){ }
	*/

	//配置初始化
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.ChannelBufferSize = 200 // channel长度

	//消费者拉取阶段
	config.Consumer.Fetch.Min = 1 // "定期定量"原则，最少1字节，就返回
	config.Consumer.Retry.Backoff = 2 * time.Second

	//消费者处理阶段
	config.Consumer.MaxProcessingTime = 3 * time.Second

	//消费者提交阶段; 默认自动提交，不阻塞； 异步尝试提交三次
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.Retry.Max = 3
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	//消费组配置
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Group.Rebalance.Retry.Max = 4
	config.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second

	config.Consumer.Group.Rebalance.Timeout = 60 * time.Second //rebalance之后，如果60s之后还没有加入的，就从group剔除。

	return config
}
