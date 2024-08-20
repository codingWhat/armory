package entity

import (
	"github.com/IBM/sarama"
	"log"
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

func NewConsumerConfig(assignor string, channelBufferSize int) *sarama.Config {
	version, err := sarama.ParseKafkaVersion(KafkaConf.Version)
	if err != nil {
		log.Fatalf("kafka's version is invalid: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	// 分区分配策略
	switch assignor {
	case AssignorSticky:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case AssignorRoundRobin:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case AssignorRange:
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		log.Fatalf("invalid partition assignor: %s", assignor)
	}

	//默认自动提交，不阻塞消费
	config.Consumer.Return.Errors = false
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.MaxProcessingTime = 10 * time.Second
	config.ChannelBufferSize = channelBufferSize // channel长度
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	//鉴权
	// 认证信息
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	//config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
	//	return nil
	//}
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.SASL.User = KafkaConf.User
	config.Net.SASL.Password = KafkaConf.Pwd

	return config
}
