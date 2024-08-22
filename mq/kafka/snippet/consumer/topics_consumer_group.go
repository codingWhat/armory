package consumer

import (
	"github.com/codingWhat/armory/mq/kafka/snippet/consumer/entity"
)

// 单节点多topic 多partition消费

func main() {
	topicConsumeConf := entity.LoadTopicConsumeConfig()

	for topic, conf := range topicConsumeConf {
		ConsumeGroupForTopic(topic, &conf)
	}

	select {}
}

func ConsumeGroupForTopic(topic string, conf *entity.ConsumerConf) {
	for i := 0; i < conf.ConsumerNum; i++ {
		go func() {
			_ = entity.AddConsumer2ConsumerGroup([]string{topic}, conf)
		}()
	}
}

func ListenTopicConsumeConf() {
	// notify chan
	c := make(chan map[string]entity.ConsumerConf)
	go func() {
		for msg := range c {
			addTopicsAndGroup := msg
			for topic, conf := range addTopicsAndGroup {
				ConsumeGroupForTopic(topic, &conf)
			}

			entity.SetTopicConsumeConfig(addTopicsAndGroup)
		}
	}()
}
