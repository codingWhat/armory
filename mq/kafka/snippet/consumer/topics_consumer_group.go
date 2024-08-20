package consumer

import (
	"github.com/codingWhat/armory/mq/kafka/snippet/consumer/entity"
	"strconv"
)

// 单节点多topic 多partition消费

func main() {
	topicConsumeConf := entity.LoadTopicConsumeConfig()

	for topic, conf := range topicConsumeConf {
		group := conf[0]
		num, _ := strconv.Atoi(conf[1])
		ConsumeGroupForTopic(topic, group, num)
	}

	select {}
}

func ConsumeGroupForTopic(topic string, group string, num int) {
	for i := 0; i < num; i++ {
		go func() {
			_ = entity.AddConsumer2ConsumerGroup([]string{topic}, group)
		}()
	}
}

func ListenTopicConsumeConf() {
	// notify chan
	c := make(chan map[string][]string)
	go func() {
		for msg := range c {
			addTopicsAndGroup := msg
			for topic, conf := range addTopicsAndGroup {
				group := conf[0]
				num, _ := strconv.Atoi(conf[1])
				ConsumeGroupForTopic(topic, group, num)
			}

			entity.SetTopicConsumeConfig(addTopicsAndGroup)
		}
	}()
}
