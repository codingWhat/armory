package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	//测试百万消息积压，并行消费

	conf := sarama.NewConfig()
	conf.Version = sarama.V2_8_1_0

	//准备阶段:  批量批次
	conf.Producer.Flush.Messages = 1000
	conf.Producer.Flush.MaxMessages = 5000
	conf.Producer.Flush.Frequency = 1 * time.Second

	//请求阶段: 失败重试,
	conf.Producer.Retry.Max = 3
	conf.Producer.Retry.Backoff = 200 * time.Millisecond

	//响应阶段: 结果收集
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true

	//broker侧: 消息分区策略，消息幂等
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	conf.Producer.RequiredAcks = sarama.WaitForAll

	//conf.Producer.Idempotent = true   保证幂等了，但是吞吐就下降了。 消费者保证幂等
	//conf.Net.MaxOpenRequests = 1

	producer, err := sarama.NewAsyncProducer([]string{""}, conf)
	if err != nil {
		panic(err)
	}

	topic := "million"
	child := []*Comments{}
	var partition int32 = 8
	ch := make(chan struct{})
	go func() {
		defer func() {
			ch <- struct{}{}
		}()

		for i := 1; i <= 1000000; i++ {
			p := rand.Int() % 200000
			end := i + 3
			for i >= 200000 && i < end {
				child = append(child, &Comments{
					ID:       int64(i),
					Parentid: int64(p),
					Content:  "test comments: " + strconv.Itoa(i),
					Targetid: 1000001019,
				})
				i++
			}

			if len(child) > 0 {
				for _, c := range child {
					v, _ := json.Marshal(c)
					producer.Input() <- &sarama.ProducerMessage{
						Topic:     topic,
						Value:     sarama.StringEncoder(v),
						Key:       sarama.StringEncoder("1000001019"),
						Partition: partition,
					}
				}
				child = child[:0]
			} else {
				c := &Comments{
					ID:       int64(i),
					Parentid: 0,
					Content:  "test comments: " + strconv.Itoa(i),
					Targetid: 1000001019,
				}
				v, _ := json.Marshal(c)
				producer.Input() <- &sarama.ProducerMessage{
					Topic:     topic,
					Value:     sarama.StringEncoder(v),
					Key:       sarama.StringEncoder("1000001019"),
					Partition: partition,
				}
			}

		}
	}()

	go func() {
		for m := range producer.Successes() {
			fmt.Println("success-->", m.Key, m.Value)
		}
	}()

	go func() {
		for m := range producer.Errors() {
			fmt.Println("error-->", m.Error(), m.Msg.Value)
		}
	}()

	<-ch
	fmt.Println("finish, after 3s exit")
	time.Sleep(30 * time.Second)
}

type Comments struct {
	ID       int64  `json:"commentid"`
	Content  string ` json:"content"`
	Targetid int    `json:"targetid"`
	Parentid int64  ` json:"parentid"`
}
