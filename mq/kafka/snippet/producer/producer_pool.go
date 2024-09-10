package main

import (
	"github.com/codingWhat/armory/mq/kafka/snippet/producer/entity"
)

func main() {

	// 拉取远程配置，初始化producer池子
	if err := entity.InitProducerPoolWithRemoteConfig(); err != nil {
		panic(err)
	}
	defer entity.GetProducerPool().Close()

	/*
		     //本地自定义配置
			if err := entity.InitProducerPool(); err != nil {
				panic(err)
			}
			err := entity.GetProducerPool().SetProducer(&entity.ProducerConfig{
				Name: "producer1",
			})
			if err != nil {
				return
			}
	*/

	// 获取同步Producer
	p, err := entity.GetProducerPool().GetSyncProducer("parallel_consume")
	if err != nil {
		panic("get producer failed")
	}
	err = p.SendMessage("demotopic", "", "test msg")
	if err != nil {
		return
	}

}
