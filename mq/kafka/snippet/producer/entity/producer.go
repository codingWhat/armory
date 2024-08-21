package entity

import (
	"github.com/IBM/sarama"
)

type InstanceConf struct {
	Name      string
	BrokerIPs []string //从配置中心获取
	IsSync    bool
}

func InitProducerInstances(insConf *InstanceConf, ops ...Option) error {
	conf := DefaultProducerConfig()
	for _, op := range ops {
		op(conf)
	}

	if insConf.IsSync {
		p, err := sarama.NewSyncProducer(insConf.BrokerIPs, conf)
		if err != nil {
			return err
		}
		_PI.sync.set(insConf.Name, p)
	} else {
		p, err := sarama.NewAsyncProducer(insConf.BrokerIPs, conf)
		if err != nil {
			return err
		}
		_PI.async.set(insConf.Name, p)
	}
	return nil
}

func CloseProducerInstances() {
	_PI.close()
}

func GetAsyncProducer(name string) *Producer {
	p, ok := _PI.async.get(name)
	if !ok {
		panic("kafka instance:" + name + " is not reg")
	}
	return &Producer{ap: p}
}

func GetSyncProducer(name string) *Producer {
	p, ok := _PI.sync.get(name)
	if !ok {
		panic("kafka instance:" + name + " is not reg")
	}
	return &Producer{sp: p, isSync: true}
}
