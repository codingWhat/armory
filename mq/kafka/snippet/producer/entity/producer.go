package entity

import (
	"errors"
	"github.com/IBM/sarama"
	"sync/atomic"
)

// GetProducerPool 获取producer池
func GetProducerPool() ProducerPool {
	return _pI
}

// InitProducerPool 初始化producer池
func InitProducerPool() error {
	if _pI != nil {
		return errors.New("pool has initialized")
	}
	_pI = NewProducerInstancesPool()
	return nil
}

// InitProducerPoolWithRemoteConfig 初始化producer池
func InitProducerPoolWithRemoteConfig() error {
	//1.远程配置: 多个produer实例配置
	//_ = LoadKafkaConfigInfo()
	//2.解析远程配置: 最终构造出[]*ProducerConfig

	var configs []*ProducerConfig
	if _pI != nil {
		return nil
	}
	_pI = NewProducerInstancesPool()
	for _, conf := range configs {
		if err := _pI.SetProducer(conf); err != nil {
			return err
		}
	}
	return nil
}

type ProducerConfig struct {
	Name      string   //区分不同producer
	BrokerIPs []string //从配置中心获取
	IsSync    bool     //同步or异步生产者标识

	Options []Option
}

type producerInstances struct {
	async *asyncInstances
	sync  *syncInstances

	isClosed atomic.Bool
}

func (pi *producerInstances) SetProducer(pConf *ProducerConfig) error {
	conf := DefaultProducerConfig()
	for _, op := range pConf.Options {
		op(conf)
	}

	if pConf.IsSync {
		p, err := sarama.NewSyncProducer(pConf.BrokerIPs, conf)
		if err != nil {
			return err
		}
		pi.sync.set(pConf.Name, p) // 关闭旧实例，并覆盖
	} else {
		p, err := sarama.NewAsyncProducer(pConf.BrokerIPs, conf)
		if err != nil {
			return err
		}
		pi.async.set(pConf.Name, p) //// 关闭旧实例，并覆盖
	}
	return nil
}
func (pi *producerInstances) Close() {
	if pi.isClosed.Load() {
		return
	}

	pi.isClosed.Store(true)
	pi.sync.close()
	pi.async.close()
}

var ErrProducerNotReg = errors.New("producer is not reg")

func (pi *producerInstances) GetAsyncProducer(name string) (*Producer, error) {
	p, ok := pi.async.get(name)
	if !ok {
		return nil, ErrProducerNotReg
	}
	return &Producer{ap: p}, nil
}

func (pi *producerInstances) GetSyncProducer(name string) (*Producer, error) {
	p, ok := pi.sync.get(name)
	if !ok {
		return nil, ErrProducerNotReg
	}
	return &Producer{sp: p, isSync: true}, nil
}
