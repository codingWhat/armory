package entity

import (
	"errors"
	"github.com/IBM/sarama"
	"sync/atomic"
)

var _pI ProducerPool

// GetProducerPool 获取producer池
func GetProducerPool() ProducerPool {
	return _pI
}

// InitProducerInstances 初始化producer池
func InitProducerInstances(insConf *Conf, ops ...Option) (ProducerPool, error) {
	if _pI != nil {
		return _pI, nil
	}
	_pI = NewProducerInstancesPool()
	return _pI, nil
}

// NewProducerInstancesPool 自定义使用
func NewProducerInstancesPool() ProducerPool {
	return &producerInstances{
		async: newAsyncInstances(),
		sync:  newSyncInstances(),
	}
}

type ProducerPool interface {
	AddProducer(conf *Conf, ops ...Option) error
	GetAsyncProducer(name string) (*Producer, error)
	GetSyncProducer(name string) (*Producer, error)
	Close()
}

type Conf struct {
	Name      string
	BrokerIPs []string //从配置中心获取
	IsSync    bool
}

type producerInstances struct {
	async *asyncInstances
	sync  *syncInstances

	isClosed atomic.Bool
}

func (pi *producerInstances) AddProducer(insConf *Conf, ops ...Option) error {
	conf := DefaultProducerConfig()
	for _, op := range ops {
		op(conf)
	}

	if insConf.IsSync {
		p, err := sarama.NewSyncProducer(insConf.BrokerIPs, conf)
		if err != nil {
			return err
		}
		pi.sync.set(insConf.Name, p)
	} else {
		p, err := sarama.NewAsyncProducer(insConf.BrokerIPs, conf)
		if err != nil {
			return err
		}
		pi.async.set(insConf.Name, p)
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
