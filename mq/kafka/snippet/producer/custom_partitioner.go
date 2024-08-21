package producer

import (
	"github.com/IBM/sarama"
	"github.com/codingWhat/armory/mq/kafka/snippet/producer/entity"
	"hash"
	"hash/crc32"
	"sync"
	"sync/atomic"
)

// 实现自定义动态配置hash槽
// 通过配置中心控制hash槽和partition的映射关系，来动态调整partition负载
func main() {
	config := entity.NewProducerConfig()
	config.Producer.Partitioner = NewCustomConsistentHashSlotPartitioner()
}

type ConsistentHashSlotPartitioner struct {
	random sarama.Partitioner
	hasher hash.Hash32

	slots []slot

	onceInit sync.Once
	sp       *SlotPartitionManager
}

type slot struct {
	ID          uint32
	partitionID atomic.Int32
}

func NewCustomConsistentHashSlotPartitioner() sarama.PartitionerConstructor {
	return func(topic string) sarama.Partitioner {
		return &ConsistentHashSlotPartitioner{
			random: sarama.NewRandomPartitioner(topic),
			hasher: crc32.NewIEEE(),
			slots:  make([]slot, 1024),
		}
	}
}

type SlotPartitionManager struct {
	slots   []*slot
	slotNum uint32

	partitionNum uint32
}

func (sp *SlotPartitionManager) ListenSlotInfo() {
	//todo listen from remote config center
	updateSlotPartitionInfo := map[int]int32{
		122: 1,
	}

	//动态调整
	for slotID, partitionID := range updateSlotPartitionInfo {
		sp.slots[slotID].partitionID.Store(partitionID)
	}
}

func (sp *SlotPartitionManager) getPartition(hashVal uint32) int32 {
	return sp.slots[hashVal%sp.slotNum].partitionID.Load()
}

func NewSlotPartitionManager(partitionNum uint32) *SlotPartitionManager {
	sp := &SlotPartitionManager{
		slots:        make([]*slot, 1024),
		slotNum:      1024,
		partitionNum: partitionNum,
	}
	step := sp.slotNum / (sp.partitionNum)
	for i := uint32(0); i < sp.slotNum; i++ {
		partitionID := (i / step) % sp.partitionNum
		s := &slot{ID: i}
		s.partitionID.Store(int32(partitionID))
		sp.slots[i] = s
	}

	return sp
}

func (c *ConsistentHashSlotPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return c.random.Partition(message, numPartitions)
	}

	c.onceInit.Do(func() {
		if c.sp == nil {
			c.sp = NewSlotPartitionManager(uint32(numPartitions))
		}
	})

	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	c.hasher.Reset()
	_, err = c.hasher.Write(bytes)
	if err != nil {
		return -1, err
	}

	return c.sp.getPartition(c.hasher.Sum32()), nil
}

func (c *ConsistentHashSlotPartitioner) RequiresConsistency() bool {
	return true
}
