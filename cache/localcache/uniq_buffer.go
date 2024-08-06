package localcache

import (
	"container/list"
)

type ringConsumer interface {
	consume([]*list.Element) bool
}

const buffersNum = 256

func newUniqRingBuffer(consumer ringConsumer, consumeThreshold int64) RingBuffer {
	urb := &uniqRingBuffer{
		buffers: make([]*UniqBuffer, buffersNum),
		size:    buffersNum,
	}

	for i := range urb.buffers {
		urb.buffers[i] = &UniqBuffer{
			uniq:             make(map[string]struct{}),
			data:             make([]*list.Element, 0, consumeThreshold),
			consumer:         consumer,
			consumeThreshold: consumeThreshold,
		}
	}

	return urb
}

type uniqRingBuffer struct {
	buffers []*UniqBuffer
	next    uint32
	size    int
}

func (urb *uniqRingBuffer) Push(ele *list.Element) {
	ub := urb.get()
	if !ub.Push(ele) {
		urb.advance()
	}
}

func (urb *uniqRingBuffer) get() *UniqBuffer {
	return urb.buffers[urb.next]
}

func (urb *uniqRingBuffer) advance() {
	urb.next = ((urb.next) + 1) % uint32(urb.size)
}

type UniqBuffer struct {
	uniq map[string]struct{}
	data []*list.Element

	accessCnt        int64
	consumeThreshold int64

	consumer ringConsumer
}

func (ub *UniqBuffer) reset() {
	ub.uniq = make(map[string]struct{})
	ub.data = ub.data[:0]
	ub.accessCnt = 0
}

func (ub *UniqBuffer) Push(ele *list.Element) bool {
	if ub.accessCnt >= ub.consumeThreshold {
		if ub.consumer.consume(ub.data) {
			ub.reset()
		}
		return false
	}

	ub.accessCnt++
	_, ok := ub.uniq[ele.Value.(*entry).key]
	if !ok {
		ub.data = append(ub.data, ele)
	}
	return true
}
