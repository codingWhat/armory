package localcache

import (
	"container/list"
	"sync"
)

type RingBuffer interface {
	Push(item *list.Element)
}

// ringStripe is a singular ring buffer that is not concurrent safe.
type ringStripe struct {
	cons ringConsumer
	data []*list.Element
	capa int
}

func newRingStripe(cons ringConsumer, capa int64) *ringStripe {
	return &ringStripe{
		cons: cons,
		data: make([]*list.Element, 0, capa),
		capa: int(capa),
	}
}

// Push appends an item in the ring buffer and drains (copies items and
// sends to Consumer) if full.
func (s *ringStripe) Push(item *list.Element) {
	s.data = append(s.data, item)
	// Decide if the ring buffer should be drained.
	if len(s.data) >= s.capa {
		// Send elements to ringConsumer and create a new ring stripe.
		if s.cons.consume(s.data) {
			s.data = make([]*list.Element, 0, s.capa)
		} else {
			s.data = s.data[:0]
		}
	}
}

// ringBuffer stores multiple buffers (stripes) and distributes Pushed items
// between them to lower contention.
//
// This implements the "batching" process described in the BP-Wrapper paper
// (section III part A).
type ringBuffer struct {
	pool *sync.Pool
}

// newRingBuffer returns a striped ring buffer. The Consumer in ringConfig will
// be called when individual stripes are full and need to drain their elements.
func newRingBuffer(cons ringConsumer, capa int64) RingBuffer {
	// LOSSY buffers use a very simple sync.Pool for concurrently reusing
	// stripes. We do lose some stripes due to GC (unheld items in sync.Pool
	// are cleared), but the performance gains generally outweigh the small
	// percentage of elements lost. The performance primarily comes from
	// low-level runtime functions used in the standard library that aren't
	// available to us (such as runtime_procPin()).
	return &ringBuffer{
		pool: &sync.Pool{
			New: func() interface{} { return newRingStripe(cons, capa) },
		},
	}
}

// Push adds an element to one of the internal stripes and possibly drains if
// the stripe becomes full.
func (b *ringBuffer) Push(ele *list.Element) {
	// Reuse or create a new stripe.
	stripe := b.pool.Get().(*ringStripe)
	stripe.Push(ele)
	b.pool.Put(stripe)
}
