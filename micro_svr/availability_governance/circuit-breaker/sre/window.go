package sre

import (
	"errors"
	"sync"
	"time"
)

type Bucket struct {
	Total   int64
	Success int64
	Next    *Bucket
}

type Window struct {
	buckets    []*Bucket
	bucketSize int
}

func NewWindow(bucketSize int) *Window {
	w := &Window{buckets: make([]*Bucket, bucketSize)}

	for i := 0; i < bucketSize; i++ {
		b := Bucket{}
		w.buckets[i] = &b
		if i == bucketSize-1 {
			w.buckets[i].Next = w.buckets[0]
		}
		if i > 0 {
			w.buckets[i-1].Next = w.buckets[i]
		}
	}
	return w
}

func (w *Window) Add(offset int, val int64) {
	w.buckets[offset].Success += val
	w.buckets[offset].Total++
}
func (w *Window) ResetBucket(i int) {
	w.buckets[i].Success = 0
	w.buckets[i].Total = 0
}

func (w *Window) Iterator(offset, count int) *BucketIterator {
	return &BucketIterator{
		cur:         w.buckets[offset],
		count:       count,
		iteratedCnt: 0,
	}
}

type RollingCounter struct {
	sync.RWMutex
	window         *Window
	windowDuration time.Duration
	offset         int
	bucketNum      int
	bucketDuration time.Duration
	lastAppendTime time.Time
}

func (r *RollingCounter) timespan() int {
	v := int(time.Since(r.lastAppendTime) / r.bucketDuration)
	if v > -1 { // maybe time backwards
		return v
	}
	return r.bucketNum
}

type BucketIterator struct {
	count       int
	iteratedCnt int
	cur         *Bucket
}

func (b *BucketIterator) IsValid() bool {
	return b.iteratedCnt != b.count
}

func (b *BucketIterator) GetAndNext() (Bucket, error) {
	if !b.IsValid() {
		return Bucket{}, errors.New("current item is invalid")
	}
	bucket := *b.cur
	b.iteratedCnt++
	b.cur = b.cur.Next

	return bucket, nil

}

func (r *RollingCounter) Reduce(fn func(iterator *BucketIterator) float64) float64 {
	r.RLock()
	timespan := r.timespan()
	var val float64
	if count := r.bucketNum - timespan; count > 0 {
		offset := r.offset + timespan + 1
		if offset >= r.bucketNum {
			offset = offset - r.bucketNum
		}
		val = fn(r.window.Iterator(offset, count))
	}
	r.RUnlock()
	return val
}

func (r *RollingCounter) Add(val int64) {
	r.Lock()
	defer r.Unlock()

	timespan := r.timespan()
	if timespan > 0 {
		r.lastAppendTime = r.lastAppendTime.Add(time.Duration(timespan * int(r.bucketDuration)))
		offset := r.offset
		// reset the expired buckets
		s := offset + 1
		if timespan > r.bucketNum {
			timespan = r.bucketNum
		}
		e, e1 := s+timespan, 0 // e: reset offset must start from offset+1
		if e > r.bucketNum {
			e1 = e - r.bucketNum
			e = r.bucketNum
		}
		for i := s; i < e; i++ {
			r.window.ResetBucket(i)
			offset = i
		}
		for i := 0; i < e1; i++ {
			r.window.ResetBucket(i)
			offset = i
		}
		r.offset = offset
	}

	r.window.Add(r.offset, val)
}

func NewRollingCounter(sampleDuration time.Duration, bucketsNum int) *RollingCounter {
	bucketDuration := sampleDuration / time.Duration(bucketsNum)
	return &RollingCounter{
		window:         NewWindow(bucketsNum),
		bucketDuration: bucketDuration,
		lastAppendTime: time.Now(),
		offset:         0,
		bucketNum:      bucketsNum,
		windowDuration: sampleDuration,
	}
}
