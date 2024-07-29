package rolling_window_counter

import (
	"fmt"
	"time"
)

type Bucket struct {
	BucketDuration time.Duration
	Point          []int
	Counter        int
	Next           *Bucket //核心
}

func (b *Bucket) Reset() {
	b.Counter = 0
	b.Point = b.Point[:0]
}

type Window struct {
	offset           int
	buckets          []*Bucket
	bucketSize       int
	Size             time.Duration
	bucketDuration   time.Duration
	lastAppendedTime time.Time
}

func NewWindow(size time.Duration, bucketSize int) *Window {
	w := &Window{
		buckets:          make([]*Bucket, bucketSize),
		Size:             size,
		bucketSize:       bucketSize,
		lastAppendedTime: time.Now(),
		offset:           0,
	}

	w.bucketDuration = size / time.Duration(bucketSize)
	for i := range w.buckets {
		w.buckets[i] = &Bucket{
			Point: make([]int, 0),
			//BucketDuration: bucketDuration,
			Counter: 0,
		}
	}

	for i := range w.buckets {
		next := i + 1
		if next == len(w.buckets) {
			next = 0
		}
		w.buckets[i].Next = w.buckets[next]
	}

	return w
}
func (w *Window) timespan() int {
	return int(time.Since(w.lastAppendedTime) / w.bucketDuration)

}

func (w *Window) ResetBuckets(start int, cnt int) {
	cur := w.buckets[start%w.bucketSize]
	for i := 0; i < cnt; i++ {
		b := cur.Counter
		cur.Reset()
		fmt.Println(i, "-->reseting", b, cur.Counter)
		cur = cur.Next
	}
}

func (w *Window) Add(val int) {

	ts := w.timespan()
	if ts > 0 {
		start := (w.offset + 1) % w.bucketSize
		end := (w.offset + ts) % w.bucketSize
		w.ResetBuckets(start, ts)
		w.offset = end
		w.lastAppendedTime = w.lastAppendedTime.Add(time.Duration(ts) * w.bucketDuration)
	}
	w.buckets[w.offset%w.bucketSize].Counter++
	w.buckets[w.offset%w.bucketSize].Point = append(w.buckets[w.offset%w.bucketSize].Point, val)
}
