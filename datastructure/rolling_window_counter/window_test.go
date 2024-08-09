package rolling_window_counter

import (
	"fmt"
	"testing"
	"time"
)

func TestNewWindow(t *testing.T) {

	w := NewWindow(300*time.Millisecond, 3)
	fmt.Println("---->", len(w.buckets), w.bucketDuration)

	w.Add(0)

	time.Sleep(101 * time.Millisecond)
	w.Add(1)

	time.Sleep(101 * time.Millisecond)
	w.Add(2)

	time.Sleep(201 * time.Millisecond)

	w.Add(4)

	for i, bucket := range w.buckets {
		fmt.Println("Bucket:", i, ", cnt:", bucket.Counter, bucket.Point)
	}

}

func TestNewWindowAdd(t *testing.T) {

	w := NewWindow(300*time.Millisecond, 3)
	fmt.Println("---->", len(w.buckets), w.bucketDuration)

	w.Add(0)

	time.Sleep(101 * time.Millisecond)
	w.Add(1)

	time.Sleep(101 * time.Millisecond)
	w.Add(2)

	for i, bucket := range w.buckets {
		fmt.Println("Bucket:", i, ", cnt:", bucket.Counter, bucket.Point)
	}

}
