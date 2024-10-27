package simple

import (
	"sync/atomic"
	"unsafe"
)

// 存在内存问题，会开辟很多Node，GC
type Node struct {
	value interface{}
	next  unsafe.Pointer
}

type Queue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

func NewQueue() *Queue {
	n := unsafe.Pointer(&Node{})
	return &Queue{head: n, tail: n}
}

func (q *Queue) Enqueue(v interface{}) {
	n := &Node{value: v, next: nil}
	for {
		tail := q.tail
		next := (*Node)(atomic.LoadPointer(&tail)).next
		if tail == q.tail {
			if next == nil {
				if atomic.CompareAndSwapPointer(&(*Node)(tail).next, nil, unsafe.Pointer(n)) {
					atomic.CompareAndSwapPointer(&q.tail, tail, unsafe.Pointer(n))
					return
				}
			} else {
				atomic.CompareAndSwapPointer(&q.tail, tail, unsafe.Pointer(next))
			}
		}
	}
}

func (q *Queue) Dequeue() interface{} {
	for {
		head := q.head
		tail := q.tail
		next := (*Node)(atomic.LoadPointer(&head)).next
		if head == q.head {
			if head == tail {
				if next == nil {
					return nil
				}
				atomic.CompareAndSwapPointer(&q.tail, tail, unsafe.Pointer(next))
			} else {
				v := (*Node)(next).value
				if atomic.CompareAndSwapPointer(&q.head, head, unsafe.Pointer(next)) {
					return v
				}
			}
		}
	}
}
