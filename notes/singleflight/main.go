package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type HandleFn func() (interface{}, error)

type call struct {
	sync.WaitGroup
	val interface{}
	err error
}

var (
	groups = make(map[string]*call)
	mu     sync.RWMutex
)

func main() {
	var wg sync.WaitGroup
	num := 5
	wg.Add(num)
	var cnt int32
	for i := 0; i < num; i++ {
		go func(gid int) {
			defer wg.Done()
			v, err := Do("key1", func() (interface{}, error) {
				queryDB(gid)
				atomic.AddInt32(&cnt, 1)
				return time.Now().Unix(), nil
			})
			fmt.Println("Goroutine:", gid, "----> get data ", v, err)
		}(i)
	}
	wg.Wait()
}

func queryDB(gid int) {
	// 模拟查询DB
	time.Sleep(1 * time.Second)
	fmt.Println("Goroutine:", gid, "---> querying DB .... ")
}

func Do(key string, fn HandleFn) (interface{}, error) {
	mu.Lock()
	w, ok := groups[key]
	if ok {
		mu.Unlock()
		w.Wait()
		return w.val, w.err
	}
	c := new(call)
	c.Add(1)
	groups[key] = c
	mu.Unlock()

	fmt.Println("--->call")
	c.val, c.err = fn()

	mu.Lock()
	c.Done()
	delete(groups, key)
	mu.Unlock()

	return c.val, c.err
}
