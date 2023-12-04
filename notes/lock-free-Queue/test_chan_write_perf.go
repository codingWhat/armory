package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		t1_10us     = uint64(0) // 1-10微秒
		t10_100us   = uint64(0) // 10-100微秒
		t100_1000us = uint64(0) // 100-1000微秒
		t1_10ms     = uint64(0) // 1-10毫秒
		t10_100ms   = uint64(0) // 10-100毫秒
		t100_ms     = uint64(0) // 大于100毫秒
	)

	var (
		length   = 1024 * 1024
		goSize   = 100
		numPerGo = 10000
		counter  = uint64(0)
		slower   = uint64(0)
		wg       sync.WaitGroup
	)
	ch := make(chan uint64, length)
	// 消费端
	go func() {
		var ts time.Time
		var count int32
		for {
			x := <-ch
			atomic.AddInt32(&count, 1)
			if count == 1 {
				ts = time.Now()
			}
			if x%100000 == 0 {
				fmt.Printf("read %d\n", x)
			}
			if count == int32(goSize*numPerGo) {
				tl := time.Since(ts)
				fmt.Printf("read time = %d ms\n", tl.Milliseconds())
			}
		}
	}()
	wg.Add(goSize)
	totalS := time.Now()
	for i := 0; i < goSize; i++ {
		go func() {
			for j := 0; j < numPerGo; j++ {
				x := atomic.AddUint64(&counter, 1)
				ts := time.Now()
				ch <- x
				tl := time.Since(ts)
				ms := tl.Microseconds()
				if ms > 1 {
					atomic.AddUint64(&slower, 1)
					if ms < 10 { // t1_10us
						atomic.AddUint64(&t1_10us, 1)
					} else if ms < 100 {
						atomic.AddUint64(&t10_100us, 1)
					} else if ms < 1000 {
						atomic.AddUint64(&t100_1000us, 1)
					} else if ms < 10000 {
						atomic.AddUint64(&t1_10ms, 1)
					} else if ms < 100000 {
						atomic.AddUint64(&t10_100ms, 1)
					} else {
						atomic.AddUint64(&t100_ms, 1)
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	totalL := time.Since(totalS)
	fmt.Printf("write total time = [%d ms]\n", totalL.Milliseconds())
	time.Sleep(time.Second * 3)
	fmt.Printf("slow ratio = %.2f \n", float64(slower)*100.0/float64(counter))
	fmt.Printf("quick ratio = %.2f \n", float64(goSize*numPerGo-int(slower))*100.0/float64(goSize*numPerGo))
	fmt.Printf("[<1us][%d] \n", counter-slower)
	fmt.Printf("[1-10us][%d] \n", t1_10us)
	fmt.Printf("[10-100us][%d] \n", t10_100us)
	fmt.Printf("[100-1000us][%d] \n", t100_1000us)
	fmt.Printf("[1-10ms][%d] \n", t1_10ms)
	fmt.Printf("[10-100ms][%d] \n", t10_100ms)
	fmt.Printf("[>100ms][%d] \n", t100_ms)
}
