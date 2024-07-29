package static

import (
	"sync"
	"time"
)

/*
优点:
 - 实现简单
缺点:
 - 无法应对突发流量
 - 请求分布不均，可能会出现某些窗口超限的情况
*/

// 1s 5次
type FixedWindow struct {
	windowSize time.Duration
	maxCnt     int

	startTime int64
	curCnt    int
	sync.RWMutex
}

func NewFixedWindow(windowSize time.Duration, maxCnt int) *FixedWindow {
	return &FixedWindow{windowSize: windowSize, maxCnt: maxCnt, startTime: time.Now().Unix()}
}

func (f *FixedWindow) Allow() bool {

	f.Lock()
	defer f.Unlock()
	now := time.Now().Unix()
	if now-f.startTime >= int64(f.windowSize.Seconds()) {
		f.startTime = now
		f.curCnt = 0
	} else {
		if f.curCnt >= f.maxCnt {
			return false
		}
	}
	f.curCnt++
	return true
}
