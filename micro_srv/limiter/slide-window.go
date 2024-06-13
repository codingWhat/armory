package limiter

import (
	"sync"
	"time"
)

/*
优点:
 - 精确限流
缺点:
 - 无法应对突发流量
*/

type SlideWindow struct {
	window     []int64
	windowSize time.Duration
	MaxCnt     int

	sync.Mutex
}

func NewSlideWindow(windowSize time.Duration, maxCnt int) *SlideWindow {
	return &SlideWindow{window: make([]int64, 0, maxCnt), windowSize: windowSize, MaxCnt: maxCnt}
}

func (s *SlideWindow) Allow() bool {

	s.Lock()
	defer s.Unlock()

	now := time.Now().Unix()

	for len(s.window) > 0 && now-s.window[0] >= int64(s.windowSize.Seconds()) {
		s.window = s.window[1:]
	}

	if len(s.window) >= s.MaxCnt {
		return false
	}
	s.window = append(s.window, now)
	return true
}
