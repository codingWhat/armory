package localcache

import (
	"sync"
	"time"

	"github.com/RussellLuo/timingwheel"
)

type expireKeyTimers struct {
	mu     sync.RWMutex
	timers map[string]*timingwheel.Timer

	tick      time.Duration
	wheelSize int64
	tw        *timingwheel.TimingWheel
}

func (eq *expireKeyTimers) set(key string, d time.Duration, f func()) {
	eq.mu.Lock()

	timer, ok := eq.timers[key]
	if ok {
		timer.Stop()
	}

	eq.timers[key] = eq.tw.AfterFunc(d, func() {
		f()
		eq.mu.Lock()
		delete(eq.timers, key)
		eq.mu.Unlock()
	})
	eq.mu.Unlock()
}

func (eq *expireKeyTimers) get(key string) *timingwheel.Timer {
	eq.mu.Lock()
	defer eq.mu.Unlock()
	return eq.timers[key]

}

func (eq *expireKeyTimers) remove(key string) {
	eq.mu.Lock()
	if timer, ok := eq.timers[key]; ok {
		timer.Stop()
		delete(eq.timers, key)
	}
	eq.mu.Unlock()
}

func (eq *expireKeyTimers) stop() {
	eq.tw.Stop()
}

func (eq *expireKeyTimers) clear() {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	if eq.tw != nil {
		eq.tw.Stop()
	}

	eq.timers = make(map[string]*timingwheel.Timer)
	eq.tw = timingwheel.NewTimingWheel(eq.tick, eq.wheelSize)

	eq.tw.Start()
}

func newExpireKeyTimers(tick time.Duration, wheelSize int64) *expireKeyTimers {
	eq := &expireKeyTimers{
		timers: make(map[string]*timingwheel.Timer),
	}

	tw := timingwheel.NewTimingWheel(tick, wheelSize)
	eq.tw = tw

	tw.Start()

	return eq
}
