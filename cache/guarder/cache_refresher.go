package guarder

import (
	"context"
	"github.com/RussellLuo/timingwheel"
	"sync"
	"sync/atomic"
	"time"
)

// RefreshTask 刷新缓存任务
type RefreshTask struct {
	isCron    bool
	name      string
	Run       func()
	interval  time.Duration
	refresher *CacheRefresher
	timer     *timingwheel.Timer
}

// NewTimerTask 定时刷新
func NewTimerTask(name string, ttl time.Duration, fn func(), r *CacheRefresher) *RefreshTask {
	return &RefreshTask{
		name:      name,
		interval:  ttl,
		Run:       fn,
		refresher: r,
	}
}

// NewCronTask 周期刷新
func NewCronTask(name string, ttl time.Duration, fn func(), r *CacheRefresher) *RefreshTask {
	return &RefreshTask{
		isCron:    true,
		name:      name,
		interval:  ttl,
		Run:       fn,
		refresher: r,
	}
}

func (t *RefreshTask) AfterRun(ctx context.Context) {
	if !t.isCron {
		return
	}
	t.refresher.AddTask(ctx, t)
}

// CacheRefresher  缓存刷新器
type CacheRefresher struct {
	tw    *timingwheel.TimingWheel
	tasks map[string]*RefreshTask
	mu    sync.RWMutex

	isClosed atomic.Bool
}

// NewRefresher 实例化缓存刷新器
func NewRefresher() *CacheRefresher {
	tw := timingwheel.NewTimingWheel(time.Second, 3600)
	tw.Start()
	return &CacheRefresher{
		tw:    tw,
		tasks: make(map[string]*RefreshTask),
	}
}

func (r *CacheRefresher) AddTask(ctx context.Context, task *RefreshTask) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	r.mu.Lock()
	task.timer = r.tw.AfterFunc(task.interval, func() {
		task.Run()
		task.AfterRun(ctx)

	})
	r.tasks[task.name] = task
	r.mu.Unlock()
}

func (r *CacheRefresher) RemoveTask(ctx context.Context, taskName string) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	r.mu.Lock()
	task, ok := r.tasks[taskName]
	if ok {
		task.timer.Stop()
		delete(r.tasks, taskName)
	}
	r.mu.Unlock()
}

func (r *CacheRefresher) Close() {
	if r.isClosed.Load() {
		return
	}

	r.isClosed.Store(true)
	r.tw.Stop()
	r.tasks = make(map[string]*RefreshTask)
}
