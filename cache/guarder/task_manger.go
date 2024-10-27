package guarder

import (
	"context"
	"github.com/RussellLuo/timingwheel"
	"github.com/robfig/cron/v3"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	isCron    bool
	name      string
	Run       func()
	interval  time.Duration
	refresher *TaskManger
}

func NewTimerTask(name string, ttl time.Duration, fn func(), r *TaskManger) *Task {
	return &Task{
		name:      name,
		interval:  ttl,
		Run:       fn,
		refresher: r,
	}
}

func NewCronTask(name string, ttl time.Duration, fn func(), r *TaskManger) *Task {
	return &Task{
		isCron:    true,
		name:      name,
		interval:  ttl,
		Run:       fn,
		refresher: r,
	}
}

func (t *Task) AfterRun(ctx context.Context) {
	if !t.isCron {
		return
	}
	t.refresher.AddTask(ctx, t)
}

type TaskManger struct {
	tw   *timingwheel.TimingWheel
	cron *cron.Cron

	timers map[string]*timingwheel.Timer
	mu     sync.RWMutex

	isClosed atomic.Bool
}

func NewTaskManager() *TaskManger {
	tw := timingwheel.NewTimingWheel(time.Second, 3600)
	tw.Start()
	return &TaskManger{
		tw:     tw,
		timers: make(map[string]*timingwheel.Timer),
	}
}

func (r *TaskManger) AddTask(ctx context.Context, task *Task) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	r.mu.Lock()
	timer := r.tw.AfterFunc(task.interval, func() {
		task.Run()
		task.AfterRun(ctx)

	})
	r.timers[task.name] = timer
	r.mu.Unlock()
}

func (r *TaskManger) RemoveTask(ctx context.Context, taskName string) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	r.mu.Lock()
	timer, ok := r.timers[taskName]
	if ok {
		timer.Stop()
		delete(r.timers, taskName)
	}
	r.mu.Unlock()
}

func (r *TaskManger) Close() {
	if r.isClosed.Load() {
		return
	}

	r.isClosed.Store(true)
	r.tw.Stop()
	r.timers = make(map[string]*timingwheel.Timer)
}
