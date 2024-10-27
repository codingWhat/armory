package guarder

import (
	"context"
	"github.com/RussellLuo/timingwheel"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	isCron    bool
	name      string
	Run       func()
	interval  time.Duration
	refresher *Refresher
	timer     *timingwheel.Timer
}

func NewTimerTask(name string, ttl time.Duration, fn func(), r *Refresher) *Task {
	return &Task{
		name:      name,
		interval:  ttl,
		Run:       fn,
		refresher: r,
	}
}

func NewCronTask(name string, ttl time.Duration, fn func(), r *Refresher) *Task {
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

type Refresher struct {
	tw *timingwheel.TimingWheel

	tasks map[string]*Task

	mu sync.RWMutex

	isClosed atomic.Bool
}

func NewRefresher() *Refresher {
	tw := timingwheel.NewTimingWheel(time.Second, 3600)
	tw.Start()
	return &Refresher{
		tw:    tw,
		tasks: make(map[string]*Task),
	}
}

func (r *Refresher) AddTask(ctx context.Context, task *Task) {
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

func (r *Refresher) RemoveTask(ctx context.Context, taskName string) {
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

func (r *Refresher) Close() {
	if r.isClosed.Load() {
		return
	}

	r.isClosed.Store(true)
	r.tw.Stop()
	r.tasks = make(map[string]*Task)
}
