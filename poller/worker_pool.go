package event_poll

type WorkerPool []chan Task

type Task func() error
