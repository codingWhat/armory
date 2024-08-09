package sre

import "time"

type Breaker struct {
	stat *RollingCounter
}

func (b *Breaker) ReportRet(err error) {
	if err == nil {
		b.stat.Add(1)
		return
	}
	b.stat.Add(0)
}

func NewBreaker() *Breaker {
	return &Breaker{stat: NewRollingCounter(
		5*time.Second, 100,
	)}
}
