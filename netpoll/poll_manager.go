package netpoll

import "github.com/codingWhat/armory/netpoll/poller"

type PollManager struct {
	lb LoadBalance

	loops int
}

func (pm *PollManager) PickPoller() *poller.Epoll {
	return pm.lb.Next().(*poller.Epoll)
}
