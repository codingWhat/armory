package netpoll

import "github.com/codingWhat/armory/netpoll/poller"

type Desc struct {
	OnRead func()
	poller *poller.Epoll
}

func NewDesc() *Desc {
	return &Desc{}
}
