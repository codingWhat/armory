// Package epoll
package event_poll

import (
	"golang.org/x/sys/unix"
	"log"
	"syscall"
)

type Poller struct {
	kFd    int
	s      *Server
	events []syscall.Kevent_t
}

func NewPoller() (*Poller, error) {
	kqueueFd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}

	_ = syscall.Kevent_t{
		Ident:  uint64(kqueueFd),
		Filter: syscall.EVFILT_VNODE,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE | syscall.EV_CLEAR,
		Fflags: syscall.NOTE_WRITE,
	}

	return &Poller{
		kFd:    kqueueFd,
		events: make([]syscall.Kevent_t, 1),
	}, nil

}

func (p *Poller) AddReadEvent(fd int32) error {
	ev := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
	}

	_, err := syscall.Kevent(p.kFd, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

func (p *Poller) AddWriteEvent(fd int32) error {
	ev := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_WRITE,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
	}

	_, err := syscall.Kevent(p.kFd, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

func (p *Poller) ClearEvent(fd int32) error {
	ev := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_CLEAR,
	}

	_, err := syscall.Kevent(p.kFd, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

func (p *Poller) WaitEvt() (int, error) {
	return syscall.Kevent(p.kFd, nil, p.events, nil)
}

func (p *Poller) loopRead() {

}

const EVFilterSock = -0xd

func (p *Poller) Polling(callback func(fd int, filter int16) error) (err error) {
	el := newEventList(64)
	var wakenUp bool
	for {
		n, err0 := unix.Kevent(p.kFd, nil, el.events, nil)
		if err0 != nil && err0 != unix.EINTR {
			log.Println(err0)
			continue
		}
		var evFilter int16
		for i := 0; i < n; i++ {
			if fd := int(el.events[i].Ident); fd != 0 {
				evFilter = el.events[i].Filter
				if (el.events[i].Flags&unix.EV_EOF != 0) || (el.events[i].Flags&unix.EV_ERROR != 0) {
					evFilter = EVFilterSock
				}
				if err = callback(fd, evFilter); err != nil {
					return
				}
			} else {
				wakenUp = true
			}
		}
		if wakenUp {
			wakenUp = false
			//if err = p.asyncJobQueue.ForEach(); err != nil {
			//	return
			//}
		}

		if n == el.size {
			el.increase()
		}
	}
}

// ModRead renews the given file-descriptor with readable event in the poller.
func (p *Poller) ModRead(fd int) error {
	if _, err := unix.Kevent(p.kFd, []unix.Kevent_t{
		{Ident: uint64(fd), Flags: unix.EV_DELETE, Filter: unix.EVFILT_WRITE}}, nil, nil); err != nil {
		return err
	}
	return nil
}

// Delete removes the given file-descriptor from the poller.
func (p *Poller) Delete(fd int) error {
	event := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: unix.EVFILT_READ,
		Flags:  unix.EV_DELETE,
	}
	// 移除文件描述符的事件监听
	_, err := unix.Kevent(p.kFd, []unix.Kevent_t{event}, nil, nil)
	return err
}

type eventList struct {
	size   int
	events []unix.Kevent_t
}

func newEventList(size int) *eventList {
	return &eventList{size, make([]unix.Kevent_t, size)}
}

func (el *eventList) increase() {
	el.size <<= 1
	el.events = make([]unix.Kevent_t, el.size)
}

//func (p *Poller) HandleEvt(i int, task EventTask) {
//	c := p.s.GetConnByFD(int(p.events[i].Ident))
//	if c == nil {
//		return
//	}
//
//	if p.events[i].Filter == syscall.EVFILT_READ {
//		_ = task(c)
//	} else if p.events[i].Filter&syscall.EV_ERROR != 0 {
//		c.Close()
//	}
//}
