//go:build darwin || dragonfly || freebsd || netbsd || openbsd
// +build darwin dragonfly freebsd netbsd openbsd

package poller

import (
	"fmt"
	"golang.org/x/sys/unix"
	"syscall"
)

type Epoll struct {
	Fd int
}

func (e *Epoll) Wait() {
	events := make([]syscall.Kevent_t, 100)
	for {
		n, err := syscall.Kevent(e.Fd, nil, events, nil)
		if err != nil {
			panic(err)
		}

		for i := 0; i < n; i++ {
			ev := events[i]
			if ev.Filter&syscall.EVFILT_READ != 0 {
				fmt.Println("Readable event")
			}
			if ev.Filter&unix.EVFILT_WRITE != 0 {
				fmt.Println("Writable event")
			}
		}
	}
}

func (e *Epoll) Add(fd int) error {
	event := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}
	_, err := syscall.Kevent(e.Fd, []syscall.Kevent_t{event}, nil, nil)
	return err
}

func New() *Epoll {
	// 创建kqueue实例
	fd, err := syscall.Kqueue()
	if err != nil {
		panic(err)
	}

	return &Epoll{Fd: fd}
}
