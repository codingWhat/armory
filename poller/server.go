package event_poll

import (
	"log"
	"math"
	"net"
	"runtime"
	"sync"

	prb "github.com/codingWhat/smalltoy/pool/ringbuffer"
	"github.com/codingWhat/smalltoy/ringbuffer"
	"golang.org/x/sys/unix"
)

type EventHandler interface {
	OnOpened(c Conn) (out []byte)
	OnClosed(c Conn, err error)
	React(frame []byte, c Conn) (out []byte)
}

type ICodec interface {
	Decode(conn *conn) ([]byte, error)
	Encode(src []byte) []byte
}

type Handler interface {
	Handle([]byte) ([]byte, error)
}

type EventTask func(conn *Conn) error

type Conn struct {
	ID int
	FD int
	c  *net.TCPConn
	d  []byte
}

func (cc *Conn) Close() {
	_ = cc.c.Close()
}

func (cc *Conn) Write(b []byte) (int, error) {
	return cc.c.Write(b)
}

type reactorGroup interface {
	next() *eventLoop
	register(loop *eventLoop)
	iterate(func(int, *eventLoop) error)
}

type subReactorGroups struct {
	iterateIdx int64
	r          []*eventLoop
}

func (s *subReactorGroups) init(num int) {
	s.iterateIdx = 0
	s.r = make([]*eventLoop, num)
}

func (s *subReactorGroups) register(i int, loop *eventLoop) {
	s.r[i] = loop
}

func (s *subReactorGroups) iterate(fn func(i int, e *eventLoop)) {
	for i, e := range s.r {
		fn(i, e)
	}
}

func (s *subReactorGroups) next() *eventLoop {
	if s.iterateIdx+1 == math.MaxInt64 {
		s.iterateIdx = 0
	}

	defer func() {
		s.iterateIdx++
	}()
	return s.r[s.iterateIdx]
}

type conn struct {
	fd    int
	codec ICodec

	inboundBuffer  *ringbuffer.RingBuffer // buffer for data from client
	outboundBuffer *ringbuffer.RingBuffer // buffer for data that is ready to write to client
}

func (c *conn) releaseTCP() {
	//c.opened = false

	//c.buffer = nil
	prb.Put(c.inboundBuffer)
	prb.Put(c.outboundBuffer)
	c.inboundBuffer = nil
	c.outboundBuffer = nil
	//bytebuffer.Put(c.byteBuffer)
	//c.byteBuffer = nil
}

func (c *conn) read() ([]byte, error) {
	return c.codec.Decode(c)
}

type eventLoop struct {
	poller       *Poller
	s            *Server
	eventHandler EventHandler // user eventHandler
	connections  map[int]*conn
}

func (e *eventLoop) loopRead() {

}

func (e *eventLoop) loopCloseConn(c *conn, err error) error {
	err0, err1 := e.poller.Delete(c.fd), unix.Close(c.fd)
	if err0 == nil && err1 == nil {
		delete(e.connections, c.fd)
		//switch e.eventHandler.OnClosed(c, err) {
		//case Shutdown:
		//	return ErrServerShutdown
		//}
		c.releaseTCP()
	} else {
		if err0 != nil {
			log.Printf("failed to delete fd:%d from poller, error:%v\n", c.fd, err0)
		}
		if err1 != nil {
			log.Printf("failed to close fd:%d, error:%v\n", c.fd, err1)
		}
	}
	return nil
}

func (e *eventLoop) loopWrite(c *conn) error {
	//e.eventHandler.PreWrite()

	head, tail := c.outboundBuffer.LazyReadAll()
	n, err := unix.Write(c.fd, head)
	if err != nil {
		if err == unix.EAGAIN {
			return nil
		}
		return e.loopCloseConn(c, err)
	}
	c.outboundBuffer.Shift(n)

	if len(head) == n && tail != nil {
		n, err = unix.Write(c.fd, tail)
		if err != nil {
			if err == unix.EAGAIN {
				return nil
			}
			return e.loopCloseConn(c, err)
		}
		c.outboundBuffer.Shift(n)
	}

	if c.outboundBuffer.IsEmpty() {
		_ = e.poller.ModRead(c.fd)
	}
	return nil
}

type Server struct {
	wg       sync.WaitGroup
	acceptFD int

	subReactors reactorGroup
	mainReactor *eventLoop

	workers WorkerPool

	codec ICodec
}

func (s *Server) Start() error {

	//启动subReactor
	err := s.activeSubReactors()
	if err != nil {
		return err
	}

	s.wg.Add(1)
	err = s.activeMainReactors()
	if err != nil {
		return err
	}

	s.wg.Wait()
	//启动mainReactor, - accept connection

	return nil
}

func (s *Server) activeSubReactors() error {

	for i := 0; i < runtime.NumCPU(); i++ {
		p, err := NewPoller()
		if err != nil {
			return err
		}
		e := &eventLoop{
			poller: p,
		}
		s.subReactors.register(e)
	}

	return nil
}

func (s *Server) activeMainReactors() error {
	defer func() {
		s.wg.Done()
	}()

	p, err := NewPoller()
	if err != nil {
		return err
	}
	s.mainReactor = &eventLoop{
		poller: p,
	}

	return s.mainReactor.poller.Polling(func(fd int, filter int16) error {
		connFD, _, err := unix.Accept(fd)
		if err != nil {
			if err == unix.EAGAIN {
				return nil
			}
			return err
		}
		if err := unix.SetNonblock(connFD, true); err != nil {
			return err
		}
		el := s.subReactors.next()
		return el.poller.AddReadEvent(int32(connFD))
	})
}

func (s *Server) Stop() {

}

func Serve(eventHandler EventHandler, addr string) error {
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	tcpListen := listen.(*net.TCPListener)
	file, err := tcpListen.File()
	if err != nil {
		tcpListen.Close()
		return err
	}

	err = unix.SetNonblock(int(file.Fd()), true)
	if err != nil {
		return err
	}

	s := &Server{
		acceptFD: int(file.Fd()),
	}
	if err := s.Start(); err != nil {
		return err
	}
	defer s.Stop()
	return nil
}

/*
实现类redis
单线程连接管理
单线程处理请求

//绑定读事件
//协议层 codeC-Decode
//业务处理
//协议层 codeC-EnCode
*/
