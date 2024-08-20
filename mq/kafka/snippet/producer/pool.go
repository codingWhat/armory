package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// https://github.com/buraksezer/connpool/tree/master

var (
	// ErrClosed is the error resulting if the pool is closed via pool.Close().
	ErrClosed = errors.New("pool is closed")
)

// Pool interface describes a pool implementation. A pool should have maximum
// capacity. An ideal pool is thread-safe and easy to use.
type Pool interface {
	// Get returns a new connection from the pool. Closing the connections puts
	// it back to the Pool. Closing it when the pool is destroyed or full will
	// be counted as an error.
	Get(context.Context) (Conn, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable.
	Close()

	// Len returns the current number of idle connections of the pool.
	Len() int

	// NumberOfConns returns the total number of alive connections of the pool.
	NumberOfConns() int
}

// PoolConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type PoolConn struct {
	Conn
	mu       sync.RWMutex
	c        *channelPool
	unusable bool

	createAt time.Time
}

func (p *PoolConn) IsStaleConn() bool {
	return time.Now().Sub(p.createAt) > p.c.MaxConnAge
}

// Close() puts the given connects back to the pool instead of closing it.
func (p *PoolConn) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		if p.Conn != nil {
			<-p.c.semaphore
			return p.Conn.Close()
		}
		return nil
	}
	return p.c.put(p)
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *PoolConn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn Conn) Conn {
	p := &PoolConn{c: c}
	p.Conn = conn
	return p
}

type Conn interface {
	Close() error
}

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.RWMutex
	conns chan Conn

	MaxConnAge time.Duration // 最大使用时限

	maxCap    int
	semaphore chan struct{}

	// net.Conn generator
	factory Factory
}

// Factory is a function to create new connections.
type Factory func() (Conn, error)

// NewChannelPool returns a new pool based on buffered channels with an initial
// capacity and maximum capacity. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool, a new connection will be created via the Factory()
// method.
func NewChannelPool(initialCap, maxCap int, connMaxAge time.Duration, factory Factory) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		conns:      make(chan Conn, maxCap),
		semaphore:  make(chan struct{}, maxCap),
		maxCap:     maxCap,
		factory:    factory,
		MaxConnAge: connMaxAge,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCap; i++ {
		c.semaphore <- struct{}{}
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %w", err)
		}

		c.conns <- c.wrapConn(conn)
	}

	return c, nil
}

func (c *channelPool) getConnsAndFactory() (chan Conn, Factory) {
	c.mu.RLock()
	conns := c.conns
	factory := c.factory
	c.mu.RUnlock()
	return conns, factory
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get(ctx context.Context) (Conn, error) {
	conns, factory := c.getConnsAndFactory()
	if conns == nil {
		return nil, ErrClosed
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}

		//检查连接使用时间
		return c.checkAndCreateNewConn(conn, factory)
	default:
	}

	select {
	case c.semaphore <- struct{}{}:
		conn, err := factory()
		if err != nil {
			// restore claimed slot, otherwise max is permanently decreased
			<-c.semaphore
			return nil, err
		}
		return conn, nil
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}

		return c.checkAndCreateNewConn(conn, factory)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (c *channelPool) checkAndCreateNewConn(conn Conn, factory Factory) (Conn, error) {
	pc, ok := conn.(*PoolConn)
	if !ok {
		return nil, nil
	}

	if pc.IsStaleConn() {
		pc.Conn.Close()
		return factory()
	}
	return pc, nil
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(conn Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- conn:
		return nil
	default:
		<-c.semaphore
		// pool is full, close passed connection
		return conn.Close()
	}
}

func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

func (c *channelPool) Len() int {
	conns, _ := c.getConnsAndFactory()
	return len(conns)
}

func (c *channelPool) NumberOfConns() int {
	return len(c.semaphore)
}
