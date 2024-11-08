package localcache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrKeyNoExists  = errors.New("key is not exists")
	ErrKeyIsExpired = errors.New("key is expired")
)

type Cache interface {
	Set(ctx context.Context, k string, v any, ttl time.Duration) bool
	Get(k string) (v any, err error)
	Del(k string)
	Len() uint64
	Close()
}

var entPool = &sync.Pool{
	New: func() any {
		return &entExtendFunc{}
	},
}

func getEntExtendFunc() *entExtendFunc {
	return entPool.Get().(*entExtendFunc)
}
func putEntExtendFunc(e *entExtendFunc) {
	e.afterDo = nil
	entPool.Put(e)
}

type entExtendFunc struct {
	ent     *entry
	afterDo func()
}

type keyExtendFunc struct {
	key     string
	afterDo func()
}

func New(ops ...Option) Cache {
	c := &cache{
		size:        100,
		store:       newShardedMap(),
		ekt:         newExpireKeyTimers(time.Second, 60),
		setEvtCh:    make(chan *entExtendFunc, 1024),
		delEvtCh:    make(chan *keyExtendFunc, 1024),
		accessEvtCh: make(chan []*list.Element, 1024),
		stop:        make(chan struct{}),
	}

	for _, op := range ops {
		op(c)
	}

	//c.accessRingBuffer = newUniqRingBuffer(c, 10)
	c.accessRingBuffer = newRingBuffer(c, 3)
	c.policy = newPolicy(c.size)

	go c.evtProcessor()
	return c
}

type cache struct {
	size int

	store            store
	policy           policy
	ekt              *expireKeyTimers
	accessRingBuffer RingBuffer

	accessEvtCh chan []*list.Element //需要合并
	setEvtCh    chan *entExtendFunc
	delEvtCh    chan *keyExtendFunc

	stop chan struct{}

	isSync     bool
	setTimeout time.Duration
}

func (c *cache) consume(elements []*list.Element) bool {
	select {
	case c.accessEvtCh <- elements:
		return true
	default:
		//如果阻塞，直接丢弃
		return false
	}
}

func (c *cache) access(elements []*list.Element) {
	c.policy.batchRenew(elements)
}

func (c *cache) evtProcessor() {
	var ef *entExtendFunc
	for {
		select {
		case elements := <-c.accessEvtCh:
			c.access(elements)
		case ef = <-c.setEvtCh:
			if ef.ent.isUpdate {
				c.update(ef.ent)
			} else {
				c.add(ef.ent)
			}
			if ef.afterDo != nil {
				ef.afterDo()
			}
		case kef := <-c.delEvtCh:
			c.del(kef.key)
			if kef.afterDo != nil {
				kef.afterDo()
			}
		case <-c.stop:
			fmt.Println("cache quit")
			return
		}
		if ef != nil {
			putEntExtendFunc(ef)
			ef = nil
		}
	}
}

func (c *cache) del(key string) {
	val, ok := c.store.get(key)
	if !ok {
		return
	}
	c.policy.remove(val.(*list.Element))
	c.store.del(key)
	c.ekt.remove(key)
}

func (c *cache) expireTask(k string) func() {
	return func() {
		c.store.del(k)
		c.delEvtCh <- &keyExtendFunc{key: k, afterDo: nil}
	}
}

func (c *cache) add(e *entry) {

	add, victim := c.policy.add(e)
	if add == nil && victim == nil {
		return
	}

	c.store.set(e.key, add)
	c.ekt.set(e.key, e.expireAt.Sub(time.Now()), c.expireTask(e.key))
	if victim != nil {
		k := getEntry(victim).key
		c.store.del(k)
		c.ekt.remove(k)
	}
}

func (c *cache) update(e *entry) {
	k := e.key
	val, ok := c.store.get(k)
	if ok {
		ele := val.(*list.Element)
		c.policy.update(e, ele)
		c.ekt.remove(k)
		c.ekt.set(e.key, e.expireAt.Sub(time.Now()), c.expireTask(e.key))
	}
}

var waiterPool = sync.Pool{
	New: func() any {
		return &sync.WaitGroup{}
	},
}

func (c *cache) Set(ctx context.Context, k string, v any, ttl time.Duration) bool {

	var (
		ent *entry
	)
	val, hit := c.store.get(k)
	eeF := getEntExtendFunc()
	if hit {
		ent = getEntry(val.(*list.Element))
		ent.mu.Lock()
		ent.val = v
		ent.isUpdate = true
		ent.expireAt = time.Now().Add(ttl)
		ent.mu.Unlock()
	} else {
		ent = &entry{key: k, val: v, expireAt: time.Now().Add(ttl)}
	}
	eeF.ent = ent

	var push2SetCh = func() bool {
		select {
		case c.setEvtCh <- eeF:
			return true
		case <-ctx.Done():
			return false
		default:
			if c.isSync {
				c.setEvtCh <- eeF
				return true
			}
			return false
		}
	}
	if c.isSync {
		waiter := waiterPool.Get().(*sync.WaitGroup)
		waiter.Add(1)
		eeF.afterDo = waiter.Done
		if push2SetCh() {
			waiter.Wait()
		}
		return true
	}

	return push2SetCh()
}

func (c *cache) Del(k string) {
	kef := &keyExtendFunc{key: k, afterDo: nil}
	if c.isSync {
		waiter := waiterPool.Get().(*sync.WaitGroup)
		kef.afterDo = waiter.Done
		c.delEvtCh <- kef
		waiter.Wait()
		return
	}
	c.delEvtCh <- kef
}

func (c *cache) Get(k string) (any, error) {
	v, exists := c.store.get(k)
	if exists {
		ele := v.(*list.Element)
		ent := getEntry(ele)
		if time.Now().After(ent.expireAt) {
			return nil, ErrKeyIsExpired
		}
		c.accessRingBuffer.Push(ele)
		return ent.val, nil
	}
	return nil, ErrKeyNoExists
}

func (c *cache) Close() {
	c.stop <- struct{}{}
	close(c.stop)
	close(c.setEvtCh)
	close(c.delEvtCh)
	close(c.accessEvtCh)

	c.ekt.stop()
}

func (c *cache) Len() uint64 {
	return c.store.len()
}
