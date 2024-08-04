package localcache

import (
	"container/list"
	"errors"
	"fmt"
	"time"
)

var (
	ErrKeyNoExists  = errors.New("key is not exists")
	ErrKeyIsExpired = errors.New("key is expired")
)

type Cache interface {
	Set(k string, v any, ttl time.Duration) bool
	Get(k string) (v any, err error)
	Del(k string)
	Len() uint64
	Close()
}

type entExtendFunc struct {
	ent     *entry
	afterDo func()
}

type keyExtendFunc struct {
	key     string
	afterDo func()
}

func New(size int) Cache {
	c := &cache{
		store:       newShardedMap(),
		ekt:         newExpireKeyTimers(time.Second, 60),
		policy:      newLRU(size),
		isSync:      true,
		setTimeout:  1 * time.Second,
		updateEvtCh: make(chan *entExtendFunc, 1),
		addEvtCh:    make(chan *entExtendFunc, 100),
		delEvtCh:    make(chan *keyExtendFunc, 1),
	}

	go c.evtProcessor()
	return c
}

type cache struct {
	store  store
	policy Policy
	ekt    *expireKeyTimers

	updateEvtCh chan *entExtendFunc
	addEvtCh    chan *entExtendFunc
	delEvtCh    chan *keyExtendFunc

	isSync     bool
	setTimeout time.Duration
}

func (c *cache) evtProcessor() {
	for {
		select {
		case wf := <-c.addEvtCh:
			c.add(wf.ent)
			if wf.afterDo != nil {
				wf.afterDo()
			}
		case wf := <-c.updateEvtCh:
			c.update(wf.ent)
			if wf.afterDo != nil {
				wf.afterDo()
			}
		case wf := <-c.delEvtCh:
			c.del(wf.key)
			if wf.afterDo != nil {
				wf.afterDo()
			}
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
		fmt.Println(k, "expireTask--->", time.Now().Format("2006-01-02 15:04:05"))
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
	fmt.Println("add--->", add, victim, e.expireAt.Format("2006-01-02 15:04:05"))
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

func (c *cache) Set(k string, v any, ttl time.Duration) bool {
	val, hit := c.store.get(k)

	var (
		waitSig chan struct{}
		afterDo func()
	)
	if c.isSync {
		waitSig = make(chan struct{})
		afterDo = func() {
			close(waitSig)
		}
	}

	if hit {
		ent := getEntry(val.(*list.Element))
		//细节: 防止读到脏数据，立即更新；注意add是异步操作，多写多读 -> 单写多读, 减少锁竞争
		ent.mu.Lock()
		ent.val = v
		ent.expireAt = time.Now().Add(ttl)
		ent.mu.Unlock()

		if c.isSync {
			select {
			case c.updateEvtCh <- &entExtendFunc{
				afterDo: afterDo,
				ent:     ent,
			}:
				<-waitSig
				return true
			case <-time.After(c.setTimeout):
				return false
			default:
				return false
			}
		} else {
			select {
			case c.updateEvtCh <- &entExtendFunc{ent: ent}:
				return true
			default:
				return false
			}
		}

	}

	ent := &entry{key: k, val: v, expireAt: time.Now().Add(ttl)}
	if c.isSync {
		select {
		case c.addEvtCh <- &entExtendFunc{afterDo: afterDo, ent: ent}:
			<-waitSig
			return true
		case <-time.After(c.setTimeout):
			return false
		default:
			return false
		}
	} else {
		select {
		case c.addEvtCh <- &entExtendFunc{ent: ent}:
			return true
		default:
			return false
		}
	}

}

func (c *cache) Del(k string) {
	if c.isSync {
		waitSig := make(chan struct{})
		c.delEvtCh <- &keyExtendFunc{key: k, afterDo: func() {
			close(waitSig)
		}}
		<-waitSig
		return
	}

	c.delEvtCh <- &keyExtendFunc{key: k, afterDo: nil}
}

func (c *cache) Get(k string) (any, error) {
	v, exists := c.store.get(k)
	fmt.Println(v, exists, "GEt--->", time.Now().Format("2006-01-02 15:04:05"))
	if exists {
		ele := v.(*list.Element)
		ent := getEntry(ele)
		ent.mu.Lock()
		defer ent.mu.Unlock()
		if time.Now().After(ent.expireAt) {
			return nil, ErrKeyIsExpired
		}

		return ent.val, nil
	}
	return nil, ErrKeyNoExists
}

func (c *cache) Close() {

	close(c.updateEvtCh)
	close(c.addEvtCh)
	close(c.delEvtCh)

	//todo 关闭timers
}

func (c *cache) Len() uint64 {
	return c.store.len()
}
