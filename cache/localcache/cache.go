package localcache

import (
	"container/list"
	"time"
)

type Cache interface {
	Set(k string, v any, ttl time.Duration) bool
	Get(k string) (v any, exists bool)
	Len() uint64
	Clear()
}

type waitFinish struct {
	ent    *entry
	signal chan struct{}
}

func (wf *waitFinish) isNeedNotify() bool {
	return wf.signal != nil
}

type cache struct {
	store  store
	policy Policy

	updateEvtCh chan *waitFinish
	addEvtCh    chan *waitFinish

	isSync     bool
	setTimeout time.Duration
}

func (c *cache) evtProcessor() {
	for {
		select {
		case wf := <-c.addEvtCh:
			c.add(wf.ent)
			if wf.isNeedNotify() {
				close(wf.signal)
			}
		case wf := <-c.addEvtCh:
			c.update(wf.ent)
			if wf.isNeedNotify() {
				close(wf.signal)
			}
		}
	}
}

func (c *cache) add(e *entry) {

	add, victim := c.policy.add(e)
	if add == nil && victim == nil {
		return
	}

	c.store.set(e.key, add)
	if victim != nil {
		c.store.del(getEntry(victim).key)
	}
}

func (c *cache) update(e *entry) {
	k := e.key
	val, ok := c.store.get(k)
	if ok {
		ele := val.(*list.Element)
		c.policy.update(e, ele)
	}
}

func (c *cache) Set(k string, v any, ttl time.Duration) bool {
	val, hit := c.store.get(k)
	sigCh := make(chan struct{})
	if hit {
		ent := getEntry(val.(*list.Element))
		ent.val = v
		ent.expireAt = time.Now().Add(ttl)

		select {
		case c.updateEvtCh <- &waitFinish{
			signal: sigCh,
			ent:    ent,
		}:
			if c.isSync {
				<-sigCh
			}
			return true
		default:
			return false
		}
	}

	ent := &entry{key: k, val: v, expireAt: time.Now().Add(ttl)}
	select {
	case c.addEvtCh <- &waitFinish{
		signal: sigCh,
		ent:    ent,
	}:
		if c.isSync {
			<-sigCh
		}
		return true
	default:
		return false
	}
}

func (c *cache) Get(k string) (v any, exists bool) {
	//TODO implement me
	panic("implement me")
}

func (c *cache) Len() uint64 {
	//TODO implement me
	panic("implement me")
}

func (c *cache) Clear() {
	//TODO implement me
	panic("implement me")
}
