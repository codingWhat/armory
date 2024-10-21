package etcd

import (
	"context"
	"github.com/pkg/errors"

	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type lock struct {
	cli *clientv3.Client
}

func (l *lock) getKey(rkey, owner string) string {
	return fmt.Sprintf("%s/%s", rkey, owner)
}

var ErrWatchLastAcquireLock = "watch last lock failed"

func (l *lock) Lock(ctx context.Context, key string, owner string, ttl time.Duration) (err error) {
	defer func() {
		if err != nil && err.Error() == ErrWatchLastAcquireLock {
			_ = l.UnLock(context.Background(), key, owner)
		}
	}()

	grantRsp, err := l.cli.Grant(ctx, time.Now().Add(ttl).Unix())
	if err != nil {
		return err
	}
	lease := grantRsp.ID
	lkey := l.getKey(key, owner)
	cmp := clientv3.Compare(clientv3.CreateRevision(l.getKey(key, owner)), "=", 0)
	then := clientv3.OpPut(lkey, "1")

	for {
		resp, err := l.cli.Txn(ctx).If(
			cmp,
		).Then(then, clientv3.OpGet(key, clientv3.WithFirstCreate()...)).Else(
			clientv3.OpGet(lkey),
		).Commit()
		if err != nil {
			return err
		}

		//如果不存在
		if resp.Succeeded {
			//判断当前的revision是不是最小的那个，如果是，抢锁成功
			if resp.Responses[1].GetResponseRange().Kvs[0].ModRevision == resp.Header.Revision {
				return nil
			}

			//说明没抢到锁，必须要等待前一个持有者，释放锁
			if err := l.watchLastAcquireLock(ctx, key, resp.Header.Revision-1); err != nil {
				return errors.WithMessage(err, ErrWatchLastAcquireLock)
			}
			//抢到锁了
			return nil
		}

		if len(resp.Responses[0].GetResponseRange().Kvs) == 0 {
			cmp = clientv3.Compare(clientv3.CreateRevision(l.getKey(key, owner)), "=", 0)
			then = clientv3.OpPut(lkey, "1")
			continue
		}

		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		cmp = clientv3.Compare(clientv3.ModRevision(string(kv.Key)), "=", kv.ModRevision)
		then = clientv3.OpPut(lkey, string(kv.Value)+"1", clientv3.WithLease(lease))
	}

}

func (l *lock) watchLastAcquireLock(ctx context.Context, key string, rev int64) error {

	op := append(clientv3.WithLastCreate(), clientv3.WithMaxCreateRev(rev))
	resp, err := l.cli.Get(ctx, key, op...)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	lastkey := resp.Kvs[0].Key
	rev = resp.Header.Revision
	for watch := range l.cli.Watch(ctx, string(lastkey), clientv3.WithRev(rev)) {
		for _, evt := range watch.Events {
			if evt.Type == mvccpb.DELETE {
				return nil
			}
		}
	}

	return errors.New("lock failed. watch closed")
}

func (l *lock) UnLock(ctx context.Context, key string, owner string) error {
	lkey := l.getKey(key, owner)

	cmp := clientv3.Compare(clientv3.Value(lkey), "=", "1")
	then := clientv3.OpDelete(lkey)
	for {
		resp, err := l.cli.Txn(ctx).If(cmp).
			Then(then).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return err
		}
		if resp.Succeeded || len(resp.Responses[0].GetResponseRange().Kvs) == 0 {
			return nil
		}
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		if string(kv.Value) == "1" {
			cmp = clientv3.Compare(clientv3.ModRevision(lkey), "=", resp.Header.Revision)
			then = clientv3.OpDelete(lkey)
			continue
		}

		cmp = clientv3.Compare(clientv3.ModRevision(lkey), "=", resp.Header.Revision)
		then = clientv3.OpPut(lkey, string(kv.Value[:len(kv.Value)-1]), clientv3.WithLease(clientv3.LeaseID(kv.Lease)))
	}
}
