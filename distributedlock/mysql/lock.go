package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/codingWhat/armory/distributedlock"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type lock struct {
	transactions sync.Map
	db           *sql.DB
	locks        sync.Map
}

func New(db *sql.DB) distributedlock.Locker {
	return &lock{
		transactions: sync.Map{},
		locks:        sync.Map{},
		db:           db,
	}
}

func (l *lock) lockWith(k string) {
	val, ok := l.locks.Load(k)
	if !ok {
		mu := &sync.Mutex{}
		l.locks.Store(k, mu)
		mu.Lock()
		return
	}
	mu := val.(*sync.Mutex)
	mu.Lock()
}

func (l *lock) unLockWith(k string) {
	val, ok := l.locks.Load(k)
	if !ok {
		return
	}
	mu := val.(*sync.Mutex)
	mu.Unlock()
}
func (l *lock) getKey(rkey, owner string) string {
	return fmt.Sprintf("%s%s", rkey, owner)
}
func (l *lock) Lock(ctx context.Context, key string, owner string, ttl time.Duration) error {
	l.lockWith(l.getKey(key, owner))
	defer l.unLockWith(l.getKey(key, owner))
	v, ok := l.transactions.Load(l.getKey(key, owner))
	if ok {
		//正常逻辑, 锁持有者重入，需要更新计数，关闭上次事务
		tx := v.(*sql.Tx)
		// 更新reentrant
		quitSQL := "UPDATE tbl_lock SET reentrant = reentrant+1, expiredAt= IF (expiredAt < NOW() + INTERVAL ? SECOND, NOW() + INTERVAL ? SECOND, expiredAt)"
		quitSQL = quitSQL + " WHERE  owner = ? AND rkey = ?"
		_, err := tx.ExecContext(ctx, quitSQL, ttl.Seconds(), owner, key)
		if err != nil {
			return errors.WithMessage(err, "update reentrant failed.")
		}
		err = l.releaseSession(key, owner)
		if err != nil {
			return errors.WithMessage(err, "Lock() releaseSession failed.")
		}
		return nil
	} else {
		for {
			var reentrant int
			err := l.db.QueryRowContext(ctx, "SELECT reentrant FROM tbl_lock where `owner`= ? and rkey = ? and expiredAt > NOW()", owner, key).Scan(&reentrant)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return errors.WithMessage(err, "lock failed, err:"+err.Error())
			}
			if err == nil && reentrant >= 0 {
				// 异常逻辑, 当for update失败时，再次Lock, 更新reentrant,
				quitSQL := "UPDATE tbl_lock SET reentrant = reentrant+1, expiredAt= NOW() + INTERVAL ? SECOND WHERE expiredAt > NOW() AND owner = ? AND rkey = ?"
				_, err = l.db.ExecContext(ctx, quitSQL, ttl.Seconds(), owner, key)
				if err != nil {
					return errors.WithMessage(err, "lock failed, err:"+err.Error())
				}
			} else {
				//正常逻辑, 抢占锁
				preemptSQL := "INSERT  INTO tbl_lock (`rkey`,`owner`,`expiredAt`) values (?, ?, ?) on duplicate key update "
				preemptSQL = preemptSQL + " owner = IF (expiredAt < NOW(), VALUES(`owner`), `owner`) ,"
				preemptSQL = preemptSQL + " expiredAt = IF (expiredAt < NOW(), NOW() + INTERVAL ? SECOND, `expiredAt`) ,"
				preemptSQL = preemptSQL + " reentrant = IF (expiredAt < NOW(), 1, `reentrant`) "

				//只要抢占成功的Goroutine, for update成功了，其他goroutine就会阻塞在这
				ret, err := l.db.ExecContext(ctx, preemptSQL, key, owner, time.Now().Add(ttl), ttl.Seconds())
				if err != nil {
					return errors.WithMessage(err, "insert lock failed")
				}
				affected, err := ret.RowsAffected()
				if err != nil || affected == 0 {
					fmt.Println("---->", affected, err)
					time.Sleep(1 * time.Second)
					continue
				}
			}
			break
		}
	}

	//只要重入成功了或者抢到锁了,就for update
	txnMutexErr := l.holdSession(ctx, key, owner, ttl)
	if txnMutexErr != nil {
		//到这说明for update失败了，必须要清理脏数据，重新Lock
		l.rollbackManual(ctx, key, owner)
	}
	return txnMutexErr
}

func (l *lock) rollbackManual(ctx context.Context, key, owner string) error {
	reSQL := "SELECT reentrant FROM tbl_lock WHERE expiredAt > NOW() AND owner = ? AND rkey = ?"
	var reentrant int
	err := l.db.QueryRowContext(ctx, reSQL, owner, key).Scan(&reentrant)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	//更新reentrant, 若为1，则删除
	if reentrant == 1 {
		delLockSQL := "DELETE FROM tbl_lock WHERE owner = ? AND rkey = ?"
		_, err = l.db.ExecContext(ctx, delLockSQL, owner, key)
		if err != nil {
			return errors.WithMessage(err, "delete lock failed.")
		}
	} else {
		quitSQL := "UPDATE tbl_lock set reentrant = reentrant-1 WHERE expiredAt > NOW() AND owner = ? AND rkey = ?"
		l.db.ExecContext(ctx, quitSQL, owner, key)
	}
	return nil
}

func (l *lock) holdSession(ctx context.Context, key, owner string, ttl time.Duration) error {
	tx, err := l.db.Begin()
	if err != nil {
		return errors.WithMessage(err, " txn Begin() failed")
	}
	//防止lock之后panic, 一直阻塞其他请求, 异步提交事务
	time.AfterFunc(ttl, func() {
		_ = l.releaseSession(key, owner)
	})

	l.transactions.Store(l.getKey(key, owner), tx)
	_, err = tx.ExecContext(ctx, "SELECT * FROM tbl_lock WHERE rkey = ? FOR UPDATE", key)
	return errors.WithMessage(err, "delete lock failed")
}

func (l *lock) releaseSession(key, owner string) error {
	v, ok := l.transactions.Load(l.getKey(key, owner))
	if !ok {
		return nil
	}
	l.transactions.Delete(l.getKey(key, owner))
	tx := v.(*sql.Tx)
	return errors.WithMessage(tx.Commit(), "release session failed.")
}

func (l *lock) UnLock(ctx context.Context, key string, owner string) error {
	l.lockWith(l.getKey(key, owner))
	defer l.unLockWith(l.getKey(key, owner))
	v, ok := l.transactions.Load(l.getKey(key, owner))
	if !ok {
		return nil
	}

	tx := v.(*sql.Tx)
	reSQL := "SELECT reentrant FROM tbl_lock WHERE expiredAt > NOW() AND owner = ? AND rkey = ?"
	var reentrant int
	err := tx.QueryRowContext(ctx, reSQL, owner, key).Scan(&reentrant)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		//更新reentrant, 若为0，则删除
		quitSQL := "UPDATE tbl_lock SET reentrant = reentrant-1 WHERE expiredAt > NOW() AND owner = ? AND rkey = ?"
		tx.ExecContext(ctx, quitSQL, owner, key)
	}

	if err == nil && reentrant == 0 {
		delLockSQL := "DELETE FROM tbl_lock WHERE owner = ? AND rkey = ?"
		_, err = tx.ExecContext(ctx, delLockSQL, owner, key)
		if err != nil {
			return errors.WithMessage(err, "delete lock failed")
		}
		//只有锁持有者完全退出重入了，才提交事务
		return l.releaseSession(key, owner)
	}
	return nil

}
