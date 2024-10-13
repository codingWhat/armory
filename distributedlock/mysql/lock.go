package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"sync"
	"time"
)

type lock struct {
	transactions sync.Map
	db           *gorm.DB
	locks        sync.Map
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

	for {
		var reentrant int
		err := l.db.WithContext(ctx).Exec("SELECT reentrant from tbl_lock where `owner`= ? and rkey = ? and expiredAt > NOW()", owner, key).Row().Scan(&reentrant)
		if err != nil && err != sql.ErrNoRows {
			return errors.New("lock failed")
		}
		if err == nil && reentrant > 0 {
			// 更新reentrant
		} else {
			//抢占锁
			preemptSQL := "Insert into tbl_lock (`rkey`,`owner`,`expiredAt`) values (?, ?, ?) on duplicate key update "
			preemptSQL = preemptSQL + " owner = IF (expiredAt < NOW(), VALUES(`owner`), `owner`) ,"
			preemptSQL = preemptSQL + " expiredAt = IF (expiredAt < NOW(), NOW() + INTERVAL ? SECOND, `expiredAt`) ,"
			preemptSQL = preemptSQL + " reentrant = IF (expiredAt < NOW(), 0, `reentrant`) ,"

			ret := l.db.WithContext(ctx).Exec(preemptSQL, key, owner, ttl)
			if ret.Error != nil || ret.RowsAffected == 0 {
				time.Sleep(1 * time.Second)
				continue
			}
		}

		break
	}

	tx := l.db.Begin()
	l.transactions.Store(l.getKey(key, owner), tx)
	return tx.Exec("Select * from tbl_lock where rkey = ? for update", key).Error
}

func (l *lock) UnLock(ctx context.Context, key string, owner string, ttl time.Duration) error {

	v, ok := l.transactions.Load(l.getKey(key, owner))
	if ok {
		tx := v.(*gorm.DB)
		//更新reentrant, 若为0，则删除

		tx.Commit()
	}
	//更新reentrant, 若为0，则删除
	return nil
}
