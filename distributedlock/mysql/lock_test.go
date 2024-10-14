package mysql

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"testing"
	"time"
)

var host = "localhost"
var pwd = ""

func Test_lock_UnLock(t *testing.T) {
	db, _ := createDB()

	err := createLockTable(context.Background(), db)
	if err != nil {
		t.Errorf("%+v", err)
	}

	defer func() {
		//db.Exec("truncate tbl_lock")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	locker := New(db)
	//err = locker.Lock(ctx, "a", "b", 100*time.Second)
	//fmt.Println("--->", err)
	go func() {
		locker.Lock(ctx, "a", "o", 30*time.Second)
		fmt.Println("--->o getLock")
		time.Sleep(10 * time.Second)
		locker.UnLock(ctx, "a", "o")
		fmt.Println("--->o unlock")
	}()
	//
	go func() {
		time.Sleep(1 * time.Second)
		locker.Lock(ctx, "a", "b", 5*time.Second)
		locker.Lock(ctx, "a", "b", 5*time.Second)
		fmt.Println("--->b getLock")
		locker.UnLock(ctx, "a", "b")
		locker.UnLock(ctx, "a", "b")
		fmt.Println("--->b unlock")

	}()

	time.Sleep(30 * time.Second)

}

func createDB() (*sql.DB, error) {
	db, err := sql.Open("mysql", "root:"+pwd+"@tcp("+host+":3306)/?parseTime=true&loc=Local")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS `dlock`")
	if err != nil {
		return nil, err
	}
	db, err = sql.Open("mysql", "root:"+pwd+"@tcp("+host+":3306)/dlock?parseTime=true&loc=Local")
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createLockTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS `tbl_lock` ("+
		"	`id` bigint unsigned NOT NULL AUTO_INCREMENT,"+
		"	`rkey` varchar(128) NOT NULL,"+
		"	`owner` varchar(128) NOT NULL DEFAULT '',"+
		"	 `reentrant`  int unsigned NOT NULL default 0,"+
		"	`expiredAt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
		"	PRIMARY KEY (`id`),"+
		"	UNIQUE KEY `uniq_key` (`rkey`)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	return err
}
