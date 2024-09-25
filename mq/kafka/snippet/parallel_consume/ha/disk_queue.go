package main

import (
	"fmt"
	"github.com/nsqio/go-diskqueue"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

type tbLog interface {
	Log(...interface{})
}

func NewTestLogger(tbl tbLog) diskqueue.AppLogFunc {
	return func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		tbl.Log(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
}

func main() {
	l := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		log.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := diskqueue.New(dqName, tmpDir, 1024, 4, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()

	for i := 0; i < 9; i++ {
		msg := []byte(fmt.Sprintf("test:%d", i))
		dq.Put(msg)
	}

	go func() {
		for val := range dq.ReadChan() {
			fmt.Println("------>", string(val))

		}
	}()

	time.Sleep(3 * time.Second)
}
