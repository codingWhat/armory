package main

import (
	"flag"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/codingWhat/armory/consensus/raft"
)

func main() {
	defer func() {
		err := recover()
		if err != nil {
			debug.PrintStack()
			fmt.Printf("recover ---> %+v \n", err)
		}
	}()
	var (
		addr   string
		servId int
		host   string
		port   int
		leader string
	)
	//flag.StringVar(&addr, "addr", "", "addr 127.0.0.1:8081")
	flag.IntVar(&servId, "id", -1, "addr 127.0.0.1:8081")

	flag.StringVar(&host, "host", "localhost", "host")
	flag.IntVar(&port, "p", 8081, "port")
	flag.StringVar(&leader, "join", "", "-join leader-addr ")

	flag.Parse()

	raftServer := raft.NewRaft(servId, host, port, leader)

	//对外
	http.HandleFunc("/cmd", raftServer.HandleCmd)

	//内部通信
	http.HandleFunc("/requestVote", raftServer.HandleVote)
	http.HandleFunc("/join", raftServer.HandleJoinRequest)
	http.HandleFunc("/appendEntries", raftServer.HandleAppendEntryRequest)

	addr = fmt.Sprintf("%s:%d", host, port)
	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			panic(err)
			return
		}
	}()

	time.Sleep(1 * time.Second)
	fmt.Println("consensus run:", time.Now().Format("2006-01-02 15:04:05"))

	raftServer.Run()

}
