package raft

import (
	"encoding/json"
	"fmt"
)

type Command interface {
	Apply(raft *Raft) error
	Name() string
	RspChannel() chan error
}

var _commands map[string]Command

func init() {
	_commands = make(map[string]Command)
	_commands["create"] = &CreateCommand{}
	_commands["delete"] = &DeleteCommand{}
	_commands["join"] = &JoinConsensusCommand{}
	_commands["nop"] = &NopCommand{}
}

func RegisterCommand(k string, v Command) {
	_commands[k] = v
}

func GetCommand(k string) (Command, bool) {
	v, ok := _commands[k]
	return v, ok
}

type NopCommand struct {
}

func (c *NopCommand) Apply(r *Raft) error {
	return nil
}

func (c *NopCommand) Name() string {
	return "nop"
}

func (c *NopCommand) RspChannel() chan error {
	return nil
}

type CreateCommand struct {
	ch    chan error
	Key   string
	Value []byte
}

func (c *CreateCommand) Apply(r *Raft) error {
	return nil
}

func (c *CreateCommand) Name() string {
	return "create"
}

func (c *CreateCommand) RspChannel() chan error {
	return c.ch
}

type DeleteCommand struct {
	c   chan error
	Key string
}

func (d *DeleteCommand) Apply(r *Raft) error {
	return nil
}

func (d *DeleteCommand) Name() string {
	return "delete"
}

func (d *DeleteCommand) RspChannel() chan error {
	return d.c
}

type JoinConsensusCommand struct {
	AddrList string `json:"addr_list"`
	c        chan error
}

func (j *JoinConsensusCommand) Name() string {
	return "join-consensus"
}
func (j *JoinConsensusCommand) RspChannel() chan error {
	return j.c
}
func (j *JoinConsensusCommand) Apply(r *Raft) error {
	var addrs map[int]string
	err := json.Unmarshal([]byte(j.AddrList), &addrs)
	if err != nil {
		return err
	}

	peers := make(map[int]*Peer)
	for i, info := range addrs {
		p, ok := r.peers[i]
		if ok {
			peers[i] = p
		} else {
			peers[i] = &Peer{Addr: info, stopCh: make(chan struct{}), r: r}
		}
	}

	r.Lock()
	r.peers = peers
	r.Unlock()
	return nil
}

type joinCommand struct {
	c chan error
	p *JoinRequest
}

func (j *joinCommand) RspChannel() chan error {
	return j.c
}

func (j *joinCommand) Name() string {
	return "join"
}
func (j *joinCommand) Apply(r *Raft) error {
	fmt.Println("joinCommand---->1")

	r.Lock()
	fmt.Println("joinCommand---->2")
	if r.currentState == Follower {
		r.currentState = Leader //必须的，刚开始是follower, 需要切到leader
	}
	p := &Peer{
		Addr:   j.p.AddrInfo,
		stopCh: make(chan struct{}, 1),
		r:      r,
		ServID: j.p.ServId,
	}
	r.addPeer(j.p.ServId, p)
	r.Unlock()
	if !r.isPeerStarted(j.p.ServId) {
		_, idx := r.lastLogInfo()
		p.SetPrevLogIndex(idx)
		p.start()
	}
	fmt.Println("joinCommand---->3")
	return nil
}
