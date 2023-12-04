package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	"sync"
	"time"
)

const (
	_ = iota
	Leader
	Follower
	Candidate
)

type Peer struct {
	ServID       int
	Addr         string
	PrevLogIndex uint64
	stopCh       chan struct{}
	sync.RWMutex
	r *Raft

	isStarted bool
}

func (p *Peer) IsStarted() bool {
	p.RLock()
	defer p.RUnlock()
	return p.isStarted == true
}

func (p *Peer) start() {
	p.Lock()
	p.isStarted = true
	p.Unlock()

	go p.heartbeat()
}
func (p *Peer) heartbeat() {
	timeout := 100 * time.Millisecond
	ticker := time.Tick(timeout)

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker:
			p.sendAppendEntries()
		}
	}
}

func (p *Peer) sendAppendEntries() {

	var (
		prevLogIndex = p.GetPrevLogIndex()
		currTerm     = p.r.getCurrentTerm()
		serID        = p.r.getServID()
	)
	ae := &AppendEntries{
		Term:         currTerm,
		LeaderId:     serID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  p.r.getTermByIndex(prevLogIndex),
		LeaderCommit: p.r.getCommitID(),
		Entries:      p.r.log.GetEntriesAfter(prevLogIndex),
	}
	resp := p.r.sendAppendEntryRequest(p.Addr, ae)
	fmt.Printf("---->leader send ae->after %d, %+v \n", len(ae.Entries), resp)
	if resp == nil {
		fmt.Println("[", time.Now().Format("2006-01-02 15:04:05"), "] ", "node:", p.Addr, " sendAppendEntryRequest failed..")
		return
	}

	//r.Lock()
	//defer r.Unlock()
	if resp.Success() {
		if len(ae.Entries) > 0 {
			lastLog := ae.Entries[len(ae.Entries)-1]
			//当前leader只能提交当前任期的日志,
			//看论文Figure-8 如果当前任期比日志的任期新，此时这批日志就不能提交，否则就会发生Figure8中，任期3会把已经提交的数据给覆盖了，raft是不允许的。
			//只能等待新请求，这就引入了NOP-COMMAND, 根据日志匹配属性，如果返回成功了，说明保证一致性了。
			if lastLog.Term() == currTerm {
				p.r.si.incr()
				p.SetPrevLogIndex(lastLog.Index())
				//todo 状态更新合并到内部通信的channel，减少锁竞争
				//刷盘操作异步
				//if !r.isSyncedToQuorum() {
				//	return
				//}
				//pendingCommitIdx := r.getSmallestPrevLogIndex()
				//if pendingCommitIdx > r.commitIndex {
				//	r.setCommitIndex(pendingCommitIdx)
				//	for i := r.commitIndex + 1; i <= pendingCommitIdx; i++ {
				//		entry := r.log.entries[i-r.log.startIndex-1]
				//		err := entry.Encode(r.log.f)
				//		if entry.cmd != nil {
				//			entry.c <- err
				//		}
				//
				//	}
				//	r.log.Sync()
				//}
			}
		}
	} else {
		if resp.Term > currTerm {
			//todo 状态更新合并到内部通信的channel，减少锁竞争
			//r.currTerm = resp.Term
			//r.leaderId = -1
			//r.vi.reset()
			//if r.currentState == Leader {
			//	r.stopHeartBeat()
			//	r.currentState = Follower
			//}

		} else if resp.Term == p.r.currTerm && resp.CommitIndex > p.PrevLogIndex {
			p.SetPrevLogIndex(resp.CommitIndex)
		} else if p.PrevLogIndex > resp.LastIndex {
			p.SetPrevLogIndex(resp.LastIndex)
		}
	}

	p.r.evChan <- &event{payload: resp}
}

func (p *Peer) GetPrevLogIndex() uint64 {
	p.RLock()
	defer p.RUnlock()
	return p.PrevLogIndex
}

func (p *Peer) SetPrevLogIndex(i uint64) {
	p.Lock()
	p.PrevLogIndex = i
	p.Unlock()
}

type event struct {
	payload     interface{}
	returnValue []byte
	c           chan error
}

type Raft struct {
	host     string
	port     int
	leaderId int
	servID   int

	currTerm     uint64
	currentState int
	curNode      string
	peers        map[int]*Peer
	evChan       chan *event
	cmdChan      chan Command

	log *Log
	vi  *voteInfo
	si  *syncInfo
	//sync.RWMutex
	deadlock.RWMutex
	// all servers
	commitIndex uint64
}

func NewRaft(id int, host string, port int) *Raft {
	r := &Raft{
		servID:       id,
		currentState: Follower,
		peers:        make(map[int]*Peer),
		curNode:      fmt.Sprintf("%s:%d", host, port),
		vi:           newVoteInfo(),
		si:           newSyncInfo(),
		evChan:       make(chan *event, 10),
		cmdChan:      make(chan Command, 10),
		log:          newLog(),
		host:         host,
		port:         port,
	}

	err := r.log.init("./persist.log", r)
	if err != nil {
		panic(err)
	}

	return r
}

func (r *Raft) isPeerStarted(servID int) bool {
	r.RLock()
	defer r.RUnlock()
	p, ok := r.peers[servID]
	if !ok {
		return false
	}
	return p.IsStarted()

}

func (r *Raft) Run() {

	fmt.Println("--->node Run", r.curNode, r.currentState)
	for {
		//判断当前节点的状态
		switch r.currentState {
		case Leader:
			fmt.Printf("---->[%s], node:[%s] now is  leader, %+v \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode, r.peers)
			r.leaderLoop()
		case Follower:
			fmt.Printf("---->[%s], node:[%s] now is  follower \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode)
			r.followerLoop()
		case Candidate:
			fmt.Printf("---->[%s], node:[%s] now is  candidate \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode)
			r.candidateLoop()
		}
	}
}

func (r *Raft) addPeer(servID int, p *Peer) {
	r.peers[servID] = p
}

type JoinRequest struct {
	ServId   int    `json:"serv_id"`
	AddrInfo string `json:"addr_info"`
}

func (j *JoinRequest) Encode() ([]byte, error) {
	return json.Marshal(j)
}

func (j *JoinRequest) Decode(b []byte) error {
	return json.Unmarshal(b, &j)
}

func newVoteRequestResponse(granted bool, term uint64, err error) *VoteRequestResponse {
	v := &VoteRequestResponse{
		Granted: granted,
		Term:    term,
	}
	if err != nil {
		v.ErrMsg = err.Error()
	}
	return v
}

type VoteRequestResponse struct {
	Granted bool   `json:"granted"`
	Term    uint64 `json:"term"`
	ErrMsg  string `json:"err,omitempty"`
}

func (v *VoteRequestResponse) Encode() ([]byte, error) {
	return json.Marshal(v)
}

func (v *VoteRequestResponse) Decode(b []byte) error {
	return json.Unmarshal(b, &v)
}

func (v *VoteRequestResponse) Return() ([]byte, error) {
	ret, err := v.Encode()
	if err != nil {
		return nil, err
	}
	if v.ErrMsg != "" {
		return ret, errors.New(v.ErrMsg)
	}
	return ret, nil
}

func newAppendEntryResponse(t uint64, commitIdx uint64, s bool, err error) *AppendEntryResponse {
	r := &AppendEntryResponse{
		Succ:        s,
		Term:        t,
		CommitIndex: commitIdx,
		//ErrMsg:      err.Error(),
	}
	if err != nil {
		r.ErrMsg = err.Error()
	}

	return r
}

type AppendEntryResponse struct {
	Succ        bool   `json:"success"`
	Term        uint64 `json:"term"`
	LastIndex   uint64 `json:"last_index"`
	CommitIndex uint64 `json:"commit_index"`
	ErrMsg      string `json:"-"`
	append      bool
}

func (a *AppendEntryResponse) Decode(data []byte) error {
	return json.Unmarshal(data, &a)
}

func (a *AppendEntryResponse) Encode() ([]byte, error) {
	return json.Marshal(a)
}

func (a *AppendEntryResponse) Return() ([]byte, error) {
	ret, err := a.Encode()
	if err != nil {
		return nil, err
	}
	if a.ErrMsg != "" {
		return ret, errors.New(a.ErrMsg)
	}
	return ret, nil
}

func (a *AppendEntryResponse) Success() bool {
	return a.Succ
}

type RequestVote struct {
	Term         uint64 `json:"term"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
	CommitIdx    uint64 `json:"commitIdx"`
	CandidateId  int    `json:"candidateId"`
	ServId       int    `json:"servId"`
}

func (v *RequestVote) Json() ([]byte, error) {
	return json.Marshal(v)
}

func (r *Raft) electionTimeout() time.Duration {

	base := 150 // 选举超时时间150-300ms
	source := rand.NewSource(time.Now().UnixMilli())
	tm := rand.New(source).Int()%150 + base
	return time.Duration(tm) * time.Millisecond

}
func (r *Raft) quorumSize() int {
	return (len(r.peers) + 1) / 2
}

func newVoteInfo() *voteInfo {
	return &voteInfo{
		ret:      1,
		votedFor: -1,
	}
}

func newSyncInfo() *syncInfo {
	return &syncInfo{
		c: 0,
	}
}

type syncInfo struct {
	c int
	sync.RWMutex
}

func (s *syncInfo) getCount() int {
	s.RLock()
	defer s.RUnlock()
	return s.c
}

func (s *syncInfo) incr() {
	s.Lock()
	s.c += 1
	s.Unlock()
}

func (s *syncInfo) reset() {
	s.Lock()
	s.c = 0
	s.Unlock()
}

func (r *Raft) isSyncedToQuorum() bool {
	return r.si.getCount() >= r.quorumSize()
}

type voteInfo struct {
	ret int32
	sync.RWMutex
	votedFor int
}

func (v *voteInfo) reset() {
	v.Lock()
	v.votedFor = -1
	v.ret = 0
	v.Unlock()
}

func (v *voteInfo) voteFor(servID int) {
	v.Lock()
	v.votedFor = servID
	v.ret = 0
	v.Unlock()
}

func (v *voteInfo) Incr() {
	v.Lock()
	v.ret += 1
	v.Unlock()
}

func (v *voteInfo) isReachQuorumSize(quorumSize int) bool {
	v.RLock()
	defer v.RUnlock()
	return v.ret >= int32(quorumSize)
}

type AppendEntries struct {
	Term         uint64      `json:"term"`
	LeaderId     int         `json:"leader_id"`
	PrevLogIndex uint64      `json:"prev_log_idx"`
	PrevLogTerm  uint64      `json:"prev_log_term"`
	Entries      []*logEntry `json:"entries"`
	LeaderCommit uint64      `json:"leader_commit"`
}

func (ae *AppendEntries) Json() ([]byte, error) {
	return json.Marshal(&ae)
}
