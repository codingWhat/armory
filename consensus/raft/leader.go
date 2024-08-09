package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
)

func (r *Raft) leaderLoop() {

	/*
		All Servers:
		• If commitIndex > lastApplied: increment lastApplied, apply
		log[lastApplied] to state machine (§5.3)
		• If RPC request or response contains term T > currentTerm:
		set currentTerm = T, convert to follower (§5.1)

		Upon election: send initial empty AppendEntries RPCs
		(heartbeat) to each server; repeat during idle periods to
		prevent election timeouts (§5.2)
		• If command received from client: append entry to local log,
		respond after entry applied to state machine (§5.3)
		• If last log index ≥ nextIndex for a follower: send
		AppendEntries RPC with log entries starting at nextIndex
		• If successful: update nextIndex and matchIndex for
		follower (§5.3)
		• If AppendEntries fails because of log inconsistency:
		decrement nextIndex and retry (§5.3)
		• If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N (§5.3, §5.4).
	*/

	//重置同步信息(同步到follower的信息)
	r.si.reset()
	_, idx := r.lastLogInfo()
	fmt.Println("---->leader Loop", r.curNode, r.peers)
	for _, p := range r.peers {
		if p.Addr == r.curNode {
			continue
		}
		if p.IsStarted() { //第一次加入的follower, 已经在joinCommand执行一次了。就不重复执行
			continue
		}
		p.SetPrevLogIndex(idx)
		p.start()
	}

	r.cmdChan <- &NopCommand{}
	for r.currentState == Leader {

		select {
		case e := <-r.evChan:
			fmt.Println("--->leader evChan", e, e.payload)
			var err error
			switch req := e.payload.(type) {
			case *AppendEntryResponse:
				r.processAppendEntryRsp(req)
			case *RequestVote:
				e.returnValue, err = r.processVoteRequest(req)
			case *AppendEntries:
				e.returnValue, err = r.processAppendEntryRequestForLeader(req)
			}
			if e.c != nil {
				e.c <- err
			}

		case cmd := <-r.cmdChan:
			var err error
			switch c := cmd.(type) {
			case *joinCommand:
				fmt.Println("---> ready to execute sendJoinRequest, before, ", r.peers)
				err = c.Apply(r)
				fmt.Println("---> ready to execute sendJoinRequest,after ", r.peers)
				if c.RspChannel() != nil {
					c.RspChannel() <- err
				}
			case *JoinConsensusCommand:
				err := r.appendEntries(c)
				if err != nil {
					c.RspChannel() <- err
				}
			case *NopCommand:
				err := r.appendEntries(c)
				if err != nil {
					c.RspChannel() <- err
				}
			}
		}
	}
}

func (r *Raft) processAppendEntryRsp(rsp *AppendEntryResponse) {

	if rsp.Term < r.currTerm {
		return
	}
	if rsp.Term > r.currTerm {
		r.currTerm = rsp.Term
		r.leaderId = -1
		r.vi.reset()
		if r.currentState == Leader {
			r.stopHeartBeat()
			r.currentState = Follower
		}
	}

	//刷盘操作异步
	if !r.isSyncedToQuorum() {
		return
	}
	pendingCommitIdx := r.getSmallestPrevLogIndex()
	if pendingCommitIdx > r.commitIndex {
		r.setCommitIndex(pendingCommitIdx)
		for i := r.commitIndex + 1; i <= pendingCommitIdx; i++ {
			//entry := r.log.entries[i-r.log.startIndex-1]
			//err := entry.Encode(r.log.f)
			//if entry.cmd != nil {
			//	entry.c <- err
			//}

		}
		_ = r.log.Sync()
	}
	fmt.Println("---->> commit")
	return
}

func (r *Raft) appendEntries(c Command) error {
	entry := newLogEntry(r.currTerm, r.log.nextIndex(), c)
	err := r.log.Write(entry)
	if err != nil {
		return err
	}
	r.si.incr()
	return nil
}

func (r *Raft) stopHeartBeat() {
	for _, p := range r.peers {
		p.stopCh <- struct{}{}
	}
}
func (r *Raft) lastLogInfo() (term uint64, index uint64) {
	return r.log.lastLogInfo()
}

func (r *Raft) getTermByIndex(idx uint64) uint64 {
	t := r.log.getTermByIndex(idx)
	if t < r.currTerm {
		return r.currTerm
	}
	return t
}

func (r *Raft) getCurrentTerm() uint64 {
	r.RLock()
	defer r.RUnlock()

	return r.currTerm
}

func (r *Raft) getServID() int {
	r.RLock()
	defer r.RUnlock()

	return r.servID
}

func (r *Raft) getCommitID() uint64 {
	r.RLock()
	defer r.RUnlock()

	return r.commitIndex
}

//
//func (r *Raft) sendAppendEntries(p *Peer) {
//	prevLogIndex := p.GetPrevLogIndex()
//	fmt.Println("sendAppendEntries---->", p.Addr, prevLogIndex)
//
//	ae := &AppendEntries{
//		Term:         r.getCurrentTerm(),
//		LeaderId:     r.getServID(),
//		PrevLogIndex: prevLogIndex,
//		PrevLogTerm:  r.getTermByIndex(prevLogIndex),
//		LeaderCommit: r.getCommitID(),
//		Entries:      r.log.GetEntriesAfter(prevLogIndex),
//	}
//	resp := r.sendAppendEntryRequest(p.Addr, ae)
//	if resp == nil {
//		fmt.Println("[", time.Now().Format("2006-01-02 15:04:05"), "] ", "node:", p.Addr, " sendAppendEntryRequest failed..")
//		return
//	}
//
//	//r.Lock()
//	//defer r.Unlock()
//	if resp.Success() {
//		if len(ae.Entries) > 0 {
//			lastLog := ae.Entries[len(ae.Entries)-1]
//			p.PrevLogIndex = lastLog.Index()
//			//当前leader只能提交当前任期的日志,
//			//看论文Figure-8 如果当前任期比日志的任期新，此时这批日志就不能提交，否则就会发生Figure8中，任期3会把已经提交的数据给覆盖了，raft是不允许的。
//			//只能等待新请求，这就引入了NOP-COMMAND, 根据日志匹配属性，如果返回成功了，说明保证一致性了。
//			if lastLog.Term() == r.getCurrentTerm() {
//				r.si.incr()
//				//todo 状态更新合并到内部通信的channel，减少锁竞争
//				//刷盘操作异步
//				//if !r.isSyncedToQuorum() {
//				//	return
//				//}
//				//pendingCommitIdx := r.getSmallestPrevLogIndex()
//				//if pendingCommitIdx > r.commitIndex {
//				//	r.setCommitIndex(pendingCommitIdx)
//				//	for i := r.commitIndex + 1; i <= pendingCommitIdx; i++ {
//				//		entry := r.log.entries[i-r.log.startIndex-1]
//				//		err := entry.Encode(r.log.f)
//				//		if entry.cmd != nil {
//				//			entry.c <- err
//				//		}
//				//
//				//	}
//				//	r.log.Sync()
//				//}
//			}
//		}
//	} else {
//		if resp.Term > r.getCurrentTerm() {
//			//todo 状态更新合并到内部通信的channel，减少锁竞争
//			//r.currTerm = resp.Term
//			//r.leaderId = -1
//			//r.vi.reset()
//			//if r.currentState == Leader {
//			//	r.stopHeartBeat()
//			//	r.currentState = Follower
//			//}
//
//		} else if resp.Term == r.currTerm && resp.CommitIndex > p.PrevLogIndex {
//			p.PrevLogIndex = resp.CommitIndex
//		} else if p.PrevLogIndex > resp.LastIndex {
//			p.PrevLogIndex = resp.LastIndex
//		}
//	}
//
//	fmt.Println("---->111222", resp)
//	r.evChan <- &event{payload: resp}
//	fmt.Println("---->1112233", resp)
//}

func (r *Raft) getSmallestPrevLogIndex() uint64 {

	minIdx := uint64(math.MaxInt64)
	for _, peer := range r.peers {
		if peer.GetPrevLogIndex() < minIdx {
			minIdx = peer.GetPrevLogIndex()
		}
	}
	return minIdx
}

func (r *Raft) processAppendEntryRequestForLeader(req *AppendEntries) ([]byte, error) {
	//！！！ 这个阶段重点判断任期term
	if req.Term < r.currTerm {
		return newAppendEntryResponse(r.currTerm, r.commitIndex, false, ErrAEClientSmallTerm).Return()
	}
	if req.Term > r.currTerm {
		for _, p := range r.peers {
			if p.Addr == r.curNode {
				continue
			}
			p.stopCh <- struct{}{}
		}

		//新leader被选举出来了， follower 直接跟随，更新votedFor信息
		r.Lock()
		r.currTerm = req.Term
		r.leaderId = req.LeaderId //leader发生变更了
		r.vi.votedFor = -1
		r.Unlock()
	} else {
		r.leaderId = req.LeaderId
	}

	//!!! 这个阶段重点判断prevLogIndex
	if req.PrevLogIndex < r.commitIndex {
		//这种情况就是上次给leader的回包出问题了，leader没有及时更新prevLogIndex
		//这种情况直接忽略
		return newAppendEntryResponse(r.currTerm, r.commitIndex, false, ErrAEPrevRspLost).Return()
	}

	// 这种情况只是简单的校验当前节点较leader有不少节点需要被截断，需要leader进行多次prevLogIndex--
	// 性能比较低，这里直接改成循环遍历到正确的index
	//if req.PrevLogIndex > r.startIndex+int64(len(r.log)) {
	//	return newAppendEntryResponse(r.currTerm, r.commitIdx, false).Encode()
	//}
	r.log.safeAppendEntries(req.PrevLogIndex, req.PrevLogTerm, req.Entries)

	//设置commitIdx
	r.setCommitIndex(req.LeaderCommit)
	//if req.LeaderCommit > r.commitIdx && len(req.Entries) != 0 {
	//	r.commitIdx = min(req.LeaderCommit, req.Entries[len(req.Entries)-1].Index())
	//}

	return newAppendEntryResponse(r.currTerm, r.commitIndex, true, nil).Return()
}

func (r *Raft) HandleCmd(w http.ResponseWriter, req *http.Request) {
	if r.currentState != Leader {
		newClientRsp(w, 1, "not leader, leader is "+r.getLeaderInfo())
		return
	}
}

func (r *Raft) Do(cmd Command) error {
	r.cmdChan <- cmd
	return <-cmd.RspChannel()
}

func (r *Raft) getLeaderInfo() string {
	if r.leaderId != -1 && len(r.peers) != 0 {
		return r.peers[r.leaderId].Addr
	}
	return ""
}
func newClientRsp(w io.Writer, c int, m string) *ClientResponse {
	return &ClientResponse{
		Writer: w,
		Code:   c,
		Msg:    m,
	}
}

type ClientResponse struct {
	Writer io.Writer
	Code   int    `json:"sdk"`
	Msg    string `json:"msg"`
}

func (c *ClientResponse) Return() (int, error) {
	b, err := json.Marshal(c)
	if err != nil {
		c.Msg = err.Error()
	}
	return c.Writer.Write(b)
}
