package raft

import (
	"fmt"
	"time"
)

// 专注选举
func (r *Raft) candidateLoop() {
	t := r.electionTimeout()
	electTimeoutCh := time.After(t)
	respCh := make(chan *VoteRequestResponse, len(r.peers))
	isVoted := false
	for r.currentState == Candidate {
		if !isVoted {
			r.currTerm++
			r.vi.voteFor(r.servID)
			r.vi.Incr()
			lastIndex, lastTerm := r.log.lastLogInfo()
			req := &RequestVote{
				Term:         r.currTerm,
				CandidateId:  r.servID,
				CommitIdx:    r.commitIndex,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			jsonData, _ := req.Json()
			for _, peer := range r.peers {
				if peer.Addr == r.curNode {
					continue
				}
				fmt.Printf("---->[%s], node:[%s] start elect, peer:%s, --- %+v \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode, peer.Addr, req)
				go func(p *Peer) {
					rsp := r.sendRequestVote(p.Addr, jsonData)
					if rsp != nil {
						respCh <- rsp
					}
				}(peer)
			}
			isVoted = true
		}

		select {
		case <-electTimeoutCh:
			//重新选举
			electTimeoutCh = time.After(r.electionTimeout())
			r.vi.reset() //重置投票
			isVoted = false
		case e := <-r.evChan:
			var err error
			switch req := e.payload.(type) {
			case *AppendEntries:
				e.returnValue, err = r.processAppendEntryRequestForCandidate(req)
			case *RequestVote:
				e.returnValue, err = r.processVoteRequest(req)
			}
			e.c <- err
		case vr := <-respCh:
			if vr.Term > r.currTerm {
				r.currTerm = vr.Term
				r.currentState = Follower
				return
			}
			if vr.Granted {
				r.vi.Incr()
			}
			//计算获取投票结果是否过半?
			//立即变更为leader，并开始心跳通知其他节点
			if r.vi.isReachQuorumSize(r.quorumSize()) && r.currentState != Leader {
				r.currentState = Leader
				//退出选举
				return
			}
		}
	}
}

func (r *Raft) processAppendEntryRequestForCandidate(req *AppendEntries) ([]byte, error) {

	if req.Term < r.currTerm {
		return newAppendEntryResponse(r.currTerm, r.commitIndex, false, ErrAEClientSmallTerm).Encode()
	}

	r.Lock()
	if req.Term > r.currTerm {
		r.currTerm = req.Term
		r.currentState = Follower
		r.currTerm = req.Term
		r.leaderId = req.LeaderId //leader发生变更了
		r.vi.votedFor = -1

	} else {
		r.leaderId = req.LeaderId
		r.currentState = Follower
	}
	r.Unlock()
	//!!! 这个阶段重点判断prevLogIndex
	if req.PrevLogIndex < r.commitIndex {
		//这种情况就是上次给leader的回包出问题了，leader没有及时更新prevLogIndex
		//这种情况直接忽略
		return newAppendEntryResponse(r.currTerm, r.commitIndex, false, ErrAEPrevRspLost).Encode()
	}

	//清理日志
	offset := len(r.log.entries) - 1
	for i := offset; i >= 0; i-- {
		if r.log.entries[i].Index() == req.PrevLogIndex && r.log.entries[i].Term() == req.PrevLogTerm {
			offset = i
			break
		}
	}
	//追加日志
	if offset == len(r.log.entries)-1 {
		r.log.entries = append(r.log.entries, req.Entries...)
	} else {
		r.log.entries = append(r.log.entries[offset:], req.Entries[offset+1:]...)
	}

	r.setCommitIndex(req.LeaderCommit)
	return newAppendEntryResponse(r.currTerm, r.commitIndex, true, nil).Return()
}
