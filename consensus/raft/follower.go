package raft

import (
	"errors"
	"time"
)

func (r *Raft) followerLoop() {

	timeoutChan := time.After(r.electionTimeout())
	isResetElectTimer := false
	for r.currentState == Follower {
		select {
		case e := <-r.evChan:
			var err error
			switch req := e.payload.(type) {
			case *AppendEntries:
				e.returnValue, err = r.processAppendEntryRequestForFollower(req)
				if err == nil {
					isResetElectTimer = true
				}
			case *RequestVote:
				e.returnValue, err = r.processVoteRequest(req)
			}
			e.c <- err
		case cmd := <-r.cmdChan:
			var err error
			switch c := cmd.(type) {
			case *joinCommand:
				err = c.Apply(r)
				c.c <- err
			}

		case <-timeoutChan:
			r.Lock()
			if r.commitIndex == 0 && len(r.log.entries) > 0 {
				r.currentState = Candidate
				return
			} else {
				isResetElectTimer = true
			}
			r.Unlock()
		}

		if isResetElectTimer {
			timeoutChan = time.After(r.electionTimeout())
		}
	}
}

var (
	ErrVRClientSmallTerm     = errors.New("[VoteRequest] request term smaller than current term")
	ErrHasVoted              = errors.New("current server has voted")
	ErrCommitIndexIsOutdated = errors.New("peer's log commit index server is outdated")
	ErrLogIsOutdated         = errors.New("peer's log index or term  is outdated")
)

func (r *Raft) processVoteRequest(req *RequestVote) ([]byte, error) {
	if req.Term < r.currTerm {
		return newVoteRequestResponse(false, r.currTerm, ErrVRClientSmallTerm).Return()
	}
	//说明有新任期的Candidate发来, 更新任期
	if req.Term > r.currTerm {
		//当前不管是什么角色，先重置为Follower,leader 改为未知
		if r.currentState == Leader {
			r.stopHeartBeat()
			r.currentState = Follower
		} else if r.currentState == Candidate {
			r.currentState = Follower
		}
		r.leaderId = -1
		r.currTerm = req.Term
	} else if req.Term == r.currTerm {
		if r.vi.votedFor != -1 && r.vi.votedFor != req.CandidateId {
			return newVoteRequestResponse(false, r.currTerm, ErrHasVoted).Return()
		}
	}

	if len(r.log.entries) > 0 {
		lastLog := r.log.entries[len(r.log.entries)-1]
		if req.LastLogTerm < lastLog.Term() || req.LastLogIndex < lastLog.Index() {
			return newVoteRequestResponse(false, r.currTerm, ErrLogIsOutdated).Return()
		}
	}

	if req.CommitIdx < r.commitIndex {
		return newVoteRequestResponse(false, r.currTerm, ErrCommitIndexIsOutdated).Return()
	}

	r.vi.votedFor = req.CandidateId
	return newVoteRequestResponse(true, r.currTerm, nil).Return()
}

var (
	ErrAEClientSmallTerm = errors.New("[AppendEntries] request term smaller than current term")
	ErrAEPrevRspLost     = errors.New("[AppendEntries] prev rsp lost")
)

func (r *Raft) processAppendEntryRequestForFollower(req *AppendEntries) ([]byte, error) {
	//！！！ 这个阶段重点判断任期term
	if req.Term < r.currTerm {
		return newAppendEntryResponse(r.currTerm, r.commitIndex, false, ErrAEClientSmallTerm).Return()
	}
	if req.Term > r.currTerm {
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
		//或者是新节点
		//这种情况直接忽略
		return newAppendEntryResponse(r.currTerm, r.commitIndex, false, ErrAEPrevRspLost).Return()
	}

	// 这种情况只是简单的校验当前节点较leader有不少节点需要被截断，需要leader进行多次prevLogIndex--
	// 性能比较低，这里直接改成循环遍历到正确的index
	//if req.PrevLogIndex > r.startIndex+int64(len(r.log.entries)) {
	//	return newAppendEntryResponse(r.currTerm, r.commitIdx, false).Encode()
	//}
	//清理日志
	//r.truncate(req.PrevLogIndex, req.PrevLogTerm)
	offset := -1
	if len(r.log.entries) > 0 {
		offset = len(r.log.entries) - 1
		for i := offset; i >= 0; i-- {
			if r.log.entries[i].Index() == req.PrevLogIndex && r.log.entries[i].Term() == req.PrevLogTerm {
				offset = i
				break
			}
		}
	}

	//追加日志
	if offset == len(r.log.entries)-1 {
		r.log.entries = append(r.log.entries, req.Entries...)
	} else {
		r.log.entries = append(r.log.entries[offset:], req.Entries[offset+1:]...)
	}

	//设置commitIdx
	r.setCommitIndex(req.LeaderCommit)
	//if req.LeaderCommit > r.commitIdx && len(req.Entries) != 0 {
	//	r.commitIdx = min(req.LeaderCommit, req.Entries[len(req.Entries)-1].Index())
	//}

	return newAppendEntryResponse(r.currTerm, r.commitIndex, true, nil).Return()
}

func (r *Raft) setCommitIndex(commitIdx uint64) error {

	r.Lock()
	defer r.Unlock()
	//这是因为leader会按一定长度(batchSize)推送，如果leader已经提交超过batchSize
	//此时commitIdx 会大于当前的日志数据，此时，就按当前的日志数据走，不会影响，等下次
	//心跳时，会继续按prevLogIndex推送, leader提交的时候，会取所有follower中有最小的prevLogIdx的follower的PrevLogIndex作为临时commitIdx
	//,最后会拿这个临时commitIdx 和leader的commitIndex,做比较，如果大于leader的commitIndex, 则成功提交。
	if commitIdx > (r.log.startIndex + uint64(len(r.log.entries))) {
		commitIdx = r.log.startIndex + uint64(len(r.log.entries))
	}

	//出现在旧leader[1,2,3]3还没提交，出现网络分区, commit2
	//新leader[1,2,4]，4已经提交了，此时收到旧leaderde commitIdx是小于当前的commitIndex直接忽略
	if commitIdx < r.commitIndex {
		return nil
	}

	for i := r.commitIndex + 1; i < commitIdx; i++ {
		entryIndex := i - r.log.startIndex - 1
		entry := r.log.entries[entryIndex]
		err := entry.cmd.Apply(r)
		if err != nil {
			return err
		}
		r.commitIndex = entry.Index()
	}

	return nil
}

func min(x int64, y int64) int64 {
	if x > y {
		return y
	}

	return x
}
