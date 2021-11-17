// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

//返回role
func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	//node的id
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	//其他node
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	//就是raft中的election timeout
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	//心跳检测的时间间隔
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	//存储结构
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	//是最后一个执行的日志的index,raft不会返回已经执行过的log
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
//leader维持follower的进度，match，next和6.824差不多。next表示下一次要给follower发送的index，
//match是follower匹配的index
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionTimeout int
}



// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _ , p := range c.peers{
		prs[p] = &Progress{}
	}
	state, _, _ := c.Storage.InitialState()
	raft := &Raft{
		id:               c.ID,
		Term: state.Term,
		Vote:             state.Vote,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool,0),
		Lead:             None,
		RaftLog: newLog(c.Storage),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed: 0,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
	return raft
}

func (r *Raft) resetTime(){
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool , 0)
	r.Lead = None
}
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	nextIndex := r.Prs[to].Next-1
	entries := r.RaftLog.entries[nextIndex:]
	logterm := r.RaftLog.entries[nextIndex].Term
	m :=pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              logterm,
		Index:                nextIndex,
		Entries:              make([]*pb.Entry,0),
		Commit:               r.RaftLog.committed,
	}
	//m.Entries = append(m.Entries , noopMesg)
	for _ , entry := range entries{
		m.Entries = append(m.Entries,&entry)
	}
	DPrintf("[%d] 给 [%d] 发送sendappend发送的条目个数为[%d],条目为[%+v]" ,r.id , to,len(m.Entries), m)
	r.msgs = append(r.msgs , m)

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeat,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
	}
	r.msgs = append(r.msgs , m)
}

func (r *Raft) sendRequestVote(to uint64) {
	var logTerm uint64
	if len(r.RaftLog.entries) > 0{
		logTerm = r.RaftLog.entries[len(r.RaftLog.entries)-1].Term
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index: 	 r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}



// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower ,StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout{
			r.electionElapsed = 0
			_= r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    0,
				Term:    0,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout{
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				Term:    r.Term,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term < r.Term{
		return
	}
	r.resetTime()
	r.Term = term
	r.Vote = None
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetTime()
	r.Term ++
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true
	if len(r.Prs) <= 1{
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateLeader{
		r.State = StateLeader
		r.Lead = r.id
		r.resetTime()
	}
	DPrintf("[%d]当选leader ",r.id)
	for p := range r.Prs{
		r.Prs[p].Next = r.RaftLog.LastIndex()+1
		DPrintf("对[%d]的prs为%d",p , r.Prs[p].Next)
		r.Prs[p].Match = 0
	}


	msg := pb.Message{MsgType: pb.MessageType_MsgPropose}
	msg.Entries = make([]*pb.Entry,0)
	msg.Entries = append(msg.Entries , &pb.Entry{Data: nil})
	r.Step(msg)

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//这三个消息没有term，不能根据term来判断,任何时候，收到term比自己大的message，都需要变成follower
	//if !IsLocalMsg(m.MsgType) && m.Term > r.Term {
	//	r.becomeFollower(m.Term, m.From)
	//}

	switch m.MsgType {
	//发起election
	case pb.MessageType_MsgHup:
		switch r.State {
		case StateFollower,StateCandidate:
			r.becomeCandidate()
			for p := range r.Prs {
				if p != r.id {
					r.sendRequestVote(p)
				}
			}
		}
	//	发送心跳检测
	case pb.MessageType_MsgBeat:
		switch r.State {
		case StateLeader:
			for p := range r.Prs {
				if p != r.id {
					r.sendHeartbeat(p)
				}
			}
		}
	//	通知leader添加日志，同时leader发送日志给其他node。如果只有一个节点，直接commit
	case pb.MessageType_MsgPropose:
		switch r.State {
		case StateLeader:
			for  _ , entry := range m.Entries{
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			//DPrintf("节点[%d]添加日志，日志为[%+v]",r.id , r.RaftLog.entries)
			//if len(r.Prs) == 1{
			//	r.RaftLog.committed += uint64(len(m.Entries))
			//}
			for p := range r.Prs {
				if p != r.id {
					r.sendAppend(p)
				}
			}
		}
	//	todo
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term{
			r.becomeFollower(m.Term , m.From)
		}
		switch r.State {
		case StateCandidate,StateFollower:
			if r.State == StateCandidate{
				if r.Lead != m.From{
					r.becomeFollower(m.Term , m.From)
				}
			}
			r.handleAppendEntries(m)
		}
	//	todo
	case pb.MessageType_MsgAppendResponse:

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > r.Term{
			r.becomeFollower(m.Term , m.From)
		}
		r.handleRequest(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term{
			r.becomeFollower(m.Term , m.From)
		}
		switch r.State {
		case StateFollower,StateCandidate:
			r.handleHeartbeat(m)
		}

	case pb.MessageType_MsgHeartbeatResponse:
		if m.Term > r.Term{
			r.becomeFollower(m.Term , m.From)
		}
		switch r.State {
		//如果心跳被拒绝接受，则从leader转换成follower
		case StateLeader:
			if m.Reject{
				r.becomeFollower(m.Term , m.From)
			}
		}
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:

	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term < r.Term{
		r.msgs = append(r.msgs , message)
		DPrintf("m.Term < r.Term")
		return
	}
	if m.Term > r.Term{
		r.becomeFollower(m.Term,m.From)
	}
	//message.Reject = false
	//for _ , entry := range m.Entries{
	//	r.RaftLog.entries = append(r.RaftLog.entries , *entry)
	//}
	if m.Index > r.RaftLog.LastIndex() {
		message.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, message)
		DPrintf("m.Index > r.RaftLog.LastIndex()")
		return
	}
	var lastTerm uint64
	lastTerm = 0
	if r.RaftLog.LastIndex() > 0 && m.Index > 0{
		lastTerm = r.RaftLog.entries[m.Index-1].Term
	}
	if lastTerm != m.LogTerm{
		message.Index = m.Index
		r.msgs = append(r.msgs , message)
		DPrintf("lastTerm != m.LogTerm")
		return
	}

	message.Reject = false
	r.RaftLog.entries = r.RaftLog.entries[:m.Index]
	for _ , entry := range m.Entries{
		r.RaftLog.entries = append(r.RaftLog.entries , *entry)
	}
	if m.Commit > r.RaftLog.committed{
		r.RaftLog.committed = min(m.Commit , r.RaftLog.LastIndex()-1)
	}
	r.msgs = append(r.msgs , message)


}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	if m.Term < r.Term{
		message.Reject = true
	} else {
		r.becomeFollower(m.Term , m.From)
		r.Lead = m.From
	}
	r.msgs = append(r.msgs , message)
}

func (r *Raft) handleRequestVote(m pb.Message){
	reject := true
	if m.Term < r.Term{
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs , msg)
		return
	}
	if m.Term > r.Term{
		r.becomeFollower(m.Term , m.From)
	}
	//DPrintf(" id[%d][%d] leader[%d][%d]",r.id,r.RaftLog.LastIndex() , m.From,m.Index)
	if ((r.Vote == None && r.Lead == None) || r.Vote == m.From) &&
		(m.LogTerm > r.RaftLog.LastTerm() ||
			(m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())) {
		reject = false
		r.Vote = m.From
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}

	r.msgs = append(r.msgs, msg)
}

//处理requestresponse
func (r *Raft)handleRequest(m pb.Message){
	r.votes[m.From] = !m.Reject
	agree, finished := 0, 0
	for _, v := range r.votes {
		if v {
			agree++
		}
		finished++
	}
	if finished == len(r.Prs) {
		return
	}
	if len(r.Prs) == 1 || agree > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
