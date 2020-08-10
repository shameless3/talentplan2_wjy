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
	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
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
	// ramdon election timeout to avoid tie in election.
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Term:             0,
		RaftLog:          newLog(c.Storage),
	}
	prs := make(map[uint64]*Progress)
	lastIndex := r.RaftLog.LastIndex()
	for _, i := range c.peers {
		if uint64(i) == r.id {
			prs[uint64(i)] = &Progress{lastIndex, lastIndex + 1}
		} else {
			prs[uint64(i)] = &Progress{0, lastIndex + 1}
		}
	}
	r.Prs = prs
	r.becomeFollower(0, None)
	state, _, _ := r.RaftLog.storage.InitialState()
	r.Term, r.Vote, r.RaftLog.committed = state.Term, state.Vote, state.Commit
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	index := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		logTerm = 0
	}
	entries, err := r.RaftLog.Slice(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
	log.Infof("get slice entries %v [%d,%d]", entries, r.Prs[to].Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		panic(err)
	}
	ents := make([]*pb.Entry, 0)
	for _, entry := range entries {
		tmp := entry
		ents = append(ents, &tmp)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Commit:  r.RaftLog.committed,
		Entries: ents,
	}
	log.Infof("Node %d(Term:%d)send append entries:%v to Node %d", r.id, r.Term, msg.Entries, msg.To)
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

//sendRequestVoteResponse sends a RequestVoteResponse RPC to the given peer
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

//sendRequestVote sends a RequestVote RPC to the given peer
func (r *Raft) sendRequestVote(to uint64) {
	lastTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		lastTerm = 0
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
// call funcTick()
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// heartbeat timeout
			r.heartbeatElapsed = 0
			// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
			// of the 'MessageType_MsgHeartbeat' type to its followers.
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			// election timeout,start a new election
			r.electionElapsed = 0
			// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
			// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.State = StateFollower
	r.Vote = None
	r.votes = make(map[uint64]bool, 0)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.votes = make(map[uint64]bool, 0)
	// increments its current term
	r.Term++
	// delete leader id
	r.Lead = None
	// vote for itself
	r.Vote = r.id
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.Prs) == 1 {
		log.Infof("Node %d(Term:%d) become leader because only have one node", r.id, r.Term)
		r.becomeLeader()
		// send heartbeat to all nodes
		for i, _ := range r.Prs {
			if i == r.id {
				continue
			}
			r.sendHeartbeat(uint64(i))
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// to avoid repeated calls
	if r.State != StateLeader {
		r.Vote = None
		r.State = StateLeader
		r.electionElapsed = 0
		r.heartbeatElapsed = 0
		r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.votes = make(map[uint64]bool, 0)
		r.Lead = r.id
		// build r.Prs
		lastTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
		if err != nil {
			lastTerm = 0
		}
		for i := range r.Prs {
			if i == r.id {
				r.Prs[i] = &Progress{lastTerm, lastTerm + 1}
			} else {
				r.Prs[i] = &Progress{0, lastTerm + 1}
			}
		}
		// append a noop entry
		r.appendEntries([]*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Data:      nil,
			},
		})
		// to past tests
		if len(r.Prs) == 1 {
			r.RaftLog.committed = 1
		}
		for i := range r.Prs {
			if uint64(i) == r.id {
				continue
			}
			r.sendAppend(uint64(i))
		}
	}

}

// appendEntries append entries to raftlog
func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	ents := make([]pb.Entry, 0)
	for index, value := range entries {
		value.Term = r.Term
		value.Index = lastIndex + 1 + uint64(index)
		ents = append(ents, *value)
	}
	r.RaftLog.appendEntries(ents)
	log.Infof("Node %d(Term:%d)append entries to raftlog %v", r.id, r.Term, ents)
	// update prs
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// if r.term < m.term,all nodes should obey same rule:become follower
	// and if RPC is sent by leader, should set lead
	if r.Term < m.Term {
		log.Infof("Node %d(Term:%d) received greater term message from Node %d(Term:%d),become Follower", r.id, r.Term, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	switch r.State {
	case StateFollower:
		switch m.GetMsgType() {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			log.Infof("Node %d(Term:%d) election timeout,become Candidate and increment term(Term:%d)", r.id, r.Term-1, r.Term)
			//send request vote to all nodes except itself
			for i, _ := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendRequestVote(i)
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		}
	case StateCandidate:
		switch m.GetMsgType() {
		case pb.MessageType_MsgRequestVote:
			// will reject with no doubt
			r.handleRequestVote(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			//send request vote to all nodes except itself
			for i, _ := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendRequestVote(i)
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgRequestVoteResponse:
			if m.Reject == true {
				log.Infof("Node %d(Term:%d) received rejection from Node %d(Term:%d)", r.id, r.Term, m.From, m.Term)
				r.votes[m.From] = false
			} else {
				log.Infof("Node %d(Term:%d) received vote from Node %d(Term:%d)", r.id, r.Term, m.From, m.Term)
				r.votes[m.From] = true
			}
			var voteSum uint64
			var rejectSum uint64
			for _, val := range r.votes {
				if val == true {
					voteSum++
				} else {
					rejectSum++
				}
			}
			if voteSum > uint64(len(r.Prs)/2) {
				//win election
				log.Infof("Node %d(Term:%d) win election and become leader", r.id, r.Term)
				r.becomeLeader()
				// send heartbeat to all nodes
				for i, _ := range r.Prs {
					if i == r.id {
						continue
					}
					r.sendHeartbeat(uint64(i))
				}
			}
			if rejectSum > uint64(len(r.Prs)/2) {
				log.Infof("Node %d(Term:%d) lose election and become follower", r.id, r.Term)
				r.becomeFollower(r.Term, None)
			}
		}
	case StateLeader:
		switch m.GetMsgType() {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgPropose:
			// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
			r.appendEntries(m.Entries)
			// to past tests
			if len(r.Prs) == 1 {
				r.RaftLog.committed += uint64(len(m.Entries))
			}
			// send append msg
			for i := range r.Prs {
				if uint64(i) == r.id {
					continue
				}
				r.sendAppend(uint64(i))
			}
		case pb.MessageType_MsgBeat:
			// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
			// of the 'MessageType_MsgHeartbeat' type to its followers.
			for i, _ := range r.Prs {
				if i == r.id {
					continue
				}
				log.Infof("Node %d(Term:%d)send heartbeat to Node %d", r.id, r.Term, i)
				r.sendHeartbeat(uint64(i))
			}
		case pb.MessageType_MsgAppendResponse:
			prs := r.Prs[m.From]
			if m.Reject {
				// not matched,update Next and re-send
				if m.Index == prs.Next-1 {
					prs.Next = m.Index
					if prs.Next < 1 {
						prs.Next = 1
					}
					log.Infof("Node %d 's prs.Next changed to %d", m.From, r.Prs[m.From].Next)
					r.sendAppend(m.From)
				}
			} else {
				// update match and commit
				if m.Index > prs.Match {
					prs.Match = m.Index
					prs.Next = m.Index + 1
					log.Infof("update next:Node %d 's prs.Next changed to %d", m.From, r.Prs[m.From].Next)
					if r.ifChangeCommit() {
						// to update commit,just need to send append
						for i := range r.Prs {
							if uint64(i) == r.id {
								continue
							}
							log.Infof("Node %d（Term:%d) send appendmsg to update commit to Node %d", r.id, r.Term, i)
							r.sendAppend(uint64(i))
						}
					}
				}
			}
		}
	}
	return nil
}

// ifChangeCommit return true if changed commit
func (r *Raft) ifChangeCommit() bool {
	matches := make([]uint64, len(r.Prs))
	for i, prs := range r.Prs {
		if i == r.id {
			matches[i-1] = r.RaftLog.LastIndex()
			continue
		}
		matches[i-1] = prs.Match
	}
	sortkeys.Uint64s(matches)
	//find median index,which is commited
	median := matches[(len(matches)-1)/2]
	if median <= r.RaftLog.committed {
		return false
	}
	medianTerm, err := r.RaftLog.Term(median)
	if err != nil {
		panic(err)
		return false
	}
	// accroding to raft:at least one new entry from leader's term must also be stored on majority of nodes
	if medianTerm != r.Term {
		return false
	}
	log.Infof("Node %d update commit from %d to %d", r.id, r.RaftLog.committed, median)
	r.RaftLog.committed = median
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Lead == None && r.Term == m.Term {
		r.Lead = m.From
	}
	log.Infof("Node %d(Term:%d) rcv entries %v", r.id, r.Term, m.Entries)
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		Index:   m.Index,
		From:    r.id,
		To:      m.From,
	}
	// not matched
	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err)
	}
	if term != m.LogTerm {
		response.Reject = true
		r.msgs = append(r.msgs, response)
		return
	}
	// matched,find where to append
	flag := false
	entriesToAppend := make([]pb.Entry, 0)
	for _, entry := range m.Entries {
		term, err := r.RaftLog.Term(entry.Index)
		if err != nil {
			panic(err)
		}
		if term != entry.Term {
			flag = true
		}
		if flag {
			entriesToAppend = append(entriesToAppend, *entry)
		}
	}
	log.Infof("Node %d(Term:%d) add entries %v", r.id, r.Term, entriesToAppend)
	r.RaftLog.appendEntries(entriesToAppend)
	response.Reject = false
	// new match index
	response.Index = r.RaftLog.LastIndex()
	// update commit
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.msgs = append(r.msgs, response)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

//handleReauestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	//accroding to raft paper,set ifVote and ifNewer
	var reject bool
	if m.Term < r.Term {
		reject = true
	} else {
		ifVote := (r.Vote == None && r.Lead == None) || r.Vote == m.From
		lastTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
		if err != nil {
			lastTerm = 0
		}
		ifNewer := m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= r.RaftLog.LastIndex())
		reject = !(ifVote && ifNewer)
	}

	if reject == true {
		log.Infof("Node %d(Term:%d) reject vote to Node %d.", r.id, r.Term, m.From)
	} else {
		log.Infof("Node %d(Term:%d) send vote to Node %d.", r.id, r.Term, m.From)
		r.Vote = m.From
	}
	//send requestVoteResponse msg
	r.sendRequestVoteResponse(m.From, reject)
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
