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
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
// None 是没有leader时使用的占位符节点ID。
const None uint64 = 0

// StateType represents the role of a node in a cluster.
// StateType 代表一个节点在集群中的角色。
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
// Config 包含启动 raft 的参数。
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// ID raft id
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// peers raft 集群中所有节点（包括自身）的 ID。
	// 仅应在启动新的 raft 集群时设置。
	// 如果设置了对等点，则从以前的配置重新启动 raft 将出现恐慌。
	// peer 是私有的，现在只用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// ElectionTick 如果在这个定时器超时之前都没有收到来自leader的心跳请求，
	// 那么follower将认为当前集群中没有leader了，将发起一次新的选举。
	// ElectionTick 必须大于 HeartbeatTick。
	// 我们建议 ElectionTick = 10 * HeartbeatTick 以避免不必要的领导者切换。
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// HeartbeatTick 领导者会在每个 HeartbeatTick 滴答声中发送心跳消息以维持其领导地位。
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	// storage 是 raft 的存储。 raft 生成要存储在 storage 中的条目和状态。
	// raft 在需要时从存储中读取持久化的条目和状态。
	// raft 重启时会从存储中读出之前的状态和配置。
	Storage Storage

	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// Applied 是最后应用的索引。 仅应在重新启动 raft 时设置。
	// raft 不会向应用程序返回小于或等于 Applied 的条目。
	// 如果在重新启动时未设置 Applied，raft 可能会返回以前应用的条目。
	// 这是一个非常依赖于应用程序的配置。
	Applied uint64
}

//检验项
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

//Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
//Progress 代表一个follower在leader视图中的进度。
// 领导者维护所有追随者的进度，并根据其进度向追随者发送entry。
// ——该数据结构用于在leader中保存每个follower的状态信息，
// leader将根据这些信息决定发送给节点的日志
//Match：保存该follower节点上的最大日志索引。
//Next：保存下一次leader发送append消息给该follower时的日志索引。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	//任期号
	Term uint64
	//投票id
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peer
	//记录每个peer的复制进度
	Prs map[uint64]*Progress

	// this peer's role
	// 这个peer的角色
	State StateType

	// votes records
	votes       map[uint64]bool
	voteCount   int
	denialCount int

	// msgs need to send
	//在entries被写入持久化存储中以后，需要发送出去的数据
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	// 心跳间隔，应该发送
	heartbeatTimeout int

	// baseline of election interval
	// 选举间隔的基线
	electionTimeout int

	// real election timeout time.
	// random between et and 2*et
	// 真正的选举超时时间。
	// 在 et 和 2*et 之间随机
	realElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 自到达最后一次 heartbeatTimeout 以来的滴答数。
	// 只有领导者保持 heartbeatElapsed。
	heartbeatElapsed int

	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 当它是领导者或候选人时，自上次选举超时以来的滴答声。
	// 自到达上次选举超时或从当前领导者作为跟随者时收到有效消息以来的滴答数。
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	//leadTransferee 值不为零时为leader transfer target的id。
	// 按照 Raft 博士论文第 3.10 节中定义的过程。
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	//（用于3A的leader transfer）
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 一次只能有一个 conf 配置更改处于挂起状态（在日志中，但尚未应用）。
	// 这是通过 PendingConfIndex 强制执行的，
	// 它设置为一个值 >= 最新挂起配置更改的日志索引（如果有）。
	// 仅当领导者应用的索引大于此值时，才允许提出配置更改。
	//（用于3A conf更改）
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
// newRaft 返回一个具有给定配置的 raft 节点
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	//实例化raft
	//Follower
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		Vote:             hardState.Vote,
		Term:             hardState.Term,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		voteCount:        1,
		denialCount:      0,
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	if c.peers == nil {
		//获取配置信息中的node
		c.peers = confState.Nodes
	}
	//记录raftId为节点id的Progress
	r.Prs[r.id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
	//记录其余Progress
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	//raftId为Follower
	r.becomeFollower(0, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend 发送一个附带有新entry的RPC和当前commit的index 给 to节点
// 如果成功发送了消息，则返回 true。
func (r *Raft) sendAppend(to uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	preLogIndex := r.Prs[to].Next - 1
	if lastIndex < preLogIndex {
		return true
	}
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		return false
	}
	entries := r.RaftLog.Entries(preLogIndex+1, lastIndex+1)
	if err != nil {
		return false
	}
	sendEntreis := make([]*pb.Entry, 0)
	//调整entry参数
	for _, en := range entries {
		sendEntreis = append(sendEntreis, &pb.Entry{
			EntryType: en.EntryType,
			Term:      en.Term,
			Index:     en.Index,
			Data:      en.Data,
		})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntreis,
		Commit:  r.RaftLog.committed,
	}
	//打包成message存储到leader的msgs
	//mags中含有需要发送的entry
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// sendHeartbeat 向给定对等方发送心跳 RPC。
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

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
}

// tick advances the internal logical clock by a single tick.
// tick 将内部逻辑时钟提前一个刻度。
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.isLeader() {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed == r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed += 1
	if r.electionElapsed >= r.realElectionTimeout {
		r.electionElapsed = 0
		r.campaign()
	}
}

// bcastHeartbeat is used by leader to bcast append request to followers
// 领导者使用 bcastHeartbeat 向追随者发送追加请求
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	//根据消息类型处理
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		lastIndex := r.RaftLog.LastIndex()
		ents := make([]*pb.Entry, 0)
		//编辑entry进行存储
		//存储点为lastIndex的下一个下标
		for _, e := range m.Entries {
			ents = append(ents, &pb.Entry{
				EntryType: e.EntryType,
				Term:      r.Term,
				Index:     lastIndex + 1,
				Data:      e.Data,
			})
			//递增下标
			lastIndex += 1
		}
		r.appendEntries(ents...)
		r.bcastAppend()
		r.updateCommit()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransfer(m)
	}
	return nil
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// leader can send log to follower when
	// it received a heartbeat response which
	// indicate it doesn't have update-to-date log
	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

func (r *Raft) isMoreUpToDateThan(logTerm, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if lastTerm > logTerm || (lastTerm == logTerm && r.RaftLog.LastIndex() > index) {
		return true
	}
	return false
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	} else if r.Prs[m.From].Next > 0 {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	}
	r.updateCommit()
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
	})
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	if m.From == r.id {
		return
	}
	if r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.sendTimeoutNow(m.From)
	}
}

// handleVoteRequest handle vote request
func (r *Raft) handleVoteRequest(m pb.Message) {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if r.Term > m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.Vote == m.From {
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.isFollower() &&
		r.Vote == None &&
		(lastTerm < m.LogTerm || (lastTerm == m.LogTerm && m.Index >= r.RaftLog.LastIndex())) {
		r.sendVoteResponse(m.From, false)
		return
	}
	r.resetRealElectionTimeout()
	r.sendVoteResponse(m.From, true)
}

// sendVoteResponse send vote response
func (r *Raft) sendVoteResponse(nvote uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      nvote,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// campaign becomes a candidate and start to request vote
// 竞选成为候选人并开始请求投票
func (r *Raft) campaign() {
	r.becomeCandidate()
	r.bcastVoteRequest()
}

// handleVoteResponse handle vote response
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		return
	}
	if !m.Reject {
		r.votes[m.From] = true
		r.voteCount += 1
	} else {
		r.votes[m.From] = false
		r.denialCount += 1
	}
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.denialCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// bcastVoteRequest is used by candidate to send vote request
func (r *Raft) bcastVoteRequest() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
// becomeFollower 将此节点的状态转换为 Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.votes = nil
	r.voteCount = 0
	r.denialCount = 0
	r.leadTransferee = None
	r.resetTick()
}

// becomeCandidate transform this peer's state to candidate
// becomeCandidate 将此节点的状态转换为候选者
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Debugf("%v become candidate", r.id)
	//修改State
	r.State = StateCandidate
	//任期+1
	r.Term += 1
	//选举投票(的ID)
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	//投票id
	r.votes[r.id] = true
	r.voteCount = 1
	r.resetTick()
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 注意：领导者应该在其任期内提出一个 noop 条目
	log.Debugf("%v become leader", r.id)
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	for _, v := range r.Prs {
		v.Match = 0
		v.Next = r.RaftLog.LastIndex() + 1
	}
	// NOTE: Leader should propose a noop entry on its term
	r.resetTick()
	r.appendEntries(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	r.bcastAppend()
	r.updateCommit()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 进入处理消息的入口，看 `eraftpb.proto` 的 `Message Type` 了解应该处理什么消息
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// node is not in cluster or has been removed
	// 节点不在集群中或已被移除
	if _, ok := r.Prs[r.id]; !ok {
		return nil
	}
	//节点的身份
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(m.Entries) > 0 {
		appendStart := 0
		for i, ent := range m.Entries {
			if ent.Index > r.RaftLog.LastIndex() {
				appendStart = i
				break
			}
			validTerm, _ := r.RaftLog.Term(ent.Index)
			if validTerm != ent.Term {
				r.RaftLog.RemoveEntriesAfter(ent.Index)
				break
			}
			appendStart = i
		}
		if m.Entries[appendStart].Index > r.RaftLog.LastIndex() {
			for _, e := range m.Entries[appendStart:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
		}
	}
	//  If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := m.Index
		if len(m.Entries) > 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
	}
	r.sendAppendResponse(m.From, false)
}

// sendAppendResponse send append response
func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// most logic is same as `AppendEntries`
	// Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

// sendHeartbeatResponse send heartbeat response
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false)
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	// clear log
	r.RaftLog.entries = nil
	// install snapshot
	r.RaftLog.firstIndex = meta.Index + 1
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.pendingSnapshot = m.Snapshot
	// update conf
	r.Prs = make(map[uint64]*Progress)
	for _, p := range meta.ConfState.Nodes {
		r.Prs[p] = &Progress{}
	}
	r.sendAppendResponse(m.From, false)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// resetTick reset elapsed time to 0
// resetTick 将经过时间重置为 0
func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRealElectionTimeout()
}

// 节点启动时都以follower状态启动，同时随机选择自己的选举超时时间。
// 因为为了避免同时有两个节点同时进行选举，
// 这种情况下会出现没有任何一个节点赢得半数以上的投票从而这一轮选举失败，
// 继续再进行下一轮选举
func (r *Raft) resetRealElectionTimeout() {
	// 随机生成
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// appendEntries append entry to raft log entries
// appendEntries 将entry添加到 raft log entries
func (r *Raft) appendEntries(entries ...*pb.Entry) {
	ents := make([]pb.Entry, 0)
	for _, e := range entries {
		//检查消息的entries数组，看其中是否带有配置变更的数据。
		//如果其中带有数据变更而raft.pendingConf为true，
		//说明当前有未提交的配置更操作数据，
		//根据raft论文，每次不同同时进行一次以上的配置变更，
		//因此这里会将entries数组中的配置变更数据置为空数据。
		if e.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = e.Index
		}
		ents = append(ents, pb.Entry{
			EntryType: e.EntryType,
			Term:      e.Term,
			Index:     e.Index,
			Data:      e.Data,
		})
	}
	//加入到log.entry
	r.RaftLog.appendEntries(ents...)
	//根据raft log index 调整 leader的index
	//修改最新的log entry index
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// isLeader return if is leader
func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

// isFollower return if is follower
func (r *Raft) isFollower() bool {
	return r.State == StateFollower
}

// bcastAppend is used by leader to bcast append request to followers
// Leader 使用 bcastAppend 向followers发送追加请求
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// updateCommit ...
func (r *Raft) updateCommit() {
	commitUpdated := false
	for i := r.RaftLog.committed; i <= r.RaftLog.LastIndex(); i += 1 {
		if i <= r.RaftLog.committed {
			continue
		}
		matchCnt := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				matchCnt += 1
			}
		}
		// leader only commit on it's current term (§5.4.2)
		// 领导者只在当前任期内提交 (§5.4.2)
		term, _ := r.RaftLog.Term(i)
		if matchCnt > len(r.Prs)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			commitUpdated = true
		}
	}
	if commitUpdated {
		r.bcastAppend()
	}
}
