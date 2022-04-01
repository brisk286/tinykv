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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
// RaftLog 管理日志条目，其结构如下：
//
// 快照     /第一个......应用......提交......稳定......最后
// --------|-----------------  ----- ------------- ---------|
//                        日志条目
//
// 为了简化 RaftLog 实现应该管理所有未被截断的日志条目
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	//前面提到的存放已经持久化数据的Storage接口。
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	//保存当前提交的日志数据索引。
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	//保存当前传入状态机的数据最高索引。
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 索引 <= 稳定的日志条目被持久化到存储中。
	// 用于记录还没有被storage持久化的日志。
	// 每次处理 `Ready` 时，都会包含不稳定的日志。
	//前面分析过的unstable结构体，用于保存应用层还没有持久化的数据。
	stabled uint64

	// all entries that have not yet compact.
	// 所有尚未压缩的条目。
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 传入的不稳定快照，如果有的话。
	//（用于2C）
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// newLog 使用给定的存储返回日志。 它将日志恢复到它刚刚提交并应用最新快照的状态。
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	ents := make([]pb.Entry, 0)
	if lo <= hi {
		ents, err = storage.Entries(lo, hi+1)
		if err != nil {
			panic(err)
		}
	}
	return &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    lo - 1,
		stabled:    hi,
		entries:    ents,
		firstIndex: lo,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.firstIndex {
		if len(l.entries) > 0 {
			entries := l.entries[first-l.firstIndex:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.firstIndex = first
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.stabled+1 >= l.firstIndex {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : min(l.committed-l.firstIndex+1, uint64(len(l.entries)))]
	}
	return nil
}

// LastIndex return the last index of the log entries
// LastIndex 返回日志条目的最后一个索引
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var snapLast uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		snapLast = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, snapLast)
	}
	index, _ := l.storage.FirstIndex()
	return max(index-1, snapLast)
}

// Term return the term of the entry in the given index
// term返回给定索引中entry的term
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.firstIndex {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		return l.entries[i-l.firstIndex].Term, nil
	}
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if i < l.pendingSnapshot.Metadata.Index {
			return term, ErrCompacted
		}
	}
	return term, err
}

// appendEntries append entry to entries
func (l *RaftLog) appendEntries(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

// Entries returns entries in [lo, hi)
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	if lo >= l.firstIndex && hi-l.firstIndex <= uint64(len(l.entries)) {
		return l.entries[lo-l.firstIndex : hi-l.firstIndex]
	}
	ents, _ := l.storage.Entries(lo, hi)
	return ents
}

// RemoveEntriesAfter remove entries from index lo to the last
func (l *RaftLog) RemoveEntriesAfter(lo uint64) {
	l.stabled = min(l.stabled, lo-1)
	if lo-l.firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:lo-l.firstIndex]
}
