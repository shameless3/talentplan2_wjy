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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry
	// firstIndex,init from storage
	firstIndex uint64
	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{storage: storage, stabled: lastIndex, entries: entries, firstIndex: firstIndex, applied: firstIndex - 1}
}

// appendEntries append entries to raftlog.entries
func (l *RaftLog) appendEntries(entries []pb.Entry) {
	if len(entries) <= 0 {
		return
	}
	switch {
	case entries[0].Index-1 == l.LastIndex():
		// no conflict entry
		l.entries = append(l.entries, entries...)
	case entries[0].Index-1 < l.firstIndex:
		//should set firstIndex = entries[0].index to delete all conflict entry
		l.firstIndex = entries[0].Index
		l.entries = entries
	default:
		// delete conflict entry
		l.entries = append([]pb.Entry{}, l.entries[0:entries[0].Index-l.firstIndex]...)
		l.entries = append(l.entries, entries...)
	}
	// update stabled
	if l.stabled > entries[0].Index-1 {
		l.stabled = entries[0].Index - 1
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled+1 > l.LastIndex() {
		// all entries is stabled
		return make([]pb.Entry, 0)
	}
	// get (stabled:lastindex]
	entries, err := l.Slice(l.stabled+1, l.LastIndex()+1)
	if err != nil {
		panic(err)
	}
	log.Infof("return unstableEntries %v", entries)
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied+1 > l.LastIndex() {
		// all entries is applied
		return make([]pb.Entry, 0)
	}
	// get (applied,commited]
	log.Infof("try to get %d-%d", l.applied+1, l.committed+1)
	entries, err := l.Slice(l.applied+1, l.committed+1)
	if err != nil {
		panic(err)
	}
	log.Infof("return nextEnts:%v", entries)
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.firstIndex {
		return l.entries[i-l.firstIndex].Term, nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) Slice(lo uint64, hi uint64) ([]pb.Entry, error) {
	lastIndex := l.LastIndex()
	//invalid slice
	if lo > hi || hi > lastIndex+1 {
		log.Panicf("Invalid slice [%d, %d),lastIndex:%d", lo, hi, lastIndex)
	}
	if lo == hi {
		return nil, nil
	}
	if len(l.entries) > 0 {
		var entries []pb.Entry
		var entriesInStorage []pb.Entry
		var entriesInMemory []pb.Entry
		if lo < l.firstIndex {
			// need to get entries from storage[lo,max(l.firstIndex, hi))
			var err error
			entriesInStorage, err = l.storage.Entries(lo, max(l.firstIndex, hi))
			if err != nil {
				return nil, err
			}
			entries = entriesInStorage
		}
		if hi > l.firstIndex {
			// continue to get entries from l.entries
			entriesInMemory = l.entries[max(lo, l.firstIndex)-l.firstIndex : hi-l.firstIndex]
		}
		// combine
		entries = append(entries, entriesInStorage...)
		entries = append(entries, entriesInMemory...)
		return entries, nil
	} else {
		entriesInStorage, err := l.storage.Entries(lo, hi)
		if err != nil {
			log.Infof("%d", len(l.entries))
			return nil, err
		}
		return entriesInStorage, nil
	}
}
