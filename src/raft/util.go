package raft

import (
	"log"
	"time"
	"math/rand"
	"bytes"
	"../labgob"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


// define the state of current node
type NodeState uint8

const (
	Follower	NodeState = iota
	Candidate
	Leader
)

// Hint from lecture:
// Make sure the election timeouts in different peers don't always fire at the same time,
// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
const (
	HeartbeatTimeoutValue 	= 100
	ElectionTimeoutValue	= 1000
)

func HeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeoutValue) * time.Millisecond
}

func ElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(ElectionTimeoutValue + rand.Intn(ElectionTimeoutValue)) * time.Millisecond
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs) - 1]
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	firstIndex := rf.logs[0].Index
	entries := make([]Entry, len(rf.logs[prevLogIndex - firstIndex + 1:]))
	copy(entries, rf.logs[prevLogIndex - firstIndex + 1:])
	return &AppendEntriesRequest {
		Term:			rf.currentTerm,
		LeaderId:		rf.me,
		PrevLogIndex:	prevLogIndex,
		PrevLogTerm:	rf.logs[prevLogIndex - firstIndex].Term,
		Entries:		entries,
		LeaderCommit: 	rf.commitIndex,
	}
}

// if requestIndex < current index, need to delete the wrong log
func (rf *Raft) logMatch(requestTerm, requestIndex int) bool {
	if requestIndex <= rf.getLastLog().Index && rf.logs[requestIndex - rf.logs[0].Index].Term == requestTerm {
		return true
	}
	return false
}

func cutEntriesSpace(entries []Entry) []Entry {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]Entry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	firstLog := rf.logs[0]
	return &InstallSnapshotRequest{
		Term:				rf.currentTerm,
		LeaderId:			rf.me,
		LastIncludedIndex:	firstLog.Index,
		LastIncludedTerm:	firstLog.Term,
		Data:				rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.persist()
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(ElectionTimeout())

	// snapshot is outdated
	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) handleInstallSnapshotReply(peer int, request *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	if rf.state == Leader && rf.currentTerm == request.Term {
		if reply.Term > rf.currentTerm {
			rf.ChangeState(Follower)
			rf.currentTerm, rf.votedFor = reply.Term, -1
			rf.persist()
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = request.LastIncludedIndex, request.LastIncludedIndex + 1
		}
	}
}

// use snapshot to trim logs and restore its state
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	startIndex := rf.logs[0].Index
	if index <= startIndex {
		// has been snapshot before
		return
	}
	rf.logs = cutEntriesSpace(rf.logs[index - startIndex:])
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

func (rf *Raft) CondInstallSnapshot(term int, index int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.commitIndex {
		DPrintf("snapshot is outdated")
		return false
	}

	if index > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1)
	} else {
		rf.logs = cutEntriesSpace(rf.logs[index - rf.logs[0].Index:])
		rf.logs[0].Command = nil
	}
	// first entry is dummy entry
	rf.logs[0].Term, rf.logs[0].Index = term, index
	rf.lastApplied, rf.commitIndex = index, index
	
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	return true
}
