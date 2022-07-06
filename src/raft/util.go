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