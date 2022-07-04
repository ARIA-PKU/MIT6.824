package raft

import (
	"log"
	"time"
	"math/rand"
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

// define the entry
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

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