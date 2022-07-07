package raft

import (
	"sync"
	"../labrpc"
	"time"
)

// define the entry
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm	 int	
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh			chan ApplyMsg
	applyCond   	*sync.Cond    // used to wakeup applier goroutine after committing new entries
	state			NodeState
	replicatorCond	[]*sync.Cond  // used to signal replicator goroutine to batch replicating entries

	// definition in Figure 2 
	currentTerm int
	votedFor    int
	logs        []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer	*time.Timer
	heartbeatTimer 	*time.Timer
}
