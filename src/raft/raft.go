package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	 "../labrpc"
	 "time"
) 

// import "bytes"
// import "../labgob"



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
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node: %d} changes state from %v to %v in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(ElectionTimeout())
	case Leader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i ++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index + 1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HeartbeatTimeout())
	}
}

func (rf *Raft) startElection() {
	lastLog := rf.getLastLog()
	request := &RequestVoteArgs {
		Term:			rf.currentTerm,
		CandidateId:	rf.me,
		LastLogIndex:	lastLog.Index,
		LastLogTerm:	lastLog.Term,
	}
	DPrintf("{Node: %v} starts election with request: %v", rf.me, request)

	grantedCount := 1
	rf.votedFor = rf.me

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node: %v} receive reply: %v from {Node: %v} in term %v", rf.me, reply, peer, request.Term)
				// avoid current node receive heartbeat or 
				// already become follower before send request
				if rf.currentTerm == request.Term && rf.state == Candidate {
					if reply.VoteGranted {
						grantedCount += 1
						if grantedCount > len(rf.peers) / 2 {
							DPrintf("{Node: %v} receive majority vote in term: %v", rf.me, request.Term)
							rf.ChangeState(Leader)
							// todo: do heartbeat
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node: %v} find larger term as leader {Node: %v} with term: %v in term: %v", rf.me, peer, reply.Term, request.Term)
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
					}
				}
			}
		}(peer)
	}
}

// watch all timer, choose to do heartbeat or start a new election
func (rf *Raft) watchTimer() {
	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				// Todo: replicate entries
				rf.heartbeatTimer.Reset(HeartbeatTimeout())
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.startElection()
			rf.electionTimer.Reset(ElectionTimeout())
			rf.mu.Unlock()
		}
	
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers : 	peers,
		persister : persister,
		me : 		me,
		dead : 		0,

		applyCh: 		applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:			Follower,

		currentTerm:	0,
		votedFor:		-1,
		logs:			make([]Entry, 1),
		nextIndex:		make([]int, len(peers)),
		matchIndex:		make([]int, len(peers)),

		heartbeatTimer: time.NewTimer(HeartbeatTimeout()),
		electionTimer: 	time.NewTimer(ElectionTimeout()),
	}
	// Your initialization code here (2A, 2B, 2C).
	
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i ++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index + 1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start goroutine to replicate entries in batch
			// go rf.replicator(i)
		}
	}

	
	go rf.watchTimer()

	return rf
}
