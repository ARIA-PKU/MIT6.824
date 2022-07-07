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
	 "sort"
	 "bytes"
	 "../labgob"
	//  "fmt"
) 

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
	data := rf.encodeState()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs	[]Entry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || 
	   d.Decode(&logs) != nil {
	  DPrintf("{Node: %v restore failed}", rf.me)
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs = logs
	}
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
		// rf.electionTimer.Reset(ElectionTimeout())
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
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{VoteGranted: false}
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
							// fmt.Printf("total is %v\n", len(rf.peers))
							// fmt.Printf("{Node: %v} receive majority vote in term: %v and grantedCount is %v\n", rf.me, request.Term, grantedCount)
							DPrintf("{Node: %v} receive majority vote in term: %v", rf.me, request.Term)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node: %v} find larger term as leader {Node: %v} with term: %v in term: %v", rf.me, peer, reply.Term, request.Term)
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			// sync the log and maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// signal replicator goroutine to send entries
			rf.replicatorCond[peer].Signal()
		}
	}
}

// watch all timer, choose to do heartbeat or start a new election
func (rf *Raft) watchTimer() {
	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
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

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if request.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(ElectionTimeout())


	// drop unexpected logs
	if request.PrevLogIndex < rf.logs[0].Index {
		reply.Term, reply.Success = 0, false
		return
	}

	// fast back-up
	if !rf.logMatch(request.PrevLogTerm, request.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			reply.ConflictTerm, reply.ConflictIndex = -1, lastIndex + 1
		} else {
			firstIndex := rf.logs[0].Index
			reply.ConflictTerm = rf.logs[request.PrevLogIndex - firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index - firstIndex].Term == reply.ConflictTerm {
				index --
			}
			reply.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.logs[0].Index
	for index, entry := range request.Entries {
		if entry.Index - firstIndex >= len(rf.logs) || rf.logs[entry.Index - firstIndex].Term != entry.Term {
			rf.logs = cutEntriesSpace(append(rf.logs[:entry.Index - firstIndex], request.Entries[index:]...))
			break
		}
	}

	// change commitIndex and signal gorotinue to execute
	// commited logs
	newCommitIndex := Min(request.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) handleAppendEntriesReply(peer int, request *AppendEntriesRequest, reply *AppendEntriesReply) {
	if rf.state == Leader && rf.currentTerm == request.Term {
		if reply.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			// update leader's commitIndex
			n := len(rf.matchIndex)
			confirmIndex := make([]int, n)
			copy(confirmIndex, rf.matchIndex)
			sort.Ints(confirmIndex)
			// fmt.Println(confirmIndex)
			newCommitIndex := confirmIndex[(n - 1) / 2]
			if newCommitIndex > rf.commitIndex {
				if rf.logMatch(rf.currentTerm, newCommitIndex) {
					// fmt.Printf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d\n", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
					DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
					rf.commitIndex = newCommitIndex
					rf.applyCond.Signal()
				} else {
					DPrintf("{Node: %d cannot update commitIndex}", rf.me)
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.ChangeState(Follower)
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstIndex := rf.logs[0].Index
					for i := request.PrevLogIndex; i >= firstIndex; i -- {
						if rf.logs[i - firstIndex].Term == reply.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						} else if rf.logs[i - firstIndex].Term < reply.ConflictTerm {
							break
						}
					}
				}
			}
		}
	}
}

func (rf *Raft) needReplicate(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index 
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	peerCurrentLogIndex := rf.nextIndex[peer] - 1
	if peerCurrentLogIndex < rf.logs[0].Index {
		// need InstallSnapshot RPC to catch up leader
	} else {
		request := rf.genAppendEntriesRequest(peerCurrentLogIndex)
		rf.mu.RUnlock()
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, request, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, request, reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock();
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if not need to replicate to this peer, 
		// release CPU and wait for other goroutine's signal
		// else call replicateOneRound to make the 
		// peer to catch up the leader
		for !rf.needReplicate(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer);
	}
}

func (rf *Raft) coordinator() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIdx, commitIdx, lastApplied := rf.logs[0].Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIdx - lastApplied)
		copy(entries, rf.logs[lastApplied - firstIdx + 1: commitIdx - firstIdx + 1])
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg {
				CommandValid: 	true,
				Command: 		entry.Command,
				CommandIndex:	entry.Index,
				CommandTerm:	entry.Term,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIdx)
		rf.mu.Unlock()
	}
}

// used by Start function to append a new Entry to logs
func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLog := rf.getLastLog()
	newLog := Entry{lastLog.Index + 1, rf.currentTerm, command}
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	return newLog
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	// add new log
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
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
			go rf.replicator(i)
		}
	}

	
	go rf.watchTimer()

	go rf.coordinator()

	return rf
}
