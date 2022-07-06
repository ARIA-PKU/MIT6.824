package raft


type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}


type RequestVoteReply struct {
	Term		int
	VoteGranted	bool
}

func (rf *Raft) isUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if currentTerm is larger or has voted for another node, return false
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm &&  rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	// candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if !rf.isUpToDate(request.LastLogTerm, request.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = request.CandidateId
	reply.VoteGranted = true
}

type AppendEntriesRequest struct {
	Term			int			// leader’s term
	LeaderId		int			// so follower can redirect clients
	PrevLogIndex	int			// index of log entry immediately precedingnew ones
	PrevLogTerm		int			// term of prevLogIndex entry
	Entries			[]Entry		// log entries to store (empty for heartbeat;
								// may send more than one for efficiency)
	LeaderCommit	int			//  leader’s commitIndex
}

type AppendEntriesReply struct {
	Term 		int		// currentTerm, for leader to update itself
	Success  	bool	// true if follower contained entry matching
						// prevLogIndex and prevLogTerm
						
	ConflictTerm 	int	// those two are not defined in papers,
	ConflictIndex	int	// but it could used to fast back-up
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, reply)
	return ok
}