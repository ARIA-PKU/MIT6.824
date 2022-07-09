package shardmaster


import (
	"../raft"
 	"../labrpc"
 	"sync"
 	"../labgob"
	"time"
	"fmt"
	"sync/atomic"
)


type ShardMaster struct {
	mu      sync.RWMutex
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	stateMachine   ConfigStateMachine             // Config stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and Reply corresponding to the clientId
	notifyChans    map[int]chan *CommandReply // notify client goroutine by applier goroutine to Reply
}


func (sm *ShardMaster) Command(request *CommandRequest, Reply *CommandReply) {
	// defer DPrintf("{Node %v}'s state is {}, processes CommandRequest %v with CommandReply %v", sm.rf.Me(), request, Reply)
	// return result directly without raft layer's participation if request is duplicated
	sm.mu.RLock()
	if request.Op != OpQuery && sm.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastReply := sm.lastOperations[request.ClientId].LastReply
		Reply.Config, Reply.Err = lastReply.Config, lastReply.Err
		sm.mu.RUnlock()
		return
	}
	sm.mu.RUnlock()
	// do not hold lock to improve throughput
	index, _, isLeader := sm.rf.Start(Command{request})
	if !isLeader {
		Reply.Err = ErrWrongLeader
		return
	}
	sm.mu.Lock()
	ch := sm.getNotifyChan(index)
	sm.mu.Unlock()
	select {
	case result := <-ch:
		Reply.Config, Reply.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		Reply.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		sm.mu.Lock()
		sm.removeOutdatedNotifyChan(index)
		sm.mu.Unlock()
	}()
}


// a dedicated applier goroutine to apply committed entries to stateMachine
func (sm *ShardMaster) applier() {
	for sm.killed() == false {
		select {
		case message := <-sm.applyCh:
			// DPrintf("{Node %v} tries to apply message %v", sm.rf.Me(), message)
			if message.CommandValid {
				var Reply *CommandReply
				command := message.Command.(Command)
				sm.mu.Lock()

				if command.Op != OpQuery && sm.isDuplicateRequest(command.ClientId, command.CommandId) {
					// DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", sm.rf.Me(), message, sm.lastOperations[command.ClientId], command.ClientId)
					Reply = sm.lastOperations[command.ClientId].LastReply
				} else {
					Reply = sm.applyLogToStateMachine(command)
					if command.Op != OpQuery {
						sm.lastOperations[command.ClientId] = OperationContext{command.CommandId, Reply}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := sm.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sm.getNotifyChan(message.CommandIndex)
					ch <- Reply
				}

				sm.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	// DPrintf("{ShardMaster %v} has been killed", sm.rf.Me())
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
}

func (sm *ShardMaster) killed() bool {
	return atomic.LoadInt32(&sm.dead) == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	sm := &ShardMaster{
		applyCh:        applyCh,
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		stateMachine:   NewMemoryConfigStateMachine(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
	}
	// start applier goroutine to apply committed logs to stateMachine
	go sm.applier()

	// DPrintf("{ShardMaster %v} has started", sm.rf.Me())
	return sm
}
