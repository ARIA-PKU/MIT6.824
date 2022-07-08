package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"sync/atomic"
	"time"
	// "fmt"
)

func (kv *KVServer) coordinator() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf("{Node: %v} does not apply duplicated message", kv.rf.Me())
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				var reply *CommandReply
				cmd := msg.Command.(Command)
				if cmd.Op != OpGet  && kv.isDuplicateRPC(cmd.ClientId, cmd.CommandId) {
					reply = kv.lastOperation[cmd.ClientId].LastReply
				} else {
					reply = kv.applyLogToStateMachine(cmd)
					if cmd.Op != OpGet {
						kv.lastOperation[cmd.ClientId] = OperationHistory{cmd.CommandId, reply}
					}
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(msg.CommandIndex)
					ch <- reply
				}
				
				if kv.needSnapshot() {
					// fmt.Printf("take snapshot")
					kv.takeSnapshot(msg.CommandIndex)
				}

				kv.mu.Unlock()
			} else if msg.SnapshotValid{
				// recover from snapshot
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic("unexpected message in coordinator")
			}
		}
	}
}

func (kv *KVServer) Command(request *CommandRequest, reply *CommandReply) {
	// defer fmt.Printf("{Node %v} processes CommandRequest %v with CommandResponse %v\n", kv.rf.Me(), request, reply)
	// fmt.Printf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), request, reply)
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicateRPC(request.ClientId, request.CommandId) {
		lastReply := kv.lastOperation[request.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(Command{request})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	go func() {
		kv.mu.Lock()
		kv.removeOutOfDateNotifyChan(index)
		kv.mu.Unlock()
	}()
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	// fmt.Printf("{Node: %v} has been killed", kv.rf.Me())
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer {
		me:	 			me,
		maxraftstate: 	maxraftstate,
		applyCh:		applyCh,
		dead:			0,
		lastApplied:	0,
		rf:				raft.Make(servers, me, persister, applyCh),
		lastOperation:	make(map[int64]OperationHistory),
		notifyChans:	make(map[int]chan *CommandReply),
		stateMachine:	NewMemoryKV(),
	}
	
	kv.restoreSnapshot(persister.ReadSnapshot())

	go kv.coordinator()

	return kv
}
