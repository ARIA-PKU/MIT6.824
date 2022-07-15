package shardkv


import(
	"../shardmaster"
	"../labrpc"
	"../raft"
	// "sync"
	"sync/atomic"
	"../labgob"
	"time"
	"fmt"
)


// deal with the command from client
func (kv *ShardKV) Command(request *CommandRequest, reply *CommandReply) {
	kv.mu.RLock()
	// if get the duplicate rpc, return result without the participation of raft layer
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastReply := kv.lastOperations[request.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	// 
	if !kv.canServe(key2shard(request.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Execute(NewOperationCommand(request), reply)
}

func (kv *ShardKV) Serve(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var reply *CommandReply
				cmd := msg.Command.(Command)
				switch cmd.Op {
				case Operation:
					operation := cmd.Data.(CommandRequest)
					reply = kv.applyOperation(&msg, &operation)
				case Configuration:
					nextConfig := cmd.Data.(shardmaster.Config)
					reply = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					shardsInfo := cmd.Data.(ShardOperationReply)
					reply = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := cmd.Data.(ShardOperationRequest)
					reply = kv.applyDeleteShards(&shardsInfo)
				case EmptyEntry:
					reply = kv.applyEmptyEntry()
				}
				
				// only notify related channel for currentTerm's log when the node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(msg.CommandIndex)
					ch <- reply
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				fmt.Println("run into wrong state in applier")
			}
		}
	}
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(ShardOperationRequest{})
	labgob.Register(ShardOperationReply{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(CommandRequest{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV {
		maxRaftState: 	maxraftstate,
		makeEnd:		make_end,
		gid:			gid,
		sm:				shardmaster.MakeClerk(masters),
		applyCh:		applyCh,
		dead:			0,
		rf:				raft.Make(servers, me, persister, applyCh),

		lastApplied:	0,
		currentConfig:	shardmaster.DefaultConfig(),
		lastConfig:		shardmaster.DefaultConfig(),
		notifyChans:	make(map[int]chan *CommandReply),
		stateMachines:	make(map[int]*Shard),
		lastOperations:	make(map[int64]OperationContext),
	}

	kv.restoreSnapshot(persister.ReadSnapshot())

	// start a goroutine to apply committed to stateMachine
	go kv.applier()
	
	// start a goroutine to update config
	go kv.Serve(kv.configureAction, ConfigureTimeout)

	// start a goroutine to check migration
	go kv.Serve(kv.migrationAction, MigrationTimeout)

	// start a goroutine to check gc
	go kv.Serve(kv.gcAction, GCTimeout)

	// start a goroutine to solve empty entry problems
	go kv.Serve(kv.checkEntryInCurrentTermAction, EmptyEntryDetectorTimeout)

	return kv
}

