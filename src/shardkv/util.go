package shardkv

import(
	"../shardmaster"
	"time"
	"sync"
	"../raft"
	// "fmt"
)


func (kv *ShardKV) initStateMachines() {
	for i := 0; i < shardmaster.NShards; i ++ {
		if _, ok := kv.stateMachines[i]; !ok {
			kv.stateMachines[i] = NewShard()
		}
	}
}

func (kv *ShardKV) isDuplicateRequest (clientId int64, commandId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	return ok && commandId <= operationContext.MaxAppliedCommandId
}

// check whether this raft group can serve the shard
func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.stateMachines[shardID].Status == Serving || kv.stateMachines[shardID].Status == GCing)
}

func (kv *ShardKV) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}


func (kv *ShardKV) Execute(command Command, reply *CommandReply) {
	index, _, isLeader := kv.rf.Start(command)
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
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// fetch latest configuration
func (kv *ShardKV) configureAction() {
	canGetNextConfig := true
	kv.mu.RLock()

	for _, shard := range kv.stateMachines {
		if shard.Status != Serving {
			canGetNextConfig = false
			break
		}
	}

	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	// fmt.Printf("configure\n")
	if canGetNextConfig {
		nextConfig := kv.sm.Query(currentConfigNum + 1)
		// fmt.Printf("nextConfig: %v and currentNum: %v\n", nextConfig, currentConfigNum)
		if nextConfig.Num == currentConfigNum + 1 {
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandReply{})
		}
	}
}

func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	gid2shardIds := make(map[int][]int)
	for i, shard := range kv.stateMachines {
		if shard.Status == status {
			gid := kv.lastConfig.Shards[i]
			if gid != 0 {
				if _, ok := gid2shardIds[gid]; !ok {
					gid2shardIds[gid] = make([]int, 0)
				}
				gid2shardIds[gid] = append(gid2shardIds[gid], i)
			}
		}
	}
	return gid2shardIds
}


// migrate related shards by pull
func (kv *ShardKV) migrationAction() {
	kv.mu.RLock()
	gid2shardIds := kv.getShardIdsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIds}
			for _, server := range servers {
				var pullTaskReply ShardOperationReply
				serverEnd := kv.makeEnd(server) 
				if serverEnd.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskReply) && pullTaskReply.Err == OK {
					kv.Execute(NewInsertShardsCommand(&pullTaskReply), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}


func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, reply *ShardOperationReply) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// leader's config num is smaller than request's
	if kv.currentConfig.Num < request.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.Shards = make(map[int]map[string]string)
	for _, shardId := range request.ShardIds {
		reply.Shards[shardId] = kv.stateMachines[shardId].deepCopy()
	}

	reply.LastOperations = make(map[int64]OperationContext)
	for clientID, operation := range kv.lastOperations {
		reply.LastOperations[clientID] = operation.deepCopy()
	}

	reply.ConfigNum, reply.Err = request.ConfigNum, OK
}


func (kv *ShardKV) DeleteShardsData(request *ShardOperationRequest, reply *ShardOperationReply) {
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	// leader's config has updated
	if kv.currentConfig.Num < request.ConfigNum {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	
	var	commandReply CommandReply
	kv.Execute(NewDeleteShardsCommand(request), &commandReply)

	reply.Err = commandReply.Err
}

func (kv *ShardKV) gcAction() {
	kv.mu.RLock()
	gid2shardIds := kv.getShardIdsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			gcTaskRequest := ShardOperationRequest{configNum, shardIds}
			for _, server := range servers {
				var gcTaskReply ShardOperationReply
				serverEnd := kv.makeEnd(server)
				if serverEnd.Call("ShardKV.DeleteShardsData", &gcTaskRequest, &gcTaskReply) && gcTaskReply.Err == OK {
					// if leader succeed to gc, current server update config
					kv.Execute(NewDeleteShardsCommand(&gcTaskRequest), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}


func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyEntryCommand(), &CommandReply{})
	}
}


func (kv *ShardKV) applyLogToStateMachines(operation *CommandRequest, shardId int) *CommandReply {
	var value string
	var err Err
	switch operation.Op {
	case OpPut:
		err = kv.stateMachines[shardId].Put(operation.Key, operation.Value)
	case OpAppend:
		err = kv.stateMachines[shardId].Append(operation.Key, operation.Value)
	case OpGet:
		value, err = kv.stateMachines[shardId].Get(operation.Key)
	}
	return &CommandReply{err, value}
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, operation *CommandRequest) *CommandReply {
	var reply *CommandReply
	shardId := key2shard(operation.Key)
	if kv.canServe(shardId) {
		if operation.Op != OpGet && kv.isDuplicateRequest(operation.ClientId, operation.CommandId) {
			return kv.lastOperations[operation.ClientId].LastReply
		} else {
			reply = kv.applyLogToStateMachines(operation, shardId)
			if operation.Op != OpGet {
				kv.lastOperations[operation.ClientId] = OperationContext{operation.CommandId,reply}
			}
			return reply
		}
	}
	return &CommandReply{ErrWrongGroup, ""}
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardmaster.Config) {
	for i := 0; i < shardmaster.NShards; i ++ {
		// pull data from other shard
		if kv.currentConfig.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				kv.stateMachines[i].Status = Pulling
			}
		}
		// be pulled data to other shard
		if kv.currentConfig.Shards[i] == kv.gid && nextConfig.Shards[i] != kv.gid {
			gid := nextConfig.Shards[i]
			if gid != 0 {
				kv.stateMachines[i].Status = BePulling
			}
		}
	}
}


func (kv *ShardKV) applyConfiguration(nextConfig *shardmaster.Config) *CommandReply {
	// fmt.Printf("apply here\n")
	if nextConfig.Num == kv.currentConfig.Num + 1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrOutDated, ""}
}


func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationReply) *CommandReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardsInfo.Shards {
			shard := kv.stateMachines[shardId]
			if shard.Status == Pulling {
				for key, value := range shardData {
					shard.KV[key] = value
				}
				shard.Status = GCing
			} else {
				// fmt.Println("Encounter duplicated shards")
				break
			}
		}
		for clientId, operationContext := range shardsInfo.LastOperations {
			if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.MaxAppliedCommandId < operationContext.MaxAppliedCommandId {
				kv.lastOperations[clientId] = operationContext
			}
		}
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrOutDated, ""}
}


func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationRequest) *CommandReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardsInfo.ShardIds {
			shard := kv.stateMachines[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.stateMachines[shardId] = NewShard()
			} else {
				// fmt.Println("duplicated shards deletion")
				break
			}
		}
		return &CommandReply{OK, ""}
	}
	return &CommandReply{OK, ""}
}

func (kv *ShardKV) applyEmptyEntry() *CommandReply {
	return &CommandReply{OK, ""}
}