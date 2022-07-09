package shardmaster


func (sm *ShardMaster) removeOutdatedNotifyChan(index int) {
	delete(sm.notifyChans, index)
}

func (sm *ShardMaster) getNotifyChan(index int) chan *CommandReply {
	if _, ok := sm.notifyChans[index]; !ok {
		sm.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return sm.notifyChans[index]
}

func (sm *ShardMaster) applyLogToStateMachine(command Command) *CommandReply {
	var config Config
	var err Err
	switch command.Op {
	case OpJoin:
		err = sm.stateMachine.Join(command.Servers)
	case OpLeave:
		err = sm.stateMachine.Leave(command.GIDs)
	case OpMove:
		err = sm.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		config, err = sm.stateMachine.Query(command.Num)
	}
	return &CommandReply{err, config}
}

// each RPC imply that the client has seen the reply for its previous RPC
// therefore, we only need to determine whether the latest commandId of a clientId meets the criteria
func (sm *ShardMaster) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := sm.lastOperations[clientId]
	return ok && requestId <= operationContext.MaxAppliedCommandId
}


func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}