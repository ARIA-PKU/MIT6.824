package kvraft

func (kv *KVServer) isDuplicateRPC(clientId, commandId int64) bool {
	lastReply, ok := kv.lastOperation[clientId]
	return ok && commandId <= lastReply.MaxAppliedCommand
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeOutOfDateNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	var value string
	var err Err
	switch command.Op {
	case OpPut:
		err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.stateMachine.Append(command.Key, command.Value)
	case OpGet:
		value, err = kv.stateMachine.Get(command.Key)
	}
	return &CommandReply{err, value}
}