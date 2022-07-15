package kvraft

import(
	"bytes"
	"../labgob"
	// "fmt"
)

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

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperation)
	kv.rf.Snapshot(index, w.Bytes())
	// fmt.Println("finish here")
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperation map[int64]OperationHistory
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperation) != nil {
		DPrintf("{Node %v} restores snapshot failed", kv.rf.Me())
	}
	kv.stateMachine, kv.lastOperation = &stateMachine, lastOperation
}
