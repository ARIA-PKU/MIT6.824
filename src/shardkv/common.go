package shardkv


import (
	"../labrpc"
	"../raft"
	"sync"
	"../shardmaster"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ExecuteTimeout            	= 500 * time.Millisecond
	ConfigureTimeout   			= 100 * time.Millisecond
	MigrationTimeout   			= 50 * time.Millisecond
	GCTimeout          			= 50 * time.Millisecond
	EmptyEntryDetectorTimeout 	= 200 * time.Millisecond
)

type Err uint8

const (
	OK Err = iota
	ErrNoKey       
	ErrWrongGroup  
	ErrWrongLeader 
	ErrTimeout
	ErrNotReady
	ErrOutDated
)

type OperationOp uint8

const (
	OpPut OperationOp = iota
	OpAppend
	OpGet
)

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type OperationContext struct {
	MaxAppliedCommandId	int64
	LastReply			*CommandReply
}

func (context OperationContext) deepCopy() OperationContext {
	return OperationContext{context.MaxAppliedCommandId, &CommandReply{context.LastReply.Err, context.LastReply.Value}}
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type ShardKV struct {
	mu           sync.RWMutex
	dead         int32
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	sm			 *shardmaster.Clerk

	maxRaftState 	int // snapshot if log grows this big
	lastApplied 	int	// record the lastApplied to prevent StateMachine from rollback

	lastConfig		shardmaster.Config
	currentConfig	shardmaster.Config

	stateMachines	map[int]*Shard		// shardId -> Shard
	lastOperations	map[int64]OperationContext	
	notifyChans		map[int]chan *CommandReply  // notify applier's goroutine to reply
}

func NewOperationCommand(request *CommandRequest) Command {
	return Command{Operation, *request}
}

func NewConfigurationCommand(config *shardmaster.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(reply *ShardOperationReply) Command {
	return Command{InsertShards, *reply}
}

func NewDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShards, *request}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}
