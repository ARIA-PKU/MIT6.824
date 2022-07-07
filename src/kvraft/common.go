package kvraft

import (
	"log"	
	"time"
)

const ExecuteTimeout = 500 * time.Millisecond

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Err uint8

const (
	OK				Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeOut
)

type Operation uint8

const (
	OpGet	Operation = iota
	OpPut
	OpAppend
)

type CommandRequest struct {
	Key 		string
	Value		string
	Op			Operation
	ClientId	int64
	CommandId	int64
}

type Command struct {
	*CommandRequest
}

type CommandReply struct {
	Err   Err
	Value string
}

 // determine whether log is duplicated 
 // by recording the last commandId and response 
 // corresponding to the clientId
type OperationHistory struct {
	MaxAppliedCommand	int64
	LastReply			*CommandReply
}