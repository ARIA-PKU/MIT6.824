package shardkv



type CommandRequest struct {
	Key			string
	Value		string
	Op			OperationOp
	ClientId	int64
	CommandId   int64
}

type CommandReply struct {
	Err 	Err
	Value 	string
}

type Command struct {
	Op 		CommandType
	Data	interface{}
}

type ShardOperationRequest struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err				Err
	ConfigNum		int
	Shards			map[int]map[string]string
	LastOperations	map[int64]OperationContext
}