package shardkv



type Shard struct {
	KV		map[string]string
	Status	ShardStatus
}

func NewShard() *Shard {
	return &Shard{
		KV:		make(map[string]string),
		Status:	Serving,
	}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	newShardKV := make(map[string]string)
	for k, v := range shard.KV {
		newShardKV[k] = v
	}
	return newShardKV
}
