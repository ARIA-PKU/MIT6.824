package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"
// import "fmt"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       	*shardmaster.Clerk
	config   	shardmaster.Config
	makeEnd 	func(string) *labrpc.ClientEnd
	leaderIds	map[int]int
	clientId 	int64
	commandId	int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:			shardmaster.MakeClerk(masters),
		makeEnd:	make_end,
		leaderIds:	make(map[int]int),
		clientId:	nrand(),
		commandId:	0,
	}
	// pull the latest config
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}

func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			preLeaderId := ck.leaderIds[gid]
			newLeaderId := preLeaderId
			for {
				var reply CommandReply
				ok := ck.makeEnd(servers[newLeaderId]).Call("ShardKV.Command", request, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.commandId ++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					// the shard's group may have changed
					break
				} else {
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == preLeaderId {
						break
					}
					continue
				}
			}	
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
