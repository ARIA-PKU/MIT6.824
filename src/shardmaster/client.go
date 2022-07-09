package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	leaderId 	int64
	clientId	int64
	commandId	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:	servers,
		leaderId:	0,
		clientId:	nrand(),
		commandId:	0,
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.Command(&CommandRequest{Num: num, Op: OpQuery})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(&CommandRequest{Servers: servers, Op: OpJoin})
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(&CommandRequest{GIDs: gids, Op: OpLeave})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(&CommandRequest{Shard: shard, GID: gid, Op: OpMove})
}

//
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("ShardMaster.Command", &request, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Command(request *CommandRequest) Config {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		var reply CommandReply
		if !ck.servers[ck.leaderId].Call("ShardMaster.Command", request, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return reply.Config
	}
}
