package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu          sync.Mutex
	leaderNow   int
	sequenceNum int
	clientId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderNow = 0
	ck.sequenceNum = 0
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.

	for {
		ck.mu.Lock()
		args.Num = num
		args.ClientId = ck.clientId
		args.SerialNum = ck.sequenceNum
		ck.mu.Unlock()
		// try each known server.
		for _, srv := range ck.servers {

			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			//Debug(dInfo, "%v action get have reply %v ok %v", ck.clientId, reply, ok)

			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.sequenceNum++
				ck.mu.Unlock()
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	for {
		ck.mu.Lock()
		args.Servers = servers
		args.ClientId = ck.clientId
		args.SerialNum = ck.sequenceNum
		ck.mu.Unlock()
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.sequenceNum++
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.

	for {
		ck.mu.Lock()
		args.GIDs = gids
		args.ClientId = ck.clientId
		args.SerialNum = ck.sequenceNum
		ck.mu.Unlock()
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.sequenceNum++
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.

	for {
		ck.mu.Lock()
		args.Shard = shard
		args.GID = gid
		args.ClientId = ck.clientId
		args.SerialNum = ck.sequenceNum
		ck.mu.Unlock()
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.sequenceNum++
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
