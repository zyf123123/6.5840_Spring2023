package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	leaderNow   int
	sequenceNum int
	mu          sync.Mutex
	clientId    int64
	// You will have to modify this struct.
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
	ck.leaderNow = 0
	ck.sequenceNum = 0
	ck.clientId = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	for {
		ck.mu.Lock()
		serverindex := ck.leaderNow
		args := GetArgs{
			Key:       key,
			SerialNum: ck.sequenceNum,
			ClientId:  ck.clientId,
		}
		ck.mu.Unlock()
		//Debug(dClient, "%v %v", serverindex, args)

		reply := GetReply{}
		ok := ck.servers[serverindex].Call("KVServer.Get", &args, &reply)
		//Debug(dInfo, "%v action get have reply %v ok %v", ck.clientId, reply.Err, ok)
		ck.mu.Lock()
		if ok && reply.Err == "OK" {
			//Debug(d)
			ck.mu.Unlock()
			return reply.Value
		} else if reply.Err == "ErrNoKey" {
			ck.mu.Unlock()
			return ""
		} else if reply.Err == "ErrWrongLeader" {
			ck.leaderNow = int(nrand()) % len(ck.servers)
		} else { // No reply maybe err leader
			ck.leaderNow = int(nrand()) % len(ck.servers)
		}
		ck.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	// You will have to modify this function.
	//return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//Debug(dInfo, "put start")
	for {
		serverindex := ck.leaderNow
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			SerialNum: ck.sequenceNum,
			ClientId:  ck.clientId,
		}
		reply := PutAppendReply{}
		//Debug(dClient, "%v %v", serverindex, args)
		ok := ck.servers[serverindex].Call("KVServer.PutAppend", &args, &reply)
		//Debug(dInfo, "%v action get have reply %v ok %v", ck.clientId, reply.Err, ok)
		if ok && reply.Err == "OK" {
			ck.sequenceNum++
			//Debug(dLeader, "ok")
			return
		} else if reply.Err == "ErrWrongLeader" {
			ck.leaderNow = int(nrand()) % len(ck.servers)
		} else { // No reply maybe err leader
			ck.leaderNow = int(nrand()) % len(ck.servers)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
