package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

/*
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}*/

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action    string // get put append
	Key       string
	Value     string
	SerialNum int
	ClientId  int64
	GetValue  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvSet          map[string]string
	duplicateTable map[int64][]int
	leaderTerm     int
	kvChan         map[int]chan Op
	chanIndex      int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Action: "Get",
		Key:    args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Value = ""
		reply.Err = "ErrWrongLeader"
		return
	}

	kv.mu.Lock()
	ch := make(chan Op)
	kv.kvChan[index] = ch
	kv.mu.Unlock()
	var opMsg Op

	select {
	case opMsg = <-ch:
		kv.mu.Lock()
		value := opMsg.GetValue
		kv.kvChan[index] = nil
		kv.mu.Unlock()
		if value != "" {
			reply.Value = value
			Debug(dClient, "server will return %v ", opMsg)
			reply.Err = "OK"

		} else {
			reply.Value = ""
			reply.Err = "ErrNoKey"
		}
		//Debug(dInfo, "%v", op)

	case <-time.After(500 * time.Millisecond):
		reply.Err = "ErrWrongLeader"
	}
	//Debug(dClient, "%v with reply %v", kv.me, reply.Err)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Action:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"

		//Debug(dInfo, "%v server will return %v", kv.me, reply.Err)
		return
	}
	kv.mu.Lock()
	ch := make(chan Op)
	kv.kvChan[index] = ch
	kv.mu.Unlock()

	Debug(dClient, "%v %v index ", kv.me, index)

	select {
	case getMsg := <-ch:
		kv.mu.Lock()
		kv.kvChan[index] = nil
		kv.mu.Unlock()
		reply.Err = "OK"
		Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

	case <-time.After(500 * time.Millisecond):
		reply.Err = "ErrWrongLeader"
		Debug(dInfo, "%v server will return %v", kv.me, reply.Err)

	}

	//Debug(dClient, "%v with reply %v %v", kv.me, reply.Err, args.Op)
}

func (kv *KVServer) IsDuplicate(clientId int64, serialNum int) bool {
	duplicateList, ok := kv.duplicateTable[clientId]
	duplicate := false
	if ok {
		//fmt.Printf("%v\n", duplicateList)
		for i := len(duplicateList) - 1; i >= 0; i-- { // need optimaze
			if duplicateList[i] == serialNum { //duplicate
				duplicate = true
				break
			}
		}
	}
	if !duplicate {
		kv.duplicateTable[clientId] = append(kv.duplicateTable[clientId], serialNum)
	}
	return duplicate
}

func (kv *KVServer) ApplyCommandTicker() {
	//apply log and snapshot to state machine
	for !kv.killed() {
		appmsg := <-kv.applyCh
		//Debug(dInfo, "%v get %v", kv.me, appmsg)
		if appmsg.CommandValid {
			kv.mu.Lock()
			command := appmsg.Command
			index := appmsg.CommandIndex
			op := command.(Op)

			getValue := kv.ApplyCommand(op)
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && currentTerm == appmsg.CommandTerm {
				ch := kv.kvChan[index]
				op.GetValue = getValue
				//kv.mu.Unlock()
				ch <- op
				Debug(dInfo, "%v send to %v at term %v", kv.me, index, currentTerm)
				//return

				//kv.mu.Lock()
			}
			kv.mu.Unlock()

			Debug(dInfo, "%v apply command %v in index %v", kv.me, command, index)

		}

	}

	//time.Sleep(time.Millisecond)
}

func (kv *KVServer) ApplyCommand(operation Op) string {
	action := operation.Action
	clientId := operation.ClientId
	serialNum := operation.SerialNum
	getValue := ""
	//Debug(dInfo, "duplicate %v %v", duplicate, operation)

	switch action {
	case "Get":
		getValue = kv.kvSet[operation.Key]
	case "Put":
		duplicate := kv.IsDuplicate(clientId, serialNum)
		if !duplicate {
			kv.kvSet[operation.Key] = operation.Value
			Debug(dClient, "%v put %v %v at %v", kv.me, operation.Key, operation.Value, operation.SerialNum)
		}

	case "Append":
		duplicate := kv.IsDuplicate(clientId, serialNum)
		if !duplicate {
			kv.kvSet[operation.Key] += operation.Value
			Debug(dClient, "%v append %v at %v", kv.me, operation.Value, operation.SerialNum)
		}
	}
	return getValue
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvSet = make(map[string]string)
	kv.duplicateTable = make(map[int64][]int)
	kv.leaderTerm = -1
	kv.kvChan = make(map[int]chan Op)
	kv.chanIndex = 0

	go kv.ApplyCommandTicker()
	return kv
}
