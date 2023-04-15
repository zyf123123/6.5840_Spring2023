package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvSet          map[string]string
	duplicateTable map[int64][]int
	lastSerialNum  map[int64]int
	leaderTerm     int
	kvChan         map[int]chan Op
	chanIndex      int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
			Debug(dClient, "%v server will return %v ", kv.me, opMsg)
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

	//Debug(dClient, "%v %v index ", kv.me, index)

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

func (kv *ShardKV) IsDuplicate(clientId int64, serialNum int) bool {
	duplicateList, ok := kv.duplicateTable[clientId]
	duplicate := false
	var newduplicateList []int
	if ok {
		if len(duplicateList) != 0 {
			kv.lastSerialNum[clientId] = duplicateList[0]
			sequence := true
			for i := 1; i < len(duplicateList); i++ {
				if duplicateList[i]-duplicateList[i-1] == 1 && sequence {
					kv.lastSerialNum[clientId]++
				} else {
					sequence = false
					newduplicateList = append(newduplicateList, duplicateList[i])
				}
			}
			kv.duplicateTable[clientId] = newduplicateList
		}
		if serialNum <= kv.lastSerialNum[clientId] {
			duplicate = true
		}
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

func (kv *ShardKV) ApplyCommandTicker() {
	//apply log and snapshot to state machine
	for {
		appmsg := <-kv.applyCh

		//Debug(dInfo, "%v get %v", kv.me, appmsg)
		if appmsg.CommandValid {
			kv.mu.Lock()
			command := appmsg.Command
			index := appmsg.CommandIndex
			if index <= kv.chanIndex { // outdated apply
				Debug(dDrop, "%v got outdated index %v", kv.me, index)
				kv.mu.Unlock()
				continue
			}
			op := command.(Op)

			getValue := kv.ApplyCommand(op)
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && currentTerm == appmsg.CommandTerm {
				ch := kv.kvChan[index]
				op.GetValue = getValue
				ch <- op
				kv.chanIndex = index
				//Debug(dInfo, "%v send to %v at term %v", kv.me, index, currentTerm)
				//
			}
			kv.mu.Unlock()
			Debug(dInfo, "%v apply command %v in index %v", kv.me, command, index)

			raftstate := kv.rf.GetRaftStateSize()
			if raftstate > kv.maxraftstate && kv.maxraftstate != -1 { // need snapshot
				Debug(dSnap, "%v has %v bytes, need to snapshot in index %v", kv.me, raftstate, index)
				kv.rf.Snapshot(index, kv.SaveSnapshot())
			}
		} else if appmsg.SnapshotValid { //install snapshot
			//currentTerm, _ := kv.rf.GetState()
			//
			//

			if appmsg.SnapshotIndex >= kv.chanIndex {
				kv.ReadSnapshot(appmsg.Snapshot)
				Debug(dSnap, "%v have snapshot in index %v", kv.me, appmsg.SnapshotIndex)
				kv.chanIndex = appmsg.SnapshotIndex
			}
		}

	}

	//time.Sleep(time.Millisecond)
}

func (kv *ShardKV) ApplyCommand(operation Op) string {
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

func (kv *ShardKV) SaveSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.kvSet)
	e.Encode(kv.duplicateTable)
	e.Encode(kv.lastSerialNum)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var kvset map[string]string
	var duplicateTable map[int64][]int
	var lastSerialNum map[int64]int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if snapshot != nil {
		if d.Decode(&kvset) != nil ||
			d.Decode(&duplicateTable) != nil ||
			d.Decode(&lastSerialNum) != nil {
			Debug(dWarn, "readSnapShot ERROR for server %v", kv.me)
		} else {
			kv.kvSet = kvset
			kv.duplicateTable = duplicateTable
			kv.lastSerialNum = lastSerialNum
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	//
	kv.kvSet = make(map[string]string)
	kv.duplicateTable = make(map[int64][]int)
	kv.leaderTerm = -1
	kv.kvChan = make(map[int]chan Op)
	kv.chanIndex = 0
	kv.lastSerialNum = make(map[int64]int)
	snapshot := kv.rf.GetSnapShot()
	kv.ReadSnapshot(snapshot)

	go kv.ApplyCommandTicker()

	return kv
}
