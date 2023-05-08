package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

	KvSet          map[string]string
	DuplicateTable map[int64][]int
	LastSerialNum  map[int64]int
	Shard          int
	Config         shardctrler.Config
}

const (
	serving  = 0
	pulling  = 1
	offering = 2
	pullend  = 3
)

type Shard struct {
	KvSet          map[string]string
	DuplicateTable map[int64][]int
	LastSerialNum  map[int64]int
	State          int
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
	mck *shardctrler.Clerk
	//kvSet          map[string]string
	//duplicateTable map[int64][]int
	//lastSerialNum  map[int64]int
	shardList  []Shard
	kvChan     map[int]chan Op
	chanIndex  int
	config     shardctrler.Config
	lastconfig shardctrler.Config
	configNum  int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		Debug(dInfo, "%v server will return %v want group %v but get %v", kv.me, reply.Err, kv.config.Shards[shard], kv.gid)
		kv.mu.Unlock()
		return
	}
	if kv.shardList[shard].State != serving {
		reply.Err = ErrShardNotOk
		Debug(dInfo, "%v server will return %v shard state %v", kv.me, reply.Err, kv.shardList[shard].State)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Action: "Get",
		Key:    args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Value = ""
		reply.Err = "ErrWrongLeader"
		Debug(dInfo, "%v server will return %v", kv.me, reply.Err)

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
		Debug(dInfo, "%v time out server will return %v", kv.me, reply.Err)

	}
	//Debug(dClient, "%v with reply %v", kv.me, reply.Err)

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		Debug(dInfo, "%v server will return %v want group %v but get %v", kv.me, reply.Err, kv.config.Shards[shard], kv.gid)
		kv.mu.Unlock()
		return
	}
	if kv.shardList[shard].State != serving {
		reply.Err = ErrShardNotOk
		Debug(dInfo, "%v server will return %v shard state %v", kv.me, reply.Err, kv.shardList[shard].State)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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

		Debug(dInfo, "%v server will return %v", kv.me, reply.Err)
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
		Debug(dInfo, "%v time out server will return %v", kv.me, reply.Err)

	}

	//Debug(dClient, "%v with reply %v %v", kv.me, reply.Err, args.Op)
}

func (kv *ShardKV) IsDuplicate(shard int, clientId int64, serialNum int) bool {
	duplicateList, ok := kv.shardList[shard].DuplicateTable[clientId]
	duplicate := false
	newduplicateList := make([]int, 0)
	if ok {
		if len(duplicateList) != 0 {

			kv.shardList[shard].LastSerialNum[clientId] = duplicateList[0]
			sequence := true
			for i := 1; i < len(duplicateList); i++ {
				if duplicateList[i]-duplicateList[i-1] == 1 && sequence {
					kv.shardList[shard].LastSerialNum[clientId]++
				} else {
					sequence = false
					newduplicateList = append(newduplicateList, duplicateList[i])
				}
			}
			kv.shardList[shard].DuplicateTable[clientId] = newduplicateList
		}
		if serialNum <= kv.shardList[shard].LastSerialNum[clientId] {
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

		//fmt.Printf("%v\n", kv.shardList[shard].duplicateTable)
		kv.shardList[shard].DuplicateTable[clientId] = append(kv.shardList[shard].DuplicateTable[clientId], serialNum)
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
				//kv.mu.Unlock()
				ch <- op
				//kv.mu.Lock()
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
	shard := key2shard(operation.Key)

	//Debug(dInfo, "duplicate %v %v", duplicate, operation)

	switch action {
	case "Get":
		getValue = kv.shardList[shard].KvSet[operation.Key]
	case "Put":
		duplicate := kv.IsDuplicate(shard, clientId, serialNum)
		if !duplicate {
			kv.shardList[shard].KvSet[operation.Key] = operation.Value
			Debug(dClient, "%v put %v %v at %v", kv.me, operation.Key, operation.Value, operation.SerialNum)
		}

	case "Append":
		duplicate := kv.IsDuplicate(shard, clientId, serialNum)
		if !duplicate {
			kv.shardList[shard].KvSet[operation.Key] += operation.Value
			Debug(dClient, "%v append %v at %v", kv.me, operation.Value, operation.SerialNum)
			Debug(dClient, "%v value become %v", kv.me, kv.shardList[shard].KvSet[operation.Key])
		}
	case "AskShard":
		//kv.shardList[operation.Shard].State = serving
		//

		Debug(dInfo, "%v reply %v shard ", kv.gid, operation.Shard)
	case "DeleteShard":

		kv.shardList[operation.Shard].KvSet = make(map[string]string)
		kv.shardList[operation.Shard].DuplicateTable = make(map[int64][]int)
		kv.shardList[operation.Shard].LastSerialNum = make(map[int64]int)

		kv.shardList[operation.Shard].State = serving

		//

		Debug(dInfo, "%v shard %v state is serving", kv.gid, operation.Shard)

	case "ReservingShard":
		kv.shardList[operation.Shard].State = serving
		Debug(dInfo, "%v shard %v state is serving", kv.gid, operation.Shard)

	case "FetchConfig":
		duplicate := kv.IsDuplicate(shardctrler.NShards, clientId, serialNum)
		if !duplicate {
			//Debug(dInfo, "%v start fetch config for num %v", kv.me, kv.configNum)
			kv.FetchConfig(operation.Config)
		}

	case "InstallShard":
		kv.shardList[operation.Shard].DuplicateTable = make(map[int64][]int)
		kv.shardList[operation.Shard].LastSerialNum = make(map[int64]int)
		kv.shardList[operation.Shard].KvSet = make(map[string]string)
		//
		for key, value := range operation.DuplicateTable {
			kv.shardList[operation.Shard].DuplicateTable[key] = value
		}
		for key, value := range operation.LastSerialNum {
			kv.shardList[operation.Shard].LastSerialNum[key] = value
		}
		for key, value := range operation.KvSet {
			kv.shardList[operation.Shard].KvSet[key] = value
			//Debug(dInfo, "%v install kvset key %v value %v", kv.me, key2shard(key), operation.Shard)
		}
		// install end   wait for delete origin shard
		kv.shardList[operation.Shard].State = pullend
		Debug(dInfo, "%v shard %v state is pullend", kv.gid, operation.Shard)
		//
		//

		Debug(dInfo, "%v has install shard %v, key %v", kv.me, kv.shardList[operation.Shard], operation.Key)
	}

	return getValue
}

func (kv *ShardKV) SaveSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.shardList)
	e.Encode(kv.configNum)
	e.Encode(kv.config)
	e.Encode(kv.lastconfig)

	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var shardlist []Shard
	var config shardctrler.Config
	var lastconfig shardctrler.Config
	var configNum int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if snapshot != nil {
		if d.Decode(&shardlist) != nil ||
			d.Decode(&configNum) != nil ||
			d.Decode(&config) != nil ||
			d.Decode(&lastconfig) != nil {
			Debug(dWarn, "readSnapShot ERROR for server %v", kv.me)
		} else {
			kv.shardList = shardlist
			//Debug(dSnap, "%v get snapshot %v", kv.me, kv.shardList)
			kv.configNum = configNum
			kv.config = config
			kv.lastconfig = lastconfig
			for i := 0; i < shardctrler.NShards+1; i++ {
				if kv.shardList[i].DuplicateTable == nil {
					kv.shardList[i].DuplicateTable = make(map[int64][]int)
				}
				if kv.shardList[i].KvSet == nil {
					kv.shardList[i].KvSet = make(map[string]string)
				}
				if kv.shardList[i].LastSerialNum == nil {
					kv.shardList[i].LastSerialNum = make(map[int64]int)
				}

			}
			//Debug(dSnap, "readSnapShot %v", kv.shardList)
			//

		}
	}
}

func (kv *ShardKV) FetchConfigTicker() {
	for {
		_, isleader := kv.rf.GetState()

		if isleader {
			kv.mu.Lock()
			flag := true
			for i := 0; i < shardctrler.NShards; i++ {
				// all shard are serving go fetch
				if kv.shardList[i].State != serving {
					flag = false
					Debug(dInfo, "%v %v shard %v state is %v!", kv.gid, kv.me, i, kv.shardList[i].State)

					break
				}
			}
			kv.mu.Unlock()
			if !flag {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			config := kv.mck.Query(kv.configNum + 1)
			if config.Num != kv.configNum+1 { // config not change
				continue
			}
			op := Op{
				Action:    "FetchConfig",
				Config:    config,
				ClientId:  int64(kv.gid),
				SerialNum: config.Num,
			}

			index, _, isLeader := kv.rf.Start(op)
			if !isLeader {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			kv.mu.Lock()
			ch := make(chan Op)
			kv.kvChan[index] = ch
			kv.mu.Unlock()

			select {
			case getMsg := <-ch:
				kv.mu.Lock()
				kv.kvChan[index] = nil
				kv.mu.Unlock()
				Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

			case <-time.After(500 * time.Millisecond):
				Debug(dInfo, "%v server out time in index %v", kv.me, index)
				continue
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) FetchConfig(config shardctrler.Config) {

	kv.lastconfig = kv.config
	kv.config = config
	kv.configNum = config.Num

	for i := 0; i < shardctrler.NShards; i++ {
		targetServerGid := kv.lastconfig.Shards[i]
		if kv.config.Shards[i] == kv.gid && kv.config.Shards[i] != targetServerGid && targetServerGid != 0 {
			// need pull data from target
			kv.shardList[i].State = pulling
			Debug(dInfo, "%v %v shard %v state is pulling at %v", kv.gid, kv.me, i, config.Num)
		} else if kv.lastconfig.Shards[i] == kv.gid && kv.config.Shards[i] != targetServerGid && kv.config.Shards[i] != 0 {
			// need offer data to others
			kv.shardList[i].State = offering
			Debug(dInfo, "%v %v shard %v state is offering at %v", kv.gid, kv.me, i, config.Num) //
		}
	}

	//if !reflect.DeepEqual(kv.config, config) { // config change
	// allocated new shards
	/*
		targetServerGid := kv.config.Shards[i]
		if config.Shards[i] == kv.gid && config.Shards[i] != targetServerGid && targetServerGid != 0 {
			targetServer := kv.config.Groups[targetServerGid]
			Debug(dInfo, "%v ask for shard %v for server %v", kv.gid, i, targetServerGid)
			kv.AskForShard(targetServer, i)
		} else if kv.config.Shards[i] == kv.gid && config.Shards[i] != kv.config.Shards[i] {
			// shard was allocate to other server
			kv.WaitForShard()
		}*/
	//}
	//kv.config = config
}

func (kv *ShardKV) ShardTransTicker() {
	for {
		_, isleader := kv.rf.GetState()
		if isleader {
			//Debug(dInfo, "%v shard trans", kv.me)
			for i := 0; i < shardctrler.NShards; i++ {
				kv.mu.Lock()
				if kv.shardList[i].State == pulling {
					targetServerGid := kv.lastconfig.Shards[i]
					targetServer := kv.lastconfig.Groups[targetServerGid]
					Debug(dInfo, "%v ask for shard %v for %v", kv.gid, i, targetServerGid)
					kv.mu.Unlock()
					kv.AskForShard(targetServer, i)
				} else {
					kv.mu.Unlock()
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) AskForShard(targetServer []string, shard int) {
	si := 0
	for si < len(targetServer) {

		//Debug(dInfo, "%v try get shard form %v", kv.gid, si)
		target := kv.make_end(targetServer[si])
		args := AskShardArgs{
			Shard:     shard,
			ConfigNum: kv.configNum,
		}
		reply := AskShardReply{}
		ok := target.Call("ShardKV.AskShard", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dInfo, "%v get shard info %v", kv.gid, reply)
			kv.InstallShard(reply.KvSet, reply.DuplicateTable, reply.LastSerialNum, args.Shard)
			//kv.kvSet = reply.KvSet
			//kv.lastSerialNum = reply.LastSerialNum
		}
		si++

	}
}

func (kv *ShardKV) InstallShard(kvSet map[string]string, duplicateTable map[int64][]int, lastSerialNum map[int64]int, shard int) {
	/*
		if kv.shardList[shard].State != pulling {
			return
		}*/
	Debug(dInfo, "%v try install shard %v", kv.me, shard)
	op := Op{
		Action:         "InstallShard",
		KvSet:          kvSet,
		DuplicateTable: duplicateTable,
		LastSerialNum:  lastSerialNum,
		Shard:          shard,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.mu.Lock()
	ch := make(chan Op)
	kv.kvChan[index] = ch
	kv.mu.Unlock()

	select {
	case getMsg := <-ch:
		kv.mu.Lock()
		kv.kvChan[index] = nil

		kv.mu.Unlock()
		Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

	case <-time.After(500 * time.Millisecond):
		Debug(dInfo, "%v server out time in index %v", kv.me, index)
	}
}

func (kv *ShardKV) DeleteShardTicker() {
	for {
		_, isleader := kv.rf.GetState()
		if isleader {
			//Debug(dInfo, "%v shard trans", kv.me)
			for i := 0; i < shardctrler.NShards; i++ {
				//kv.mu.Lock()
				if kv.shardList[i].State == pullend {
					targetServerGid := kv.lastconfig.Shards[i]
					targetServer := kv.lastconfig.Groups[targetServerGid]
					Debug(dInfo, "%v delete shard %v for %v", kv.gid, i, targetServerGid)
					//kv.mu.Unlock()
					kv.DeleteForShard(targetServer, i)
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

//
//

func (kv *ShardKV) DeleteForShard(targetServer []string, shard int) {
	si := 0
	for si < len(targetServer) {

		//Debug(dInfo, "%v try get shard form %v", kv.gid, si)
		target := kv.make_end(targetServer[si])
		args := DeleteShardArgs{
			Shard:     shard,
			ConfigNum: kv.configNum,
		}
		reply := DeleteShardReply{}
		ok := target.Call("ShardKV.DeleteShard", &args, &reply)
		if ok && reply.Err == OK {
			Debug(dInfo, "%v get shard info %v", kv.gid, reply)
			kv.ReservingShard(shard)
		}
		si++

	}
}
func (kv *ShardKV) ReservingShard(shard int) {
	/*
		if kv.shardList[shard].State != pullend {
			return
		}*/
	Debug(dInfo, "%v reserve shard %v", kv.me, shard)
	op := Op{
		Action: "ReservingShard",
		Shard:  shard,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.mu.Lock()
	ch := make(chan Op)
	kv.kvChan[index] = ch
	kv.mu.Unlock()

	select {
	case getMsg := <-ch:
		kv.mu.Lock()
		kv.kvChan[index] = nil

		kv.mu.Unlock()
		Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

	case <-time.After(500 * time.Millisecond):
		Debug(dInfo, "%v server out time in index %v", kv.me, index)
	}
}

func (kv *ShardKV) AskShard(args *AskShardArgs, reply *AskShardReply) {
	/*if kv.shardList[args.Shard].State != offering {
		reply.Err = ErrShardNotOk
		return
	}*/
	reply.Err = ErrShardNotOk
	kv.mu.Lock()
	if args.ConfigNum != kv.configNum {
		reply.Err = ErrShardNotOk
		Debug(dInfo, "%v has confignum %v but arg has %v", kv.gid, kv.configNum, args.ConfigNum)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Action: "AskShard",
		Shard:  args.Shard,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//

	kv.mu.Lock()
	ch := make(chan Op)
	kv.kvChan[index] = ch
	kv.mu.Unlock()

	select {
	case getMsg := <-ch:
		kv.mu.Lock()
		kv.kvChan[index] = nil

		replyKvSet := make(map[string]string)
		for key, value := range kv.shardList[args.Shard].KvSet {
			replyKvSet[key] = value
		}

		duplicateTable := make(map[int64][]int)
		for key, value := range kv.shardList[args.Shard].DuplicateTable {
			duplicateTable[key] = value
		}

		lastSerialNum := make(map[int64]int)
		for key, value := range kv.shardList[args.Shard].LastSerialNum {
			lastSerialNum[key] = value
		}
		//duplicateTable = kv.duplicateTable

		reply.KvSet = replyKvSet
		reply.DuplicateTable = duplicateTable
		reply.LastSerialNum = lastSerialNum
		reply.Err = OK

		kv.mu.Unlock()

		Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		Debug(dInfo, "%v server will return %v", kv.me, reply.Err)
	}
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	/*if kv.shardList[args.Shard].State != offering {
		reply.Err = ErrShardNotOk
		return
	}*/
	reply.Err = ErrShardNotOk
	kv.mu.Lock()
	if args.ConfigNum != kv.configNum {
		reply.Err = ErrShardNotOk
		kv.mu.Unlock()
		Debug(dInfo, "%v has confignum %v but arg has %v", kv.gid, kv.configNum, args.ConfigNum)
		return
		//
		//
	}
	kv.mu.Unlock()
	op := Op{
		Action:    "DeleteShard",
		Shard:     args.Shard,
		ClientId:  int64(kv.gid),
		SerialNum: kv.configNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
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
		reply.Err = OK
		kv.mu.Unlock()

		Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		Debug(dInfo, "%v server will return %v", kv.me, reply.Err)
	}
}
func (kv *ShardKV) EmptyCommitTicker() {
	for {
		if !kv.rf.HasLogAtCurrentTerm() {
			op := Op{
				Action: "EmptyCommit",
			}
			index, _, isLeader := kv.rf.Start(op)
			if isLeader {
				kv.mu.Lock()
				ch := make(chan Op)
				kv.kvChan[index] = ch
				kv.mu.Unlock()

				//Debug(dClient, "%v %v index ", kv.me, index)

				select {
				case <-ch:
					kv.mu.Lock()
					kv.kvChan[index] = nil
					kv.mu.Unlock()
					//Debug(dInfo, "%v server get command in index %v will return %v", kv.me, index, getMsg)

				case <-time.After(500 * time.Millisecond):
				}
			}

		}
		time.Sleep(100 * time.Millisecond)
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
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	//
	//
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	//
	//
	//kv.kvSet = make(map[string]string)
	//kv.duplicateTable = make(map[int64][]int)
	kv.shardList = make([]Shard, shardctrler.NShards+1)
	for i := 0; i < shardctrler.NShards+1; i++ {
		kv.shardList[i].DuplicateTable = make(map[int64][]int)
		kv.shardList[i].KvSet = make(map[string]string)
		kv.shardList[i].LastSerialNum = make(map[int64]int)
		kv.shardList[i].State = serving
	}
	kv.kvChan = make(map[int]chan Op)
	kv.chanIndex = 0
	kv.configNum = 0
	kv.lastconfig = shardctrler.Config{}
	//kv.lastSerialNum = make(map[int64]int)
	snapshot := kv.rf.GetSnapShot()
	kv.ReadSnapshot(snapshot)

	go kv.ApplyCommandTicker()
	go kv.FetchConfigTicker()
	go kv.ShardTransTicker()
	go kv.DeleteShardTicker()
	go kv.EmptyCommitTicker()
	///

	return kv
}
