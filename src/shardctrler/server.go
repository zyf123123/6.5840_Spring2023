package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs        []Config // indexed by config num
	configNum      int
	duplicateTable map[int64][]int
	lastSerialNum  map[int64]int
	chanIndex      int
	scChan         map[int]chan Op
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action      string // get put append
	SerialNum   int
	ClientId    int64
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
	QueryNum    int
	QueryValue  Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = false
	op := Op{
		Action:      "Join",
		JoinServers: args.Servers,
		ClientId:    args.ClientId,
		SerialNum:   args.SerialNum,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
		//Debug(dInfo, "%v server will return %v", sc.me, reply.Err)
		return
	}
	reply.WrongLeader = false
	opMsg := Op{}

	error := sc.ReceiveChan(index, &opMsg)
	if error {
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = false
	op := Op{
		Action:    "Leave",
		LeaveGIDs: args.GIDs,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
		//Debug(dInfo, "%v server will return %v", sc.me, reply.Err)
		return
	}
	reply.WrongLeader = false
	opMsg := Op{}

	error := sc.ReceiveChan(index, &opMsg)
	if error {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = false
	op := Op{
		Action:    "Move",
		MoveShard: args.Shard,
		MoveGID:   args.GID,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
		//Debug(dInfo, "%v server will return %v", sc.me, reply.Err)
		return
	}
	opMsg := Op{}
	error := sc.ReceiveChan(index, &opMsg)
	if error {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = false
	op := Op{
		Action:    "Query",
		QueryNum:  args.Num,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
		//Debug(dInfo, "%v server will return %v", sc.me, reply.Err)
		return
	}

	opMsg := Op{}

	error := sc.ReceiveChan(index, &opMsg)
	reply.Config = opMsg.QueryValue
	if error {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) ReceiveChan(index int, op *Op) bool {
	sc.mu.Lock()
	ch := make(chan Op)
	sc.scChan[index] = ch
	sc.mu.Unlock()

	select {
	case *op = <-ch:
		sc.mu.Lock()
		sc.scChan[index] = nil
		sc.mu.Unlock()
		//Debug(dInfo, "%v", op)

	case <-time.After(500 * time.Millisecond):
		return true
	}
	return false
}

func (sc *ShardCtrler) IsDuplicate(clientId int64, serialNum int) bool {
	duplicateList, ok := sc.duplicateTable[clientId]
	duplicate := false
	var newduplicateList []int
	if ok {
		if len(duplicateList) != 0 {
			sc.lastSerialNum[clientId] = duplicateList[0]
			sequence := true
			for i := 1; i < len(duplicateList); i++ {
				if duplicateList[i]-duplicateList[i-1] == 1 && sequence {
					sc.lastSerialNum[clientId]++
				} else {
					sequence = false
					newduplicateList = append(newduplicateList, duplicateList[i])
				}
			}
			sc.duplicateTable[clientId] = newduplicateList
		}
		if serialNum <= sc.lastSerialNum[clientId] {
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
		sc.duplicateTable[clientId] = append(sc.duplicateTable[clientId], serialNum)
	}
	return duplicate
}

func (sc *ShardCtrler) ApplyCommandTicker() {
	//apply log and snapshot to state machine
	for {
		appmsg := <-sc.applyCh

		//Debug(dInfo, "%v get %v", sc.me, appmsg)
		if appmsg.CommandValid {
			sc.mu.Lock()
			command := appmsg.Command
			index := appmsg.CommandIndex
			if index <= sc.chanIndex { // outdated apply
				Debug(dDrop, "%v got outdated index %v", sc.me, index)
				sc.mu.Unlock()
				continue
			}
			op := command.(Op)
			queryValue := sc.ApplyCommand(op)
			op.QueryValue = queryValue

			currentTerm, isLeader := sc.rf.GetState()
			if isLeader && currentTerm == appmsg.CommandTerm {
				ch := sc.scChan[index]
				ch <- op
				sc.chanIndex = index
				//Debug(dInfo, "%v send to %v at term %v", sc.me, index, currentTerm)
				//
			}
			sc.mu.Unlock()
			Debug(dInfo, "%v apply command %v in index %v", sc.me, command, index)

		}

	}

	//time.Sleep(time.Millisecond)
}

func (sc *ShardCtrler) ApplyCommand(operation Op) Config {
	action := operation.Action
	clientId := operation.ClientId
	serialNum := operation.SerialNum
	queryValue := Config{}

	Debug(dInfo, "%v got action %v", sc.me, operation)
	switch action {
	case "Join":
		duplicate := sc.IsDuplicate(clientId, serialNum)
		if duplicate {
			Debug(dInfo, "%v %v duplicate %v", sc.me, duplicate, operation)

			return queryValue
		}
		oldGroups := sc.configs[sc.configNum].Groups
		Shards := sc.configs[sc.configNum].Shards

		var keys []int
		for key := range operation.JoinServers {
			keys = append(keys, key)
		}
		sort.Ints(keys)

		for i, shard := range Shards { // not allocated
			if shard == 0 {
				Shards[i] = keys[0]
			}
		}

		Groups := MergeMap(oldGroups, operation.JoinServers)
		newShards := BalanceGroup(Groups, Shards)

		newconfig := Config{Num: sc.configNum + 1, Shards: newShards, Groups: Groups}
		sc.configs = append(sc.configs, newconfig)
		sc.configNum++

	case "Leave":
		duplicate := sc.IsDuplicate(clientId, serialNum)
		if duplicate {
			Debug(dInfo, "%v %v duplicate %v", sc.me, duplicate, operation)

			return queryValue
		}
		oldGroups := sc.configs[sc.configNum].Groups
		Shards := sc.configs[sc.configNum].Shards

		for i := range Shards {
			for _, gid := range operation.LeaveGIDs {
				if Shards[i] == gid {
					Shards[i] = 0
				}
			}
		}

		Groups := LeaveMap(oldGroups, operation.LeaveGIDs)
		newShards := BalanceGroup(Groups, Shards)

		newconfig := Config{Num: sc.configNum + 1, Shards: newShards, Groups: Groups}
		sc.configs = append(sc.configs, newconfig)
		sc.configNum++
	case "Move":
		duplicate := sc.IsDuplicate(clientId, serialNum)
		if duplicate {
			Debug(dInfo, "%v %v duplicate %v", sc.me, duplicate, operation)
			return queryValue
		}
		oldGroups := sc.configs[sc.configNum].Groups
		newShards := sc.configs[sc.configNum].Shards
		newShards[operation.MoveShard] = operation.MoveGID

		newconfig := Config{Num: sc.configNum + 1, Shards: newShards, Groups: oldGroups}
		sc.configs = append(sc.configs, newconfig)
		sc.configNum++
	case "Query":
		if operation.QueryNum == -1 || operation.QueryNum > sc.configNum {
			queryValue = sc.configs[sc.configNum]
		} else {
			queryValue = sc.configs[operation.QueryNum]
		}
	}
	Debug(dInfo, "%v config %v become %v", sc.me, sc.configNum, sc.configs[sc.configNum])
	return queryValue
}

func BalanceGroup(groups map[int][]string, Shards [NShards]int) [NShards]int {
	//NShards := len(Shards)
	zeroNum := 0
	tmpShard := 0
	var keys []int
	for i := range Shards {
		if Shards[i] == 0 {
			zeroNum++
		} else if tmpShard == 0 {
			tmpShard = Shards[i]
		}
	}

	if zeroNum == NShards { // all empty
		return Shards
	} else { // allocate some values
		for i := range Shards {
			if Shards[i] == 0 {
				Shards[i] = tmpShard
			}
		}
	}

	shardNum := make(map[int]int)

	for key := range groups { // got every group use shard num
		for i := range Shards {
			if Shards[i] == key {
				shardNum[key]++
			} else {
				_, ok := shardNum[key]
				if !ok {
					shardNum[key] = 0
				}
			}
		}
	}

	for key := range shardNum {
		keys = append(keys, key)
	}

	sort.Ints(keys)

	for _, key := range keys { // balance shard num
		for _, key2 := range keys {
			value, value2 := shardNum[key], shardNum[key2]
			if key != key2 {
				for value-value2 >= 2 { // not balance
					for k := range Shards {
						if Shards[k] == key {
							Shards[k] = key2 // allocate shard to key2
							break
						}
					}
					shardNum[key]--
					value--
					shardNum[key2]++
					value2++
				}
			}
		}
	}

	return Shards
}

func MergeMap(mObj ...map[int][]string) map[int][]string {
	newObj := map[int][]string{}
	for _, m := range mObj {
		for k, v := range m {
			newObj[k] = v
		}
	}
	return newObj
}

func LeaveMap(group map[int][]string, leaveGID []int) map[int][]string {
	newGroup := make(map[int][]string, 0)
	//deleteGid := make([]int, 0)
	for key, value := range group {
		flag := true
		for _, gid := range leaveGID {
			if key == gid {
				flag = false
			}
		}
		if flag {
			newGroup[key] = value
		}
	}
	return newGroup
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by ShardCtrler tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configNum = 0
	sc.scChan = make(map[int]chan Op)
	sc.chanIndex = 0
	sc.lastSerialNum = make(map[int64]int)
	sc.duplicateTable = make(map[int64][]int)

	go sc.ApplyCommandTicker()

	return sc
}
