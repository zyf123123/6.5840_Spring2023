package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrShardNotOk  = "ErrShardNotOk"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	SerialNum int
	ClientId  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SerialNum int
	ClientId  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type AskShardArgs struct {
	Shard     int
	ConfigNum int
}

type AskShardReply struct {
	KvSet          map[string]string
	DuplicateTable map[int64][]int
	LastSerialNum  map[int64]int
	Err            Err
}

type DeleteShardArgs struct {
	Shard     int
	ConfigNum int
}

type DeleteShardReply struct {
	Err Err
}
