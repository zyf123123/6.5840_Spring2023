package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	isLeader    bool
	log         []int

	commitIndex     int
	lastApplied     int
	electionTimeout int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	//lastLogIndex int
	//lastLogTerm  int

	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		Debug(dVote, "%v's term is %v but %v's term is %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	} else if rf.currentTerm < args.Term { //become follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.electionTimeout = 0
		Debug(dTerm, "%v is turning to term %v", rf.me, args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		Debug(dVote, "%v vote for %v", rf.me, rf.votedFor)
		Debug(dTerm, "%v is during term %v", rf.me, rf.currentTerm)
		reply.VoteGranted = true
	}
	//args.lastLogIndex
	// Your code here (2A, 2B).
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		Debug(dWarn, "%v 's term is higher than %v, is %v", rf.me, args.LeaderId, rf.currentTerm)
		reply.Success = false
		return
	}
	rf.electionTimeout = 0
	rf.isLeader = false
	//rf.votedFor = -1
	rf.currentTerm = args.Term
	Debug(dClient, "%v received heartbeat during term %v", rf.me, rf.currentTerm)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Debug(dWarn, "%v is unconnected", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		timeout := 2
		receivedVote := 1
		finishedVote := 1

		// Your code here (2A)
		// Check if a leader election should be started.
		reqArgs := RequestVoteArgs{}
		reqReply := RequestVoteReply{}
		appArgs := AppendEntriesArgs{}
		appReply := AppendEntriesReply{}

		rf.electionTimeout += 1
		//fmt.Printf("%v\n", rf.electionTimeout)

		if rf.electionTimeout > timeout { // become candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			Debug(dInfo, "%v Election is begin during term %v", rf.me, rf.currentTerm)
			cond := sync.NewCond(&rf.mu)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					serverIndex := i

					go func() {
						reqArgs.CandidateId = rf.me
						reqArgs.Term = rf.currentTerm

						vote := rf.sendRequestVote(serverIndex, &reqArgs, &reqReply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if vote {
							Debug(dVote, "%v received vote from %v", rf.me, serverIndex)
							receivedVote++
							Debug(dVote, "%v have %v votes now", rf.me, receivedVote)

						}
						finishedVote++
						cond.Broadcast()
					}()
				}
			}
			rf.mu.Lock()
			for receivedVote <= len(rf.peers)/2 && finishedVote != len(rf.peers) {
				cond.Wait()
			}
			if receivedVote > len(rf.peers)/2 { // become leader
				rf.isLeader = true
				rf.electionTimeout = 0
				Debug(dLeader, "%v become leader at term %v!", rf.me, rf.currentTerm)
				appArgs.LeaderId = rf.me
				appArgs.Term = rf.currentTerm
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						//fmt.Printf("%v %v\n", len(rf.peers), i)
						Debug(dInfo, "%v send new leader to %v", rf.me, i)
						go rf.sendAppendEntries(i, &appArgs, &appReply)

					}
				}
			} else {
				Debug(dInfo, "%v lost at term %v!", rf.me, rf.currentTerm)
			}
			rf.mu.Unlock()
			//fmt.Printf("%v\n", len(rf.peers)/2)

		} else if rf.isLeader {
			rf.electionTimeout = 0
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					appArgs.LeaderId = rf.me
					appArgs.Term = rf.currentTerm
					//fmt.Printf("%v %v\n", len(rf.peers), i)
					Debug(dInfo, "%v send heartbeat to %v", rf.me, i)
					rf.sendAppendEntries(i, &appArgs, &appReply)
					if appReply.Term > rf.currentTerm {
						rf.currentTerm = appReply.Term
						rf.isLeader = false
						Debug(dLeader, "%v is not a leader ", rf.me)
					}
				}
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		if rf.electionTimeout < timeout {
			time.Sleep(time.Duration(50) * time.Millisecond)
		} else {
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.isLeader = false
	//rf.electionTimeout = 100
	//fmt.Printf("%v\n", len(peers))
	//for i := 0; i < len(peers); i++ {
	//	fmt.Printf("%v", rf.peers[i])
	//}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	Debug(dInfo, "%v start", rf.me)
	go rf.ticker()

	return rf
}
