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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	snapshot  []byte

	// persister
	currentTerm       int
	votedFor          int
	log               []Log
	lastLogIndex      int
	lastIncludedIndex int
	lastIncludedTerm  int

	state           int
	commitIndex     int
	lastApplied     int
	electionTimeout int

	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == LEADER)

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	//e.Encode(rf.snapshot)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	log := []Log{}
	lastLogIndex := 0
	currentTerm := 0
	votedFor := 0
	lastIncludedIndex := 0
	lastIncludedTerm := 0

	if d.Decode(&log) != nil ||
		d.Decode(&lastLogIndex) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		Debug(dPersist, "%v happen read persist error!", rf.me)
	} else {
		rf.mu.Lock()
		rf.log = log
		rf.lastLogIndex = lastLogIndex
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex // read snapshot
		if rf.commitIndex < lastIncludedIndex {
			rf.commitIndex = lastIncludedIndex
		}
		rf.mu.Unlock()
	}
	rf.snapshot = rf.persister.ReadSnapshot()

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Debug(dVote, "%v's term is %v but %v's term is %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.mu.Unlock()
		return
	}
	if rf.lastLogIndex > args.LastIncludedIndex { // have same index
		pos := rf.lastLogIndex - args.LastIncludedIndex + 1
		if rf.log[pos].Term == args.LastIncludedTerm {
			Debug(dDrop, "%v has same term with %v snapshot  return", rf.me, args.LeaderId)
			rf.mu.Unlock()
			return
		}
	}
	rf.mu.Unlock()
	rf.Snapshot(args.LastIncludedIndex, args.Data) // discard

	go func() {
		rf.mu.Lock()
		applymsg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- applymsg
	}()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	if index > rf.lastLogIndex {
		rf.lastLogIndex = index
	}
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index

	}
	// discard the log before index (include index)

	lastLog := len(rf.log) - 1
	newLog := make([]Log, 1)

	pos := len(rf.log) - rf.lastLogIndex + index // new log index start from 1
	for i := pos; i <= lastLog; i++ {
		newLog = append(newLog, rf.log[i])
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[pos-1].Term
	rf.log = newLog
	Debug(dLog, "%v 's log is become %v", rf.me, rf.log)
	rf.snapshot = snapshot
	Debug(dSnap, "%v store snapshot for index %v", rf.me, index)
	rf.persist()
	rf.mu.Unlock()

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		Debug(dVote, "%v's term is %v but %v's term is %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	} else if rf.currentTerm < args.Term { //become FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
		rf.state = FOLLOWER
		Debug(dTerm, "%v is turning to term %v", rf.me, args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		((rf.log[len(rf.log)-1].Term < args.LastLogTerm) || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.lastLogIndex <= args.LastLogIndex)) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTimeout = 0
		Debug(dVote, "%v vote for %v", rf.me, rf.votedFor)
		Debug(dTerm, "%v have %v log and candidate has %v log", rf.me, rf.lastLogIndex, args.LastLogIndex)
		Debug(dTerm, "%v have last log in term %v and candidate in term %v", rf.me, rf.log[len(rf.log)-1].Term, args.LastLogTerm)
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
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	if rf.currentTerm > args.Term {
		Debug(dWarn, "%v 's term is higher than %v, is %v", rf.me, args.LeaderId, rf.currentTerm)
		reply.Success = false
		return
	}
	rf.electionTimeout = 0
	rf.state = FOLLOWER
	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	if rf.lastLogIndex < args.PrevLogIndex { //dont exist
		Debug(dWarn, "%v dont have log in %v", rf.me, args.PrevLogIndex)
		//Debug(dWarn, "%v ")
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogIndex
		reply.XIndex = rf.lastLogIndex
		Debug(dLog, "%v xlen is %v", rf.me, reply.XLen)
		return
	}
	prevLogPos := len(rf.log) - 1 - rf.lastLogIndex + args.PrevLogIndex //prev log position
	//Debug(dClient, "%v has prevlog pos %v, lastlog %v, prevlogindex %v", rf.me, prevLogPos, rf.lastLogIndex, args.PrevLogIndex)

	if rf.log[prevLogPos].Term != args.PrevLogTerm { // log dont match
		Debug(dWarn, "%v doesn't match in log %v", rf.me, args.PrevLogIndex)
		Debug(dWarn, "%v has log in term %v but args in term %v", rf.me, rf.log[prevLogPos].Term, args.PrevLogTerm)
		reply.Success = false
		reply.XTerm = rf.log[prevLogPos].Term
		index := prevLogPos
		term := rf.log[index].Term
		for rf.log[index-1].Term == term {
			index--
		}
		reply.XIndex = index
		Debug(dLog, "%v xIndex is %v", rf.me, reply.XIndex)

		return
	}

	//rf.lastLog = args.PrevLogIndex // if log match before prevlogindex
	//rf.persist()

	index := len(rf.log) - rf.lastLogIndex + args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		if index == len(rf.log) { //dont exist dirictly insert
			rf.log = append(rf.log, args.Entries[i])
			rf.lastLogIndex++
		} else if rf.log[index] != args.Entries[i] { // exist but confilct
			Debug(dLog2, "%v log %v different from %v in index %v", rf.me, args.Entries[i], rf.log[index], args.PrevLogIndex)
			rf.log = rf.log[0:index] // remove log after index
			rf.log = append(rf.log, args.Entries[i])
			rf.lastLogIndex = args.PrevLogIndex + i + 1

		}
		rf.persist()
		index++
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex)
		Debug(dCommit, "%v commit index = %v as FOLLOWER", rf.me, rf.commitIndex)
		Debug(dCommit, "%v lastlog %v prevlog %v", rf.me, rf.lastLogIndex, args.PrevLogIndex)
		//Debug(dCommit, "%v entries %v", rf.me, args.Entries)

	}

	//Debug(dClient, "%v received heartbeat during term %v", rf.me, rf.currentTerm)
	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) checkLogsAgreement() {

	//Debug(dLeader, "%v start for agreement at term %v", rf.me, rf.currentTerm)
	//receivedLog := 1
	//finishedLog := 1
	//cond := sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			serverIndex := i
			go func() {
				if rf.nextIndex[serverIndex]-1 < rf.lastIncludedIndex { // need to send snapshot

					Debug(dSnap, "%v last include index %v term %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.snapshot,
					}
					reply := InstallSnapshotReply{}
					rf.mu.Unlock()
					ok := rf.sendInstallSnapshot(serverIndex, &args, &reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm { // term error
						rf.currentTerm = reply.Term
						rf.persist()
						rf.state = FOLLOWER
						//rf.electionTimeout = 0
						Debug(dLeader, "%v is not a leader ", rf.me)
					} else if ok {
						Debug(dLog, "%v has snapshot now", serverIndex)

						// next index > last log
						rf.nextIndex[serverIndex] = rf.lastIncludedIndex + 1
						rf.matchIndex[serverIndex] = rf.lastIncludedIndex
						Debug(dLeader, "%v nextindex of %v is become %v", rf.me, serverIndex, rf.nextIndex[serverIndex])

					}
					rf.mu.Unlock()
				} else { // dont need snapshot  normal heartbeat
					//
					//fmt.Printf("%v %v\n", startIndex, rf.nextIndex[serverIndex])
					entities := make([]Log, 0)
					startIndex := len(rf.log) - 1 - rf.lastLogIndex + rf.nextIndex[serverIndex]
					prevLogIndex := rf.nextIndex[serverIndex] - 1
					//Debug(dLeader, "%v startindex of %v is %v, log size is %v, last index is %v, next %v", rf.me, serverIndex, startIndex, len(rf.log), rf.lastLogIndex, rf.nextIndex[serverIndex])

					prevLogTerm := rf.log[startIndex-1].Term
					for i := startIndex; i <= len(rf.log)-1; i++ {
						entities = append(entities, Log{rf.log[i].Term, rf.log[i].Command})
					}

					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entities,
						LeaderCommit: rf.commitIndex,
					}
					reps := AppendEntriesReply{}

					//Debug(dLeader, "%v send entries %v to %v", rf.me, entities, serverIndex)

					rf.mu.Unlock()
					log := rf.sendAppendEntries(serverIndex, &args, &reps)
					rf.mu.Lock()
					if log && reps.Success {
						//receivedLog++
						//Debug(dInfo, "%v have received log from %v", rf.me, serverIndex)
						rf.matchIndex[serverIndex] = Max(rf.matchIndex[serverIndex], prevLogIndex+len(entities))
						rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1
						rf.mu.Unlock()

					} else if !log {
						//Debug(dWarn, "%v can't send to server %v", rf.me, serverIndex)
						rf.mu.Unlock()

					} else if reps.Term > rf.currentTerm { // term error
						rf.currentTerm = reps.Term
						rf.persist()
						rf.state = FOLLOWER
						//rf.electionTimeout = 0
						Debug(dLeader, "%v is not a leader ", rf.me)
						rf.mu.Unlock()

					} else { //log inconsistency
						Debug(dCommit, "%v happen log inconsistency with %v", rf.me, serverIndex)
						if reps.XIndex > 0 && rf.nextIndex[serverIndex] > reps.XIndex { // log dont match
							//Debug(dLog, "%v log dont match with XIndex %v", rf.me, reps.XIndex)
							Debug(dLog, "%v back nextindex of %v from %v to %v", rf.me, serverIndex, rf.nextIndex[serverIndex], reps.XIndex)

							rf.nextIndex[serverIndex] = reps.XIndex

						} else if rf.nextIndex[serverIndex] > prevLogIndex {
							Debug(dLog, "%v back nextindex of %v from %v to %v", rf.me, serverIndex, rf.nextIndex[serverIndex], prevLogIndex)
							rf.nextIndex[serverIndex] = prevLogIndex

						}

						//rf.heartbeatTimeout = 100
						rf.mu.Unlock()
					}
					//rf.mu.Lock()
					//finishedLog++
					// rf.mu.Unlock()
					//cond.Broadcast()
				}
			}()
		}
	}
	rf.mu.Lock()
	/*
		for finishedLog != len(rf.peers) && rf.state == leader {
		cond.Wait()
	}*/
	if rf.state == LEADER {
		for i := rf.lastLogIndex; i > rf.commitIndex; i-- {
			logIndex := len(rf.log) - 1 - rf.lastLogIndex + i
			flag := 1
			for j, x := range rf.matchIndex {
				if x >= i && j != rf.me {
					flag++
				}
			}
			if flag > len(rf.peers)/2 && rf.log[logIndex].Term == rf.currentTerm {
				rf.commitIndex = i
				Debug(dLog, "%v has %v log now", rf.me, rf.lastLogIndex)
				Debug(dCommit, "%v commit index = %v", rf.me, rf.commitIndex)
			}
		}
	}
	rf.mu.Unlock()

}

func (rf *Raft) CommitCommand() {
	//if commitIndex > lastApplied
	//apply log[lastApplied] to state machine
	for !rf.killed() {
		go func() {
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				Debug(dCommit, "%v lastapplied %v lastlogindex %v commitindex %v", rf.me, rf.lastApplied, rf.lastLogIndex, rf.commitIndex)
				applyIndex := len(rf.log) - 1 - rf.lastLogIndex + rf.lastApplied
				app := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[applyIndex].Command,
					CommandIndex: rf.lastApplied,
				}
				Debug(dCommit, "%v apply command %v to the server for id %v", rf.me, rf.log[applyIndex], rf.lastApplied)
				rf.mu.Unlock()
				rf.applyCh <- app

			} else {
				rf.mu.Unlock()
			}
		}()

		time.Sleep(50 * time.Millisecond)
	}
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
	rf.mu.Lock()

	if rf.state != LEADER {
		rf.mu.Unlock()
		isLeader = false
		//Debug(dWarn, "%v is not leader cant start", rf.me)
		return index, term, isLeader
	}
	newLog := Log{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.lastLogIndex++
	rf.log = append(rf.log, newLog)
	rf.persist()

	//Debug(dLeader, "%v have received command %v in term %v", rf.me, command, rf.currentTerm)
	//rf.checkLogsAgreement()
	index = rf.lastLogIndex
	isLeader = (rf.state == LEADER)
	term = rf.currentTerm
	rf.mu.Unlock()
	rf.checkLogsAgreement()

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

func (rf *Raft) BecomeLeader() {

	rf.mu.Lock()
	rf.state = LEADER
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = -1
	}
	rf.mu.Unlock()
	Debug(dLeader, "%v become leader at term %v!", rf.me, rf.currentTerm)
	rf.checkLogsAgreement()

}

func (rf *Raft) AttemptElection() bool {
	receivedVote := 1
	finishedVote := 1

	Debug(dInfo, "%v Election is begin during term %v", rf.me, rf.currentTerm)
	cond := sync.NewCond(&rf.mu)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			serverIndex := i
			go func() {
				reqArgs := RequestVoteArgs{}
				reqReply := RequestVoteReply{}
				vote := rf.sendRequestVote(serverIndex, &reqArgs, &reqReply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if vote && reqReply.VoteGranted {
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
	if receivedVote > len(rf.peers)/2 && rf.state == CANDIDATE { // become leader
		rf.mu.Unlock()
		rf.BecomeLeader()
		return true
	} else {
		rf.state = FOLLOWER
		Debug(dInfo, "%v lost at term %v!", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return false
	}
}

func (rf *Raft) SendHeartbeat() {

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.checkLogsAgreement()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) ticker() {
	timeout := 250 + (rand.Int() % 150)
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		rf.electionTimeout += 1
		if rf.electionTimeout > timeout && rf.state != LEADER { // become candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist() // votedfor
			rf.state = CANDIDATE
			rf.electionTimeout = 0
			rf.mu.Unlock()
			go rf.AttemptElection()

		} else { //FOLLOWER or leader
			rf.mu.Unlock()
			time.Sleep(time.Millisecond)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.

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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.lastLogIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]Log, 1)
	rf.snapshot = nil
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	Debug(dInfo, "%v start", rf.me)
	go rf.ticker()
	go rf.SendHeartbeat()
	go rf.CommitCommand()

	return rf
}
