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
	"sync"
	"labrpc"
	"math/rand"
	"time"
	"bytes"
	"encoding/gob"
	)

// import "bytes"
// import "encoding/gob"

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int//persistent state on all servers
	votedFor    int
	logs        []LogEntry
	ssIndex     int
	commitIndex int//volatile state on all servers
	lastApplied int
	nextIndex   []int//volatile state on leaders
	matchIndex  []int
	ssarr       []int

	voteCount int//record how many servers vote for this server
	state   string//server state(leader,follower,candidate)
	applyCh chan ApplyMsg
	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = false
	if rf.state == "leader" {
		isleader = true
	}
	return term, isleader
}

func (rf *Raft) GetLastIndex() int {
	if len(rf.logs) < 1 {
		return rf.ssIndex
	}else {
		return rf.logs[len(rf.logs) - 1].Index
	}
}


func (rf *Raft) GetLogSize() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.ssIndex)
	e.Encode(rf.ssarr)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data != nil{
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.logs)
		d.Decode(&rf.ssIndex)
		d.Decode(&rf.ssarr)
		rf.commitIndex = rf.ssIndex
		rf.lastApplied = rf.ssIndex
	}
}

func (rf *Raft) ApplyUnsnapshot() {
	go func() {
		if len(rf.logs) > 0 {
			for i := 0; i < len(rf.logs); i++{
				rf.applyCh <- ApplyMsg{UseSnapshot: true, Command: rf.logs[i].Command}
			}
		}
	}()
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		return
	}
	if index < rf.ssIndex+1 {
		return
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	baseIndex := rf.ssIndex
	rf.ssIndex = rf.logs[index-rf.ssIndex-2].Index
	rf.ssarr[rf.me] = rf.ssIndex
	rf.logs = rf.logs[index-baseIndex-1:]
	e.Encode(rf.ssarr)
	rf.persist()
	//fmt.Printf("state:%v server:%v snapshotsize:%v logsize:%v\n", rf.state, rf.me, rf.ssIndex, len(rf.logs))
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

type SnapshotArgs struct {
	Term              int
	LeaderId          int
	SSIndex           int
	Snapshot          []byte
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshot(server int,args SnapshotArgs,reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Snapshot", args, reply)
	return ok
}

func (rf *Raft) handleSnapshot(server int,reply SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		rf.votedFor = -1
		return
	}
	rf.nextIndex[server] = rf.ssIndex + 1
	rf.matchIndex[server] = rf.ssIndex
	rf.ssarr[server] = rf.ssIndex
	rf.persist()
}

func (rf *Raft) Snapshot(args SnapshotArgs,reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetTimer()
	rf.state = "follower"
	rf.currentTerm = args.Term
	r := bytes.NewBuffer(args.Snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&rf.ssarr)
	rf.ssIndex = args.SSIndex
	rf.logs = make([]LogEntry, 0)
	rf.lastApplied = args.SSIndex
	rf.commitIndex = args.SSIndex
	rf.ssarr[rf.me] = rf.ssIndex
	rf.persist()
	rf.persister.SaveSnapshot(args.Snapshot)
	rf.applyCh <- ApplyMsg{UseSnapshot:true}
	//fmt.Printf("server:%v size:%v logsize:%v msg:%v\n", rf.me, rf.ssIndex, len(rf.logs), msg.UseSnapshot)
}


func (rf *Raft) handleSendSnapshot(i int) {
	var args SnapshotArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.SSIndex = rf.ssIndex
	args.Snapshot = rf.persister.snapshot
	//args.Logs = rf.logs
	go func(server int,args SnapshotArgs) {
		var reply SnapshotReply
		ok := rf.sendSnapshot(server, args, &reply)
		if ok {
			rf.handleSnapshot(server, reply)
		}
	}(i,args)
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  
	VoteGranted bool
}


func Majority(n int) int {
	return n/2 + 1
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs)-1 > 0 && (rf.logs[len(rf.logs)-1].Term > args.LastLogTerm || (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && rf.GetLastIndex() > args.LastLogIndex)){
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
	}
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm == args.Term {
		rf.state = "follower"
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.resetTimer()
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		}
		return
	}
	if rf.currentTerm < args.Term {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetTimer()
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		}
		return
	}
}

func (rf *Raft) handleVoteReply(reply RequestVoteReply) {
	if rf.currentTerm > reply.Term {
		return
	}
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.voteCount = 0
		rf.persist()
		rf.resetTimer()
		return
	}
	if rf.state == "candidate" && reply.VoteGranted {
		rf.voteCount += 1
		if rf.voteCount >= Majority(len(rf.peers)) { //if get vote from majority, convert to leader
			rf.state = "leader"
			rf.votedFor = -1
			rf.persist()
			rf.resetTimer()
			//fmt.Printf("Server:%v Term:%v\n", rf.me, rf.currentTerm)
			for i := 0; i < len(rf.peers); i++ { //initial leader volatile state
				if i == rf.me {
					continue
				}
				rf.voteCount = 0
				rf.nextIndex[i] = rf.GetLastIndex()+1
				rf.matchIndex[i] = -1
			}
			for i := 0; i < len(rf.peers); i++ { //send append msg to reset vote information and prevent other followers becoming candidate
				if i == rf.me {
					continue
				}
				rf.handleSendAppendEntry(i)
			}
		}
		return
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//Append RPC
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	SSarr        []int 
	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	PrevTIndex  int //record index of last term(if conflict with leader) or last index of server
	Ssf         int
}

func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.state = "follower"
	rf.votedFor = -1
	rf.currentTerm = args.Term //reset vote information
	rf.persist()
	reply.Term = args.Term
	reply.Ssf = rf.ssIndex
	if rf.GetLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.PrevTIndex = rf.GetLastIndex()
		return
	}
	if args.PrevLogIndex-rf.ssIndex-1 >= 0 && len(rf.logs) >= args.PrevLogIndex-rf.ssIndex-1 && rf.logs[args.PrevLogIndex-rf.ssIndex-1].Term != args.PrevLogTerm { // if log conflict with leaders'
		reply.Success = false
		reply.PrevTIndex = rf.GetLastIndex()
		if rf.GetLastIndex() > args.PrevLogIndex {
			reply.PrevTIndex = args.PrevLogIndex
		}
		term1 := rf.logs[reply.PrevTIndex-rf.ssIndex-1].Term
		//s := 0
		for reply.PrevTIndex-rf.ssIndex-1 >= 0 && reply.PrevTIndex < rf.GetLastIndex()+1 && rf.logs[reply.PrevTIndex-rf.ssIndex-1].Term == term1 {
			reply.PrevTIndex--
			//s++
		}// subtract the entire term
		return
	}
	if args.Entries != nil {
		if args.PrevLogIndex-rf.ssIndex >= 0 {
			rf.logs = rf.logs[:args.PrevLogIndex-rf.ssIndex]
			rf.logs = append(rf.logs, args.Entries...)
		}
		rf.ssarr = args.SSarr
		//fmt.Printf("server:%v length:%v\n", rf.me, len(rf.logs))
		rf.persist()
		if rf.GetLastIndex() >= args.LeaderCommit && args.LeaderCommit>rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			go rf.commit()
		}
		reply.PrevTIndex = rf.GetLastIndex()
		reply.Success = true
	}else {// heartbeat
		if rf.GetLastIndex() >= args.LeaderCommit && args.LeaderCommit>rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			go rf.commit()
		}
		reply.PrevTIndex = args.PrevLogIndex
		reply.Success = true
	}
	rf.resetTimer()
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntry(server int, reply AppendEntryReply) {
	rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.state != "leader" {
        return
    }
    if reply.Term > rf.currentTerm { // convert to follower
        rf.state = "follower"
        rf.votedFor = -1
        rf.currentTerm = reply.Term
        rf.persist()
        rf.resetTimer()
        return
    }
    if reply.Success {
		rf.nextIndex[server] = reply.PrevTIndex+1
		rf.matchIndex[server] = reply.PrevTIndex
		rf.ssarr[server] = reply.Ssf
		commitCount := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				commitCount += 1//check if all server have this entry
			}
		}
		if commitCount >= Majority(len(rf.peers)) && rf.commitIndex < rf.matchIndex[server] && rf.GetLastIndex() >= rf.matchIndex[server] && rf.logs[rf.matchIndex[server]-rf.ssIndex-1].Term == rf.currentTerm && len(rf.logs) >= rf.matchIndex[server]-rf.ssIndex-1{
			rf.commitIndex = rf.matchIndex[server] //if majority of server have up-to-date entry
			//fmt.Printf("leaderid:%v term:%v commitlength:%v\n", rf.me, rf.currentTerm, rf.matchIndex[server])
			go rf.commit()
		}
	} else {
		if reply.PrevTIndex-rf.ssIndex-1 >=-1 {
			rf.nextIndex[server] = reply.PrevTIndex + 1
		}else {
			rf.nextIndex[server] = 0
		}
		rf.handleSendAppendEntry(server)
	}
}

func (rf *Raft) handleSendAppendEntry(i int) {
	var args AppendEntryArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1
	if args.PrevLogIndex-rf.ssIndex-1 >= 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.ssIndex-1].Term
	}
	if rf.nextIndex[i]-rf.ssIndex-1 < len(rf.logs) && rf.nextIndex[i]-rf.ssIndex-1 >=0{
		args.Entries = rf.logs[rf.nextIndex[i]-rf.ssIndex-1:]
	}
	args.LeaderCommit = rf.commitIndex
	args.SSarr = rf.ssarr
	go func(server int, args AppendEntryArgs) {
		var reply AppendEntryReply
		ok := rf.sendAppendEntry(server, args, &reply)
		if ok {
			rf.handleAppendEntry(server, reply)
		}
	}(i,args)
}

func (rf *Raft) commit() {//followers apply or leader commit
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Index: i+1, Command: rf.logs[i-rf.ssIndex-1].Command}
		rf.lastApplied = i
	}
	//rf.lastApplied = rf.commitIndex
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
    defer rf.mu.Unlock()
    index := -1
    term := -1
    isLeader := true
    if rf.state != "leader" {
    	isLeader := false
        return index, term, isLeader
    }
    log := LogEntry{command, rf.currentTerm, rf.GetLastIndex()+1}
    rf.logs = append(rf.logs, log)
    index = rf.GetLastIndex()+1
    term = rf.currentTerm
    rf.persist()
    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) handleTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		if rf.state == "follower"{
			rf.state = "candidate"
			rf.currentTerm += 1 //only follower increase its own term
			//fmt.Printf("server:%v term:%v\n", rf.me, rf.currentTerm)
		}
		rf.votedFor = rf.me
		rf.voteCount = 1
		rf.persist()
		var args RequestVoteArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.GetLastIndex()
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[args.LastLogIndex-rf.ssIndex-1].Term
		}
		for i := 0; i < len(rf.peers); i++ {// send voterequest to all servers
			if i == rf.me {
				continue
			}
			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server,&args, &reply)
				if ok {
					rf.handleVoteReply(reply)
				}
			}(i, args)
		}
	} else {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.ssarr[i] < rf.ssIndex {
				rf.handleSendSnapshot(i)
			}else {
				rf.handleSendAppendEntry(i)
			}
		}
		
	}
	rf.resetTimer()
}

func (rf *Raft) resetTimer() {
	node_timeout := time.Millisecond * 100 //Heartbeat timeout
	if rf.state != "leader" {
		node_timeout = time.Millisecond * time.Duration(300+rand.Int63n(150)) //Election timeout
	}
	if rf.timer == nil {// initialize timer
		rf.timer = time.NewTimer(node_timeout)
		go func() {
			for{
				<-rf.timer.C
				rf.handleTimeout()
			}
		}()
	} 
	rf.timer.Reset(node_timeout)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.logs = make([]LogEntry, 0)
	rf.ssIndex = -1

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.ssarr = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++{
    	rf.ssarr[i] = -1
    }

	rf.state = "follower"
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ApplyUnsnapshot()
	rf.resetTimer()


	return rf
}
