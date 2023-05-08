package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new Entry entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Entry, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Identification int

const (
	leader    Identification = 0
	follower  Identification = 1
	candidate Identification = 2
)

// as each Raft peer becomes aware that successive Entry entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Entry entry.
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

	identification Identification

	currentTerm int
	voteFor     int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type Entry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term = rf.currentTerm
	var isleader = rf.identification == leader
	// Your code here (2A).
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
	if data == nil || len(data) < 1 {
		// bootstrap without any state?
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
// service no longer needs the Entry through (and including)
// that index. Raft should now trim its Entry as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//compare Term and index
	rf.mu.Lock()
	defer rf.mu.Unlock()
	hisTerm := args.Term
	myTerm := rf.currentTerm

	if hisTerm < myTerm {
		reply.VoteGranted = false
		reply.Term = myTerm
		DPrintf("vote not granted from %d", rf.me)
		return
	}

	if hisTerm > myTerm {
		rf.setTerm(hisTerm)
		rf.voteFor = -1
		rf.becomeFollower()
		reply.Term = hisTerm
		if rf.isLogUpToDate(args.LastLogIndex) {
			rf.voteFor = args.CandidateId
			DPrintf("vote granted from %d", rf.me)
			reply.VoteGranted = true
		} else {
			DPrintf("vote not granted from %d", rf.me)
			reply.VoteGranted = false
		}
		return
	}

	if hisTerm == myTerm {
		// already vote for another server or
		// request server's log not up-to-date
		if (rf.voteFor != -1 && rf.voteFor != args.CandidateId) || !rf.isLogUpToDate(args.LastLogIndex) {
			reply.VoteGranted = false
			DPrintf("vote not granted from %d", rf.me)
		} else {
			reply.VoteGranted = true
			DPrintf("vote granted from %d", rf.me)
			rf.voteFor = args.CandidateId
		}
		reply.Term = myTerm
	}
}

func (rf *Raft) isLogUpToDate(index int) bool {
	//return len(rf.log) > index
	return true
}

// While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
// If the leader’s Term (included in its RPC) is at least as large as the candidate’s current Term,
// then the candidate recognizes the leader as legitimate and returns to follower state.
// If the Term in the RPC is smaller than the candidate’s current Term,
// then the candidate rejects the RPC and con- tinues in candidate state.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("heartbeat not accept")
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.becomeFollower()
		rf.voteFor = -1
	}

	//not pass consistency check
	//if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.Term {
	//	reply.Success = false
	//	reply.Term = rf.currentTerm
	//}
	DPrintf("heartbeat accept")
	ok := rf.electionTimer.Reset(rf.electionTimeOut())
	if ok {
		DPrintf("timer still wait,reset now")
	} else {
		DPrintf("timer already trigger")
	}
}

func (rf *Raft) sendHeartbeatToPeers() {
	for idx, peer := range rf.peers {
		if idx == rf.me {
			continue
		}
		DPrintf("heartbeat send to %d", idx)
		peer.Call("Raft.AppendEntries", &AppendEntriesArgs{Term: rf.currentTerm}, &AppendEntriesReply{})
	}
}

func (rf *Raft) becomeLeader() {
	//only candidate can be a leader
	DPrintf("%d become leader", rf.me)
	rf.identification = leader
	rf.electionTimer.Stop()
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatTimeOut())
}

func (rf *Raft) becomeCandidate() {
	if rf.identification == leader {
		rf.electionTimer = time.NewTimer(rf.electionTimeOut())
		rf.heartbeatTimer.Stop()
	}
	rf.identification = candidate
}

func (rf *Raft) becomeFollower() {
	if rf.identification == leader {
		rf.electionTimer = time.NewTimer(rf.electionTimeOut())
		rf.heartbeatTimer.Stop()
	}
	rf.identification = follower

}

func (rf *Raft) electionTimeOut() time.Duration {
	return time.Duration(rf.me*100+150) * time.Millisecond
}

func (rf *Raft) heartbeatTimeOut() time.Duration {
	return time.Duration(2) * time.Millisecond
}

func (rf *Raft) setTerm(term int) {
	rf.currentTerm = term
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

func (rf *Raft) voteForResult(server int, args *RequestVoteArgs, vote *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//DPrintf("reply Term:%d, current Term:%d", reply.Term, rf.currentTerm)
		if reply.Term > rf.currentTerm {
			// update Term
			rf.setTerm(reply.Term)
			// quit election
			rf.becomeFollower()
		} else if reply.Term == rf.currentTerm && reply.VoteGranted {
			*vote += 1
			if *vote > len(rf.peers)/2 && rf.identification == candidate {
				rf.becomeLeader()
				// send heartbeat
				rf.sendHeartbeatToPeers()
			}
		}
	}
}

// To begin an election, a follower increments its current Term and transitions to candidate state.
// It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
func (rf *Raft) startElection() {
	DPrintf("start election %d", rf.me)

	//var lastLogTerm int
	//if len(rf.log) > 0 {
	//	lastLogTerm = rf.log[len(rf.log)-1].Term
	//} else {
	//	lastLogTerm = -1
	//}
	requestVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		//LastLogIndex: len(rf.log),
		//LastLogTerm:  lastLogTerm,
	}
	vote := 1
	for idx := range rf.peers {
		// skip self
		if idx != rf.me {
			go rf.voteForResult(idx, &requestVoteArgs, &vote)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's Entry. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft Entry, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// leader

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			// change state to candidate
			if rf.identification != leader {
				rf.becomeCandidate()
				// Term +1
				rf.currentTerm += 1
				// vote for self
				rf.voteFor = rf.me
				rf.startElection()
				rf.electionTimer.Reset(rf.electionTimeOut())
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.identification == leader {
				DPrintf("leader %d send heartbeat", rf.me)
				rf.sendHeartbeatToPeers()
				rf.heartbeatTimer.Reset(rf.heartbeatTimeOut())
			}
			rf.mu.Unlock()
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
	rf.identification = follower
	rf.currentTerm = 1
	rf.voteFor = -1
	rf.electionTimer = time.NewTimer(rf.electionTimeOut())
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatTimeOut())
	rf.heartbeatTimer.Stop()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
