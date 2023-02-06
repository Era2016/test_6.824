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

	"labrpc"
)

const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

const CHECK_PERIOD = 300         // sleep check period
const ELECTION_TIMEOUT_LOW = 500 // timeout period 500ms~1000ms
const ELECTION_TIMEOUT_HIGH = 1000

const HEARTBEAT_INTERVAL = 150 // heartbeat per 150ms
const REQUEST_VOTE_REPLY_TIME = 100

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//
	// Persistent state on all servers
	//
	currentTerm int // latest term server has seen, initialized to 0 on first boot
	votedFor    int // candidateId that received vote in current term, null if none

	// log entries; each entry contains command for state machine and term
	// when entry was received by leader (first index is 1)
	log []LogEntry // map: term->command

	//
	// Volatile state on all servers
	//
	commitIndex int // index of highest log entry known to be committed, initialized to 0
	lastApplied int // index of highest log entry applied to state machine, initialized to 0

	//
	// Volatile state on leaders
	//
	// for each server, index of the next log entry to send to that serve
	// (initialized to leader last log index + 1
	nextIndex []int // serverID => index
	// index of highest log entry known to be replicated on server
	// initialized to 0
	matchIndex []int // serverID => index

	state        int // [0, 1, 2] => [follower, candidate, leader]
	applyCh      chan ApplyMsg
	lastReceived time.Time // last time the peer heard from the leader
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader, term = rf.state == STATE_LEADER, rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
// example RequestVote RPC handler.
// requestVote parallels with appendEntries
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term %d]: Raft[%d] receive requestVote from Raft[%d]", rf.currentTerm, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		lastLogIndex := len(rf.log) - 1

		if lastLogTerm < args.LastLogTerm ||
			lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex {

			rf.votedFor = args.CandidateId
			rf.lastReceived = time.Now()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			DPrintf("[term %d]: Raft [%d] vote for Raft [%d]", rf.currentTerm, rf.me, rf.votedFor)
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

//
// also heartbeats
//
func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[term %d]: Raft[%d] [state %d] receive AppendEntries from Raft[%d]",
		rf.currentTerm, rf.me, rf.state, args.LeaderId)
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.lastReceived = time.Now()

	lastLogTerm := rf.log[len(rf.log)-1].Term
	lastLogIndex := len(rf.log) - 1
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		return
	}

	if args.PrevLogIndex == lastLogIndex && args.PrevLogTerm != lastLogTerm {
		reply.Success = false
		return
	}

	// to delete conflicts entries
	// to append entries

	if args.LeaderCommit > rf.commitIndex {
		newLogIndex := len(rf.log) - 1
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex < newLogIndex {
			rf.commitIndex = newLogIndex
		}
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

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	r := rand.New(rand.NewSource(int64(rf.me)))
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// needs random otherwise all vote together
		timeout := int(r.Float64()*(ELECTION_TIMEOUT_HIGH-ELECTION_TIMEOUT_LOW) + ELECTION_TIMEOUT_LOW)

		rf.mu.Lock()
		curState := rf.state
		lstReceived := rf.lastReceived
		rf.mu.Unlock()
		if !(time.Since(lstReceived) > time.Duration(timeout)*time.Millisecond && curState != STATE_LEADER) {
			time.Sleep(CHECK_PERIOD * time.Millisecond)
			continue
		}

		// start election
		ch := make(chan *RequestVoteReply, len(rf.peers)-1)
		term := callRequestVote(rf, ch)
		voted := collectVotes(rf, term, ch)
		//DPrintf("collectedVote result, [peer %d] [term %d] [cnt: %d]", rf.me, term, voted)

		// candidate => leader
		if voted > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.state = STATE_LEADER
			rf.mu.Unlock()

			go callAppendEntry(rf)
		}
	}
}

func callRequestVote(rf *Raft, ch chan *RequestVoteReply) int {
	// send vote request
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.state = STATE_CANDIDATE
	rf.lastReceived = time.Now()

	candidateId := rf.me
	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	rf.mu.Unlock()

	DPrintf("[term %d]:Raft [%d][state %d] starts an election\n", term, rf.me, rf.state)
	for peer := range rf.peers {
		if peer == rf.me {
			DPrintf("vote for self : Raft[%d]", rf.me)
			continue
		}

		DPrintf("[term %d]:Raft [%d][state %d] sends requestvote RPC to server[%d]",
			term, rf.me, rf.state, peer)
		go func(end *labrpc.ClientEnd) {
			req := RequestVoteArgs{
				CandidateId:  candidateId,
				Term:         term,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			reply := RequestVoteReply{}
			if ret := end.Call("Raft.RequestVote", &req, &reply); ret == true {
				ch <- &reply
			}
			// else {
			// 	ch <- &RequestVoteReply{Term: -1, VoteGranted: false}
			// }
		}(rf.peers[peer])
	}

	return term
}

func collectVotes(rf *Raft, term int, ch chan *RequestVoteReply) int {
	// var voteMu sync.Mutex
	voted := 1
	tt := time.NewTimer(REQUEST_VOTE_REPLY_TIME * time.Millisecond)
	defer tt.Stop()
	for voted <= len(rf.peers)/2 {
		select {
		case r := <-ch:
			//DPrintf("requestVote reply: [term %d] [isVoted %t]\n", r.Term, r.VoteGranted)
			rf.mu.Lock()
			// ** attention, term cannot be changed **
			if term != rf.currentTerm {
				rf.mu.Unlock()
				break
			}

			if r.Term > rf.currentTerm {
				rf.currentTerm = r.Term
				rf.state = STATE_FOLLOWER
				rf.votedFor = -1
				rf.mu.Unlock()
				break
			}

			rf.mu.Unlock()

			if r.VoteGranted == true {
				voted++
			}
		case <-tt.C:
			goto END
		}
	}

END:
	return voted
}

// appendEntries / heartbeats
func callAppendEntry(rf *Raft) {
	DPrintf("[term %d]:Raft [%d] [state %d] becomes leader !", rf.currentTerm, rf.me, rf.state)

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != STATE_LEADER {
			rf.mu.Unlock()
			break
		}

		term := rf.currentTerm
		leaderId := rf.me
		prevLogIndex := len(rf.log) - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		leaderCommit := rf.lastApplied
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go func(index int) {
				req := RequestAppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: leaderCommit,
				}

				DPrintf("[term %d]:Raft [%d] [state %d] sends appendentries RPC to server[%d]",
					term, rf.me, rf.state, index)
				rf.peers[index].Call("Raft.AppendEntries", &req, &RequestAppendEntriesReply{})

			}(peer)
		}

		time.Sleep(time.Millisecond * HEARTBEAT_INTERVAL)
	}
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

	//DPrintf("init a raft object... [%d]\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastReceived = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
