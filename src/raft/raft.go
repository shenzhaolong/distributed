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
	"log"
	"math/rand"
	"rewrite/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Follower  State = 1
	Candidate State = 2
	Leader    State = 3
)

func getUUID() int {
	// 设置种子值为当前时间
	rand.NewSource(time.Now().UnixNano())
	return rand.Intn(1 << 30) // [0-10]
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int

	nextIndex      map[int]int
	matchIndex     map[int]int
	lastLeaderTime int64

	state           State
	electionTimeout int

	applyCh *chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == Leader
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	UUID         int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	UUID        int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.UUID = args.UUID
	log.Printf("node %d get RequestVote %v from %d", rf.me, args, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = args.Term
	}
	reply.VoteGranted = false
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state != Follower) {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 1
		rf.votedFor = -1
	}
	isNew := false
	lastIdx := len(rf.log) - 1
	if (args.LastLogTerm > rf.log[lastIdx].Term) ||
		(args.LastLogTerm == rf.log[lastIdx].Term && args.LastLogIndex >= lastIdx+1) {
		isNew = true
	}
	defer log.Printf("node %d reply to %d is %v, isNew is %v, lastTerm is %d, LastLogIndex is %d", rf.me, args.CandidateId, reply, isNew, rf.log[lastIdx].Term, lastIdx)
	if isNew && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastLeaderTime = time.Now().UnixMilli()
	}
	defer log.Printf("node %d vote result %t on node %d, my last Idx %d, last term is %d",
		rf.me, reply.VoteGranted, args.CandidateId, lastIdx, rf.log[lastIdx].Term)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []Entry
	LeaderCommit int
	UUID         int
}

type AppendEntriesReply struct {
	Success       bool
	Term          int
	ConflictIndex int
	ConflictTerm  int
	UUID          int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.UUID = args.UUID
	reply.Success = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.lastLeaderTime = time.Now().UnixMilli()
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.votedFor = -1
	defer log.Printf("node %d get AppendEntries from %d, args is %v, reply is %v",
		rf.me, args.LeaderId, args, reply)
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Term = rf.currentTerm
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Term = rf.currentTerm
		return
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.PrevLogIndex < args.LeaderCommit {
			rf.commitIndex = args.PrevLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
	rf.log = append(rf.log, args.Logs...)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.votedFor = rf.me
	needWaitNum, agreeNum, peerNum := int((len(rf.peers)+1)/2), 1, len(rf.peers)
	forTerm := rf.currentTerm
	rf.mu.Unlock()
	agreeMutex := sync.Mutex{}
	for i := 0; i < peerNum && !rf.killed(); i++ {
		if i != rf.me {
			go func(target int, forTerm int) {
				for !rf.killed() {
					rf.mu.Lock()
					state := rf.state
					term := rf.currentTerm
					UUID := getUUID()
					request := RequestVoteArgs{
						Term:         forTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log),
						LastLogTerm:  rf.log[(len(rf.log) - 1)].Term,
						UUID:         UUID,
					}
					reply := RequestVoteReply{}
					rf.mu.Unlock()
					if state != Candidate || term != forTerm {
						return
					}
					log.Printf("node %d send  RequestVote %v to %d", rf.me, request, target)
					ok := rf.sendRequestVote(target, &request, &reply)
					if ok {
						if reply.Term > forTerm {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.lastLeaderTime = time.Now().UnixMilli()
							rf.mu.Unlock()
						} else if reply.VoteGranted && reply.UUID == UUID {
							agreeMutex.Lock()
							agreeNum++
							agreeMutex.Unlock()
						}
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
			}(i, forTerm)
		}
	}
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			state := rf.state
			term := rf.currentTerm
			rf.mu.Unlock()
			if state != Candidate || term != forTerm {
				return
			}
			agreeMutex.Lock()
			am := agreeNum
			agreeMutex.Unlock()
			if am >= needWaitNum {
				rf.mu.Lock()
				rf.state = Leader
				rf.votedFor = -1
				nextIndex := len(rf.log)
				rf.nextIndex = make(map[int]int)
				rf.matchIndex = make(map[int]int)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = nextIndex
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
				go rf.listenSendHeart()
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.listenAgreementEntry(i)
					}
				}
				go rf.listenMajorCopy()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
}

func (rf *Raft) listenElection() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		lastTime := rf.lastLeaderTime
		rf.mu.Unlock()
		nowTime := time.Now().UnixMilli()
		if state != Leader && nowTime-lastTime >= int64(rf.electionTimeout) {
			rf.mu.Lock()
			log.Printf("node %d lastTime is %d, nowTime is %d, ele is %d", rf.me, lastTime, nowTime, rf.electionTimeout)
			rf.lastLeaderTime = time.Now().UnixMilli()
			rf.mu.Unlock()
			go rf.startElection()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) sendEntity(target int, logs []Entry, prevLogIndex int, prevLogTerm int) {
	rf.mu.Lock()
	forTerm := rf.currentTerm
	UUID := getUUID()
	request := AppendEntriesArgs{
		Term:         forTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Logs:         logs,
		LeaderCommit: rf.commitIndex,
		UUID:         UUID,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(target, &request, &reply)
	if ok {
		rf.mu.Lock()
		forTerm = rf.currentTerm
		if reply.Term > forTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.lastLeaderTime = time.Now().UnixMilli()
		} else if reply.UUID == UUID && rf.state == Leader {
			if reply.Success {
				if rf.matchIndex[target] < prevLogIndex {
					rf.matchIndex[target] = prevLogIndex
				}
				if rf.nextIndex[target] < prevLogIndex+len(logs)+1 {
					rf.nextIndex[target] = prevLogIndex + len(logs) + 1
				}
			} else {
				if rf.nextIndex[target] > prevLogIndex-1 {
					rf.nextIndex[target] = prevLogIndex - 1
					log.Printf("node %d next change %v", rf.me, rf.nextIndex)
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) listenSendHeart() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		me := rf.me
		prevLogIndex := len(rf.log) - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		rf.mu.Unlock()
		if state == Leader {
			for i := 0; i < len(rf.peers); i++ {
				if i != me {
					go rf.sendEntity(i, []Entry{}, prevLogIndex, prevLogTerm)
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (rf *Raft) listenAgreementEntry(target int) {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		prevLogIndex := rf.nextIndex[target] - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		logs := rf.log[prevLogIndex+1:]
		rf.mu.Unlock()
		if state != Leader {
			return
		}
		if len(logs) != 0 {
			go rf.sendEntity(target, logs, prevLogIndex, prevLogTerm)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) listenMajorCopy() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		if state != Leader {
			rf.mu.Unlock()
			return
		}
		cnt := make(map[int]int)
		cnt[len(rf.log)] = 1
		targetNum := int((len(rf.peers) + 1) / 2)
		maxIndex := 0
		for i := 0; i < len(rf.log); i++ {
			if i != rf.me {
				_, ok := cnt[rf.matchIndex[i]]
				if ok {
					cnt[rf.matchIndex[i]]++
				} else {
					cnt[rf.matchIndex[i]] = 1
				}
			}
		}
		for index, cnt := range cnt {
			if cnt >= targetNum && index > maxIndex {
				maxIndex = index
			}
		}
		rf.commitIndex = maxIndex
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) listenCommitApply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			*rf.applyCh <- applyMsg
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(20 * time.Millisecond)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.log = append(rf.log, Entry{term, command})

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

	rf.state = Follower
	rf.log = append(rf.log, Entry{0, nil})
	rand.NewSource(time.Now().UnixNano())
	rf.electionTimeout = 200 + (rf.me+1)*400/len(rf.peers)
	rf.commitIndex = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.lastLeaderTime = time.Now().UnixMilli()
	rf.applyCh = &applyCh

	// Your initialization code here (2A, 2B, 2C).
	go rf.listenElection()
	go rf.listenCommitApply()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
