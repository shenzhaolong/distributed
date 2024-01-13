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
	"6824/labrpc"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var NoLeaderTime int64 = 2     //s,超过该时间无心跳，进入canditate状态开启选举
var LeaderHeartTime int = 1000 //ms,每一段时间发送心跳

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

type Entry struct {
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 所有服务器都有的持久的状态
	currentTerm int
	votedFor    int
	Entries     []Entry

	leaderId int
	// 1 follower 2 canadiate 3 leader
	peerKind int

	// 所有服务器都有的易变的状态
	commitIndex int
	lastApplied int

	lastLeaderTime int64 // 上次收到心跳或者appendEntity的时间

	// leader才有的状态
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()

	term = rf.currentTerm
	if rf.peerKind == 3 {
		isleader = true
	} else {
		isleader = false
	}

	rf.mu.Unlock()
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
	Term         int // 候选者的任期号
	CandidateId  int // 候选者的服务器编号
	LastLogIndex int // 候选者的最后一个日志条目的索引号
	LastLogTerm  int // 候选者的最后一个日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // 返回的当前的任期号
	VoteGranted int // 是否同意投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Println("node " + strconv.Itoa(rf.me) + " get RequestVote from " + strconv.Itoa(args.CandidateId) +
		" with term " + strconv.Itoa(args.Term) + ", my term:" + strconv.Itoa(rf.currentTerm) + " and my votedfor is " +
		strconv.Itoa(rf.votedFor))
	if l := len(rf.Entries); args.Term > rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(l == 0 || (l <= args.LastLogIndex && rf.Entries[l-1].Term <= args.LastLogTerm)) {
		reply.VoteGranted = 1
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = 0
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntity(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 { // 心跳包
		reply.Success = true
		rf.mu.Lock()
		log.Println("node " + strconv.Itoa(rf.me) + " get heart from " + strconv.Itoa(args.LeaderId) +
			", my term is " + strconv.Itoa(rf.currentTerm) + " and heart term is " + strconv.Itoa(args.Term) +
			", my peerkind is " + strconv.Itoa(rf.peerKind))
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.lastLeaderTime = time.Now().Unix() // 更新最后一次收到的时间
			rf.peerKind = 1
			rf.votedFor = -1
		} else if args.Term == rf.currentTerm {
			rf.lastLeaderTime = time.Now().Unix() // 更新最后一次收到的时间
		}
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
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

func (rf *Raft) sendAppendEntity(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntity", args, reply)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {

	defer log.Printf("node %d end election for term %d", rf.me, rf.currentTerm)

	// 等待0-500ms随机时间后开启选举
	rand.NewSource(time.Now().UnixNano())
	electionWaitTime := rand.Intn(200)
	rf.mu.Lock()
	log.Println(strconv.Itoa(rf.me) + " start current term " + strconv.Itoa(rf.currentTerm) + " election wait time " + strconv.Itoa(electionWaitTime))
	electionTerm := rf.currentTerm // 缓存当前竞选的任期
	rf.votedFor = -1
	rf.mu.Unlock()
	time.Sleep(time.Duration(electionWaitTime) * time.Millisecond)

	rf.mu.Lock()

	rf.currentTerm = electionTerm + 1
	rf.peerKind = 2
	l := len(rf.Entries)
	lastLogTerm := 0
	if l > 0 {
		lastLogTerm = rf.Entries[l-1].Term
	}
	rf.votedFor = rf.me
	needWaitNum := int((len(rf.peers) + 1) / 2)
	agreeNum := 1
	agreeNumMu := sync.Mutex{}
	peerNum := len(rf.peers)
	startTime := time.Now().Unix()
	log.Println("node " + strconv.Itoa(rf.me) + " start term " + strconv.Itoa(rf.currentTerm) + " election")

	rf.mu.Unlock()

	for i := 0; i < peerNum && !rf.killed(); i++ {
		if i != rf.me {
			go func(i int, startTime int64) {
				defer log.Printf("node %d exit send %d", rf.me, i)
				log.Println("node " + strconv.Itoa(rf.me) + " send RequestVote to " + strconv.Itoa(i) + " term " + strconv.Itoa(rf.currentTerm))
				for !rf.killed() {
					req := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: l - 1,
						LastLogTerm:  lastLogTerm,
					}
					rep := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &req, &rep)
					if ok {
						log.Println("node " + strconv.Itoa(rf.me) + " get term " + strconv.Itoa(rf.currentTerm) + ", node " +
							strconv.Itoa(i) + " reply:" + strconv.Itoa(rep.VoteGranted))
						if rep.VoteGranted == 1 {
							agreeNumMu.Lock()
							agreeNum += 1
							agreeNumMu.Unlock()
							log.Println("node " + strconv.Itoa(rf.me) + " get term " + strconv.Itoa(rf.currentTerm) + ", agree " + strconv.Itoa(i))
						} else if rep.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.currentTerm = rep.Term
							rf.peerKind = 1
							rf.mu.Unlock()
						}
						break
					} else {
						log.Println("node " + strconv.Itoa(rf.me) + " term " + strconv.Itoa(rf.currentTerm) + ", network from " + strconv.Itoa(i))
						rf.mu.Lock()
						if rf.peerKind != 2 {
							log.Printf("node %d election exit with peer kind %d", rf.me, rf.peerKind)
							rf.mu.Unlock()
							break
						} else {
							rf.mu.Unlock()
						}
					}
					// 选举超时结束进程
					if time.Now().Unix()-startTime > 4 {
						log.Println("node " + strconv.Itoa(rf.me) + " term" + strconv.Itoa(rf.currentTerm) + " election timeout")
						break
					}
					// 需要重发时等待10ms
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
			}(i, startTime)
		}
	}

	for !rf.killed() {
		agreeNumMu.Lock()
		if agreeNum >= needWaitNum {
			rf.mu.Lock()
			rf.peerKind = 3
			log.Println("node " + strconv.Itoa(rf.me) + " get term" + strconv.Itoa(rf.currentTerm) + " success")
			rf.mu.Unlock()
			agreeNumMu.Unlock()
			return
		} else {
			rf.mu.Lock()
			if rf.peerKind != 2 {
				rf.mu.Unlock()
				agreeNumMu.Unlock()
				return
			} else {
				rf.mu.Unlock()
			}
			agreeNumMu.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond)

			if time.Now().Unix()-startTime > 4 {
				rf.mu.Lock()
				log.Println("check know node " + strconv.Itoa(rf.me) + " term" + strconv.Itoa(rf.currentTerm) + " election timeout and my kind is " +
					strconv.Itoa(rf.peerKind))
				if rf.peerKind != 2 {
					log.Printf("node %d end election for term %d", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				} else {
					rf.mu.Unlock()
					rf.startElection()
				}
				break
			}
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
	rf.currentTerm = 1
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).
	rf.peerKind = 1                       // 初始化是follower
	rf.lastLeaderTime = time.Now().Unix() // 初始化开启选举定时器
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			peerKind := rf.peerKind
			lastLeaderTime := rf.lastLeaderTime
			// fmt.Println("start len: " + strconv.Itoa(peerNum) + " " + strconv.Itoa(me))
			rf.mu.Unlock()
			if peerKind == 1 {
				timeNow := time.Now().Unix()
				log.Printf("node %d check heart timeout, timeNow is %d, lastLeaderTime is %d", rf.me, timeNow, lastLeaderTime)
				if timeNow-lastLeaderTime >= NoLeaderTime {
					// 开启选举
					peerKind = 2
					log.Println("time out, start election, " + strconv.Itoa(me))
				}
			}
			if peerKind == 2 {
				rf.startElection()
			}
			rf.mu.Lock()
			peerKind = rf.peerKind
			// fmt.Println("start len: " + strconv.Itoa(peerNum) + " " + strconv.Itoa(me))
			rf.mu.Unlock()
			if peerKind == 3 {
				log.Println("node " + strconv.Itoa(rf.me) + " start term " + strconv.Itoa(rf.currentTerm) +
					" heart")
				rf.mu.Lock()
				peerNum := len(rf.peers)
				rf.mu.Unlock()
				for i := 0; i < peerNum; i++ {
					if i != me {
						rf.mu.Lock()
						var l int = len(rf.Entries)
						rf.mu.Unlock()
						go func(i int) {
							appendEntriesArgs := AppendEntriesArgs{
								Term:         rf.currentTerm,
								LeaderId:     me,
								PrevLogIndex: l - 1,
								Entries:      nil,
								LeaderCommit: rf.commitIndex,
							}
							if l == 0 {
								appendEntriesArgs.PrevLogTerm = 0
							} else {
								appendEntriesArgs.PrevLogTerm = rf.Entries[l-1].Term
							}
							appendEntriesReply := AppendEntriesReply{}
							log.Println("leader Id: " + strconv.Itoa(me) + " send heart to " + strconv.Itoa(i))

							ok := rf.sendAppendEntity(i, &appendEntriesArgs, &appendEntriesReply)
							if ok {
								rf.mu.Lock()
								if rf.currentTerm < appendEntriesReply.Term {
									rf.currentTerm = appendEntriesReply.Term
									rf.peerKind = 1
								}
								rf.mu.Unlock()
							} else {
								log.Printf("Leader %d send to %d heart fail", rf.me, i)
							}
						}(i)
					}
				}
				time.Sleep(time.Millisecond * time.Duration(LeaderHeartTime))
			}
			time.Sleep(time.Millisecond * 100) // 100ms检测一次
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
