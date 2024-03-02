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
	"sync"
	"sync/atomic"
	"time"
)

const (
	NO_LEADER_GAP        int64 = 500 // 超过该时间无心跳，进入canditate状态开启选举
	LEADER_HEART_GAP     int64 = 200 // Leader发送心跳的时间间隔
	ELECTION_RANDOM_GAP  int   = 150 // 开启选举前等待的最大时间间隔
	NET_RETRY_GAP        int64 = 10  // 网络超时时间
	STATE_CHECK_GAP      int64 = 10  // 选举是否成功检查间隔
	ELECTION_TIMEOUT_GAP int64 = 800 // 选举超时时间
)

// 获取当前毫秒数
func getTimeNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

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
	applyCh *chan ApplyMsg
	// 所有服务器都有的持久的状态
	currentTerm  int
	votedFor     int
	VotedForTerm int
	Entries      []Entry

	leaderId int
	// 1 follower 2 canadiate 3 leader
	peerKind int

	// 所有服务器都有的易变的状态
	commitIndex int
	lastApplied int

	lastLeaderTime int64 // 上次收到心跳或者appendEntity的时间

	// leader才有的状态
	nextIndex  map[int]int
	matchIndex map[int]int
	startMutex sync.Mutex
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
	// log.Printf("node %d get vote request from %d with term %d, my term is %d and my votefor is %d",
	//	rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	/* 需要投票的情况
	** 1.投票者任期比当前节点大，且没有投别的候选人
	** 这导致对一个任期只能投一次票，两个节点同时超时就会导致一起进入选举状态立即投票给自己，进而不能给其他节点投票
	** 改进措施：2.一旦选举期收到任期更大的请求则立即投票
	 */
	if l := len(rf.Entries); ((args.Term > rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) ||
		(rf.peerKind == 2 && rf.VotedForTerm < args.Term)) &&
		(l <= args.LastLogIndex && rf.Entries[l-1].Term <= args.LastLogTerm) {
		reply.VoteGranted = 1
		rf.peerKind = 1
		rf.VotedForTerm = args.Term
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = 0
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendApplyMsg(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex {
		log.Panicf("node %d need send apply msg out of index %d", rf.me, index)
	}
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.Entries[index].Command,
		CommandIndex: index,
	}
	*rf.applyCh <- applyMsg
}

func (rf *Raft) AppendEntity(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("2222 node %d get append entity request from %d", rf.me, args.LeaderId)
	rf.mu.Lock()
	log.Printf("node %d get append entity request from %d", rf.me, args.LeaderId)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.PrevLogIndex > len(rf.Entries)-1 { // 忽略过期的消息
		rf.mu.Unlock()
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.peerKind = 1
		rf.votedFor = -1
		if len(rf.Entries)-1 >= args.PrevLogIndex && args.PrevLogIndex != -1 &&
			rf.Entries[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.mu.Unlock()
			reply.Success = false
			// 截断不匹配的所有日志
			rf.Entries = rf.Entries[:args.PrevLogIndex]
			return
		}
		reply.Success = true
		if args.LeaderCommit > rf.commitIndex {
			lastCommitIndex := rf.commitIndex
			if args.PrevLogIndex+1 < args.LeaderCommit {
				rf.commitIndex = args.PrevLogIndex + 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			for i := lastCommitIndex + 1; i <= rf.commitIndex; i++ {
				go rf.sendApplyMsg(i)
			}
			// log.Printf("node %d need update commitindex, %v, my commitindex is %d, my entry is %v",
			// 	rf.me, args, rf.commitIndex, rf.Entries)
		}
		if len(args.Entries) == 0 { // 心跳包
			// log.Printf("node %d get heart from %d, my term is %d and heart term is %d, my kind is %d",
			//	rf.me, args.LeaderId, rf.currentTerm, args.Term, rf.peerKind)
			rf.currentTerm = args.Term
			rf.lastLeaderTime = getTimeNowMs() // 更新最后一次收到的时间
			rf.peerKind = 1
			rf.votedFor = -1
			log.Printf("node %d get heart %v", rf.me, args)
			rf.mu.Unlock()
			return
		} else {
			log.Printf("node %d get entity from %d", rf.me, args.LeaderId)
		}
		// 需要新增
		if len(rf.Entries)-1 == args.PrevLogIndex {
			rf.Entries = append(rf.Entries, args.Entries...)
			// log.Printf("node %d add entry, mu entrys is %v", rf.me, rf.Entries)
		}
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

// 使得某个节点和自己同步
func (rf *Raft) agreementNode(target int) {
	// 尝试从nextInt[target]复制到领导的最后一条日志
	rf.mu.Lock()
	targetIndex := rf.nextIndex[target]
	entryLen := len(rf.Entries)
	peerKind := rf.peerKind
	rf.mu.Unlock()
	for !rf.killed() && targetIndex != entryLen && peerKind == 3 {
		rf.mu.Lock()
		preTerm := -1
		if targetIndex != 0 {
			preTerm = rf.Entries[targetIndex-1].Term
		}
		rf.mu.Unlock()
		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: targetIndex - 1,
			PrevLogTerm:  preTerm,
			Entries:      rf.Entries[targetIndex : targetIndex+1],
			LeaderCommit: rf.commitIndex,
		}
		rep := AppendEntriesReply{}
		log.Printf("node %d try send %d index to %d", rf.me, targetIndex, target)
		ok := rf.sendAppendEntity(target, &req, &rep)
		if ok {
			log.Printf("node %d sent %d index to %d net success", rf.me, targetIndex, target)
			if rep.Success {
				log.Printf("node %d sent %d index to %d success", rf.me, targetIndex, target)
				rf.mu.Lock()
				rf.nextIndex[target] = targetIndex + 1
				rf.matchIndex[target] = targetIndex
				rf.mu.Unlock()
				return
			} else {
				log.Printf("node %d sent %d index to %d failed", rf.me, targetIndex, target)
				rf.mu.Lock()
				if rep.Term > rf.currentTerm {
					rf.currentTerm = rep.Term
					rf.peerKind = 1
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				} else {
					rf.nextIndex[target]--
					rf.mu.Unlock()
				}
			}
		} else {
			log.Printf("node %d send %d index to %d failed", rf.me, targetIndex, target)
		}
		rf.mu.Lock()
		targetIndex = rf.nextIndex[target]
		entryLen = len(rf.Entries)
		peerKind = rf.peerKind
		rf.mu.Unlock()
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
	rf.startMutex.Lock()
	defer rf.startMutex.Unlock()
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	if !rf.killed() {
		rf.mu.Lock()
		term = rf.currentTerm
		index = rf.nextIndex[rf.me]
		if rf.peerKind != 3 {
			isLeader = false
			rf.mu.Unlock()
			return index, term, isLeader
		}
		log.Printf("node %d is leader for start", rf.me)
		isLeader = true
		rf.Entries = append(rf.Entries, Entry{-1, nil})
		rf.Entries[index].Term = term
		rf.Entries[index].Command = command
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	// 如果不提取缓存当前的Num，将导致在有成员退出后依然访问导致越界
	peerNum := len(rf.peers)
	rf.mu.Unlock()
	for i := 0; i < peerNum; i++ {
		if i != rf.me {
			go rf.agreementNode(i)
		}
	}

	// 检测是否超过半数的节点已经完成了复制
	cnt := 0
	for !rf.killed() {
		time.Sleep(time.Millisecond * time.Duration(STATE_CHECK_GAP))
		cnt++
		if cnt%50 == 0 {
			log.Printf("check %d times", cnt)
		}
		rf.mu.Lock()
		needWaitNum := int((len(rf.peers) + 1) / 2)
		agreeNum := 1
		peerKind := rf.peerKind
		peerSize := len(rf.peers)
		for i := 0; i < peerSize; i++ {
			if rf.matchIndex[i] >= index {
				agreeNum++
			}
			if cnt%50 == 0 {
				log.Printf("check %d times", cnt)
				log.Printf("%d match index is %d, need is %d , agree num is %d",
					i, rf.matchIndex[i], index, agreeNum)

			}
		}
		if peerKind != 3 {
			rf.mu.Unlock()
			return index, term, false
		} else {
			isLeader = true
		}
		rf.mu.Unlock()
		if agreeNum >= needWaitNum {
			rf.mu.Lock()
			rf.commitIndex = index
			go rf.sendApplyMsg(index)
			rf.nextIndex[rf.me] = index + 1
			rf.mu.Unlock()
			return index, term, isLeader
		}
	}
	return index, term, false
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

	// defer log.Printf("node %d end election for term %d", rf.me, rf.currentTerm)
	// 等待0-300ms随机时间后开启选举
	rand.NewSource(time.Now().UnixNano())
	electionWaitTime := rand.Intn(ELECTION_RANDOM_GAP)

	rf.mu.Lock()
	// log.Printf("%d start current term %d election wait time %d", rf.me, rf.currentTerm, electionWaitTime)
	electionTerm := rf.currentTerm // 缓存当前竞选的任期
	// 如果有其他节点已经开启选举任期更高的选举，那么开启比其低的选举可能毫无意义
	if electionTerm < rf.VotedForTerm {
		electionTerm = rf.VotedForTerm
	}
	lastLeaderTime := rf.lastLeaderTime // 选举开始前要是有更新lastLeader表示已经再收到心跳
	rf.votedFor = -1
	rf.mu.Unlock()

	time.Sleep(time.Duration(electionWaitTime) * time.Millisecond)

	rf.mu.Lock()
	// 表明重受到过心跳，此时不应开启选举
	if rf.lastLeaderTime != lastLeaderTime {
		rf.mu.Unlock()
		return
	}
	rf.VotedForTerm = electionTerm + 1
	rf.peerKind = 2
	l := len(rf.Entries)
	lastLogTerm := rf.Entries[l-1].Term
	rf.votedFor = rf.me
	needWaitNum := int((len(rf.peers) + 1) / 2)
	agreeNum := 1
	agreeNumMu := sync.Mutex{}
	peerNum := len(rf.peers)
	startTime := getTimeNowMs()
	log.Printf("node %d start term %d election", rf.me, rf.currentTerm)

	rf.mu.Unlock()

	for i := 0; i < peerNum && !rf.killed(); i++ {
		if i != rf.me {
			// 多线程向其他节点发送选举请求
			go func(i int, startTime int64) {
				// defer log.Printf("node %d exit send %d", rf.me, i)
				// log.Printf("node %d send vote request to %d for term %d", rf.me, i, rf.VotedForTerm)
				for !rf.killed() {

					rf.mu.Lock()
					req := RequestVoteArgs{
						Term:         rf.VotedForTerm,
						CandidateId:  rf.me,
						LastLogIndex: l,
						LastLogTerm:  lastLogTerm,
					}
					rep := RequestVoteReply{}
					rf.mu.Unlock()

					ok := rf.sendRequestVote(i, &req, &rep)
					if ok {
						// log.Printf("node %d get term %d election, node %d reply %d", rf.me, rf.VotedForTerm, i, rep.VoteGranted)
						rf.mu.Lock()
						if rep.VoteGranted == 1 {
							agreeNumMu.Lock()
							agreeNum += 1
							agreeNumMu.Unlock()
						} else if rep.Term > rf.VotedForTerm {
							rf.currentTerm = rep.Term
							rf.peerKind = 1
						}
						rf.mu.Unlock()
						break
					} else {
						// log.Printf("node %d to %d vote request for term %d is send fail", rf.me, rf.VotedForTerm, i)
						rf.mu.Lock()
						if rf.peerKind != 2 {
							// log.Printf("node %d election exit with peer kind %d", rf.me, rf.peerKind)
							rf.mu.Unlock()
							break
						} else {
							rf.mu.Unlock()
						}
					}
					// 选举超时结束进程
					if getTimeNowMs()-startTime > ELECTION_TIMEOUT_GAP {
						// log.Printf("node %d term %d election timeout", rf.me, rf.VotedForTerm)
						break
					}
					// 需要重发时等待10ms
					time.Sleep(time.Duration(NET_RETRY_GAP) * time.Millisecond)
				}
			}(i, startTime)
		}
	}

	// 检测选举状态
	for !rf.killed() {
		agreeNumMu.Lock()
		// 选举成功
		if agreeNum >= needWaitNum {
			rf.mu.Lock()
			rf.peerKind = 3
			rf.currentTerm = rf.VotedForTerm
			log.Printf("node %d term %d election success", rf.me, rf.currentTerm)
			// log.Printf("node %d term %d election success", rf.me, rf.currentTerm)
			entrySize, peerSize := len(rf.Entries), len(rf.peers)
			rf.nextIndex = make(map[int]int)
			rf.matchIndex = make(map[int]int)
			for i := 0; i < peerSize; i++ {
				rf.nextIndex[i] = entrySize
				rf.matchIndex[i] = 0
			}
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
			time.Sleep(time.Duration(STATE_CHECK_GAP) * time.Millisecond)

			if getTimeNowMs()-startTime > ELECTION_TIMEOUT_GAP {
				rf.mu.Lock()
				// log.Printf("check know node %d term %d election timeout and kind is %d", rf.me, rf.VotedForTerm,
				// 	rf.peerKind)
				if rf.peerKind != 2 {
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				} else {
					rf.mu.Unlock()
					defer rf.startElection()
				}
				break
			}
		}
	}
}

// Make调用后负责监听选举相关的身份功能
func (rf *Raft) listenElection() {
	for !rf.killed() {
		rf.mu.Lock()
		peerKind := rf.peerKind
		lastLeaderTime := rf.lastLeaderTime
		rf.mu.Unlock()
		if peerKind == 1 {
			timeNow := getTimeNowMs()
			if timeNow-lastLeaderTime >= NO_LEADER_GAP {
				// 开启选举
				peerKind = 2
				// log.Printf("time out, %d start election", rf.me)
				rf.startElection()
			}
		}
		// 重新获取当前自己身份
		rf.mu.Lock()
		peerKind = rf.peerKind
		rf.mu.Unlock()
		if peerKind == 3 {
			// log.Printf("node %d start term %d heart", rf.me, rf.currentTerm)
			rf.mu.Lock()
			// 如果不提取缓存当前的Num，将导致在有成员退出后依然访问导致越界
			peerNum := len(rf.peers)
			rf.mu.Unlock()
			for i := 0; i < peerNum; i++ {
				if i != rf.me {
					go func(i int) {
						rf.mu.Lock()
						l := len(rf.Entries)
						appendEntriesArgs := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: l - 1,
							PrevLogTerm:  rf.Entries[l-1].Term,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}
						if l == 0 {
							appendEntriesArgs.PrevLogTerm = 0
						} else {
							appendEntriesArgs.PrevLogTerm = rf.Entries[l-1].Term
						}
						rf.mu.Unlock()

						appendEntriesReply := AppendEntriesReply{}
						log.Printf("leader %d send heart to %d", rf.me, i)
						ok := rf.sendAppendEntity(i, &appendEntriesArgs, &appendEntriesReply)
						if ok {
							log.Printf("node %d send heart to %d, content is %v", rf.me, i, appendEntriesArgs)
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
			// 200ms发一次心跳
			time.Sleep(time.Duration(LEADER_HEART_GAP) * time.Millisecond)
		}
		time.Sleep(time.Millisecond * 100) // 100ms检测一次
	}
}

// 检测 commitIndex和lastApplied 是否一致
func (rf *Raft) listenApplied() {
	for !rf.killed() {
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		if lastApplied != rf.commitIndex {
			for i := lastApplied; i <= rf.commitIndex; i++ {
				// 执行apply entry
				lastApplied = i
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 1000) // 1000ms检测一次
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
	rf.commitIndex = 0
	rf.Entries = append(rf.Entries, Entry{0, 0})
	rf.applyCh = &applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.peerKind = 1                    // 初始化是follower
	rf.lastLeaderTime = getTimeNowMs() // 初始化开启选举定时器
	go rf.listenElection()
	go rf.listenApplied()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
