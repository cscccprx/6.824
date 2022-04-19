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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"../labgob"
	"../labrpc"
)

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

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// 相当于java 中的 toString()
func (log *LogEntry) String() string {
	return fmt.Sprintf("{Term:%d,Index:%d,Command:%v}", log.Term, log.Index, log.Command)
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int
	// index of peer voted
	votedFor int
	log      []*LogEntry
	// all server volatile state
	commitIndex int
	lastApplied int
	// leader volatile state
	nextIndex  []int
	matchIndex []int
	// defined by yourself
	currentState RaftState
	electTimeout time.Time
	// 提交状态机的channal
	applyChan chan ApplyMsg
	// 引入cond 作为通知机制，apply 不能一直循环跑，一个是浪费cpu，另一个原因是会频繁的拿锁并且释放锁，会有影响，类似lab 2a
	applyCond *sync.Cond
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type RaftState int

const (
	FOLLOWER RaftState = iota
	CANDIDATE
	LEADER
)

func (t RaftState) raftStateToString() string {

	switch t {
	case FOLLOWER:
		return "follower"
	case CANDIDATE:
		return "candinate"
	case LEADER:
		return "leader"
	}
	return ""

}

// return currentTerm and whether this server
// believes it is the leader.
// 主要用于test拿到当前节点的状态 当前term 是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	rf.mu.Lock()
	//log.Printf("%v get lock in Getstate", rf.me)
	if rf.currentState == LEADER {
		//
		isleader = true
	}
	term = rf.currentTerm

	//log.Printf("%v release lock in Getstate", rf.me)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logs []*LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		log.Printf("read persist fail")
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = logs
	}
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
// 心跳周期
const (
	HEART_SLEEP time.Duration = 50 * time.Millisecond
)

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

// append log request
type RequestAppendLogArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

// append log reply
type RequestAppendLogReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	ConflictFlag  bool
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

// TODO
// raft 保证当前成为leader的前提是 拥有的日志都是已经提交的?
// 这个我理解是不正确的，类似于Figure 8， 只有当成为了leader 并apply了当前任期的日志才能说明这个问题
func (rf *Raft) AppendLog(args *RequestAppendLogArgs, reply *RequestAppendLogReply) {
	// Your code here (2A, 2B).
	// elect
	// all server do check
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	reply.ConflictFlag = false
	// rule for all servers 2
	if args.Term > rf.currentTerm {
		// reply false if term < currentTerm
		rf.currentState = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	if args.Term < rf.currentTerm {
		//log.Printf("%v get append entry %+v from S%v,and args.term < current term,false", rf.logBaseString(), args, args.LeaderId)
		rf.persist()
		return
	}

	rf.resetElectionTime()
	// candidate rule 3
	if rf.currentState == CANDIDATE {
		rf.currentState = FOLLOWER
	}

	// 防止没有这个日志 数组下标溢出  follower 日志中 没有这个日志
	if rf.getTailLogEntry().Index < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.ConflictFlag = true
		rf.persist()
		return
	}
	// append entries rpc rule 2
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for index := 1; index <= rf.getTailLogEntry().Index; index++ {
			if rf.log[index].Term == rf.log[args.PrevLogIndex].Term {
				// find first entry that term = args.PrevLogIndex
				reply.ConflictIndex = index
				break
			}
		}
		reply.ConflictFlag = true
		rf.persist()
		return
	}

	for idx, entry := range args.Entries {
		// append entries rpc rule 3
		if entry.Index <= rf.getTailLogEntry().Index && rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		}
		// append entries rpc 4
		if entry.Index > rf.getTailLogEntry().Index {
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}
	rf.persist()
	// append entries rpc rule 5
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getTailLogEntry().Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getTailLogEntry().Index
		}
		// 通知apply 避免apply空转拿锁导致rpc延迟，不懂看最上面初始化注释！！
		rf.apply()
		log.Printf("%v reset commit Index as : %v", rf.logBaseString(), rf.commitIndex)
	}
	log.Printf("%v get append entry %+v from S%v", rf.logBaseString(), args, args.LeaderId)

	// 自己实现 前面基本正确，在处理日志处出现问题 呼应下面label TODO
	// 2B
	// AppendEntries RPC rule 2-3
	//if args.PrevLogIndex > len(rf.log)-1 {
	//	// local log don't exist this log that index = args.PrevLogIndex
	//	// false
	//	log.Printf("%v return false because args.PrevLogIndex { %v } > len(rf.log)-1{ %v }", rf.logBaseString(), args.PrevLogIndex, len(rf.log)-1)
	//	return
	//}
	//if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	// delete log[args.PrevLogIndex to end]
	//	// false
	//	log.Printf("%v return false because rf.log[args.PrevLogIndex].Term { %v } != args.PrevLogTerm{ %v }, need delete from args.PrevLogIndex ",
	//		rf.logBaseString(), rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
	//	log.Printf("%v current log %+v", rf.logBaseString(), rf.log)
	//	rf.log = rf.log[:args.PrevLogIndex]
	//	log.Printf("%v after delete current log %+v", rf.logBaseString(), rf.log)
	//	return
	//}
	//// AppendEntries RPC rule 4
	//// rf.log[args.PrevLogIndex].Term == args.PrevLogTerm  index and term start agreement on args.PrevLogIndex
	//// S0 1 2 2 3
	//// S1 1 2 2 3
	//// check if this log has been append rf.log
	//if args.Entries != nil && args.Entries[0].Index < len(rf.log) && args.Entries[0].Term == rf.log[args.Entries[0].Index].Term{
	//	log.Printf("%v current log : %+v , this log entry has existed, return false because args.Entries[0].Index { %v } < len(rf.log) { %v }. this entry %+v",
	//		rf.logBaseString(), rf.log, args.Entries[0].Index, len(rf.log), args.Entries)
	//	reply.Success = true
	//	return
	//}
	// TODO here false
	//// S0 1 2 2 3
	//// S1 1 2 2 4  ->  S1 1 2 2 3
	//if args.Entries != nil && args.Entries[0].Index < len(rf.log) && args.Entries[0].Term != rf.log[args.Entries[0].Index].Term {
	//	log.Printf("%v entry conflict. arg.Entry : %+v，current log : %+v", rf.logBaseString(), args.Entries, rf.log)
	//	rf.log = rf.log[:args.Entries[0].Index]
	//	log.Printf("%v entry conflict after fix. arg.Entry : %+v，current log : %+v", rf.logBaseString(), args.Entries, rf.log)
	//}
	//// append this log
	//
	//rf.log = append(rf.log, args.Entries...)
	//log.Printf("%v add log entries %+v, current log entry %+v", rf.logBaseString(), args.Entries, rf.log)

	reply.Success = true

}

func (rf *Raft) logBaseString() string {
	return "	S" + strconv.Itoa(rf.me) + ", Term : " + strconv.Itoa(rf.currentTerm) +
		", status : " + rf.currentState.raftStateToString() + ", vote for : " + strconv.Itoa(rf.votedFor) +
		", commit Index : " + strconv.Itoa(rf.commitIndex) + ", lastApplied : " + strconv.Itoa(rf.lastApplied) + "   |"
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// elect
	// all server do check
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rule for all servers 2
	if args.Term > rf.currentTerm {
		// reply false if term < currentTerm
		rf.currentState = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// request vote 1
	if args.Term < rf.currentTerm {
		// reply false if term < currentTerm
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}
	// request vote 2
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && asLeastUpdate(args, rf) {
		// vote
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTime()
		log.Printf("%v success vote for S%v ", rf.logBaseString(), strconv.Itoa(rf.votedFor))
	} else {
		log.Printf("%v fail vote for S%v", rf.logBaseString(), strconv.Itoa(args.CandidateId))
		reply.VoteGranted = false
	}
	log.Printf("%v release lock in requestVote args.Term >= rf.currentTerm", rf.me)
	reply.Term = rf.currentTerm
	rf.persist()
}

// 5.4 raft 特殊保证
func asLeastUpdate(args *RequestVoteArgs, rf *Raft) bool {
	curLogEntryIndex := len(rf.log) - 1
	res := rf.log[curLogEntryIndex].Term < args.LastLogTerm ||
		(rf.log[curLogEntryIndex].Term == args.LastLogTerm && curLogEntryIndex <= args.LastLogIndex)
	return res

}

// 使用sync.Cond wait/broadcast 机制避免apply 空转 导致频繁获得锁与释放锁，进而造成rpc延迟出现 test 偶尔通过，偶尔不过的情况
func (rf *Raft) applyMsg() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		flag := 1
		if rf.commitIndex > rf.lastApplied && rf.lastApplied < rf.getTailLogEntry().Index {
			log.Printf("%v commitIndex : %v , lastApplied : %v", rf.logBaseString(), rf.commitIndex, rf.lastApplied)
			flag = 0
			rf.lastApplied++
			applyMsg := ApplyMsg{
				// 应用到状态机
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyChan <- applyMsg
			//rf.mu.Lock()
			log.Printf("%v apply state machine ,   ----- ", rf.me)
		} else {
			rf.applyCond.Wait()
		}
		// 必须要有，否则没有unlock
		if flag == 1 {
			rf.mu.Unlock()
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

func (rf *Raft) sendRequestAppendLog(server int, args *RequestAppendLogArgs, reply *RequestAppendLogReply) bool {
	ok := rf.peers[server].Call("Raft.AppendLog", args, reply)
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
//func (rf *Raft) showLog()  {
//	var s string
//	for i := 0; i < len(rf.log); i++ {
//		s = s +
//	}
//}
// 用于test 写入一条log
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState != LEADER {
		isLeader = false
		return index, rf.currentTerm, isLeader
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.getTailLogEntry().Index + 1,
		Command: command,
	}
	rf.log = append(rf.log, &entry)
	rf.persist()
	log.Printf("%v start appending log %+v in start(), current log : %+v", rf.logBaseString(), entry, rf.log)
	index = entry.Index
	term = entry.Term
	rf.broadcastHeart(false)
	return index, term, isLeader
}

func (rf *Raft) getTailLogEntry() *LogEntry {
	return rf.log[len(rf.log)-1]
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
// 固定go routine 在心跳周期出发 election 或者 append rpc or heart beat
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 两次心跳的间隔是否超过选举的时间,
		//time.Sleep(200 * time.Millisecond)
		//time.Sleep(rf.electTimeout)
		time.Sleep(HEART_SLEEP)
		rf.mu.Lock()
		log.Printf("%v sleep %v", rf.logBaseString(), HEART_SLEEP)
		//log.Printf("%v get lock elect ticker", rf.me)
		if rf.currentState == LEADER {
			rf.broadcastHeart(true)
		}
		if time.Now().After(rf.electTimeout) {
			rf.electVote()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) electVote() {

	// 1. start election
	// 1.1 add currentTerm
	// 1.2 vote self
	// 1.3 reset rf time
	// 1.4 parallel send vote rpc
	rf.resetElectionTime()
	rf.currentState = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.LastLogTerm = rf.getTailLogEntry().Term
	args.LastLogIndex = len(rf.log) - 1
	args.CandidateId = rf.me
	log.Printf("%v start elect", rf.logBaseString())
	peersLen := len(rf.peers)
	vote := 1
	var becomeLeader sync.Once
	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			continue
		}
		go func(peerId int, args RequestVoteArgs, becomeLeader *sync.Once) {
			reply := RequestVoteReply{}
			log.Printf("%v send vote rpc to S%v", strconv.Itoa(rf.me), strconv.Itoa(peerId))
			//defer wg.Done()
			ok := rf.sendRequestVote(peerId, &args, &reply)
			if !ok {
				log.Printf("%v send vote rpc fail to S%v, because S%v disconnect ", strconv.Itoa(rf.me), strconv.Itoa(peerId), strconv.Itoa(peerId))
				//wg.Done()
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//log.Printf("%v get lock in elect func inner", rf.me)
			log.Printf("%v return %+v to S%v", strconv.Itoa(rf.me), reply, strconv.Itoa(peerId))
			// rule for all servers 2
			if reply.Term > rf.currentTerm {
				log.Printf("%v reply %+v , term > current term , trans to follower", rf.logBaseString(), reply)
				rf.currentState = FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
				//log.Printf("%v release lock in elect func reply.Term > rf.currentTerm", rf.me)
				return
			}
			if !reply.VoteGranted {
				return
			}
			// candidate rule 3
			if rf.currentState != CANDIDATE || rf.currentTerm != args.Term {
				rf.votedFor = -1
				rf.persist()
				return
			}
			// TODO 指针变量➕1 是线程安全，查阅资料 直接操作地址了
			// candidate rule 4
			vote += 1
			// vote is more than half of servers
			// sync.Once : running one time  <==> flag
			if (vote) > peersLen/2 {
				becomeLeader.Do(func() {
					log.Printf("%v get ticket num : %v, success vote trans to leader, leader is S%v, current term : %v",
						rf.logBaseString(), strconv.Itoa(vote), strconv.Itoa(rf.me), strconv.Itoa(rf.currentTerm))
					rf.currentState = LEADER
					rf.persist()
					//log.Printf("%v release lock in elect func reply.VoteGranted", rf.me)
					rf.becomingLeaderToDo(&args, &reply)
				})

			}
		}(i, args, &becomeLeader)
	}
}

// leader volatile state
func (rf *Raft) becomingLeaderToDo(args *RequestVoteArgs, reply *RequestVoteReply) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getTailLogEntry().Index + 1
		rf.matchIndex[i] = 0
	}
	rf.broadcastHeart(true)
}

// 做心跳的时候也要把差距的log entry带过去，而提交的规则是figure 8
func (rf *Raft) broadcastHeart(isHeart bool) {
	//for rf.killed() == false {
	//rf.mu.Lock()
	log.Printf("%v is leader, now send append log ( %v )", rf.logBaseString(), isHeart)
	// send heart break
	peers := len(rf.peers)
	for i := 0; i < peers; i++ {
		if i != rf.me {
			args := RequestAppendLogArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			// 2B
			if rf.getTailLogEntry().Index >= rf.nextIndex[i] || isHeart {
				// leader rule 3
				nextIndex := rf.nextIndex[i]
				if rf.nextIndex[i] <= 0 {
					nextIndex = 1
				}
				if rf.getTailLogEntry().Index+1 < nextIndex {
					nextIndex = rf.getTailLogEntry().Index
				}
				args.PrevLogIndex = nextIndex - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = make([]*LogEntry, rf.getTailLogEntry().Index-nextIndex+1)
				// nextIndex -> end
				log.Printf("%v current log : %+v", rf.logBaseString(), rf.log)
				copy(args.Entries, rf.log[nextIndex:])
				log.Printf("%v send log%+v start nextIndex to [S%v]: %v", rf.logBaseString(), args.Entries, i, nextIndex)
			}
			go func(peerId int, args RequestAppendLogArgs) {

				reply := RequestAppendLogReply{}
				log.Printf("%v send append log（ %v ） rpc %+v to S%v", rf.me, isHeart, args, strconv.Itoa(peerId))
				ok := rf.sendRequestAppendLog(peerId, &args, &reply)
				if !ok {
					log.Printf("%v send append log fail to S%v", strconv.Itoa(rf.me), strconv.Itoa(peerId))
					return
				}
				log.Printf("%v heart return %+v to S%v", strconv.Itoa(rf.me), reply, strconv.Itoa(peerId))
				rf.mu.Lock()
				//log.Printf("%v get lock in heart beat inner", rf.me)
				if reply.Term > rf.currentTerm {
					log.Printf("%v heart rpc reply %+v , term > current term , trans to follower from S%v", rf.logBaseString(), reply, peerId)
					rf.currentState = FOLLOWER
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()
				} else {
					// heart beat don't care
					// reply is legal
					if reply.Success {
						// 不能这样写，因为nextIndex 在rpc的过程中可能有改动，需要用两个不变的量 也就是 args.PrevLogIndex + len(args.Entries)
						//rf.nextIndex[peerId] = rf.nextIndex[peerId] + rf.getTailLogEntry().Index - rf.nextIndex[peerId] + 1
						//rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
						match := args.PrevLogIndex + len(args.Entries)
						next := match + 1
						rf.nextIndex[peerId] = max(rf.nextIndex[peerId], next)
						rf.matchIndex[peerId] = max(rf.matchIndex[peerId], match)
						//rf.matchIndex[peerId] = match
						//rf.nextIndex[peerId] = next
						log.Printf("%v send log reply success nextIndex[S%v]: %v, matchIndex[S%v] : %v", rf.logBaseString(), peerId, rf.nextIndex[peerId], peerId, rf.matchIndex[peerId])
					} else {
						if reply.ConflictFlag {
							flagLog := false
							for i := rf.getTailLogEntry().Index; i >= 1; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									flagLog = true
									rf.nextIndex[peerId] = i + 1
									break
								}
							}
							if !flagLog {
								rf.nextIndex[peerId] = reply.ConflictIndex
							}
							log.Printf("%v send log reply fail when conflict nextIndex[S%v]: %v, matchIndex[S%v] : %v", rf.logBaseString(), peerId, rf.nextIndex[peerId], peerId, rf.matchIndex[peerId])
						} else {
							rf.nextIndex[peerId] = rf.nextIndex[peerId] - 1
							log.Printf("%v send log reply fail no conflict nextIndex[S%v]: %v, matchIndex[S%v] : %v", rf.logBaseString(), peerId, rf.nextIndex[peerId], peerId, rf.matchIndex[peerId])
						}

					}
					rf.updateCommitIndex()
				}

				rf.mu.Unlock()
			}(i, args)
		} else {
			rf.resetElectionTime()
			// leader rule 4
			//rf.updateCommitIndex()
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// figure 8
func (rf *Raft) updateCommitIndex() {

	if rf.currentState != LEADER {
		return
	}

	for i := rf.commitIndex + 1; i <= rf.getTailLogEntry().Index; i++ {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				count++
			}
			if count > len(rf.peers)/2 {
				log.Printf("%v matchIndex list %+v, get N : %v", rf.logBaseString(), rf.matchIndex, i)
				rf.commitIndex = i
				rf.apply()
				log.Printf("%v updateCommitIndex commitIndex : %v", rf.logBaseString(), rf.commitIndex)
				break
			}
		}
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

// 150-300
func (rf *Raft) resetElectionTime() {
	t := time.Now()
	electionTimeOut := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electTimeout = t.Add(electionTimeOut)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	log.Println("init peer id : " + strconv.Itoa(me))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//rf.mu.Lock()
	//log.Printf("%v get lock in make", rf.me)
	rf.currentState = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.resetElectionTime()
	// log
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.log = append(rf.log, &LogEntry{
		Term:    0,
		Index:   0,
		Command: nil,
	})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
		rf.matchIndex[i] = 0
		log.Printf("%v - nextIndex[%v] ：%v, matchIndex[%v] : %v", rf.logBaseString(), i, rf.nextIndex[i], i, rf.matchIndex[i])

	}
	//log.Printf("%v release lock in make", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyMsg()

	return rf
}
