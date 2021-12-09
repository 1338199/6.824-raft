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
	"encoding/gob"
	"labrpc"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

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

const (
	Follower  string = "follower"
	Candidate        = "candidate"
	Leader           = "leader"
)

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

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	// some other self-added states
	state           string
	electionTimeout int
	applyCh         chan ApplyMsg
	grantVoteCh     chan bool
	heartBeatCh     chan bool
	leaderCh        chan bool
	totalVotes      int
	timer           *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: got RequestVote from candidate %d, args: %+v, current currentTerm: %d, current log: %v\n", rf.me, args.CandidateId, args, rf.currentTerm, rf.log)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term == rf.currentTerm {
			if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			} else {
				// 这里要不要将LastLogIndex和LastLogTerm纳入判断?
				// 原先的错误写法, 误以为不存在term相同且voteFor为空的情况, 将其与Term相同voteFor为args.CandidateId一同处理了
				// rf.convertToFollower(rf.currentTerm, args.CandidateId)
				// rf.setGrantVoteCh()
				// reply.Term = rf.currentTerm
				// reply.VoteGranted = true

				// up-to-date check
				// 在跑2C最后三个tests N次后, 偶尔会因为上述的写法而出错! 所以下面的判断非常有必要, 只是情况比较少出现
				// 情况如下:
				// 比如有3个server, s1, s2, s3的日志分别为s1: [c1]  s2: [c1, c2]  s3: [c1, c2], 且日志都为committed状态
				// s1, s2, s3的term分别为1, 2, 2, 此时s1开始选举, s2, s3可能因为刚转为Follower所以votefor为空, 因此进入这个判断逻辑
				// 如果s2, s3都直接同意投票则s1会当选为领导, 那么后续再有添加日志的操作会造成和s2, s3 committed log不一样的情况
				// 所以在s1选举时就要做好判断！
				lastLogIndex := len(rf.log)
				lastLogTerm := 0
				if lastLogIndex > 0 {
					lastLogTerm = rf.log[lastLogIndex-1].Term
				}
				if args.LastLogTerm < lastLogTerm {
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
				} else {
					if args.LastLogTerm == lastLogTerm {
						if args.LastLogIndex < lastLogIndex {
							reply.Term = rf.currentTerm
							reply.VoteGranted = false
						} else {
							DPrintf("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
							reply.Term = rf.currentTerm
							reply.VoteGranted = true
							rf.votedFor = args.CandidateId
							rf.persist()
							rf.setGrantVoteCh()
						}
					} else {
						DPrintf("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
						reply.Term = rf.currentTerm
						reply.VoteGranted = true
						rf.votedFor = args.CandidateId
						rf.persist()
						rf.setGrantVoteCh()
					}
				}
			}
		} else {
			rf.convertToFollower(args.Term, -1)
			// up-to-date check
			lastLogIndex := len(rf.log)
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.log[lastLogIndex-1].Term
			}
			if args.LastLogTerm < lastLogTerm {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			} else {
				if args.LastLogTerm == lastLogTerm {
					if args.LastLogIndex < lastLogIndex {
						reply.Term = rf.currentTerm
						reply.VoteGranted = false
					} else {
						DPrintf("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
						reply.Term = rf.currentTerm
						reply.VoteGranted = true
						rf.votedFor = args.CandidateId
						rf.persist()
						rf.setGrantVoteCh()
					}
				} else {
					DPrintf("Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
					reply.Term = rf.currentTerm
					reply.VoteGranted = true
					rf.votedFor = args.CandidateId
					rf.persist()
					rf.setGrantVoteCh()
				}
			}
		}
	}
	DPrintf("======= server %d got RequestVote from candidate %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.CandidateId, args, rf.log, reply)
}

// 需要添加的日志的 RPC
// 由leader唤起，用法和heartbeat包一样
type AppendEntriesArgs struct {
	// leader的任期号
	Term int
	// leader的id
	LeaderId int
	// 上一条日志的序号
	PrevLogIndex int
	// 上一条日志的任期号
	PrevLogTerm int
	// 准备添加的日志条目
	// 表示heatbeat时为空
	// 可以同时添加多条日志
	Entries []Entry
	// 领导人已经提交的日志序号
	LeaderCommit int
}

// reply信息的结构体
type AppendEntriesReply struct {
	// 返回的任期号，方便leader知道自己是否过期
	Term int
	// 同步日志有没有成功
	Success bool
	// 有日志矛盾，
	// 矛盾的日志任期号
	ConflictTerm int
	// 矛盾的日志索引值
	ConflictIndex int
}

// raft服务器收到日志处理/heartBeat命令时的动作
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d: got AppendEntries from leader %d, args: %+v, current term: %d, current commitIndex: %d, current log: %v\n", rf.me, args.LeaderId, args, rf.currentTerm, rf.commitIndex, rf.log)
	// 判断日志Term任期号和服务器所知道的leaderTerm任期号的大小 前者小于后者的话代表该日志的任期号过期了，拒绝添加该日志
	if args.Term < rf.currentTerm {
		// 告诉发过来的leader，你已经不是leader了，任期号过期了
		reply.Term = rf.currentTerm
		reply.Success = false
	} else { //大于的话改变节点状态
		// 正在处理heartBeat
		rf.setHeartBeatCh()
		// 把自己变成follower，follower本来就是follower，
		// 而错误的leader也会收到真leader发来的heartBeat，把自己变成follower
		// 转换包括记录leader的任期号，改变自身状态，获得的票数，以及记录leader的id
		rf.convertToFollower(args.Term, args.LeaderId)
		// PrevLogIndex为0表示从头开始appendEntries, 不用进入后续判断, 语义上更好理解
		if args.PrevLogIndex == 0 {
			// 回复leader，该raft服务器保存的任期号是多少
			reply.Term = rf.currentTerm
			// 日志的同步是成功的
			reply.Success = true
			originLogEntries := rf.log
			lastNewEntry := 0
			// 尽管Previndex是0，但follower有更多的日志项，
			// follower可能接收到了过期的同步日志请求，超出的部分除了不一致的其余不需要替换
			// 示例：
			// raft.logs    = prev(0) n x x x
			// leader.logs  = prev(0) x x
			// args.entris  =         x x
			// 更新后
			// raft.logs    = prev(0) x x x x
			if args.PrevLogIndex+len(args.Entries) < len(originLogEntries) {
				lastNewEntry = args.PrevLogIndex + len(args.Entries)
				for i := 0; i < len(args.Entries); i++ {
					if args.Entries[i] != originLogEntries[args.PrevLogIndex+i] {
						rf.log = append(rf.log[:args.PrevLogIndex+i], args.Entries[i:]...)
						lastNewEntry = len(rf.log)
						break
					}
				}
			} else { // 没有以上情况，直接用leader给的日志项替代prevIndex后的日志项
				// 示例
				// ref.logs =      prev(0) x x
				// or ref.logs =   prev(0)
				// leader.logs  =  prev(0) x n x
				// args.entries =          x n x
				// 更新后:
				// ref.logs =      prev(0) x n x
				rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
				lastNewEntry = len(rf.log)
			}
			// 更新follower所知道的最新的提交的日志的索引
			// leaderCommit > commitIndex的时候才更新!!!
			// 惨痛的bug, 否则commitIndex可能变小
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntry)))
			}
			rf.persist()
			rf.startApplyLogs()
			return
		}
		//如果当前节点本地的log[]结构中prevLogIndex索引处不含有日志, 则返回(currentTerm, false)
		// 示例
		// ref.logs =      x x
		// leader.logs =   x x x prev x n x
		// args.entries =             x n x
		if len(rf.log) < args.PrevLogIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			// 报告矛盾的日志的索引值
			reply.ConflictIndex = len(rf.log)
			// 矛盾的类型是本地log比leader的log要更短
			reply.ConflictTerm = -1
		} else {
			prevLogTerm := 0
			// 检查PrevLogIndex处的日志任期号
			if args.PrevLogIndex > 0 {
				prevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}
			// 任期不一致
			if args.PrevLogTerm != prevLogTerm {
				// 返回本地存储的leader任期号
				reply.Term = rf.currentTerm
				// 同步失败
				reply.Success = false
				// 冲突的任期号
				reply.ConflictTerm = prevLogTerm
				// 找到哪个索引的日志任期号和PrevLog的任期号是一致的
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == prevLogTerm {
						reply.ConflictIndex = i + 1
						break
					}
				}
			} else { //该索引处的任期号和PrevLog的任期号一致
				reply.Term = rf.currentTerm
				// 可以完成同步
				reply.Success = true
				originLogEntries := rf.log
				lastNewEntry := 0
				// 必须要有这部分判断, 否则有可能使得当前最新的log被旧的log entries所替代
				// 尽管follower和leader在PrevLog处是一致的，但follower还有更多的日志项，
				// follower可能接收到了过期的同步日志请求，超出的部分除了不一致的其余不需要替换
				// 示例
				// ref.logs =      x x prev x x x x x
				// leader.logs =   x x prev x n x
				// args.entries =           x n x
				// 更新后:
				// ref.logs =      x x prev x n x x x
				if args.PrevLogIndex+len(args.Entries) < len(originLogEntries) {
					// 先检查一下要添加的日志项
					lastNewEntry = args.PrevLogIndex + len(args.Entries)
					for i := 0; i < len(args.Entries); i++ {
						// 找到开始不一致的日志项
						if args.Entries[i] != originLogEntries[args.PrevLogIndex+i] {
							// 不一致的部分的日志项用leader给的日志项替代，其余保留
							rf.log = append(rf.log[:args.PrevLogIndex+i], args.Entries[i:]...)
							lastNewEntry = len(rf.log)
							break
						}
					}
				} else { // 没有以上情况，直接用leader给的日志项替代prevIndex后的日志项
					// 示例
					// ref.logs =      x x prev x x
					// or ref.logs =   x x prev
					// leader.logs =   x x prev x n x
					// args.entries =           x n x
					// 更新后:
					// ref.logs =      x x prev x n x
					rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
					lastNewEntry = len(rf.log)
				}
				// 更新follower所知道的最新的提交的日志的索引
				// 注意只有leader确认了该日志可以被提交，follower才更新自己的commitIndex
				// 因此follower可能在本次心跳中得到了要添加的logs，但在下一个心跳包里确认leader提交了，
				// follower才会提交上一次心跳包里的logs
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntry)))
				}
				rf.persist()
				// 执行和提交未执行的日志项，执行到commitIndex索引的日志项
				rf.startApplyLogs()
			}
		}
	}
	DPrintf("======= server %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.log, reply)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 锁进程
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	// Your code here (2B).
	// 一开始可能会选错leader(比如某个leader失去连接后又恢复(状态还是保持在Leader), 这种情况下会在后续该节点发出心跳包后转为Follower, 在重新确定出Leader后开始一轮新的Start操作)
	if isLeader {
		DPrintf("Leader %d: got a new Start task, command: %v\n", rf.me, command)
		// 添加到leader的日志里，同时记录任期号，索引值
		rf.log = append(rf.log, Entry{rf.currentTerm, command})
		index = len(rf.log)
		// save Raft's persistent state to stable storage
		rf.persist()
	}
	// 解锁
	rf.mu.Unlock()
	// 如果这个raft服务器不是leader，则isleader会返回false，因为leader最先添加日志
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

type Entry struct {
	Term    int
	Command interface{}
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
	rf.log = []Entry{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.applyCh = applyCh
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.grantVoteCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.totalVotes = 0
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("--------------------- Resume server %d persistent state ---------------------\n", rf.me)
	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch {
			case state == Leader:
				DPrintf("Candidate %d: l become leader now!!! Current term is %d\n", rf.me, rf.currentTerm)
				rf.startAppendEntries()
			case state == Candidate:
				DPrintf("================ Candidate %d start election!!! ================\n", rf.me)
				go rf.startRequestVote()
				select {
				case <-rf.heartBeatCh:
					DPrintf("Candidate %d: receive heartbeat when requesting votes, turn back to follower\n", rf.me)
					rf.mu.Lock()
					rf.convertToFollower(rf.currentTerm, -1)
					rf.mu.Unlock()
				case <-rf.leaderCh:
				case <-rf.timer.C:
					rf.mu.Lock()
					if rf.state == Follower {
						DPrintf("Candidate %d: existing a higher term candidate, withdraw from the election\n", rf.me)
						rf.mu.Unlock()
						continue
					}
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case state == Follower:
				rf.mu.Lock()
				// 必须！比如之前是Leader, 重新连接后转为Follower, 此时rf.timer.C里其实已经有值了
				rf.drainOldTimer()
				rf.electionTimeout = GenerateElectionTimeout(200, 400)
				rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
				rf.mu.Unlock()
				select {
				case <-rf.grantVoteCh:
					DPrintf("Server %d: reset election time due to grantVote\n", rf.me)
				case <-rf.heartBeatCh:
					DPrintf("Server %d: reset election time due to heartbeat\n", rf.me)
				case <-rf.timer.C:
					DPrintf("Server %d: election timeout, turn to candidate\n", rf.me)
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			}

		}
	}()

	return rf
}

func GenerateElectionTimeout(min, max int) int {
	rad := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := rad.Intn(max-min) + min
	return randNum
}

func (rf *Raft) startRequestVote() {
	DPrintf("Candidate %d: start sending RequestVote, current log: %v, current term: %d\n", rf.me, rf.log, rf.currentTerm)
	// 很有必要进行这个判断
	// 一种情况是Candidate在开启startRequestVote后, 就收到心跳包转为Follower, 因此再发送requestVote请求前有必要再判断一下
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	nLeader := 0
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		go func(ii int) {
			if ii == rf.me {
				return
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(ii, &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term, -1)
					rf.mu.Unlock()
					return
				}

				// 进行这一步判断很有必要, 比如两个goroutine(线程)先后进入这个if ok {}判断, 第一个goroutine得到的reply.Term > rf.currentTerm从而转换为Follower并更新了currentTerm
				// 如果不进行这个判断, 那么第二个goroutine在进行reply.Term > rf.currentTerm判断时会有同步问题, 发现reply.Term == rf.currentTerm导致错误地进行后续流程
				if rf.currentTerm != args.Term || rf.state != Candidate {
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					rf.totalVotes++
					if nLeader == 0 && rf.totalVotes > len(rf.peers)/2 && rf.state == Candidate {
						nLeader++
						rf.convertToLeader()
						// 之前一个找了好久的bug: setLeaderCh里没有启一个新的goroutine, 可能导致阻塞, 进而造成死锁
						rf.setLeaderCh()
					}
				}
				rf.mu.Unlock()
			} else {
				DPrintf("Candidate %d: sending RequestVote to server %d failed\n", rf.me, ii)
			}
		}(i)
	}
}

func (rf *Raft) startAppendEntries() {
	for {
		// 这里rf.state == leader的判断很有必要, 见FailAgree2B
		// 如果某个刚恢复的Follower在心跳到达前开始选举, Leader状态会变为Follower, 更新Term并重新选举
		rf.mu.Lock()
		// 常规来说只有状态为leader的raft服务器才能startAppendEntries
		// 但在FailAgree2B测试中，Leader状态可能会变为Follower
		// follower不能发送AppendEntries（heartBeat）
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf("Leader %d: start sending AppendEntries, current term: %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			// heartBeat不发给leader自己
			go func(ii int) {
				if ii == rf.me {
					return
				}

				for {
					rf.mu.Lock()
					// 如果leader断开了，虽然心跳包发送不会成功，但会在循环中保持发送心跳包，
					// 重新连上的时候任期已经过了，接收到了新leader的心跳包，转化为follower了。
					// follower不能发送AppendEntries（heartBeat），返回
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					// 发给follower：ii的最后一条日志项的索引
					prevLogIndex := rf.nextIndex[ii] - 1
					// 还没给follower：ii发过日志，则没有prevLog，任期号也就是0
					prevLogTerm := 0
					if prevLogIndex > 0 {
						// 找到prevLog的任期号
						prevLogTerm = rf.log[prevLogIndex-1].Term
					}
					// 从已经发送完的最后一条日志项开始，剩余的日志项都发送给follower：ii
					entries := append([]Entry{}, rf.log[rf.nextIndex[ii]-1:]...)
					// 发送参数
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					// 接收反馈信息的结构体
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					// 发送
					ok := rf.sendAppendEntries(ii, &args, &reply)
					// DPrintf("Leader %d: send heartbeat to server %d, got reply:%v\n", rf.me, ii, reply)
					// 如果ok==false, 代表心跳包没发送出去, 有两种可能: 1. 该Leader失去连接 2. 接受心跳包的Follower失去连接
					// 如果是可能性1, 那么发送出去的所有心跳包会不成功, 但不会退出, 会一直发送。 当再次连接上的时候, 由于任期肯定小于其他服务器, 因此会退出循环, 变为Follower
					// 如果是可能性2, 不影响, 继续发送心跳包给其他连接上的服务器
					// 由上面的分析, 可知不需要对isok == false做特殊处理
					if ok {
						rf.mu.Lock()
						// 知道自己不是最新的leader了
						if reply.Term > rf.currentTerm {
							// 退出循环, 转换为follower
							DPrintf("Leader %d: turn back to follower due to existing higher term %d from server %d\n", rf.me, reply.Term, ii)
							rf.convertToFollower(reply.Term, -1)
							rf.mu.Unlock()
							return
						}
						// 进行这一步判断很有必要, 比如两个goroutine先后进入这个if ok {}判断, 第一个goroutine得到的reply.Term > rf.currentTerm从而转换为Follower并更新了currentTerm
						// 如果不进行这个判断, 那么第二个goroutine在进行reply.Term > rf.currentTerm判断时会有reply.Term == rf.currentTerm，导致错误地进行后续流程
						if rf.currentTerm != args.Term || rf.state != Leader {
							rf.mu.Unlock()
							return
						}
						// 成功同步了follower：ii
						if reply.Success == true {
							// 虽然暂时这样写没啥问题, 但根据students-guide-to-raft中分析可知这行代码并不安全(This is not safe because those values could have been updated since when you sent the RPC)
							// 所以改成更新完matchIndex再更新nextIndex
							// rf.nextIndex[ii] = len(rf.log) + 1
							// follower:ii中和leader的log可以匹配的日志的最高索引
							rf.matchIndex[ii] = prevLogIndex + len(entries)
							// 那下一个要发给 follower:ii的日志的起始位置就是matchIndex[ii] + 1
							rf.nextIndex[ii] = rf.matchIndex[ii] + 1
							// paper中Figure 8的情形, 这个实现很妙!
							// 拷贝leader的matchIndex列表
							// matchIndex:leader记录的各个server已提交的最大日志索引
							copyMatchIndex := make([]int, len(rf.peers))
							copy(copyMatchIndex, rf.matchIndex)
							copyMatchIndex[rf.me] = len(rf.log)
							// 按已经提交的最大日志索引排序
							sort.Ints(copyMatchIndex)
							// N：超半数的server已经提交的日志项
							N := copyMatchIndex[len(rf.peers)/2]
							// N大于leader已经提交的最大日志项索引
							// 并且索引为N的日志项和leader的任期号是一致的
							// leader更新自己要提交的日志索引值
							if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
								rf.commitIndex = N
							}
							DPrintf("Leader %d: start applying logs, lastApplied: %d, commitIndex: %d\n", rf.me, rf.lastApplied, rf.commitIndex)
							// leader执行提交日志项，直到索引为commitIndex的日志
							rf.startApplyLogs()
							rf.mu.Unlock()
							return
						} else { // 没有成功同步follower：ii
							// 优化逻辑
							hasTermEuqalConflictTerm := false
							for i := 0; i < len(rf.log); i++ {
								if rf.log[i].Term == reply.ConflictTerm {
									// 在leader中有日志项和follower ii的prev位置的日志项任期号是相同的
									hasTermEuqalConflictTerm = true
								}
								// 该日志项的任期号大于follower ii的prev位置的日志项任期号
								// 说明该日志项是follower ii还没有的，
								// 因为follower ii的prev位置的日志项任期号已经是follower最大的任期号了
								if rf.log[i].Term > reply.ConflictTerm {
									// 在该log之前的日志项中有和follower ii的prev位置的日志项任期号是相同的
									if hasTermEuqalConflictTerm {
										// 下一个要发送给follower ii的就是leader的该条日志
										// 示例：
										// fol.logs =    1 1 1 2 2(prev)
										// leader.logs = 1 1 1 3 3(prev) 3 3 ...
										// 要发的entries        3 3       3 3 ...
										rf.nextIndex[ii] = i
									} else { // 对应的是follower ii prevIndex位置还没日志的情况,ConflictTerm为-1
										// 下一个要发送给follower ii的是follower ii的len(log)位置的日志项
										// 示例
										// fol.logs =      x x
										// leader.logs =   x x x prev x n x
										// args.entries =             x n x
										// 要发的entries为：     x prev x n x
										rf.nextIndex[ii] = reply.ConflictIndex
									}
									break
								}
								// 不存在follower有日志项任期号比leader还大的情况
							}
							// nextIndex[ii]不能小于1
							if rf.nextIndex[ii] < 1 {
								rf.nextIndex[ii] = 1
							}
							rf.mu.Unlock()
						}
					} else { //leader发送心跳包失败
						DPrintf("Leader %d: sending AppendEntries to server %d failed\n", rf.me, ii)
						return
					}
				}
			}(i)
		}
		// 一开始设置为50ms, 会导致2C中最后三个test有一定概率不过
		// 两种比较好的参数设置:
		// a. 选举超时: 150ms-300ms, 领导者心跳: 50ms
		// b. 选举超时: 200ms-400ms, 领导者心跳: 100ms
		// ref: https://github.com/springfieldking/mit-6.824-golabs-2018/issues/1
		// 心跳包发送间隙
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) startApplyLogs() {
	// 原先写的是rf.lastApplied = len(rf.log)会很有问题, 错误地认为每次提交都会把所有日志提交完, 其实可能只提交一部分
	// 执行未执行的cmd，执行到提交的最新的日志
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		// 所谓执行命令，就是把命令放进rpc的msg，
		// msg传到config进行记录统计
		// config.logs[server][index]，记录每个server已经提交的日志项
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied
		msg.Command = rf.log[rf.lastApplied-1].Command
		rf.applyCh <- msg
	}
}

func (rf *Raft) convertToFollower(term int, voteFor int) {
	// 更新自己知道的leader的任期号
	rf.currentTerm = term
	// 状态变为follower
	rf.state = Follower
	// follower是0票
	rf.totalVotes = 0
	// leader的id
	rf.votedFor = voteFor
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.electionTimeout = GenerateElectionTimeout(200, 400)
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) setHeartBeatCh() {
	go func() {
		select {
		case <-rf.heartBeatCh:
		default:
		}
		rf.heartBeatCh <- true
	}()
}

func (rf *Raft) setGrantVoteCh() {
	go func() {
		select {
		case <-rf.grantVoteCh:
		default:
		}
		rf.grantVoteCh <- true
	}()
}

func (rf *Raft) setLeaderCh() {
	go func() {
		select {
		case <-rf.leaderCh:
		default:
		}
		rf.leaderCh <- true
	}()
}

func (rf *Raft) drainOldTimer() {
	select {
	case <-rf.timer.C:
		DPrintf("Server %d: drain the old timer\n", rf.me)
	default:
	}
}
