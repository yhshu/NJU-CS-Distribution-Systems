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
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

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

type LogEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader
	Index   int         // the first index is 1
}

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Assignment 1
	// Persistent state on all servers:
	currentTerm int        // 	latest term server has been (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders: reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// other states
	state           State // 'follower', 'candidate', or 'leader'
	hasVotes        int   // the number of votes it has
	grantVoteCh     chan bool
	heartBeatCh     chan bool
	leaderCh        chan bool
	timer           *time.Timer
	electionTimeout int
	applyCh         chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	// Assignment 1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// Assignment 3

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	// only the persistent state
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	// Assignment 3

	r := bytes.NewBuffer(data)
	if data == nil || len(data) == 0 {
		return
	}
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.

	// Assignment 1

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidates last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.

	// Assignment 1

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate receive vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	// Assignment 1
	// The candidate asks others to vote

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[INFO] Server %d received RequestVote from candidate %d, current term: %d, current log: %v\n", rf.me, args.CandidateId, rf.currentTerm, rf.log)

	if args.Term < rf.currentTerm {
		// return current term, and refuse the vote
		rf.RejectVote(reply)

	} else if args.Term == rf.currentTerm {
		// if it has votes and the candidateId is not the same, refuse the vote

		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			rf.RejectVote(reply)
		} else {
			lastLogIndex, lastLogTerm := rf.getLastLog()

			if args.LastLogTerm < lastLogTerm {
				rf.RejectVote(reply)
			} else if args.LastLogTerm == lastLogTerm {
				if args.LastLogIndex < lastLogIndex { // refuse the vote
					rf.RejectVote(reply)
				} else { // if args.LastLogIndex >= lastLogIndex
					rf.GrantVote(args, reply)
				}
			} else { // if args.LastLogTerm > lastLogTerm
				rf.GrantVote(args, reply)
			}
		}
	} else { // if args.Term > rf.currentTerm
		rf.changeToFollower(args.Term, -1)
		// up-to-date check
		lastLogIndex, lastLogTerm := rf.getLastLog()
		if args.LastLogTerm < lastLogTerm {
			rf.RejectVote(reply)
		} else if args.LastLogTerm == lastLogTerm {
			if args.LastLogIndex < lastLogIndex {
				rf.RejectVote(reply)
			} else {
				rf.GrantVote(args, reply)
			}
		} else {
			rf.GrantVote(args, reply)
		}
	}
}

func (rf *Raft) getLastLog() (int, int) {
	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) RejectVote(reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) GrantVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[INFO] Server %d grants vote to candidate %d\n", rf.me, args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.setGrantVoteCh()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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

func (rf *Raft) setGrantVoteCh() {
	go func() { // a routine
		select { // block if no case is available
		case <-rf.grantVoteCh:
		default:
		}
		rf.grantVoteCh <- true
	}()
}

func (rf *Raft) changeToFollower(term int, voteFor int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = voteFor
	rf.hasVotes = 0
	rf.persist()
}

func (rf *Raft) changeToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.hasVotes = 1
	rf.electionTimeout = RandomInt(150, 300)
	rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.persist()
}

func (rf *Raft) timerClear() {
	select {
	case <-rf.timer.C:
		DPrintf("Server %d clears the old timer\n", rf.me)
	default:
	}
}

func (rf *Raft) heartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != Leader { // the server is not leader now, return
			rf.mu.Unlock()
			return
		}
		DPrintf("[INFO] Leader %d starts sending AppendEntries, current term: %d", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			go rf.appendEntriesToPeers(i)
		}
	}
}

func (rf *Raft) appendEntriesToPeers(peerIdx int) {
	if peerIdx == rf.me { // don't heartBeat to myself
		return
	}
	for {
		rf.mu.Lock()
		if rf.state != Leader { // the server is not leader now, return
			rf.mu.Unlock()
			return
		}

		prevLogIndex := rf.nextIndex[peerIdx] - 1
		prevLogTerm := 0
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}

		newEntries := append([]LogEntry{}, rf.log[rf.nextIndex[peerIdx]-1:]...)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LogEntries:   newEntries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		rpcRes := rf.sendAppendEntries(peerIdx, args, &reply) // AppendEntries RPC

		DPrintf("[INFO] Leader %d sends heartbeat to server %d, reply:%v\n", rf.me, peerIdx, reply)

		// if rpcRes is false, the heartbeat is not send successfully, there's two possibilities
		// 1. the leader disconnects: all heartbeats can't be send, but it keeps trying until the connection gets normal;
		//    the term is out-dated, and it quits the loop
		// 2. the follower disconnects: it doesn't stop the leader from keeping sending heartbeats to others

		if rpcRes {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// quit the loop, change to follower
				DPrintf("[INFO] Old leader %d of term %d turns to a follower due to a newer term %d received from server %d",
					rf.me, rf.currentTerm, reply.Term, peerIdx)
				rf.changeToFollower(reply.Term, -1)
				rf.mu.Unlock()
				return
			}

			// after check the reply.Term, we still need to check the args.Term and the leader role,
			// since the previous goroutine may have change this server to a follower and update the term,
			// and this goroutine need to check it's still a leader now
			if args.Term != rf.currentTerm || rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			if reply.Success == true {
				rf.matchIndex[peerIdx] = prevLogIndex + len(newEntries)
				rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

				copyMatchIndex := make([]int, len(rf.peers))
				copy(copyMatchIndex, rf.matchIndex) // copy rf.matchIndex to copyMatchIndex
				copyMatchIndex[rf.me] = len(rf.log)
				sort.Ints(copyMatchIndex)
				midCommitIndex := copyMatchIndex[len(rf.peers)/2]
				if rf.commitIndex < midCommitIndex && rf.log[midCommitIndex-1].Term == rf.currentTerm {
					rf.commitIndex = midCommitIndex
				}
				DPrintf("[INFO] Leader %d starts applying logs, last applied: %d, commitIndex: %d", rf.me, rf.lastApplied, rf.commitIndex)
				//rf.applyLogs()
				rf.mu.Unlock()
				return
			} else { // if reply.Success == false

				conflict := false
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == reply.ConflictTerm { // there is conflict term
						conflict = true
					}
					if rf.log[i].Term > reply.ConflictTerm {
						if conflict {
							rf.nextIndex[peerIdx] = i
						} else {
							rf.nextIndex[peerIdx] = reply.ConflictIndex
						}
						break
					}
				}
				if rf.nextIndex[peerIdx] < 1 {
					rf.nextIndex[peerIdx] = 1
				}
				rf.mu.Unlock()
			}

		} else { // if rpcRes == false
			DPrintf("[WARN] Leader %d calls AppendEntries to server %d failed", rf.me, peerIdx)
			return
		}
		// heartbeat interval
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // it's out-dated
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.setHeartBeatCh()
		rf.changeToFollower(args.Term, args.LeaderId)

		if args.PrevLogIndex == 0 {
			reply.Term = rf.currentTerm
			reply.Success = true
			originLogEntries := rf.log
			lastNewEntry := 0
			if args.PrevLogIndex+len(args.LogEntries) < len(originLogEntries) {
				lastNewEntry = args.PrevLogIndex + len(args.LogEntries)
				for i := 0; i < len(args.LogEntries); i++ {
					if args.LogEntries[i] != originLogEntries[args.PrevLogIndex+i] {
						rf.log = append(rf.log[:args.PrevLogIndex+i], args.LogEntries[i:]...)
						lastNewEntry = len(rf.log)
						break
					}
				}
			} else {
				rf.log = append(rf.log[:args.PrevLogIndex], args.LogEntries...)
				lastNewEntry = len(rf.log)
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntry)))
			}
			rf.persist()
			rf.applyLogs()
			return
		}

		if len(rf.log) < args.PrevLogIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else {
			prevLogTerm := 0
			if args.PrevLogIndex > 0 {
				prevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}
			if args.PrevLogTerm != prevLogTerm {
				reply.Term = rf.currentTerm
				reply.Success = false
				reply.ConflictTerm = prevLogTerm
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == prevLogTerm {
						reply.ConflictIndex = i + 1
						break
					}
				}
			} else {
				reply.Term = rf.currentTerm
				reply.Success = true
				originLogEntries := rf.log
				lastNewEntry := 0
				// 必须要有这部分判断, 否则有可能使得当前最新的log被旧的log entries所替代
				if args.PrevLogIndex+len(args.LogEntries) < len(originLogEntries) {
					lastNewEntry = args.PrevLogIndex + len(args.LogEntries)
					for i := 0; i < len(args.LogEntries); i++ {
						if args.LogEntries[i] != originLogEntries[args.PrevLogIndex+i] {
							rf.log = append(rf.log[:args.PrevLogIndex+i], args.LogEntries[i:]...)
							lastNewEntry = len(rf.log)
							break
						}
					}
				} else {
					rf.log = append(rf.log[:args.PrevLogIndex], args.LogEntries...)
					lastNewEntry = len(rf.log)
				}
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntry)))
				}
				rf.persist()
				rf.applyLogs()
			}
		}
	}
	DPrintf("[INFO] Server %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.log, reply)
}

func (rf *Raft) Run() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch {
		case state == Leader:
			DPrintf("[INFO] Candidate %d becomes the leader, current term: %d\n", rf.me, rf.currentTerm)
			rf.heartBeat()
		case state == Candidate:
			DPrintf("[INFO] Candidate %d starts the election\n", rf.me)
			go rf.startsElection()
			select {
			case <-rf.heartBeatCh:
				DPrintf("[INFO] Candidate %d receives heartbeat when it requests votes, turn back to follower now\n", rf.me)
				rf.mu.Lock()
				rf.changeToFollower(rf.currentTerm, -1)
				rf.mu.Unlock()
			case <-rf.leaderCh:
			case <-rf.timer.C:
				rf.mu.Lock()
				if rf.state == Follower {
					DPrintf("[INFO] Candidate %d knows a higher term candidate now, withdraw from the election\n", rf.me)
					rf.mu.Unlock()
					continue
				}
				rf.changeToCandidate()
				rf.mu.Unlock()
			}
		case state == Follower:
			rf.mu.Lock()
			// the timer needs to be clear, because the server may have been a leader
			rf.timerClear()
			rf.electionTimeout = RandomInt(150, 300)
			rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
			rf.mu.Unlock()
			select {
			case <-rf.grantVoteCh:
				DPrintf("[INFO] Server %d resets election time due to grantVote\n", rf.me)
			case <-rf.heartBeatCh:
				DPrintf("[INFO] Server %d resets election time due to heartbeat\n", rf.me)
			case <-rf.timer.C:
				DPrintf("[WARN] Server %d election timeout, turn to candidate\n", rf.me)
				rf.mu.Lock()
				rf.changeToCandidate()
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) startsElection() {
	DPrintf("[INFO] Candidate %d starts the election, current term: %d, current log: %v\n", rf.me, rf.currentTerm, rf.log)
	rf.mu.Lock()
	// when this server starts an election, it still possible to receive heartbeat
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	lastLogIndex, lastLogTerm := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	leaderNum := 0
	rf.mu.Unlock()

	for peerIdx := 0; peerIdx < len(rf.peers); peerIdx++ {
		go rf.requestVoteFromPeer(peerIdx, args, &leaderNum)
	}
}

func (rf *Raft) requestVoteFromPeer(peerIdx int, args RequestVoteArgs, leaderNum *int) {
	if peerIdx == rf.me {
		return
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peerIdx, args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term, -1)
			rf.mu.Unlock()
			return
		}

		// after check the reply.Term, we still need to check the args.Term and the candidate role,
		// since the previous goroutine may have change this server to a follower and update the term,
		// and this goroutine need to check it's still a candidate now
		if rf.currentTerm != args.Term || rf.state != Candidate {
			rf.mu.Unlock()
			return
		}

		if reply.VoteGranted {
			rf.hasVotes++
			if *leaderNum == 0 && rf.hasVotes > len(rf.peers)/2 && rf.state == Candidate {
				*leaderNum++
				rf.changeToLeader()
				rf.setLeaderCh()
			}
		}
		rf.mu.Unlock()

	} else { // if rf.sendRequestVote is not ok
		DPrintf("[WARN] Candidate %d sends RequestVote to server %d failed\n", rf.me, peerIdx)
	}
}

func (rf *Raft) changeToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
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

func (rf *Raft) setHeartBeatCh() {
	go func() {
		select {
		case <-rf.heartBeatCh:
		default:
		}
		rf.heartBeatCh <- true
	}()
}

func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{}
		msg.Index = rf.lastApplied
		msg.Command = rf.log[rf.lastApplied-1].Command
		rf.applyCh <- msg
	}
}

func RandomInt(min int, max int) int {
	return rand.New(rand.NewSource(time.Now().UnixNano())).Intn(max-min) + min
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

	// Your initialization code here.

	// Assignment 1

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.electionTimeout = RandomInt(150, 300)
	rf.grantVoteCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.hasVotes = 0
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[INFO] Server %d persistent state is read\n", rf.me)

	go rf.Run()
	return rf
}
