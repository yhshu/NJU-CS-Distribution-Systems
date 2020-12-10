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
	"math/rand"
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
	voteNum         int   // the number of votes it has
	grantVoteCh     chan bool
	heartBeatCh     chan bool
	leaderCh        chan bool
	timer           *time.Timer
	electionTimeout int
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
			lastLogIndex := len(rf.log)
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.log[lastLogIndex-1].Term
			}

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
		lastLogIndex := len(rf.log)
		lastLogTerm := 0
		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}
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

func (rf *Raft) RejectVote(reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) GrantVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[INFO] Server %d: grant vote to candidate %d\n", rf.me, args.CandidateId)
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
	rf.voteNum = 0
	rf.persist()
}

func (rf *Raft) changeToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.electionTimeout = GenerateRandom(200, 400)
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

func GenerateRandom(min int, max int) int {
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
	rf.electionTimeout = GenerateRandom(200, 400)
	rf.grantVoteCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.voteNum = 0
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[INFO] Server %d persistent state is read\n", rf.me)

	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch {
			case state == Leader:
				DPrintf("[INFO] Candidate %d becomes the leader, current term: %d\n", rf.me, rf.currentTerm)
				rf.startAppendEntries()
			case state == Candidate:
				DPrintf("[INFO] Candidate %d starts the election\n", rf.me)
				go rf.startRequestVote()
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
				// 必须！比如之前是Leader, 重新连接后转为Follower, 此时rf.timer.C里其实已经有值了
				rf.timerClear()
				rf.electionTimeout = GenerateRandom(200, 400)
				rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
				rf.mu.Unlock()
				select {
				case <-rf.grantVoteCh:
					DPrintf("[INFO] Server %d resets election time due to grantVote\n", rf.me)
				case <-rf.heartBeatCh:
					DPrintf("[INFO] Server %d resets election time due to heartbeat\n", rf.me)
				case <-rf.timer.C:
					DPrintf("[INFO] Server %d election timeout, turn to candidate\n", rf.me)
					rf.mu.Lock()
					rf.changeToCandidate()
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}
