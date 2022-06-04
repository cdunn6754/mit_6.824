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
	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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

type LogTerm struct {
	entries []string
}

type Log struct {
	terms []LogTerm
}

type LogEntry struct {
	Message string
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	isCandidate   bool                // Indicates the state of this worker, along with the contents of nextIndex/matchIndex the state can be
	heartbeatChan chan struct{}       // Channel to indicate a successful heartbeat
	// fully determined: candidate, follower, or leader

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persisted
	log         []LogEntry
	votedFor    int
	currentTerm int

	// Volatile for all servers
	// commitIndex int
	// lastApplied int

	// Volatile leader info, both nil for followers and candidates
	nextIndex  []int
	matchIndex []int
}

// Update the Raft log to the provided log, persist raft to durable storage
func (rf *Raft) updateLog(newLog []LogEntry) {
	rf.log = newLog
	rf.persist()
}

// If the nextIndex or matchIndex arrays contain values other than -1
// this instance is a leader
func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := false
	for idx := range rf.peers {
		isLeader = isLeader && rf.nextIndex[idx] > -1
		isLeader = isLeader && rf.matchIndex[idx] > -1
	}
	return isLeader
}

// func (rf Raft) isFollower() bool {
// 	return !rf.isLeader() && !rf.isCandidate
// }

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader()
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var _log []LogEntry
	var votedFor int
	var currentTerm int
	if d.Decode(&_log) != nil || d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil {
		log.Fatalf("Error reading persisted data on server: %v", rf.me)
	} else {
		rf.log = _log
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// At startup the leader won't have a log, and PrevLogIndex == -1
	if args.PrevLogIndex >= 0 {
		// Otherwise make sure that this log contains an entry at PrevLogIndex
		if args.PrevLogIndex >= len(rf.log) {
			reply.Success = false
			return
		}
		// Check that the term matches, otherwise drop it from this log and fail
		log_entry := rf.log[args.PrevLogIndex]
		if log_entry.Term != args.PrevLogTerm {
			rf.updateLog(rf.log[:args.PrevLogIndex])
			reply.Success = false
			return
		}
	} else {
		// The leader has an empty log.
		// If this rf has a log, empty this log too and reply success.
		// This is a weird state, to be in. Maybe this should fail instead? But I'm not sure
		// what the leader would do then.
		if len(rf.log) > 0 {
			rf.updateLog(nil)
		}
	}
	// Looks good, now append the new entries, TODO
	reply.Success = true
	// A successful message from the leader was received, reset heartbeat
	rf.heartbeatChan <- struct{}{}
	// After a successful heartbeat, the election is over
	rf.votedFor = -1
	// It's a follower now
	rf.isCandidate = false
	log.Printf("Successfully received heartbeat received from AppendEntries for instance %d.", rf.me)
}

func (rf *Raft) sendAppendEntries(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
	return ok
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
	reply.Term = rf.currentTerm

	// Check on the RAFT algorithm logic here
	validCandidateTerm := args.Term >= rf.currentTerm
	noVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	validCandidateLog := args.LastLogIndex >= len(rf.log)
	if validCandidateTerm && noVote && validCandidateLog {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
}

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
func (rf *Raft) sendRequestVote(peerIdx int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peerIdx].Call("Raft.RequestVote", args, reply)
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

// Generate a timeout duration randomly spaced between [0.5, 1) seconds
func getTimeToSleep() time.Duration {
	return time.Millisecond*500 + time.Duration(rand.Intn(500))*time.Millisecond
}

func (rf *Raft) campaign(ctx context.Context) bool {
	// Channel for each goroutine to share to describe the vote result
	resultChan := make(chan bool)
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.persist()
	currentTerm := rf.currentTerm
	var lastLogTerm int
	if rf.log == nil {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	logIdx := len(rf.log)
	rf.mu.Unlock()
	// This should all be threadsafe, we don't change rf.peers, or rf.me
	// Initiate requests to all peers
	for peerIdx := range rf.peers {
		if peerIdx != rf.me {
			go func(resultChan chan bool, peerIdx int) {
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: logIdx,
					LastLogTerm:  lastLogTerm,
				}
				reply := &RequestVoteReply{}
				success := rf.sendRequestVote(peerIdx, args, reply)
				if success && reply.VoteGranted {
					resultChan <- true
				} else {
					resultChan <- false
				}
			}(resultChan, peerIdx)
		}
	}

	// Wait for results to come back in
	// But only wait until the election timeout is up
	yesVotes := 0
	votesReceived := 0
	voterCount := len(rf.peers) - 1
	for !rf.killed() {
		select {
		case <-ctx.Done():
			// This leads to waiting an extra timeout before campaining resumes, that's against the spec
			// but I think it should be fine.
			log.Printf("Raft: %d timed out while campaigning\n", rf.me)
			return false
		case <-rf.heartbeatChan:
			log.Printf("Raft: %d received valid heartbeat while campaigning, switching to follower mode\n", rf.me)
			return false
		case result := <-resultChan:
			votesReceived += 1
			if result {
				yesVotes += 1
			}
			// Did we reach a majority vote?
			if yesVotes > voterCount/2 {
				return true
			}
			// If we got all the votes and still no majority we lost
			// I guess we don't have to hear from all of them to determine that, but let's
			// keep it simple
			if votesReceived == voterCount {
				return false
			}
		}
	}
	// If this peer is killed, just return false to shutdown nicely
	return false
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-time.After(getTimeToSleep()):
			log.Println("Heartbeat not received within timeout, I am excited to announce my candidacy.")
			ctx, cancel := context.WithTimeout(context.Background(), getTimeToSleep())
			if rf.campaign(ctx) {
				// Get out of the follower loop, become  leader
				cancel()
				rf.lead()
			}
			cancel()
		case <-rf.heartbeatChan:
			log.Printf("Heartbeat received, I won't be seeking election for instance %d\n", rf.me)
		}
	}
}

func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go func() {
			// No locks are needed here, rf.me doesn't change
			// TODO: update PrevLog... and LeaderCommit once data is really sent
			args := &AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: -1,
				PrevLogTerm:  currentTerm,
				Entries:      nil,
				LeaderCommit: -1,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerIdx, args, reply)
			if !ok {
				// TODO, improve this log when entries are being sent
				log.Printf("AppendEntries to peer %v failed, lowering log idx to ...", peerIdx)
			}
		}()
	}
}

// Take over as the leader server
func (rf *Raft) lead() {
	// Start by initializing data structures and sending a one time appendentries
	// rpc to assert leadership
	rf.mu.Lock()
	lastLogIdx := len(rf.log)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		rf.nextIndex[idx] = lastLogIdx + 1
		rf.matchIndex[idx] = 0
	}
	rf.mu.Unlock()
	for !rf.killed() {
		// For now, just send heartbeats periodically
		rf.sendAllAppendEntries()
		time.Sleep(time.Millisecond * 150)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.isCandidate = false
	rf.heartbeatChan = make(chan struct{})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = nil
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// Values of -1 here are used to indicate that this instance is not a leader
	for idx := range rf.peers {
		rf.nextIndex[idx] = -1
		rf.matchIndex[idx] = -1
	}
	// rf.lastApplied = -1
	// rf.commitIndex = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
