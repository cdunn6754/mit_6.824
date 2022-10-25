package raft

import (
	"log"
	"math"
	"sync"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// log.Printf("Raft %d, term %d received append entries request from %d, term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	defer log.Printf("Raft %d returning from appendEntries to leader %d, %v", rf.me, args.LeaderId, reply)
	if args.Term < reply.Term {
		reply.Success = false
		return
	}
	// As long as the args.Term is >= rf.currentTerm, this is a valid heartbeat
	rf.heartbeatChan <- HeartbeatData{leaderId: args.LeaderId, newTerm: args.Term}

	// At startup the leader won't have a log, and PrevLogIndex == 0
	// Also when the leader is sending the first entry the PrevLogIndex == 0, in either case
	// don't expect that previous entry to exist in the follower log
	// The Raft logic uses 1-indexing for the log
	if args.PrevLogIndex > 0 {
		// Otherwise make sure that this log contains an entry at PrevLogIndex
		if args.PrevLogIndex > len(rf.log) {
			log.Printf("Raft %d can't append entry because it lacks the PrevLogIndex entry at index %d",
				rf.me, args.PrevLogIndex)
			reply.Success = false
			return
		}
		// Check that the previous log entry term matches, otherwise drop it from this log and fail
		logEntry := rf.log[args.PrevLogIndex-1]
		if logEntry.Term != args.PrevLogTerm {
			rf.mu.Lock()
			rf.updateLog(rf.log[:args.PrevLogIndex])
			rf.mu.Unlock()
			log.Printf("Raft %d can't append entry because it has a term mismatch with PrevLogTerm %d",
				rf.me, args.PrevLogTerm)
			reply.Success = false
			return
		}
	}

	// Looks good, now append the new entries
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	// Check if the Leader has decided that any of the entries are committed
	newCommitIdx := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
	if newCommitIdx > rf.commitIndex {
		// Any entries that couldn't be committed before, should be now, e.g entries from previous terms
		// The next time an appendEntry call is made, it will commit the next index until it reaches and commits newCommitIdx
		// TODO: put this in a loop so that a single appendEntry call results in this follower getting
		// caught up on the commit indexes
		rf.stepCommitIdx(newCommitIdx)
		log.Printf("Raft %d increasing commit index to %d", rf.me, rf.commitIndex)
		rf.applyMsgChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.commitIndex,
			Command:      rf.log[rf.commitIndex-1].Command,
		}
	}

	if args.Entries != nil || len(args.Entries) > 0 {
		// For the time being only one log entry at a time is supported, TODO optimization here
		if len(args.Entries) > 1 {
			log.Panic("Sending more than one command at a time is not supported")
		}
		entry := args.Entries[0]

		// TODO: it might be better to check if this entry already exists here, but this accomplishes getting
		// rid of unwanted logs and adding the new one simply
		rf.updateLog(append(rf.log[:args.PrevLogIndex], entry))
		log.Printf("Raft %d adding entry %v to log at index %d", rf.me, entry.Command, len(rf.log))
	}
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// If the args.Term exceeds rf term, then increase rf term and set as a follower
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	wg := &sync.WaitGroup{}
	if args.Term > currentTerm {
		wg.Add(1)
		data := NewTermData{term: args.Term, wg: wg}
		rf.newTermChan <- data
	}
	wg.Wait()

	lastLogTerm := rf.getLastLogTerm()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// Check on the RAFT algorithm logic here
	// validLog checks election restriction (Sec. 5.4.1 from the paper)
	validLog := false
	if args.LastLogTerm > lastLogTerm {
		validLog = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log) {
		validLog = true
	}
	noVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if validLog && noVote {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// Persist votedFor
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	log.Printf("Raft %d voted for instance %d vote %t, term %d",
		rf.me, args.CandidateId, reply.VoteGranted, rf.currentTerm)
	log.Printf("Raft %d vote reason: validLog %t, noVote %t",
		rf.me, validLog, noVote)
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
