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

// To implement the optimization from the end of section 5.3
type EarlyConflict struct {
	Term  int
	Index int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict EarlyConflict
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	log.Printf("Raft %d, term %d received append entries request from %d, for term %d and prev log (term, idx): (%d, %d)",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex)
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	defer log.Printf("Raft %d returning from appendEntries to leader %d, %v", rf.me, args.LeaderId, reply)
	if args.Term < reply.Term {
		// This should result in the leader giving up leadership, e.g. it may have become partitioned and fallen behind
		reply.Success = false
		return
	}
	// As long as the args.Term is >= rf.currentTerm, this is a valid heartbeat
	rf.heartbeatChan <- HeartbeatData{leaderId: args.LeaderId, newTerm: args.Term}
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
			if lastIdx := len(rf.log); lastIdx > 0 {
				// This follower has log entries, but isn't caught up to the leader, skip back to start comparing where
				// with entries that this follower does have
				reply.Conflict = EarlyConflict{rf.log[lastIdx-1].Term, lastIdx}
			} else {
				// This follower has no log entries, the leader should start sending them from the beginning
				reply.Conflict = EarlyConflict{0, 1}
			}
			return
		}
		// Check that the term of the previous log entry matches, otherwise drop it from this log and fail
		logEntry := rf.log[args.PrevLogIndex-1]
		if logEntry.Term != args.PrevLogTerm {
			firstIdx, err := rf.firstTermIndex(logEntry.Term)
			if err != nil {
				log.Printf("Unable to find first index of term %d: %s", logEntry.Term, err)
				// Just default to stepping back a single entry
				firstIdx = args.PrevLogIndex
			}
			rf.updateLog(rf.log[:firstIdx])
			reply.Conflict = EarlyConflict{Term: logEntry.Term, Index: firstIdx}
			log.Printf("Raft %d can't append entry because it has a term mismatch with PrevLogTerm %d, early conflict: %v",
				rf.me, args.PrevLogTerm, reply.Conflict)
			reply.Success = false
			return
		}
	}

	// Looks good, now append the new entries
	reply.Success = true

	if args.Entries != nil || len(args.Entries) > 0 {
		// TODO: it might be better to check if this entry already exists here, but this accomplishes getting
		// rid of unwanted logs and adding the new one simply
		rf.updateLog(append(rf.log[:args.PrevLogIndex], args.Entries...))
		commands := make([]interface{}, len(args.Entries))
		for i, e := range args.Entries {
			commands[i] = e.Command
		}
		log.Printf("Raft %d adding %d entries ending up at log at index %d", rf.me, len(commands), len(rf.log))
	}

	// Update the commit index for this instance if appropriate
	newCommitIdx := args.PrevLogIndex
	if args.Entries != nil && len(args.Entries) > 0 {
		// If there is a new entry on top of a valid PrevLogEntry, that can potentially be committed too
		newCommitIdx += len(args.Entries)
	}
	// Only commit up to whatever the leader has committed, note that LeaderCommit >= rf.commitIndex
	newCommitIdx = int(math.Min(float64(args.LeaderCommit), float64(newCommitIdx)))
	for newCommitIdx > rf.commitIndex {
		// Any entries that couldn't be committed before, should be now, e.g entries from previous terms
		// In this loop it will commit the next index until it reaches and commits
		// newCommitIdx.
		rf.stepCommitIdx(newCommitIdx)
		log.Printf("Raft %d increasing commit index to %d", rf.me, rf.commitIndex)
		rf.applyMsgChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.commitIndex,
			Command:      rf.log[rf.commitIndex-1].Command,
		}
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
