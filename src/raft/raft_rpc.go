package raft

import (
	"context"
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
			rf.updateLog(rf.log[:args.PrevLogIndex])
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
	if args.Entries == nil || len(args.Entries) == 0 {
		// Reset the log too, in case there was something in here from a previous term
		rf.log = make([]LogEntry, 0)
		return
	}
	// For the time being only one log at a time is supported, TODO possibly optimization here
	if len(args.Entries) > 1 {
		log.Panic("Sending more than one command at a time is not supported.a")
	}
	entry := args.Entries[0]

	// TODO: it might be better to check if this entry already exists here, but this accomplishes getting
	// rid of unwanted logs and adding the new one simply
	rf.log = append(rf.log[:args.PrevLogIndex], entry)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
	}
}

func (rf *Raft) sendAppendEntries(peerIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAllAppendEntries(failureChan chan int, ctx context.Context) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	log.Printf("Raft %d sending all append entries, term %d", rf.me, currentTerm)
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go func(peerIdx int, currentTerm int) {
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
			resChan := make(chan bool)
			go func() { resChan <- rf.sendAppendEntries(peerIdx, args, reply) }()
			select {
			case ok := <-resChan:
				if !ok {
					log.Printf("Raft %d AppendEntries to peer %d RPC network problem", rf.me, peerIdx)
				} else if !reply.Success {
					// Check to make sure that the term of the follower isn't higher than this term
					// That could happen if this instance is an outdated leader, e.g. was cutoff for a while
					// It indicates that this instance node is no longer the leader
					if reply.Term > currentTerm {
						log.Printf("Raft %d AppendEntries to peer %d shows failed leadership term %d, new term %d",
							rf.me, peerIdx, currentTerm, reply.Term)
						failureChan <- reply.Term
					} else {
						// TODO, improve this log when entries are really being sent
						log.Printf("Raft %d AppendEntries to peer %d failed, lowering idx to try again", rf.me, peerIdx)
					}

				}
			case <-ctx.Done():
				log.Printf("Raft %d canceling appendEntry request for peer %d", rf.me, peerIdx)
				return
			}
		}(peerIdx, currentTerm)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// Check on the RAFT algorithm logic here
	// validLog checks election restriction (Sec. 5.4.1 from the paper)
	validLog := false
	if args.Term > rf.currentTerm {
		validLog = true
	} else if args.Term == rf.currentTerm && args.LastLogIndex >= len(rf.log) {
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
