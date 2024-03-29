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
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

var APPEND_INTERVAl = time.Millisecond * 150

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

type LogEntry struct {
	Command interface{}
	Term    int
	// This is the algorithm index, i.e. it is one indexed so rf.log[0].Index == 1
	Index int
}

type Snapshot struct {
	State []byte
	Index int
	Term  int
}

type HeartbeatData struct {
	leaderId int
	newTerm  int
}

// A WaitGroup is necessary for the calls from the async RPC handlers, e.g.
// if a  RequestVote request comes with a higher term, then that handler kicks off
// a state change via the newTerm channel, but it needs to wait for that state change
// to be completed before it can decide on its vote and formulate a response. This is because
// its rf.votedFor will be reset by the state change.
type NewTermData struct {
	term int
	wg   *sync.WaitGroup
}

type StateChangeData struct {
	newTerm  int
	newState RaftState
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Indicates the state of this worker, along with the contents of nextIndex/matchIndex the state can be
	// fully determined: candidate, follower, or leader
	_isCandidate  bool
	heartbeatChan chan HeartbeatData // Channel to indicate a successful heartbeat from an appendEntry
	newTermChan   chan NewTermData   // Channel that rpc functions can use to interrupt when they get a higher term in a request
	applyMsgChan  chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persisted
	log               []LogEntry
	votedFor          int
	currentTerm       int
	snapshot          Snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile for all servers
	commitIndex int
	lastApplied int

	// Volatile leader info, both nil for followers and candidates
	nextIndex  []int
	matchIndex []int
}

//
// Property getters (read-only), thread-safe, should not be called with locks
//

// If the nextIndex or matchIndex arrays contain values other than -1
// this instance is a leader
func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := true
	for idx := range rf.peers {
		isLeader = isLeader && rf.nextIndex[idx] > -1
		isLeader = isLeader && rf.matchIndex[idx] > -1
	}
	return isLeader
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf._isCandidate
}

func (rf *Raft) isFollower() bool {
	return !rf.isLeader() && !rf.isCandidate()
}

//
// Property getters (read-only), not thread-safe, should be called under lock
//

// Get the most recent log entry from the log entry array
// Note that this will return a zero valued LogEntry struct if the raft log is empty,
// A valid log entry will always have a term and index >= 1.
// This also does not consider the possibility of a recent snapshot, if the
// log is empty following a snapshot, this function will still return LogEntry{}.
// Not thread safe
func (rf *Raft) getLastLogEntry() LogEntry {
	if len(rf.log) == 0 {
		return LogEntry{}
	}
	return rf.log[len(rf.log)-1]
}

// Given an index try to find the corresponding log entry.
// Return an empty LogEntry{} if not found. That can be distinguished from
// a valid entry because a valid entry will always have a term and index >= 1.
// The provided index should the the 1-indexed Raft algorithm index, not the
// 0-indexed position of the entry within the log
// Not thread safe
func (rf *Raft) getLogEntry(index int) LogEntry {
	for _, entry := range rf.log {
		if entry.Index == index {
			return entry
		}
	}
	return LogEntry{}
}

// Given an entry index, find the corresponding entry in the log
// and return a slice starting with it and including every
// entry after until the end of the log.
// If an entry with the provided index can't be found, a zero valued
// []LogEntry is returned.
// Not thread safe
func (rf *Raft) getLogFromIndex(index int) []LogEntry {
	ret := []LogEntry{}
	for idx, entry := range rf.log {
		if entry.Index == index {
			ret = append(ret, rf.log[idx:]...)
			return ret
		}
	}
	return ret
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	log.Printf("Raft %d state: term %d, leader %t", rf.me, currentTerm, rf.isLeader())
	return currentTerm, rf.isLeader()
}

//
// Property setters not thread-safe, should be called under lock
//

// Truncate the log at a provided term, all entries of the term are removed.
// If an entry with this term is not found, an error is returned.
// Not thread safe
func (rf *Raft) truncateLogTerm(term int) error {
	for idx, entry := range rf.log {
		if entry.Term == term {
			// Don't worry about garbage collection here, it will happen during log compaction
			rf.log = rf.log[:idx]
			return nil
		}
	}
	return fmt.Errorf("unable truncate term %d because no entries of that term could be found", term)
}

// Append the provided log entries to the log starting from
// the entry with the provided index.
// Not thread safe
func (rf *Raft) appendAtLogIndex(index int, entries []LogEntry) error {
	for idx, entry := range rf.log {
		if entry.Index == index {
			rf.log = append(rf.log[:idx+1], entries...)
			return nil
		}
	}
	return fmt.Errorf("unable to append at index %d because an entry with that index couldn't be found", index)
}

// Given a new valid commitIndex, step the commit index up to the next in order, the entries must be
// committed in order. I.e., don't skip any entries that were uncommitted previously because of
// an old term. The commitIndex can never be lowered.
// Not thread safe
func (rf *Raft) stepCommitIdx(newCommitIdx int) {
	if rf.commitIndex < newCommitIdx {
		rf.commitIndex += 1
	}
}

//
// Property setters thread-safe, should not be called under lock
//

// Create and append a new entry to the log
// Returns the index of the appended entry
func (rf *Raft) createAndAppendToLog(term int, command interface{}) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var index int
	if len(rf.log) == 0 {
		// Maybe we are just starting, maybe the log was just compacted
		if rf.lastIncludedIndex == -1 {
			// There hasn't been a snapshot yet, this is the initial startup
			index = 1
		} else {
			// There was a snapshot, build on that
			index = rf.lastIncludedIndex + 1
		}
	} else {
		index = rf.getLastLogEntry().Index + 1
	}
	entry := LogEntry{Index: index, Term: term, Command: command}
	rf.log = append(rf.log, entry)
	rf.persist()
	return index
}

// Set this instance's state as a follower, often this is used as the result of a receiving an RPC
// request with a higher term, that is set here too. Also reset the voting stats.
func (rf *Raft) setFollowerState(newTerm int) {
	// Set this instance state as a follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Set state to follower
	rf.currentTerm = newTerm
	// These are set to -1 to indicate this node is not a leader
	rf.setLeaderArraysTo(-1, -1)
	rf.votedFor = -1
	rf._isCandidate = false
	// Persist currentTerm and votedFor
	rf.persist()
	log.Printf("Raft %d, term %d, set as follower", rf.me, rf.currentTerm)
}

func (rf *Raft) setCandidateState(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf._isCandidate = true
	rf.currentTerm = newTerm
	rf.votedFor = rf.me
	rf.persist()
	log.Printf("Raft %d set to candidate for term %d", rf.me, newTerm)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raft_data := w.Bytes()
	ssw := new(bytes.Buffer)
	sse := labgob.NewEncoder(ssw)
	sse.Encode(rf.snapshot)
	ss_data := ssw.Bytes()
	rf.persister.SaveStateAndSnapshot(raft_data, ss_data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, ss_data []byte) {
	if (data == nil || len(data) < 1) && (ss_data == nil || len(ss_data) < 1) { // bootstrap without any state
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var _log []LogEntry
	var votedFor int
	var currentTerm int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&_log) != nil || d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("Error reading persisted data on server: %v", rf.me)
	} else {
		rf.log = _log
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		// These could be a part of the snapshot persistance, but they are a part of the Raft state too
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}

	// Snapshot
	ssr := bytes.NewBuffer(ss_data)
	ssd := labgob.NewDecoder(ssr)
	var ss Snapshot
	if ssd.Decode(&ss) != nil {
		log.Fatalf("Error reading persisted snapshot data: %v", rf.me)
	} else {
		rf.snapshot = ss
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
	rf.mu.Lock()
	index := 0
	term := rf.currentTerm
	logLen := len(rf.log)
	rf.mu.Unlock()
	isLeader := rf.isLeader()
	if isLeader {
		log.Printf("Raft %d, as leader, is appending command %v, to log index %d for term %d",
			rf.me, command, logLen+1, term)
		index = rf.createAndAppendToLog(term, command)
	}
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
	log.Printf("Raft %d killed", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) campaign() {
	// Channel for each vote getting goroutine to share to describe the vote result
	resultChan := make(chan bool)
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	lastLogEntry := rf.getLastLogEntry()
	rf.mu.Unlock()
	// This should all be thread-safe, we don't change rf.peers, or rf.me
	// Initiate requests to all peers
	for peerIdx := range rf.peers {
		if peerIdx != rf.me {
			go func(resultChan chan bool, peerIdx int) {
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogEntry.Index,
					LastLogTerm:  lastLogEntry.Term,
				}
				reply := &RequestVoteReply{}
				success := rf.sendRequestVote(peerIdx, args, reply)
				resultChan <- success && reply.VoteGranted
			}(resultChan, peerIdx)
		}
	}

	// Wait for results to come back in
	// But only wait until the election timeout is up
	// Or a heartbeat comes in from another leader
	// This instance votes for itself
	yesVotes := 1
	votesReceived := 1
	voterCount := len(rf.peers)
	timeoutChan := time.After(getTimeToSleep())
	for !rf.killed() {
		select {
		case hb := <-rf.heartbeatChan:
			// Got a valid heartbeat, revert to follower, matching the leaders term
			log.Printf("Raft %d while campaigning, received a heartbeat from raft %d", rf.me, hb.leaderId)
			rf.handleStateChange(StateChangeData{newTerm: hb.newTerm, newState: Follower}, nil)
			return
		case newTermData := <-rf.newTermChan:
			// A RequestVote RPC came in with a higher term. Revert to follower in new term
			log.Printf("Raft %d while campaigning, received a higher term rpc for term %d", rf.me, newTermData.term)
			rf.handleStateChange(StateChangeData{newTerm: newTermData.term, newState: Follower}, newTermData.wg)
			return
		case <-timeoutChan:
			log.Printf("Raft %d timed out while campaigning in term %d", rf.me, currentTerm)
			rf.handleStateChange(StateChangeData{newTerm: currentTerm, newState: Follower}, nil)
			// TODO: use a context to cancel requests here and for heartbeat
			return
		case result := <-resultChan:
			votesReceived += 1
			if result {
				yesVotes += 1
			}
			// Did we reach a majority vote?
			if yesVotes > voterCount/2 {
				log.Printf("Raft %d won the election in term %d", rf.me, currentTerm)
				rf.handleStateChange(StateChangeData{newTerm: currentTerm, newState: Leader}, nil)
				return
			}
			// If we got all the votes and still no majority we lost
			// I guess we don't have to hear from all of them to determine that, but let's
			// keep it simple
			if votesReceived == voterCount {
				log.Printf("Raft %d while campaigning failed to win in term %d", rf.me, currentTerm)
				rf.handleStateChange(StateChangeData{newTerm: currentTerm, newState: Follower}, nil)
				return
			}
		}
	}
}

func (rf *Raft) follow() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for !rf.killed() {
		select {
		case <-time.After(getTimeToSleep()):
			// Switch to candidate, we timed out waiting
			log.Printf("Raft %d timed out following, becoming a candidate", rf.me)
			rf.handleStateChange(StateChangeData{newState: Candidate, newTerm: currentTerm + 1}, nil)
			return
		case hb := <-rf.heartbeatChan:
			// Got a valid heartbeat
			if currentTerm < hb.newTerm {
				// If the term has increased, restart this function with a new term
				log.Printf("Raft %d as follower is increasing its term to %d due to a heartbeat", rf.me, hb.newTerm)
				rf.handleStateChange(StateChangeData{newState: Follower, newTerm: hb.newTerm}, nil)
				return
			}
		case newTermData := <-rf.newTermChan:
			// An RPC handler received a request from another instance with a higher term
			log.Printf("Raft %d as follower is increasing its term to %d", rf.me, newTermData.term)
			rf.handleStateChange(StateChangeData{newState: Follower, newTerm: newTermData.term}, newTermData.wg)
			return
		}
	}
}

// Initialize the leader arrays, NextIndex and MatchIndex
// This also sets a value for this leader, but, that's fine, just don't use it.
// I guess we could set the leader value to nil?
func (rf *Raft) setLeaderArraysTo(nextIndex int, matchIndex int) {
	for idx := range rf.peers {
		rf.nextIndex[idx] = nextIndex
		rf.matchIndex[idx] = matchIndex
	}
}

// Determine which entries should be sent, and the previous entry information
func (rf *Raft) getAppendEntryArgs(peerIdx int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextIdx := rf.nextIndex[peerIdx]
	lastLogIdx := rf.getLastLogEntry().Index

	// Make sure that nextIndex is valid for assumptions made below
	if nextIdx < 1 || nextIdx > lastLogIdx+1 {
		log.Panicf("Raft %d has an improperly set nextIndex for peer %d: %+v, with log %+v",
			rf.me, peerIdx, rf.nextIndex, rf.log)
	}

	if lastLogIdx == 0 {
		// There are not log entries yet, just send a heartbeat
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
		args.Entries = nil
	} else if lastLogIdx == 1 {
		if nextIdx == 1 {
			args.Entries = []LogEntry{rf.getLogEntry(lastLogIdx)}
			// These are set to 0 to prevent the follower from checking if they match a real previous entry,
			// which doesn't (shouldn't) exist
			args.PrevLogIndex = 0
			args.PrevLogTerm = 0
		} else {
			// The follower already has this single entry
			args.Entries = nil
			args.PrevLogIndex = 1
			args.PrevLogTerm = 1
		}
	} else {
		args.PrevLogIndex = nextIdx - 1
		if nextIdx == 1 {
			// The follower log is empty still, so there are no previous entries
			args.PrevLogTerm = 0
		} else {
			// Here there is necessarily a previous entry in the slice
			prevEntry := rf.getLogEntry(nextIdx - 1)
			if prevEntry == (LogEntry{}) {
				// Couldn't find the expected entry before the nextIdx, check to see if it's the last one
				// from log compaction
				if rf.lastIncludedIndex == nextIdx {
					args.PrevLogTerm = rf.lastIncludedTerm
				} else {
					// TODO: Send a snapshot to the follower, it is too far behind for this
				}
			} else {
				args.PrevLogTerm = prevEntry.Term
			}
		}

		if lastLogIdx >= nextIdx {
			// Send the rest of the entries
			args.Entries = append(args.Entries, rf.getLogFromIndex(nextIdx)...)
		} else {
			args.Entries = nil
		}
	}
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	return args
}

// Send an append entries request or heartbeat and update the leader state based on the response
func (rf *Raft) appendToFollower(peerIdx int, failureChan chan int, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	reply := &AppendEntriesReply{}
	rf.mu.Lock()
	nextIndex := rf.nextIndex[peerIdx]
	matchIndex := rf.matchIndex[peerIdx]
	currentTerm := rf.currentTerm
	lastLogIdx := rf.getLastLogEntry().Index

	// The algorithm should ensure that nextIndex in [1, lastLogIndex+1], but double check here and
	// reinitialize the fields if necessary
	if nextIndex > lastLogIdx+1 || nextIndex == 0 {
		log.Printf("Warning: Raft %d as leader has log of length %d but a next index for peer %d of %d",
			rf.me, peerIdx, len(rf.log), nextIndex)
		nextIndex = lastLogIdx + 1
		matchIndex = 0
		rf.nextIndex[peerIdx] = nextIndex
		rf.matchIndex[peerIdx] = matchIndex
		log.Printf("Raft %d restarting append entry handler for peer %d with next index %d and match index 0",
			rf.me, peerIdx, nextIndex)
	}
	rf.mu.Unlock()
	args := rf.getAppendEntryArgs(peerIdx)

	// Send the request, but drop it without waiting for a request timeout if we lose leadership
	// log.Printf("Raft %d as leader is sending an appendEntry RPC to follower %d: %+v", rf.me, peerIdx, args)
	var ok bool
	select {
	case ok = <-rf.sendAppendEntriesAsync(peerIdx, args, reply):
	case <-ctx.Done():
		log.Printf(
			"Raft %d is stopping the appendEntries request early for peer %d because it is no longer the leader",
			rf.me, peerIdx)
		return
	}

	// Handle the responses
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check that this instance didn't lose leadership between the time the response came back and when we got the lock
	// TODO: it seems like there should be a way to combine this with the ctx.Done in the request select above
	select {
	case <-ctx.Done():
		log.Printf("Raft %d is ignoring the appendEntries response for peer %d because it is no longer the leader",
			rf.me, peerIdx)
		return
	default:
	}

	// These values may have changed while the request was out, from a concurrent appendToFollower goroutine
	// for this same peerIdx
	if nextIndex != rf.nextIndex[peerIdx] || matchIndex != rf.matchIndex[peerIdx] {
		// If they did change, stop here, some other goroutine already made a request and made the updates
		log.Printf("Raft %d is ignoring the appendEntries response for peer %d because of outdated an nextIndex or matchIndex",
			rf.me, peerIdx)
		return
	}

	if !ok {
		log.Printf("Raft %d AppendEntries to peer %d RPC network problem", rf.me, peerIdx)
	} else if !reply.Success {
		// Check to make sure that the term of the follower isn't higher than this term
		// That could happen if this instance is an outdated leader, e.g. was partitioned for a while
		// It indicates that this instance node is no longer the leader
		if reply.Term > currentTerm {
			log.Printf("Raft %d AppendEntries to peer %d shows failed leadership term %d, new term %d",
				rf.me, peerIdx, currentTerm, reply.Term)

			// Don't block trying to send to failureChan if another sender already failed and cancelled ctx
			select {
			case <-ctx.Done():
			case failureChan <- reply.Term:
			}
			return
		} else {
			// Back off the nextIndex for next time, but don't let nextIndex < 1
			// Default to stepping back one
			nnIndex := nextIndex - 1
			if reply.Conflict != (EarlyConflict{}) {
				// Skip over all follower log entries that share the conflicting term if info is provided
				nnIndex = reply.Conflict.Index
			}
			if nnIndex > 0 {
				rf.nextIndex[peerIdx] = nnIndex
				log.Printf("Raft %d AppendEntries to peer %d failed, lowering nextIndex from %d to %d ",
					rf.me, peerIdx, nextIndex, nnIndex)
			} else {
				log.Printf("Raft %d AppendEntries to peer %d failed, but nextIndex was already at 1",
					rf.me, peerIdx)
			}
		}
	} else {
		// It was successful, increment the peer arrays
		if args.Entries != nil {
			rf.nextIndex[peerIdx] = nextIndex + len(args.Entries)
			rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
		}
	}
}

// Send an initial heartbeat and then periodically start a goroutine to handle updating this peer's log
func (rf *Raft) commandFollower(peerIdx int, failureChan chan int, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	// Send an initial heartbeat
	reply := &AppendEntriesReply{}
	args := rf.getAppendEntryArgs(peerIdx)
	rf.sendAppendEntriesAsync(peerIdx, args, reply)

	for !rf.killed() {
		select {
		case <-ctx.Done():
			log.Printf("Raft %d canceling appendEntry request handler for peer %d", rf.me, peerIdx)
			return
		case <-time.After(APPEND_INTERVAl):
			wg.Add(1)
			go rf.appendToFollower(peerIdx, failureChan, ctx, wg)
		}
	}
}

// Set up goroutines for each peer that will continuously work to keep follower logs up to date
func (rf *Raft) commandFollowers(failureChan chan int, ctx context.Context, wg *sync.WaitGroup) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Maps peerIdx -> successChan for each peer
	// Where a successChan is used to communicate the logIndex of a successful append by that peer
	// successChans := make(map[int][]chan int, len(rf.peers)-1)
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		wg.Add(1)
		go rf.commandFollower(peerIdx, failureChan, ctx, wg)
	}
}

// Long running goroutine to periodically check if the leader commitIndex and lastApplied can be increased
func (rf *Raft) commitIndexHandler(ctx context.Context, wg *sync.WaitGroup) {
	// From the paper:
	// "If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (sections 5.3 & 5.4)"
	log.Printf("Raft %d as leader is starting the commitIndexHandler", rf.me)
	defer wg.Done()
	for !rf.killed() {
		select {
		case <-ctx.Done():
			log.Printf("Raft %d stopping the commitIndexHandler", rf.me)
			return
		case <-time.After(time.Millisecond * 10):
			rf.mu.Lock()
			n := rf.commitIndex + 1
			lastEntry := rf.getLastLogEntry()
			if n > lastEntry.Index {
				// We are up to date, there is no new entries to commit
				rf.mu.Unlock()
				break
			}
			// Need to get to an entry with the current term at least
			var entry LogEntry
			for n <= len(rf.log) {
				entry = rf.log[n-1]
				if entry.Term == rf.currentTerm {
					// Start the search here, this could be the next to commit
					break
				}
				n++
			}
			if n > len(rf.log) {
				// We went through all of the log entries and didn't find one from this term,
				// so nothing can be committed now
				log.Printf("Raft %d can't commit because the latest entries aren't from this term", rf.me)
				rf.mu.Unlock()
				break
			}

			// At this point we have n > commitIndex, and n.Term == rf.currentTerm, now check
			// if a majority of instances match
			// TODO: probably just iterate upwards to find the highest possible index to be committed, but
			// keep it simple for now and just check this one
			matchCount := 0
			for idx, mIdx := range rf.matchIndex {
				if idx == rf.me {
					// The leader has this entry its log
					matchCount += 1
					continue
				}
				if mIdx >= n {
					matchCount += 1
				}
			}
			if matchCount > len(rf.peers)/2 {
				// Any entries that couldn't be committed before, should be now, e.g entries from previous terms
				// The next time this loop runs, it will commit the next index until it reaches and commits n
				rf.stepCommitIdx(n)
				cmd := rf.log[rf.commitIndex-1].Command
				log.Printf("Raft %d, as leader increasing commit index to %d for message %+v",
					rf.me, rf.commitIndex, cmd)
				rf.applyMsgChan <- ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.commitIndex,
					Command:      cmd,
				}
			}
			rf.mu.Unlock()
		}
	}
}

// Take over as the leader server
func (rf *Raft) lead() {
	// Start by initializing data structures and sending a one time AppendEntry
	// rpc to assert leadership to the other instances
	rf.mu.Lock()
	lastLogIdx := len(rf.log)
	rf.setLeaderArraysTo(lastLogIdx+1, 0)
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	log.Printf("Raft %d is leader now", rf.me)
	// Channel to indicate another leader has taken over, sends the new term number
	failureChan := make(chan int)
	// Send initial appendEntries leadership heartbeat immediately
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go rf.commitIndexHandler(ctx, wg)
	rf.commandFollowers(failureChan, ctx, wg)

	// Now just wait for a signal that this leadership has ended
	select {
	case hb := <-rf.heartbeatChan:
		// Another leader sent an appendEntries rpc with a higher term
		// exit this function to resume follower function
		if hb.newTerm == currentTerm {
			// There is a problem if two leaders were elected for the same term
			log.Panicf("Raft %d as leader in term %d received a heartbeat from leader instance %d in term %d",
				rf.me, currentTerm, hb.leaderId, hb.newTerm)
		}
		log.Printf("Raft %d as leader, received a heartbeat from leader with a greater term: %+v, now becoming follower.",
			rf.me, hb)
		cancel()
		wg.Wait()
		rf.handleStateChange(StateChangeData{newTerm: hb.newTerm, newState: Follower}, nil)
		return
	case newTerm := <-failureChan:
		// Heard from a follower that this instance is no longer the leader
		// Transition state back to follower, exit this function to resume follower function
		log.Printf("Raft %d as leader, found a follower with a higher term %d", rf.me, newTerm)
		cancel()
		wg.Wait()
		rf.handleStateChange(StateChangeData{newTerm: newTerm, newState: Follower}, nil)
		return
	case newTermData := <-rf.newTermChan:
		// Heard from a candidate with a higher term.
		// Transition state back to follower, exit this function to resume follower function, in the higher term
		log.Printf("Raft %d as leader, found a candidate with a higher term %d", rf.me, newTermData.term)
		cancel()
		wg.Wait()
		rf.handleStateChange(StateChangeData{newTerm: newTermData.term, newState: Follower}, newTermData.wg)
		return
	}
}

// Given a desired new state, transition the instance to that state
func (rf *Raft) handleStateChange(stateChangeData StateChangeData, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	// log.Printf("Raft %d getting lock in state change", rf.me)
	rf.mu.Lock()
	// log.Printf("Raft %d got the lock in state change", rf.me)
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	// TODO: Could probably make a map of valid transition pairs, and provide a function for each, that way
	// all of the invalid transitions could be handled at once, rather than having to write out all 9.

	// TODO: if not the thing above, then invert this at least, the outer loop should be the state transition, not
	// the current state, a couple of these can be grouped then, regardless of the current state. E.g. when making a
	// transition to follower the current state is irrelevant.
	switch {
	case rf.isLeader():
		switch stateChangeData.newState {
		case Leader:
			// This is weird, how did it get elected while it's already a leader?
			log.Panicf("Raft %d, as a leader, was reelected", rf.me)
		case Candidate:
			// Also weird, this is not an expected transition.
			log.Panicf("Raft %d is attempting to transition from leader to candidate, term %d -> term %d",
				rf.me, currentTerm, stateChangeData.newTerm)
		case Follower:
			rf.setFollowerState(stateChangeData.newTerm)
		}
	case rf.isCandidate():
		switch stateChangeData.newState {
		case Leader:
			// TODO: make a setToLeader fcn, could defer the lock at least
			rf.mu.Lock()
			// set state to leader
			rf._isCandidate = false
			rf.setLeaderArraysTo(len(rf.log)+1, 0)
			rf.mu.Unlock()
		case Candidate:
			// This means that the candidate timed out. Start a new campaign in a new term
			rf.setCandidateState(stateChangeData.newTerm)
		case Follower:
			// Was campaigning but received a valid heartbeat
			rf.setFollowerState(stateChangeData.newTerm)
		}
	case rf.isFollower():
		switch stateChangeData.newState {
		case Leader:
			// A follower can't become a leader directly
			log.Panicf("Raft %d is a follower, attempting to become a leader directly, state change: %+v",
				rf.me, stateChangeData)
		case Candidate:
			// Got a timeout waiting for a heartbeat, become candidate
			rf.setCandidateState(stateChangeData.newTerm)
		case Follower:
			// If starting a new term, reset the follower state, that will include resetting vote state.
			if currentTerm < stateChangeData.newTerm {
				rf.setFollowerState(stateChangeData.newTerm)
			}
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		term := rf.currentTerm
		rf.mu.Unlock()
		switch {
		case rf.isLeader():
			log.Printf("Raft %d starting as a leader", rf.me)
			rf.lead()
		case rf.isCandidate():
			log.Printf("Raft %d starting a new campaign, term %d", rf.me, term)
			rf.campaign()
		case rf.isFollower():
			log.Printf("Raft %d starting follow sequence, term %d", rf.me, term)
			rf.follow()
		default:
			log.Panicf("Raft %d invalid state", rf.me)
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf._isCandidate = false
	rf.heartbeatChan = make(chan HeartbeatData)
	rf.newTermChan = make(chan NewTermData)
	rf.applyMsgChan = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	// Set these as -1 until the first snapshot is taken
	rf.lastIncludedTerm = -1
	rf.lastIncludedIndex = -1
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// Values of -1 here are used to indicate that this instance is not a leader
	for idx := range rf.peers {
		rf.nextIndex[idx] = -1
		rf.matchIndex[idx] = -1
	}
	rf.lastApplied = 0
	rf.commitIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
