package raft

import "fmt"

type RaftState int

const (
	Follower RaftState = iota + 1
	Leader
	Candidate
)

func (rs RaftState) String() string {
	switch rs {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return fmt.Sprintf("%d", rs)
	}
}
