package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//

type GetMapArgs struct{}
type GetMapReply struct {
	Task          MapTask
	PhaseComplete bool
	AllClaimed    bool
}

type PushMapDoneArgs struct {
	FileName string
	OutNames map[int][]string
	TaskNum  int
	WorkerId int
}
type PushMapDoneReply struct{}

type RegisterWorkerArgs struct{}
type RegisterWorkerReply struct {
	NReduce  int
	WorkerId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	// fmt.Printf("uid: %d", os.Getuid())
	s += strconv.Itoa(os.Getuid())
	return s
}
