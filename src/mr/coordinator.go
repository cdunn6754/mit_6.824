package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	fileNames []string
	fMu       sync.Mutex
}

// RPC
func (c *Coordinator) GetMapTask(args *GetMapArgs, reply *GetMapReply) error {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	if len(c.fileNames) > 0 {
		reply.FileName, c.fileNames = c.fileNames[0], c.fileNames[1:]
	} else {
		reply.FileName = ""
	}
	fmt.Printf("Returning: %v\n", reply.FileName)
	fmt.Printf("Remaining count: %d\n\n", len(c.fileNames))
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	if len(c.fileNames) == 0 {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		fileNames: files,
	}
	c.server()
	return &c
}
