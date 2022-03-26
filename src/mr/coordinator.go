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

type taskStatus int

const (
	Pending taskStatus = iota
	Complete
	Ready
)

type MapTask struct {
	State    taskStatus
	FileName string
	TaskNum  int
}

type ReduceTask struct {
	State    taskStatus
	OutFile  string
	MapFiles []string
	TaskNum  int
}

type WorkerInstance struct {
	id int
}

type Coordinator struct {
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	fMu         sync.Mutex
	nReduce     int
	workers     []WorkerInstance
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	fmt.Print("Server running, waiting for worker registration... \n")
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

func (c *Coordinator) mapDone() bool {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	for _, task := range c.mapTasks {
		if task.State != Complete {
			return false
		}
	}
	return true
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if c.mapDone() {
		// Temporary to check that all of the reducer tasks have the correct files assigned
		for _, task := range c.reduceTasks {
			fmt.Printf("Reducer task Files: %v\n", task.MapFiles)
		}
		return true
	}
	return false
}

// Try to get the next mapTask that is available for work
// If all tasks are already claimed, return false
func (c *Coordinator) claimNextMapTask() (MapTask, bool) {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	for idx, mt := range c.mapTasks {
		if mt.State == Ready {
			c.mapTasks[idx].State = Pending
			return mt, true
		}
	}
	return MapTask{}, false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]MapTask, 0)
	reduceTasks := make([]ReduceTask, nReduce)
	for idx, filename := range files {
		mapTask := MapTask{
			State:    Ready,
			FileName: filename,
			TaskNum:  idx,
		}
		mapTasks = append(mapTasks, mapTask)
	}
	c := Coordinator{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		nReduce:     nReduce,
	}
	c.server()
	return &c
}

// RPC

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	reply.NReduce = c.nReduce
	reply.WorkerId = len(c.workers) + 1
	c.fMu.Lock()
	defer c.fMu.Unlock()
	c.workers = append(c.workers, WorkerInstance{id: reply.WorkerId})
	fmt.Printf("Registered worker: %v\n", reply.WorkerId)
	return nil
}

// Try to get a new map task to send to the worker.
func (c *Coordinator) GetMapTask(args *GetMapArgs, reply *GetMapReply) error {
	reply.PhaseComplete = c.mapDone()
	reply.AllClaimed = reply.PhaseComplete
	mt, success := c.claimNextMapTask()
	if success {
		reply.Task = mt
		fmt.Printf("Claimed %v\n", reply.Task.FileName)
	} else {
		reply.Task = MapTask{}
		reply.AllClaimed = true
		if !reply.PhaseComplete {
			fmt.Print("All map tasks claimed.\n")
		}
	}
	return nil
}

// TODO need to status the workers and swap a task back from pending to ready

// Mark a mapping task done, store the name of the map output files
func (c *Coordinator) PushMapDone(args *PushMapDoneArgs, reply *PushMapDoneReply) error {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	found := false
	for idx, mt := range c.mapTasks {
		if mt.TaskNum == args.TaskNum {
			found = true
			// Don't update a map task that was already complete
			// e.g. maybe this is a slow worker
			if mt.State == Complete {
				break
			}
			c.mapTasks[idx].State = Complete
			fmt.Printf("Completed processing for file %v\n\n", mt.FileName)
			// Disseminate the map output files for later processing by the reducer workers
			for reduceNum, onames := range args.OutNames {
				task := &c.reduceTasks[reduceNum]
				task.State = Ready
				mapfiles := &task.MapFiles
				*mapfiles = append(*mapfiles, onames...)
			}

		}
	}
	if !found {
		return fmt.Errorf("Could not find maptask in coordinator for file: %v", args.FileName)
	}
	return nil
}
