package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskstate int

const (
	Ready taskstate = iota
	Complete
	Pending
)

type MapTask struct {
	State    taskstate
	FileName string
	TaskNum  int
}

type ReduceTask struct {
	State    taskstate
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
// Wait 10 seconds, if the task isn't done yet, assume it failed
//
func (c *Coordinator) mapTaskTimeout(task *MapTask) {
	time.Sleep(10 * time.Second)
	c.fMu.Lock()
	defer c.fMu.Unlock()
	if task.State == Pending {
		log.Printf("Map task with filename %v timed out and was set to ready.\n", task.FileName)
		task.State = Ready
	}
}

func (c *Coordinator) reduceTaskTimeout(task *ReduceTask) {
	time.Sleep(10 * time.Second)
	c.fMu.Lock()
	defer c.fMu.Unlock()
	if task.State == Pending {
		log.Printf("Reduce task number %v timed out and was set to ready.\n", task.TaskNum)
		task.State = Ready
	}
}

//
// Start an RPC server to listen for the workers
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

func (c *Coordinator) reduceDone() bool {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	for _, task := range c.reduceTasks {
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
	return c.mapDone() && c.reduceDone()
}

// Try to get the next mapTask that is available for work
// If all tasks are already claimed, return false
func (c *Coordinator) claimNextMapTask() (*MapTask, bool) {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	for idx, mt := range c.mapTasks {
		if mt.State == Ready {
			c.mapTasks[idx].State = Pending
			return &c.mapTasks[idx], true
		}
	}
	return &MapTask{}, false
}

// Try to get the next reduceTask that is available for work
// If all tasks are already claimed, return false
func (c *Coordinator) claimNextReduceTask() (*ReduceTask, bool) {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	for idx, rt := range c.reduceTasks {
		if rt.State == Ready {
			prt := &c.reduceTasks[idx]
			prt.State = Pending
			return prt, true
		}
	}
	return &ReduceTask{}, false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]MapTask, 0)
	for idx, filename := range files {
		mapTask := MapTask{
			State:    Ready,
			FileName: filename,
			TaskNum:  idx,
		}
		mapTasks = append(mapTasks, mapTask)
	}
	reduceTasks := make([]ReduceTask, nReduce)
	for idx := range reduceTasks {
		// Make the TaskNum match the index in the array
		// This helps later so that we know which reduce tasks correspond to which
		// map outputs
		prt := &reduceTasks[idx]
		prt.TaskNum = idx
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
	c.fMu.Lock()
	defer c.fMu.Unlock()
	reply.WorkerId = len(c.workers) + 1
	c.workers = append(c.workers, WorkerInstance{id: reply.WorkerId})
	log.Printf("Registered worker: %v\n", reply.WorkerId)
	return nil
}

// Try to get a task to send to the worker.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Within a given phase it's possible for all of the task to be claimed before
	// the phase is complete. I.e. there is none that are ready but at least one that
	// is pending, represent that state with AllClaimed = true
	reply.AllClaimed = false
	// Finished is true when both the map and reduce phases are complete
	reply.Finished = c.Done()
	// Check which phase we are in, map vs reduce
	if !c.mapDone() {
		reply.Type = Map
		pmt, success := c.claimNextMapTask()
		if success {
			reply.MapTask = *pmt
			go c.mapTaskTimeout(pmt)
			log.Printf("Claimed map task with filename: %v\n", pmt.FileName)
		} else {
			reply.AllClaimed = true
			log.Print("All map tasks claimed.\n")
		}
	} else if !c.reduceDone() {
		reply.Type = Reduce
		prt, success := c.claimNextReduceTask()
		if success {
			reply.ReduceTask = *prt
			go c.reduceTaskTimeout(prt)
			log.Printf("Claimed reduce task number: %v\n", prt.TaskNum)
		} else {
			reply.AllClaimed = true
			log.Print("All reduce tasks claimed.\n")
		}
	}
	return nil
}

func (c *Coordinator) PushReduceDone(args *PushReduceDoneArgs, reply *PushReduceDoneReply) error {
	c.fMu.Lock()
	defer c.fMu.Unlock()
	found := false
	for idx, rt := range c.reduceTasks {
		if rt.TaskNum == args.TaskNum {
			found = true
			c.reduceTasks[idx].State = Complete
		}
	}
	if !found {
		return fmt.Errorf("Could not find reducetask for reduce number: %v, worker id: %v",
			args.TaskNum, args.WorkerId)
	}
	return nil
}

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
