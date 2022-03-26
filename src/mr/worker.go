package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getJobNum(key string, nReduce int) int {
	return ihash(key) % nReduce
}

//
// Process a map task, return the newly created file name that
// holds the intermediate data.
//
func processMapTask(mapf func(string, string) []KeyValue, task MapTask, nReduce int) (map[int][]string, bool) {
	fileName := task.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return map[int][]string{}, false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("problem reading %v", fileName)
		return map[int][]string{}, false
	}
	file.Close()

	mapped := mapf(fileName, string(content))
	// Collect all of the kvs by which reduce job they will have
	// Based on the modulo of the hash of the key to enure there are nReduce buckets
	var reduceMap = make(map[int][]KeyValue)
	for _, kv := range mapped {
		reduceNum := getJobNum(kv.Key, nReduce)
		if len(reduceMap[reduceNum]) == 0 {
			reduceMap[reduceNum] = make([]KeyValue, 0)
		}
		reduceMap[reduceNum] = append(reduceMap[reduceNum], kv)
	}
	// Sort all of the reduce buckets before writing to a file
	onames := make(map[int][]string, nReduce)
	for reduceNum, kvs := range reduceMap {
		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].Key < kvs[j].Key
		})
		oname := fmt.Sprintf("map_out_%d_%d", task.TaskNum, reduceNum)
		onames[reduceNum] = append(onames[reduceNum], oname)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("problem creating %v", oname)
			return map[int][]string{}, false
		}
		enc := json.NewEncoder(ofile)
		enc.Encode(&kvs)
	}
	fmt.Printf("Finished processing: %v\n", fileName)
	return onames, true
}

//
// Process map tasks until all are finished, then return
//
func handleMapPhase(mapf func(string, string) []KeyValue, workerId int, nReduce int) {
	for true {
		reply, success := getMapTask()
		if !success {
			// An RPC error occurred
			log.Fatalf("Failed to get next map task for worker %d", workerId)
			log.Fatal("Retrying in a second...")
			time.Sleep(time.Second)
			continue
		} else if reply.PhaseComplete {
			// This indicates that all of the map tasks are complete, time to start reducin'
			fmt.Println("Mapping phase complete, starting reduce phase")
			break
		} else if reply.AllClaimed {
			// The phase isn't done, but all of the tasks are claimed, presumably another worker is
			// working on the last one
			fmt.Println("All mapping tasks are claimed, waiting and retrying.")
			time.Sleep(time.Second)
			continue
		} else {
			// Actually do a map task
			onames, success := processMapTask(mapf, reply.Task, nReduce)
			if success {
				pushMapDone(reply.Task.FileName, onames, reply.Task.TaskNum, workerId)
				time.Sleep(time.Second)
			} else {
				log.Fatalf("Map task %d failed for worker %d", reply.Task.TaskNum, workerId)
				log.Fatal("Retrying in a second...")
				time.Sleep(time.Second)
			}
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	nReduce, workerId, success := register()
	if !success {
		log.Fatalf("Error initializing worker")
		return
	}
	handleMapPhase(mapf, workerId, nReduce)
}

//
// RPC stubs
//
func getMapTask() (GetMapReply, bool) {
	// Get the next map task, return false if RPC was not successful
	args := GetMapArgs{}
	reply := GetMapReply{}
	success := call("Coordinator.GetMapTask", &args, &reply)
	if !success {
		return GetMapReply{}, false
	}
	return reply, true
}

func pushMapDone(fileName string, outNames map[int][]string, taskNum int, workerId int) {
	args := PushMapDoneArgs{
		FileName: fileName,
		OutNames: outNames,
		TaskNum:  taskNum,
		WorkerId: workerId,
	}
	call("Coordinator.PushMapDone", &args, &PushMapDoneReply{})
}

// Register and get initial data from the coordinator
func register() (int, int, bool) {
	reply := RegisterWorkerReply{}
	success := call("Coordinator.RegisterWorker", &RegisterWorkerArgs{}, &reply)
	if !success {
		return 0, 0, false
	}
	return reply.NReduce, reply.WorkerId, true
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
