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
		log.Fatalf("cannot open file: %v in map task", fileName)
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
	log.Printf("Finished processing: %v\n", fileName)
	return onames, true
}

func processReduceTask(reducef func(string, []string) string, task ReduceTask, nReduce int) bool {
	// Map to store the key -> list of related strings
	rmap := map[string][]string{}
	for _, filename := range task.MapFiles {
		content, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Fatalf("Cannot open filename: %v during reduce task.\n", filename)
			return false
		}
		var data []KeyValue
		err = json.Unmarshal(content, &data)
		if err != nil {
			log.Fatalf("Error unmarshaling reduce file %v\n", filename)
		}
		for _, kv := range data {
			rmap[kv.Key] = append(rmap[kv.Key], kv.Value)
		}
	}
	keyList := []string{}
	for key := range rmap {
		keyList = append(keyList, key)
	}
	sort.Slice(keyList, func(i, j int) bool { return i < j })
	oname := fmt.Sprintf("mr-out-%d", task.TaskNum)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	for _, key := range keyList {
		fmt.Fprintf(ofile, "%v %v\n", key, reducef(key, rmap[key]))
	}
	log.Printf("Finished processing reduce task: %v\n", task.TaskNum)
	return true
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
	for true {
		reply, success := getTask()
		if !success {
			// An RPC error occurred
			log.Fatalf("Failed to get next map task for worker %d", workerId)
			log.Fatal("Retrying in a second...")
			time.Sleep(time.Second)
			continue
		} else if reply.Finished {
			log.Printf("\nFinished with work.")
			break
		} else if reply.AllClaimed {
			// The phase isn't done, but all of the tasks are claimed, presumably another worker is
			// working on the last one
			fmt.Printf("All %v tasks are claimed, waiting and retrying.\n", reply.Type)
			time.Sleep(time.Second)
			continue
		} else if reply.Type == Map {
			// Actually do a map task
			task := reply.MapTask
			onames, success := processMapTask(mapf, task, nReduce)
			if success {
				pushMapDone(task.FileName, onames, task.TaskNum, workerId)
				time.Sleep(time.Second)
			} else {
				log.Fatalf("Map task %d failed for worker %d", task.TaskNum, workerId)
				log.Fatal("Retrying in a second...")
				time.Sleep(time.Second)
			}
		} else {
			// It's a reduce task
			task := reply.ReduceTask
			success = processReduceTask(reducef, task, nReduce)
			if success {
				pushReduceDone(task.TaskNum, workerId)
				time.Sleep(time.Second)
				continue
			} else {
				log.Fatalf("Reduce task %d failed for worker %d", task.TaskNum, workerId)
				log.Fatal("Retrying in a second...")
				time.Sleep(time.Second)
			}
		}
	}
}

//
// RPC stubs
//

func getTask() (GetTaskReply, bool) {
	reply := GetTaskReply{}
	success := call("Coordinator.GetTask", &GetTaskArgs{}, &reply)
	if !success {
		return GetTaskReply{}, false
	}
	return reply, true
}

func pushReduceDone(taskNum int, workerId int) {
	args := PushReduceDoneArgs{
		TaskNum:  taskNum,
		WorkerId: workerId,
	}
	call("Coordinator.PushReduceDone", &args, &PushReduceDoneReply{})
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
