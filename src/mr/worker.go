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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := getMapTask()
	fileName := reply.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("problem reading %v", fileName)
	}
	file.Close()
	mapped := mapf(fileName, string(content))
	// Collect all of the kvs by which reduce job they will have
	var reduceMap = make(map[int][]KeyValue)
	for _, kv := range mapped {
		reduceNum := getJobNum(kv.Key, reply.NReduce)
		if len(reduceMap[reduceNum]) == 0 {
			reduceMap[reduceNum] = make([]KeyValue, 0)
		}
		reduceMap[reduceNum] = append(reduceMap[reduceNum], kv)
	}
	// Sort all of the slices before writing to a file
	for reduceNum, kvs := range reduceMap {
		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].Key < kvs[j].Key
		})
		oname := fmt.Sprintf("map_out_%d_%d", reply.TaskNum, reduceNum)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		enc.Encode(&kvs)
	}

	fmt.Printf("Finished processing: %v\n", fileName)
}

// RPC call
func getMapTask() GetMapReply {
	args := GetMapArgs{}
	reply := GetMapReply{}
	call("Coordinator.GetMapTask", &args, &reply)
	return reply
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
