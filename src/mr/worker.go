package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const TaskInterval = 200 // milliseconds

var workerId int
var reduceCount int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapFn func(string, string) []KeyValue, reduceFn func(string, []string) string) {
	// Your worker implementation here.
	workerId = os.Getpid()
	rCount, succ := getReduceCount()
	if !succ {
		fmt.Printf("Failed to get reduce count config. Worker-%v exiting.\n", workerId)
		return
	}

	reduceCount = rCount

	for {
		reply, succ := requestTask()
		if !succ {
			fmt.Printf("Failed to get task. Worker-%v exiting.\n", workerId)
			return
		}
		if reply.TaskType == ExitTask {
			fmt.Printf("All tasks are finished. Worker-%v exiting.\n", workerId)
			return
		}

		canExit, succ := false, true
		if reply.TaskType == MapTask {
			done := doMap(mapFn, reply.TaskId, reply.FilePath)
			if done {
				canExit, succ = reportTaskFinished(MapTask, reply.TaskId)
			} else {
				succ = reportTaskFailed(MapTask, reply.TaskId)
			}
		} else if reply.TaskType == ReduceTask {
			done := doReduce(reduceFn, reply.TaskId)
			if done {
				canExit, succ = reportTaskFinished(ReduceTask, reply.TaskId)
			} else {
				succ = reportTaskFailed(ReduceTask, reply.TaskId)
			}
		}

		if !succ {
			fmt.Printf("Coordinator has exited. Worker-%v exiting.\n", workerId)
			return
		}
		if canExit {
			fmt.Printf("All tasks are finished. Worker-%v exiting.\n", workerId)
			return
		}

		time.Sleep(time.Millisecond * TaskInterval)
	}
}

func doMap(mapFn func(string, string) []KeyValue, mapId int, filePath string) bool {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Cannot open file: %v [MapId: %v]\n", filePath, mapId)
		return false
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read content from file: %v [MapId: %v]\n", filePath, mapId)
		return false
	}

	kva := mapFn(filePath, string(content))
	return writeMapOutput(kva, mapId)
}

func writeMapOutput(kva []KeyValue, mapId int) bool {
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)
	files := make([]*os.File, 0, reduceCount)
	buffers := make([]*bufio.Writer, 0, reduceCount)
	encoders := make([]*json.Encoder, 0, reduceCount)

	for i := 0; i < reduceCount; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, workerId)
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("Cannot create file: %v [MapId: %v]\n", filePath, mapId)
			return false
		}

		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % reduceCount
		err := encoders[reduceId].Encode(kv)
		if err != nil {
			log.Fatalf("Cannot encode %v to file: %v [MapId: %v]\n", kv, files[reduceId].Name(), mapId)
			return false
		}
	}

	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			log.Fatalf("Cannot flush file: %v [MapId: %v]\n", files[i].Name(), mapId)
			return false
		}
	}

	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("Cannot rename file from %v to %v [MapId: %v]\n", files[i].Name(), newPath, mapId)
			return false
		}
	}

	return true
}

func doReduce(reduceFn func(string, []string) string, reduceId int) bool {
	filePaths, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		log.Fatalf("Cannot to list intermediate files. [ReduceId: %v]\n", reduceId)
		return false
	}

	var kvMap = make(map[string][]string)
	var kv KeyValue

	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Cannot open file: %v [ReduceId: %v]\n", filePath, reduceId)
			return false
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Cannot decode from file: %v [ReduceId: %v]\n", filePath, reduceId)
				return false
			}

			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	return writeReduceOutput(reduceFn, kvMap, reduceId)
}

func writeReduceOutput(reduceFn func(string, []string) string, kvMap map[string][]string, reduceId int) bool {
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, workerId)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Cannot create file: %v [ReduceId: %v]\n", filePath, reduceId)
		return false
	}

	buf := bufio.NewWriter(file)
	for _, k := range keys {
		v := reduceFn(k, kvMap[k])
		_, err := fmt.Fprintf(buf, "%v %v\n", k, v)
		if err != nil {
			log.Fatalf("Cannot write reduce result to file: %v [ReduceId: %v]\n", filePath, reduceId)
			return false
		}
	}

	err = buf.Flush()
	if err != nil {
		log.Fatalf("Cannot flush reduce result to file: %v [ReduceId: %v]\n", filePath, reduceId)
		return false
	}

	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	if err != nil {
		log.Fatalf("Cannot to rename file from %v to %v [ReduceId: %v]\n", filePath, newPath, reduceId)
		return false
	}

	return true
}

func reportTaskFinished(taskType TaskType, taskId int) (bool, bool) {
	args := &ReportTaskFinishedArgs{taskType, taskId, workerId}
	reply := &ReportTaskFinishedReply{}
	succ := call("Coordinator.ReportTaskFinished", args, reply)

	return reply.CanExit, succ
}

func reportTaskFailed(taskType TaskType, taskId int) bool {
	args := &ReportTaskFailedArgs{taskType, taskId, workerId}
	reply := &ReportTaskFailedReply{}
	succ := call("Coordinator.ReportTaskFailed", args, reply)

	return succ
}

func requestTask() (*RequestTaskReply, bool) {
	args := &RequestTaskArgs{workerId}
	reply := &RequestTaskReply{}
	succ := call("Coordinator.RequestTask", args, reply)

	return reply, succ
}

func getReduceCount() (int, bool) {
	args := &GetReduceCountArgs{}
	reply := &GetReduceCountReply{}
	succ := call("Coordinator.GetReduceCount", args, reply)

	return reply.ReduceCount, succ
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
