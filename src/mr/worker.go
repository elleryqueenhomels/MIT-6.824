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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId    int
	reduceCount int
}

// main/mrworker.go calls this function.
func Worker(mapFn func(string, string) []KeyValue, reduceFn func(string, []string) string) {
	// Your worker implementation here.
	workerId := os.Getpid()
	rCount, succ := getReduceCount()
	if !succ {
		log.Printf("Failed to get reduce count config. Worker-%v exiting.\n", workerId)
		return
	}

	w := &worker{
		workerId:    workerId,
		reduceCount: rCount,
	}

	for {
		reply, succ := w.requestTask()
		if !succ {
			log.Printf("Failed to get task. Worker-%v exiting.\n", workerId)
			return
		}
		if reply.TaskType == ExitTask {
			log.Printf("All tasks are finished. Worker-%v exiting.\n", workerId)
			return
		}

		canExit, succ := false, true
		if reply.TaskType == MapTask {
			err := w.doMap(mapFn, reply.TaskId, reply.FilePath)
			if err == nil {
				canExit, succ = w.reportTaskFinished(MapTask, reply.TaskId)
			} else {
				log.Printf("Failed to complete map task [TaskId: %v], error: %v", reply.TaskId, err)
				succ = w.reportTaskFailed(MapTask, reply.TaskId)
			}
		} else if reply.TaskType == ReduceTask {
			err := w.doReduce(reduceFn, reply.TaskId)
			if err == nil {
				canExit, succ = w.reportTaskFinished(ReduceTask, reply.TaskId)
			} else {
				log.Printf("Failed to complete reduce task [TaskId: %v], error: %v", reply.TaskId, err)
				succ = w.reportTaskFailed(ReduceTask, reply.TaskId)
			}
		}

		if !succ {
			log.Printf("Coordinator has exited. Worker-%v exiting.\n", workerId)
			return
		}
		if canExit {
			log.Printf("All tasks are finished. Worker-%v exiting.\n", workerId)
			return
		}

		time.Sleep(time.Millisecond * TaskInterval)
	}
}

func (w *worker) doMap(mapFn func(string, string) []KeyValue, mapId int, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("cannot open file: %v [MapId: %v], error: %v", filePath, mapId, err)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read content from file: %v [MapId: %v], error: %v", filePath, mapId, err)
	}

	kva := mapFn(filePath, string(content))
	return w.writeMapOutput(kva, mapId)
}

func (w *worker) writeMapOutput(kva []KeyValue, mapId int) error {
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)
	files := make([]*os.File, 0, w.reduceCount)
	buffers := make([]*bufio.Writer, 0, w.reduceCount)
	encoders := make([]*json.Encoder, 0, w.reduceCount)

	for i := 0; i < w.reduceCount; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, w.workerId)
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("cannot create file: %v [MapId: %v], error: %v", filePath, mapId, err)
		}

		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	for _, kv := range kva {
		reduceId := w.ihash(kv.Key)
		err := encoders[reduceId].Encode(kv)
		if err != nil {
			return fmt.Errorf("cannot encode %v to file: %v [MapId: %v], error: %v", kv, files[reduceId].Name(), mapId, err)
		}
	}

	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			return fmt.Errorf("cannot flush file: %v [MapId: %v], error: %v", files[i].Name(), mapId, err)
		}
	}

	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			return fmt.Errorf("cannot rename file from %v to %v [MapId: %v], error: %v", files[i].Name(), newPath, mapId, err)
		}
	}

	return nil
}

func (w *worker) doReduce(reduceFn func(string, []string) string, reduceId int) error {
	filePaths, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		return fmt.Errorf("cannot to list intermediate files [ReduceId: %v], error: %v", reduceId, err)
	}

	var kvMap = make(map[string][]string)
	var kv KeyValue

	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("cannot open file: %v [ReduceId: %v], error: %v", filePath, reduceId, err)
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				return fmt.Errorf("cannot decode from file: %v [ReduceId: %v], error: %v", filePath, reduceId, err)
			}

			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	return w.writeReduceOutput(reduceFn, kvMap, reduceId)
}

func (w *worker) writeReduceOutput(reduceFn func(string, []string) string, kvMap map[string][]string, reduceId int) error {
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, w.workerId)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("cannot create file: %v [ReduceId: %v], error: %v", filePath, reduceId, err)
	}

	buf := bufio.NewWriter(file)
	for _, k := range keys {
		v := reduceFn(k, kvMap[k])
		_, err := fmt.Fprintf(buf, "%v %v\n", k, v)
		if err != nil {
			return fmt.Errorf("cannot write reduce result to file: %v [ReduceId: %v], error: %v", filePath, reduceId, err)
		}
	}

	err = buf.Flush()
	if err != nil {
		return fmt.Errorf("cannot flush reduce result to file: %v [ReduceId: %v], error: %v", filePath, reduceId, err)
	}

	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	if err != nil {
		return fmt.Errorf("cannot to rename file from %v to %v [ReduceId: %v], error: %v", filePath, newPath, reduceId, err)
	}

	return nil
}

func (w *worker) reportTaskFinished(taskType TaskType, taskId int) (bool, bool) {
	args := &ReportTaskFinishedArgs{taskType, taskId, w.workerId}
	reply := &ReportTaskFinishedReply{}
	succ := call("Coordinator.ReportTaskFinished", args, reply)

	return reply.CanExit, succ
}

func (w *worker) reportTaskFailed(taskType TaskType, taskId int) bool {
	args := &ReportTaskFailedArgs{taskType, taskId, w.workerId}
	reply := &ReportTaskFailedReply{}
	succ := call("Coordinator.ReportTaskFailed", args, reply)

	return succ
}

func (w *worker) requestTask() (*RequestTaskReply, bool) {
	args := &RequestTaskArgs{w.workerId}
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func (w *worker) ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % w.reduceCount
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

	log.Println(err)
	return false
}
