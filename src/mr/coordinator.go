package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp_mr"
const TaskTimeout = 10 // seconds

type TaskType int
type TaskState int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	NoTask
)

const (
	NotStarted TaskState = iota
	InProgress
	Finished
)

type Task struct {
	TaskType  TaskType
	TaskState TaskState
	TaskId    int
	FilePath  string
	WorkerId  int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCount = len(c.reduceTasks)

	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	var task *Task
	if c.nMap > 0 {
		task = selectTask(c.mapTasks)
	} else if c.nReduce > 0 {
		task = selectTask(c.reduceTasks)
	} else {
		task = &Task{ExitTask, NotStarted, -1, "", -1}
	}

	task.WorkerId = args.WorkerId
	task.TaskState = InProgress

	reply.TaskType = task.TaskType
	reply.TaskId = task.TaskId
	reply.FilePath = task.FilePath

	c.mu.Unlock()
	go c.waitForTask(task)

	return nil
}

func (c *Coordinator) ReportTaskFinished(args *ReportTaskFinishedArgs, reply *ReportTaskFinishedReply) error {
	if args.TaskType != MapTask && args.TaskType != ReduceTask {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else {
		task = &c.reduceTasks[args.TaskId]
	}

	if task.TaskState == InProgress && task.WorkerId == args.WorkerId {
		task.TaskState = Finished
		if task.TaskType == MapTask && c.nMap > 0 {
			c.nMap--
		} else if task.TaskType == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}

	reply.CanExit = (c.nMap < 1 && c.nReduce < 1)

	return nil
}

func (c *Coordinator) ReportTaskFailed(args *ReportTaskFailedArgs, reply *ReportTaskFailedReply) error {
	if args.TaskType != MapTask && args.TaskType != ReduceTask {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else {
		task = &c.reduceTasks[args.TaskId]
	}

	if task.TaskState == InProgress && task.WorkerId == args.WorkerId {
		task.TaskState = NotStarted
		task.WorkerId = -1
	}

	return nil
}

func selectTask(tasks []Task) *Task {
	for i := 0; i < len(tasks); i++ {
		if tasks[i].TaskState == NotStarted {
			return &tasks[i]
		}
	}
	return &Task{NoTask, NotStarted, -1, "", -1}
}

func (c *Coordinator) waitForTask(task *Task) {
	if task.TaskType != MapTask && task.TaskType != ReduceTask {
		return
	}

	<-time.After(time.Second * TaskTimeout)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.TaskState == InProgress {
		task.TaskState = NotStarted
		task.WorkerId = -1
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nMap < 1 && c.nReduce < 1
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	reduceCount = nReduce

	c.nMap = nMap
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, nMap)
	c.reduceTasks = make([]Task, 0, nReduce)

	for i, filePath := range files {
		mapTask := Task{MapTask, NotStarted, i, filePath, -1}
		c.mapTasks = append(c.mapTasks, mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{ReduceTask, NotStarted, i, "", -1}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}

	c.cleanupAndPrepare()
	c.server()

	return &c
}

func (c *Coordinator) cleanupAndPrepare() {
	outFiles, _ := filepath.Glob("mr-out*")
	for _, file := range outFiles {
		err := os.Remove(file)
		if err != nil {
			log.Fatalf("Cannot remove file: %v\n", file)
		}
	}

	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory: %v\n", TempDir)
	}

	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory: %v\n", TempDir)
	}
}
