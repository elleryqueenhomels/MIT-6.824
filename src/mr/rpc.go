package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	ReduceCount int
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskId   int
	FilePath string
}

type ReportTaskFinishedArgs struct {
	TaskType TaskType
	TaskId   int
	WorkerId int
}

type ReportTaskFinishedReply struct {
	CanExit bool
}

type ReportTaskFailedArgs struct {
	TaskType TaskType
	TaskId   int
	WorkerId int
}

type ReportTaskFailedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
