package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// request type
type ReqType int

const (
	ReqTask ReqType = iota
	MapTask
	ReduceTask
	WaitTask
	MapTaskDone
	ReduceTaskDone
	AllTaskDone
)

type RequestArg struct {
	ReqId  int64
	TaskId int
	Type   ReqType
}

type ResponseArg struct {
	Success       bool
	ResContent    string
	TaskType      ReqType
	TaskID        int
	ReduceTaskNum int
	MapCnt        int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
