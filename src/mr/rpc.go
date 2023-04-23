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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

func (t TaskType) isMap() bool {
	return t == 1
}
func (t TaskType) isReduce() bool {
	return t == 2
}

type TaskType int

type Task struct {
	Tid            int
	Nreduce        int
	Ttype          TaskType
	Mapfname       string
	Mapfidx        string
	Reducefidxs    []string
	Reducefidx     string
	ReduceFilterId string
}

type Response struct {
	Tid   int
	Ttype TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
