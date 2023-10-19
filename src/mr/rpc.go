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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type HeloRequest struct {
}

type HeloReply struct {
}

type ExampleReply struct {
	Y int
}

// 请求任务
type MapRequest struct {
}

type MapReply struct {
	Filename string
	Succ     bool
	Kind     int8
	NReduce  int
	ID       uint64
}

// 完成Map任务
type MapDoneReq struct {
	ID       uint64
	Filename string
}

type MapDoneRep struct {
}

// 完成Reduce任务
type ReduceDoneRequest struct {
	ID int
}

type ReduceDoneRep struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
