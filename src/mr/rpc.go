package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
// import "fmt"

//
// example to show how to declare the arguments
// and reply for an RPC.
//


// Add your RPC definitions here.

type HeartBeatRequest struct {
}

type HeartBeatResponse struct {
	FilePath string
	WorkType WorkType
	NReduce int
	Nmap int
	Id int
}

type ResponseRequest struct{
	Id int
	Phase OperationPhase
}

type ResponseResponse struct{
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
