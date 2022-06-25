package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type HeartBeatRequest struct {
}

type HeartBeatResponse struct {
	FilePath string
	WorkType WorkType
	NReduce int
	Nmap int
	Id int
}

func (response HeartBeatResponse) String() string {
	switch response.WorkType {
	case Map:
		return fmt.Sprintf("{JobType:%v,FilePath:%v,Id:%v,NReduce:%v}", response.WorkType, response.FilePath, response.Id, response.NReduce)
	case Reduce:
		// return fmt.Sprintf("{JobType:%v,Id:%v,NMap:%v,NReduce:%v}", response.WorkType, response.Id, response.NMap, response.NReduce)
	
	}
	panic(fmt.Sprintf("unexpected JobType %d", response.WorkType))
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
