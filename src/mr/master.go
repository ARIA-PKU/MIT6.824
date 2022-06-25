package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
// import "fmt"

type (
	TaskStatus uint8
	OperationPhase string
)

type Task struct {
	fileName string
	id int
	startTime time.Time
	status TaskStatus
}

type Master struct {
	// Your definitions here.
	files []string
	nReduce int
	nMap int
	phase OperationPhase
	tasks []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files: files,
		nReduce: nReduce,
		nMap: len(files),
	}

	// Your code here.
	// fmt.Printf("%v", files)

	m.server()
	m.phase = "MapWork"
	m.tasks = make([]Task, len(m.files))
	for idx, file := m.files {
		m.task[idx] = {
			fileName: file,
			id: idx,
		}
	}
	
	return &m
}
