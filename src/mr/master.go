package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"

// definetion of data structure
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

	heartbeatCh chan heartbeatMsg
}

type heartbeatMsg struct {
	response *HeartBeatResponse
	ok chan struct{}
}

func (m *Master) arrangeTask(response *HeartBeatResponse) bool {
	TaskFinished := true
	
	for idx, task := range m.tasks {
		fmt.Printf("%v ", task.status)
		switch task.status {
		case Idle:
			fmt.Printf("%v\n", task.fileName)
			fmt.Printf("%v\n", m.phase)
			TaskFinished = false
			m.tasks[idx].status, m.tasks[idx].startTime = Working, time.Now()
			response.NReduce, response.Id = m.nReduce, idx
			if m.phase == MapPhase {
				response.WorkType, response.FilePath = Map, task.fileName
			} else {

			}
		default:
		}
	
	}
	return TaskFinished
}

// 
func (m *Master) process() {
	for {
		msg := <-m.heartbeatCh
		
		if m.phase == CompletedPhase {
			fmt.Println("master completed")
			msg.response.WorkType = Completed
		} else if m.arrangeTask(msg.response) {
			fmt.Printf("master phase: %v\n", m.phase)
			switch m.phase {
			case MapPhase:
				fmt.Printf("map finished")
				msg.response.WorkType = Completed
			}
		}
		msg.ok <- struct{}{}
	}
}

// Your code here -- RPC handlers for the worker to call.

// the RPC argument and reply types are defined in rpc.go.

func (m *Master) HeartBeat(request *HeartBeatRequest, response *HeartBeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	m.heartbeatCh<-msg
	<-msg.ok
	return nil
}


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
		heartbeatCh: make(chan heartbeatMsg),
	}

	m.server()

	// init work with 'map' state
	m.phase = MapPhase
	m.tasks = make([]Task, len(m.files))
	for idx, file := range m.files {
		m.tasks[idx] = Task{
			fileName: file,
			id: idx,
			status: Idle,
		}
	}
	
	go m.process()

	return &m
}
