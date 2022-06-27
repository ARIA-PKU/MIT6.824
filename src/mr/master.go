package mr

import (
	"log"
 	"net"
 	"os"
 	"net/rpc"
 	"net/http"
 	"time"
 	// "fmt"
)

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
	responseCh chan responseMsg
	doneCh chan struct{}
}

type heartbeatMsg struct {
	response *HeartBeatResponse
	ok chan struct{}
}

type responseMsg struct {
	request *ResponseRequest
	ok chan struct{}
}

func (m *Master) initReducePhase() {
	m.phase = ReducePhase
	m.tasks = make([]Task, m.nReduce)
	for i:= 0; i < m.nReduce; i ++ {
		m.tasks[i] = Task{
			id: i,
			status: Idle,
		}
	}
}

func (m *Master) initCompletePhase() {
	m.phase = CompletedPhase
	m.doneCh<-struct{}{}
}


func (m *Master) arrangeTask(response *HeartBeatResponse) bool {
	taskFinished, hasNewWork  := true, false
	for idx, task := range m.tasks {
		// log.Printf("%v ", task.status)
		switch task.status {
		case Idle:
			taskFinished, hasNewWork = false, true
			m.tasks[idx].status, m.tasks[idx].startTime = Working, time.Now()
			response.NReduce, response.Id = m.nReduce, idx
			if m.phase == MapPhase {
				response.WorkType, response.FilePath = Map, task.fileName
			} else if m.phase == ReducePhase {
				response.WorkType, response.Nmap = Reduce, m.nMap
			}
		//  if a worker hasn't completed its task in a reasonable amount of time 
		//  (for this lab, use ten seconds), and give the same task to a different worker.
		case Working:
			taskFinished = false
			if time.Now().Sub(task.startTime) > MaxTaskTime {
				hasNewWork = true
				response.NReduce, response.Id = m.nReduce, idx
				if m.phase == MapPhase {
					response.WorkType, response.FilePath = Map, task.fileName
				} else if m.phase == ReducePhase {
					response.WorkType, response.Nmap = Reduce, m.nMap
				}
			}	
		case Finished:
		}
		if hasNewWork {
			break
		}
	}
	if !hasNewWork {
		response.WorkType = Wait
	}
	return taskFinished
}

// watch all requests
func (m *Master) process() {
	for {
		select {
		// deal with heartbeat condition
		case msg := <-m.heartbeatCh:
			if m.phase == CompletedPhase {
				msg.response.WorkType = Completed
			} else if m.arrangeTask(msg.response) {
				switch m.phase {
				case MapPhase:
					log.Printf("map finished")
					m.initReducePhase()
					m.arrangeTask(msg.response)
				case ReducePhase:
					log.Printf("reduce finished")
					m.initCompletePhase()
					msg.response.WorkType = Completed
				default:
					log.Printf("process go into error phase")
				}
			}
			msg.ok <- struct{}{}
		case msg := <- m.responseCh:
			log.Printf("current phase is: %v, request phase is: %v\n", m.phase, msg.request.Phase)
			if msg.request.Phase == m.phase {
				// log.Printf("Master: Worker has executed task %v \n", msg.request)
				m.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
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

func (m *Master) ReceiveResponse(request *ResponseRequest, response *ResponseResponse) error {
	log.Printf("request phase: %v\n", request)
	msg := responseMsg{request, make(chan struct{})}
	m.responseCh<-msg
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

	<-m.doneCh
	ret = true
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
		responseCh: make(chan responseMsg),
		doneCh: make(chan struct{}, 1),
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
