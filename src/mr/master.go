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

func (m *Master) arrangeTask(response *HeartBeatResponse) bool {
	taskFinished, hasNewWork  := true, false
	
	for idx, task := range m.tasks {
		fmt.Printf("%v ", task.status)
		switch task.status {
		case Idle:
			taskFinished, hasNewWork = false, true
			m.tasks[idx].status, m.tasks[idx].startTime = Working, time.Now()
			response.NReduce, response.Id = m.nReduce, idx
			if m.phase == MapPhase {
				response.WorkType, response.FilePath = Map, task.fileName
			} else {

			}
		case Working:

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
				// fmt.Println("master completed")
				msg.response.WorkType = Completed
			} else if m.arrangeTask(msg.response) {
				fmt.Printf("master phase: %v\n", m.phase)
				switch m.phase {
				case MapPhase:
					fmt.Printf("map finished")
					msg.response.WorkType = Completed
					m.phase = CompletedPhase
					m.doneCh<-struct{}{}
				}
			}
			msg.ok <- struct{}{}
		case msg := <- m.responseCh:
			if msg.request.phase == m.phase {
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
