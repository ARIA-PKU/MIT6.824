package mr

import (
	"fmt"
 	"log"
	"net/rpc"
 	"hash/fnv"
 	"time"
 	"sync"
 	"os"
 	"io/ioutil"
 	"bytes"
 	"encoding/json"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doHeartbeat() *HeartBeatResponse {
	response := HeartBeatResponse{}
	
	call("Master.HeartBeat", &HeartBeatRequest{}, &response)
	
	return &response
}

// return task results
func doResponseMaster(id int, phase OperationPhase) {
	call("Master.ReceiveResponse", &ResponseRequest{id, phase}, &ResponseResponse{})
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartBeatResponse) {
	fmt.Printf("do map task, filepath:%v\n", response.FilePath)

	// read content from file
	filename := response.FilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediates := make([][]KeyValue, response.NReduce)
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		idx := ihash(kv.Key) % response.NReduce
		intermediates[idx] = append(intermediates[idx], kv)
	}

	// store the content
	var wg sync.WaitGroup
	for idx, intermediate := range intermediates {
		wg.Add(1)
		go func(idx int, intermediate []KeyValue) {
			defer wg.Done()
			storeFilePath := generateMapFilePath(response.Id, idx)
					
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				// fmt.Printf("%v\n", kv)
			  	err := enc.Encode(&kv)
			 	if err != nil {
					fmt.Printf("cannot encode: %v", kv)
				}
			}
			atomicWriteFile(storeFilePath, &buf)
		
		}(idx, intermediate)
	}
	wg.Wait()

	doResponseMaster(response.Id, MapPhase)
}

func doReduceTask(reducef func(string, []string) string, response *HeartBeatResponse) {
	fmt.Printf("do reduce task, filepath:%v\n", response.FilePath)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		response := doHeartbeat()
		fmt.Printf("%v", response.WorkType)
		switch response.WorkType {
		case Map:
			doMapTask(mapf, response)
		case Reduce:
			doReduceTask(reducef, response)
		case Wait:
			time.Sleep(time.Second)
		case Completed:
			return
		default:
			fmt.Printf("not Defined Work")
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
