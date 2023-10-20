package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

func MapDone(id uint64, filename string) {
	mapDoneReq := MapDoneReq{}
	mapDoneReq.ID = id
	mapDoneReq.Filename = filename
	mapDoneRep := MapDoneRep{}
	call("Master.MapDone", &mapDoneReq, &mapDoneRep)
}

func ReduceDone(id int) {
	reduceDoneRequest := ReduceDoneRequest{}
	reduceDoneRequest.ID = id
	reduceDoneRep := ReduceDoneRep{}
	call("Master.ReduceDone", &reduceDoneRequest, &reduceDoneRep)
}

func MapWorker(mapf func(string, string) []KeyValue, mapReply MapReply) {
	defer MapDone(mapReply.ID, mapReply.Filename)
	file, err := os.Open(mapReply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", mapReply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapReply.Filename)
	}
	file.Close()
	kva := mapf(mapReply.Filename, string(content))
	filename := "mr-med-" + fmt.Sprint(mapReply.ID)
	f, err := os.CreateTemp("./", "temp-"+filename)
	if err != nil {
		log.Fatalf("cannot create temp file " + filename)
	}
	j, err := json.Marshal(kva)
	if err != nil {
		log.Fatal(err)
	}
	f.Write(j)
	os.Rename(f.Name(), filename)
	f.Close()
}

func ReduceWorker(reducef func(string, []string) string, mapReply MapReply) {
	defer ReduceDone(mapReply.NReduce)
	files, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("reduce file error")
	}
	kvs := make(map[string][]string)
	for _, file := range files {
		if strings.Contains(file.Name(), "mr-reduce-") {
			s := strings.Split(file.Name(), "-")
			if len(s) == 0 {
				continue
			}
			i, err := strconv.Atoi(s[len(s)-1])
			if err != nil {
				log.Fatal(err)
			}
			if i == mapReply.NReduce {
				content, err := os.ReadFile(file.Name())
				if err != nil {
					log.Fatal(err)
				}
				var fileKvs []KeyValue
				err = json.Unmarshal(content, &fileKvs)
				if err != nil {
					log.Fatal(err)
				}
				for _, kv := range fileKvs {
					kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
				}
			}
		}
	}
	f, err := os.CreateTemp("./", "temp-mr-out-"+fmt.Sprint(mapReply.NReduce))
	defer f.Close()
	defer os.Rename(f.Name(), "mr-out-"+fmt.Sprint(mapReply.NReduce))
	if err != nil {
		log.Fatal(err)
	}
	for k := range kvs {
		output := reducef(k, kvs[k])
		fmt.Fprintf(f, "%v %v\n", k, output)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		mapRequest := MapRequest{}
		mapReply := MapReply{}
		call("Master.GetTask", &mapRequest, &mapReply)
		// 调用map函数
		if mapReply.Kind == 0 {
			MapWorker(mapf, mapReply)
		} else if mapReply.Kind == 1 {
			ReduceWorker(reducef, mapReply)
		} else {
			time.Sleep(time.Second)
		}
		time.Sleep(time.Millisecond * 400)
	}
	// fmt.Println("worker exit")
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
