package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapDone(id uint64, filename string) {
	mapDoneReq := MapDoneReq{}
	mapDoneReq.ID = id
	mapDoneReq.Filename = filename
	mapDoneRep := MapDoneRep{}
	call("Master.MapDone", &mapDoneReq, &mapDoneRep)
}

func MapWorker(mapf func(string, string) []KeyValue, mapReply MapReply) {
	fmt.Println("map")
	fmt.Println(mapReply.Filename)
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
	fmt.Println(filename)
	f, err := os.CreateTemp("./", "temp-"+filename)
	if err != nil {
		log.Fatalf("cannot create temp file " + filename)
	}
	enc := json.NewEncoder(f)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("json encode fail")
		}
	}
	os.Rename(f.Name(), filename)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	mapRequest := MapRequest{}
	mapReply := MapReply{}
	for call("Master.GetTask", &mapRequest, &mapReply); mapReply.Succ; call("Master.GetTask", &mapRequest, &mapReply) {
		// 调用map函数
		if mapReply.Kind == 0 {
			go MapWorker(mapf, mapReply)
		} else if mapReply.Kind == 1 {
			fmt.Println("reduce")
		} else {
			fmt.Println("other")
			time.Sleep(time.Second)
		}
		time.Sleep(time.Second)

	}
	fmt.Println("worker exit")
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
	fmt.Printf("reply.Y %v\n", reply.Y)
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
