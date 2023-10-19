package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	fileNum   int
	mapMux    sync.Mutex
	mapAlloc  map[string]int
	mapWork   map[string]int
	mapID     uint64
	isShuffle bool
}

func (m *Master) addWork(filename string) {
	m.mapMux.Lock()
	m.mapWork[filename] = 0
	m.mapMux.Unlock()
}

func (m *Master) allocWork(ID int) (string, uint64, bool) {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	m.mapID = m.mapID + 1
	if len(m.mapWork) == 0 {
		return "", m.mapID, false
	}
	filename := ""
	for k := range m.mapWork {
		filename = k
		break
	}
	m.mapAlloc[filename] = 0
	delete(m.mapWork, filename)
	fmt.Println("alloc work: " + filename + ", ID: " + fmt.Sprint(m.mapID))
	return filename, m.mapID, true
}

func (m *Master) delAlloc(filename string) {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	v, ok := m.mapAlloc[filename]
	if !ok {
		log.Fatalf("alloc work no the file: " + filename)
	}
	if v != 1 {
		delete(m.mapAlloc, filename)
		m.mapWork[filename] = 0
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) listenMapWork(filename string) {
	time.Sleep(10 * time.Second)
	m.delAlloc(filename)
}

func (m *Master) GetTask(args *MapRequest, reply *MapReply) error {
	if !m.isAllMapDone() {
		taskname, mapID, ok := m.allocWork(0)
		if !ok {
			reply.Kind = 100
			reply.Succ = true
		} else {
			reply.Filename = taskname
			reply.Kind = 0
			reply.Succ = true
			reply.ID = mapID
			go m.listenMapWork(taskname)
		}
	} else {
		reply.Kind = 1
		reply.Succ = true
	}
	return nil
}

func (m *Master) MapDone(args *MapDoneReq, reply *MapDoneRep) error {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	v, ok := m.mapAlloc[args.Filename]
	if ok && v == 0 {
		m.mapAlloc[args.Filename] = 1
		m.fileNum = m.fileNum - 1
	}
	return nil
}

func (m *Master) isAllMapDone() bool {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	return m.fileNum == 0
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	fmt.Println("server name " + sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapMux.Lock()
	m.fileNum = len(files)
	m.mapAlloc = make(map[string]int)
	m.mapWork = make(map[string]int)
	fmt.Println("file size: " + fmt.Sprint(m.fileNum))
	m.mapMux.Unlock()
	for _, filename := range files {
		m.addWork(filename)
		fmt.Println("add work: " + filename)
	}

	m.server()
	return &m
}
