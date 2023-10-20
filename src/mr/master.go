package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	fileNum    int
	mapMux     sync.Mutex
	mapAlloc   map[string]int
	mapWork    map[string]int
	mapReduce  map[int]int
	mapID      uint64
	isShuffle  bool
	nReduce    int
	reduceDone int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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

func (m *Master) getReduce(reduceID *int) bool {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	*reduceID = -1
	for k := range m.mapReduce {
		if m.mapReduce[k] == 0 {
			*reduceID = k
			// 1表示该ID已分配
			m.mapReduce[k] = 1
			break
		}
	}
	return *reduceID != -1
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

func (m *Master) shuffle() {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	if m.isShuffle {
		return
	}
	files, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("shuffle error")
	}
	mapKvs := make(map[int][]KeyValue)
	for _, file := range files {
		if strings.Contains(file.Name(), "mr-med-") {
			content, err := os.ReadFile(file.Name())
			if err != nil {
				log.Fatal(err)
			}
			var kvs []KeyValue
			err = json.Unmarshal(content, &kvs)
			if err != nil {
				log.Fatal(err)
			}
			for _, kv := range kvs {
				idx := ihash(kv.Key)%m.nReduce + 1
				mapKvs[idx] = append(mapKvs[idx], kv)
				// f, err := os.OpenFile("mr-reduce-"+fmt.Sprint(idx), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
				// if err != nil {
				// 	log.Fatal(err)
				// }
				// b, err := json.Marshal(&kv)
				// if err != nil {
				// 	log.Fatal(err)
				// }

				// f.Write(b)
				// f.Close()
			}
		}
	}
	for k := range mapKvs {
		f, err := os.OpenFile("mr-reduce-"+fmt.Sprint(k), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		b, err := json.Marshal(mapKvs[k])
		if err != nil {
			log.Fatal(err)
		}
		f.Write(b)
		f.Close()
	}
	m.isShuffle = true
}

func (m *Master) listenReduceWork(reduceID int) {
	time.Sleep(10 * time.Second)
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	if m.mapReduce[reduceID] != 2 {
		m.mapReduce[reduceID] = 0
		m.reduceDone++
	}
}

func (m *Master) ReduceDone(args *ReduceDoneRequest, reply *ReduceDoneRep) error {
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	_, ok := m.mapReduce[args.ID]
	if ok {
		m.mapReduce[args.ID] = 2
		m.reduceDone--
	}
	return nil
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
	} else if m.isShuffle {
		reply.Kind = 101
		reply.Succ = true
		var reduceID int = -1
		reply.ReduceNum = m.nReduce
		if m.getReduce(&reduceID) {
			reply.Kind = 1
			reply.NReduce = reduceID
			go m.listenReduceWork(reduceID)
		}
	} else {
		reply.Kind = 101
		reply.Succ = true
		m.shuffle()
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
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// Your code here.
	m.mapMux.Lock()
	defer m.mapMux.Unlock()
	return m.reduceDone == 0
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
	m.isShuffle = false
	m.nReduce = nReduce
	m.reduceDone = nReduce
	m.mapReduce = make(map[int]int)
	for i := 1; i <= nReduce; i++ {
		m.mapReduce[i] = 0
	}
	m.mapMux.Unlock()
	for _, filename := range files {
		m.addWork(filename)
	}

	m.server()
	return &m
}
