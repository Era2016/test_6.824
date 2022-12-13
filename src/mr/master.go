package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	nReduce        int // number of reduce task
	nMap           int // number of map task
	files          []string
	mapfinished    int        // number of finished map task
	maptasklog     []int      // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducefinished int        // number of finished map task
	reducetasklog  []int      // log for reduce task
	cntMapTask     int        // current assigned reduce number
	cntReduceTask  int        // current assigned map number
	mu             sync.Mutex // lock
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Mapfinshed(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.maptasklog[args.MapTaskNumber] = 2
	m.mapfinished++
	m.mu.Unlock()
	return nil
}

func (m *Master) Reducefinshed(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.reducetasklog[args.ReduceTaskNumber] = 2
	m.reducefinished++
	m.mu.Unlock()
	return nil
}

func (m *Master) Deploytask(args *WorkerArgs, reply *WorkerReply) error {

	fmt.Printf("receive the req, args: %v\n", args)
	fmt.Println(m.cntMapTask, m.nMap, m.mapfinished)

	if m.cntMapTask < m.nMap { // map task
		m.mu.Lock()
		m.maptasklog[m.cntMapTask] = 1
		cnt := m.cntMapTask
		m.cntMapTask++
		m.mu.Unlock()

		reply.Tasktype = 0
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce

		reply.Filename = m.files[cnt]
		reply.MapTaskNumber = cnt
	} else if m.mapfinished == m.nMap && m.cntReduceTask < m.nReduce { // reduce task
		m.mu.Lock()
		m.reducetasklog[m.cntReduceTask] = 1
		cnt := m.cntReduceTask
		m.cntReduceTask++
		m.mu.Unlock()

		reply.Tasktype = 1
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce

		reply.ReduceTaskNumber = cnt
	} else { // waiting
		reply.Tasktype = 2
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// ret := m.mapfinished == m.nMap
	ret := m.reducefinished == m.nReduce
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.nMap = len(files)
	m.nReduce = nReduce
	m.maptasklog = make([]int, m.nMap)
	m.reducetasklog = make([]int, m.nReduce)
	m.cntMapTask = 0
	m.cntReduceTask = 0
	m.server()
	return &m
}
