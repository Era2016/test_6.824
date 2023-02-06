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

type job struct {
	timer time.Timer
	stat  int
}

type Master struct {
	// Your definitions here.
	nReduce int // number of reduce task
	nMap    int // number of map task
	files   []string

	mapfinished int // number of finished map task
	//maptasklog     []int // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducefinished int // number of finished map task
	//reducetasklog  []int // log for reduce task

	maptaskRecord    []job // log for map task, 0: not allocated, 1: waiting, 2:finished
	reducetaskRecord []job // log for reduce task

	mu sync.RWMutex // lock
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Mapfinshed(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.maptaskRecord[args.MapTaskNumber].stat = 2
	m.maptaskRecord[args.MapTaskNumber].timer.Stop()
	m.mapfinished++
	m.mu.Unlock()
	return nil
}

func (m *Master) Reducefinshed(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	m.reducetaskRecord[args.ReduceTaskNumber].stat = 2
	m.reducetaskRecord[args.ReduceTaskNumber].timer.Stop()
	m.reducefinished++
	m.mu.Unlock()
	return nil
}

func (m *Master) Deploytask(args *WorkerArgs, reply *WorkerReply) error {

	//fmt.Printf("receive the req, args: %v\n", args)
	// defer func() {
	// 	fmt.Printf("[Tasktype: %d], [NMap: %d], [NReduce: %d], [MapTaskNumber: %d], [Filename: %s], [ReduceTaskNumber: %d]\n",
	// 		reply.Tasktype, reply.NMap, reply.NReduce, reply.MapTaskNumber,
	// 		reply.Filename, reply.ReduceTaskNumber)
	// }()

	m.mu.Lock()
	//defer m.mu.Unlock()

	if m.mapfinished != m.nMap {

		if m.mapfinished > m.nMap {
			fmt.Println(m.mapfinished)
		}
		for i := 0; i < m.nMap; i++ {
			if m.maptaskRecord[i].stat != 0 {
				continue
			}

			m.maptaskRecord[i].stat = 1
			m.maptaskRecord[i].timer = *time.NewTimer(time.Second * 10)

			go func(index int) {
				t := m.maptaskRecord[index].timer
				<-t.C

				m.mu.Lock()
				m.maptaskRecord[index].stat = 0
				m.maptaskRecord[index].timer.Stop()
				m.mu.Unlock()

			}(i)
			m.mu.Unlock()

			reply.Tasktype = 0
			reply.NMap = m.nMap
			reply.NReduce = m.nReduce

			reply.Filename = m.files[i]
			reply.MapTaskNumber = i
			return nil
		}

	} else if m.mapfinished == m.nMap && m.reducefinished != m.nReduce {

		for i := 0; i < m.nReduce; i++ {
			if m.reducetaskRecord[i].stat != 0 {
				continue
			}

			m.reducetaskRecord[i].stat = 1
			m.reducetaskRecord[i].timer = *time.NewTimer(time.Second * 10)

			go func(index int) {
				t := m.reducetaskRecord[index].timer
				<-t.C

				m.mu.Lock()
				m.reducetaskRecord[index].stat = 0
				m.reducetaskRecord[index].timer.Stop()
				m.mu.Unlock()

			}(i)
			m.mu.Unlock()

			reply.Tasktype = 1
			reply.NMap = m.nMap
			reply.NReduce = m.nReduce

			reply.ReduceTaskNumber = i
			return nil
		}
	}

	m.mu.Unlock()
	reply.Tasktype = 2

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

	m.maptaskRecord = make([]job, m.nMap)
	m.reducetaskRecord = make([]job, m.nReduce)

	m.server()
	return &m
}
