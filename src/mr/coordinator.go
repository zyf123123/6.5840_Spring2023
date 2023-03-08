package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files          []string
	mapIndex       int
	nReduce        int
	reduceIndex    int
	lock           *sync.Mutex
	completeMap    []bool
	completeReduce []bool
	flag           bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CoordinatorReply(args *WorkerArgs, reply *CoordinatorReply) error {
	c.lock.Lock()

	if c.mapIndex < len(c.files) {
		reply.Task = 1
		reply.MapNum = c.mapIndex
		reply.Taskname = c.files[c.mapIndex]
		c.mapIndex += 1

	} else if c.reduceIndex < c.nReduce {
		reply.Task = 2
		reply.ReduceNum = c.reduceIndex
		c.reduceIndex += 1
	}

	reply.Lock = true

	reply.NMap = len(c.files)
	//reply.MapNum = c.mapIndex
	reply.NReduce = c.nReduce
	//reply.ReduceNum = c.reduceIndex
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) WorkDone(args *WorkerArgs, reply *WorkDone) error {
	if args.Job == 1 {
		c.completeMap[args.Done] = true

	} else if args.Job == 2 {

		c.completeReduce[args.Done] = true
	}
	return nil
}

func (c *Coordinator) WorkComplete(args *WorkerArgs, reply *WorkComplete) error {
	if args.Job == 1 {
		reply.MapWorkComplete = true
		for i := 0; i < len(c.files); i++ {
			if c.completeMap[i] != true {
				time.Sleep(time.Second * 5)
				//fmt.Printf("%v ", i)
				if c.completeMap[i] != true {
					c.mapIndex = i
					reply.MapWorkComplete = false
				}
			}
		}
	} else if args.Job == 2 {
		reply.ReduceWorkComplete = true
		for i := 0; i < c.nReduce; i++ {
			if c.completeReduce[i] != true {
				//time.Sleep(time.Second)
				c.reduceIndex = i
				reply.ReduceWorkComplete = false

			}
		}

	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.flag = true
	for i := 0; i < c.nReduce; i++ {
		if c.completeReduce[i] != true {
			//c.reduceIndex = i
			c.flag = false
		}
	}
	return c.flag
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapIndex := 0
	reduceIndex := 0
	lock := &sync.Mutex{}
	completeMap := make([]bool, 100)
	completeReduce := make([]bool, 100)
	flag := false

	c := Coordinator{files, mapIndex, nReduce, reduceIndex, lock, completeMap, completeReduce, flag}

	// Your code here.

	c.server()
	return &c
}
