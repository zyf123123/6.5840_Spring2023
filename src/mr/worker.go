package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	cond := sync.NewCond(&sync.Mutex{})

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for true {
		ret := CallWorker(mapf, reducef, cond)
		//time.Sleep(time.Second)
		//fmt.Printf("%v\n", ret)
		if ret == -1 || ret == 0 {
			break
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

var mapWorkDone bool = false

func CallWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	cond *sync.Cond) int {

	args := WorkerArgs{}
	args.Job = 0
	args.Done = 0
	reply := CoordinatorReply{}
	//time.Sleep(time.Second)

	ok := call("Coordinator.CoordinatorReply", &args, &reply)
	if ok {
		if reply.Task == 1 { //map
			fmt.Printf("map work %v\n", reply.MapNum)
			//cond.L.Lock()
			//done = false
			WorkerMap(reply.Taskname, mapf, reply.MapNum, reply.NReduce)
			//done = true
			//cond.L.Unlock()
			//cond.Broadcast()
			args.Job = 1
			args.Done = reply.MapNum
			done := WorkDone{}
			call("Coordinator.WorkDone", &args, &done)

		} else if reply.Task == 2 { //reduce
			args.Job = 1
			complete := WorkComplete{}
			call("Coordinator.WorkComplete", &args, &complete)
			if !complete.MapWorkComplete {
				time.Sleep(time.Second)
				return 1
				//fmt.Printf("%v ", complete.MapWorkComplete)
			}
			fmt.Printf("reduce work %v\n", reply.ReduceNum)

			WorkerReduce(reducef, reply.ReduceNum, reply.NMap)
			args.Job = 2
			args.Done = reply.ReduceNum
			done := WorkDone{}
			call("Coordinator.WorkDone", &args, &done)
		} else { // end
			args.Job = 2
			complete := WorkComplete{}
			call("Coordinator.WorkComplete", &args, &complete)

			if !complete.ReduceWorkComplete {
				time.Sleep(time.Second)
				return 1
			}
			//time.Sleep(time.Second * 10)

			fmt.Printf("Worker exit\n")
			return 0
		}

	} else { //wrong
		fmt.Printf("call failed!\n")
		return -1
	}
	return reply.Task
}

func WorkerMap(filename string,
	mapf func(string, string) []KeyValue,
	mapNum int,
	nReduce int) {

	reduceNum := 0
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	//onamelist := []string{}
	ofilelist := []*os.File{}

	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(mapNum) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)

		//onamelist = append(onamelist, oname)
		ofilelist = append(ofilelist, ofile)

	}

	for _, kv := range intermediate {
		//fmt.Printf("%s %s\n", kv.Key, kv.Value)
		reduceNum = ihash(kv.Key) % nReduce
		enc := json.NewEncoder(ofilelist[reduceNum])
		err := enc.Encode(&kv)
		if err != nil {
			break
		}
	}

}

func WorkerReduce(reducef func(string, []string) string,
	reduceNum int,
	nMap int) {
	kva := []KeyValue{}

	for i := 0; i < nMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(reduceNum)

	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
