package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//func init() {
//	file := "./" +"message1"+ ".txt"
//	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
//	if err != nil {
//		panic(err)
//	}
//	log.SetOutput(logFile) // 将文件设置为log输出的文件
//	log.SetPrefix("[qSkipTool]")
//	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
//	return
//}
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// request
		request := RequestArg{}
		request.ReqId = time.Now().Unix()
		request.Type = ReqTask
		// response
		reply := ResponseArg{}

		call("Coordinator.Invoke", &request, &reply)
		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf, reply.ResContent, reply, request)
			break
		case ReduceTask:
			doReduceTask(reducef, reply, request)
		case WaitTask:
			log.Println("waiting....")
			time.Sleep(time.Duration(2) * time.Second)
		case AllTaskDone:
			log.Println("done")
		default:
			time.Sleep(time.Duration(10) * time.Second)
			log.Println("unexpected type")
		}

	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func doMapTask(mapf func(string, string) []KeyValue, fileName string, reply ResponseArg, request RequestArg) bool {
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("can't open %v", file.Name())
	}
	content, e := ioutil.ReadAll(file)
	if e != nil {
		log.Println("can't read %v", file.Name())
	}
	file.Close()

	// create temp file
	files := make([]*os.File, reply.ReduceTaskNum)
	for i := 0; i < reply.ReduceTaskNum; i++ {
		fileName = "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)
		createFile, err := os.Create(fileName)
		if err != nil {
			log.Println("create temp file exception")
		}

		files[i] = createFile
		log.Println("map task " + strconv.Itoa(reply.TaskID) + " " + files[i].Name())
		defer createFile.Close()

	}
	values := mapf(file.Name(), string(content))
	for i := 0; i < len(values); i++ {
		index := ihash(values[i].Key) % (reply.ReduceTaskNum)
		enc := json.NewEncoder(files[index])
		err := enc.Encode(&values[i])
		if err != nil {
			log.Println("write " + files[index].Name() + " exception ")
		}
	}
	req := RequestArg{}
	req.ReqId = request.ReqId
	req.TaskId = reply.TaskID
	req.Type = MapTaskDone

	response := ResponseArg{}
	return call("Coordinator.Invoke", &req, &response)

}

func doReduceTask(reducef func(string, []string) string, reply ResponseArg, arg RequestArg) bool {

	intermediate := []KeyValue{}
	file := os.File{}
	for i := 0; i < reply.MapCnt; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)
		file, e := os.Open(fileName)
		if e != nil {
			log.Println("reduce task open " + fileName + " has exception")
		}
		log.Println("reduce task " + strconv.Itoa(reply.TaskID))

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			e := dec.Decode(&kv)
			if e != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}
	defer file.Close()

	sort.Sort(ByKey(intermediate))
	ofilename := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, e := os.Create(ofilename)
	defer ofile.Close()
	if e != nil {
		log.Println("open " + ofilename + " fail")
	}
	log.Println(ofilename)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	req := RequestArg{}
	req.ReqId = arg.ReqId
	req.Type = ReduceTaskDone
	req.TaskId = reply.TaskID
	response := ResponseArg{}

	return call("Coordinator.Invoke", &req, &response)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
