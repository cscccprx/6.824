package mr

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapCnt int
	// change
	MapTaskNum    int
	ReduceTaskNum int
	MapTaskList   []string
	MapRunning    []int
	ReduceRunning []int
	MapChannel    chan int
	ReduceChannel chan int
	allDone       bool
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Invoke(args *RequestArg, reply *ResponseArg) error {
	if c.allDone {
		reply.TaskType = AllTaskDone
		return nil
	}

	switch args.Type {
	case ReqTask:
		if len(c.MapChannel) > 0 {
			// map task remain,allocate it
			reply.TaskID = <-c.MapChannel
			reply.ResContent = c.MapTaskList[reply.TaskID]
			reply.ReduceTaskNum = c.ReduceTaskNum
			reply.TaskType = MapTask
			log.Println("alloc map task" + strconv.Itoa(reply.TaskID) + "-" + reply.ResContent)
			go func(taskID int) {
				time.Sleep(10 * time.Second)
				// check status
				c.mu.Lock()
				if c.MapRunning[taskID] != 1 {
					c.MapChannel <- taskID
				}
				//else {
				//	// success
				//	c.MapTaskNum--
				//}
				c.mu.Unlock()

			}(reply.TaskID)
			return nil
		} else if len(c.MapChannel) == 0 {
			// map 已经完成   看是否有reduce
			c.mu.Lock()
			if c.MapTaskNum != 0 {
				log.Println("here?  map task num " + strconv.Itoa(c.MapTaskNum))
				reply.TaskType = WaitTask
				c.mu.Unlock()
				return nil
			} else {
				if len(c.ReduceChannel) > 0 {
					c.mu.Unlock()
					reply.TaskID = <-c.ReduceChannel
					reply.MapCnt = c.MapCnt
					reply.TaskType = ReduceTask
					log.Println("reduce channel length " + strconv.Itoa(len(c.ReduceChannel)))
					log.Println("alloc reduce task " + strconv.Itoa(reply.TaskID))
					go func(taskId int) {
						time.Sleep(10 * time.Second)
						c.mu.Lock()
						if c.ReduceRunning[taskId] != 1 {
							c.ReduceChannel <- taskId
						}
						//else {
						//	c.ReduceTaskNum--
						//}
						c.mu.Unlock()
					}(reply.TaskID)

					return nil
				} else if len(c.ReduceChannel) == 0 {
					if c.ReduceTaskNum != 0 {
						reply.TaskType = WaitTask
					} else {
						c.allDone = true
						reply.TaskType = ReduceTaskDone
					}
					c.mu.Unlock()

					return nil
				}

			}
		}
		return errors.New("req task has exception")

	case MapTaskDone:
		log.Println(strconv.Itoa(args.TaskId) + " map task done. map task num :" + strconv.Itoa(c.MapTaskNum))
		c.mu.Lock()
		log.Println("map task -- , current map task num : " + strconv.Itoa(c.MapTaskNum))
		c.MapTaskNum--
		c.MapRunning[args.TaskId] = 1
		c.mu.Unlock()
	case ReduceTaskDone:
		log.Println(strconv.Itoa(args.TaskId) + " reduce task done. reduce task num :" + strconv.Itoa(c.ReduceTaskNum))
		c.mu.Lock()
		c.ReduceTaskNum--
		c.ReduceRunning[args.TaskId] = 1

		c.mu.Unlock()
	default:
		log.Println("WaitTask ")

		reply.TaskType = WaitTask
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	if c.allDone {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//init
	log.Println(" init coordinator")
	c.ReduceTaskNum = nReduce
	c.MapTaskNum = len(files)
	c.MapCnt = len(files)
	c.MapChannel = make(chan int, len(files))
	c.ReduceChannel = make(chan int, nReduce)
	c.MapRunning = make([]int, len(files))
	c.ReduceRunning = make([]int, nReduce)
	c.MapTaskList = make([]string, len(files))
	c.allDone = false
	log.Println("arg finish")
	for i := 0; i < len(files); i++ {
		c.MapChannel <- i
		c.MapRunning[i] = 0
		c.MapTaskList[i] = files[i]
	}
	log.Println("arg finish map")

	for i := 0; i < nReduce; i++ {

		c.ReduceChannel <- i
		c.ReduceRunning[i] = 0
	}
	log.Println("arg finish reduce")

	// Your code here.

	c.server()
	log.Println("server running....")

	return &c
}
