package mr

import (
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
	nreduce int

	mapTaskTodo       *map[int]interface{}
	mapTaskOngoing    *map[int]interface{}
	reduceTaskTodo    *map[int]interface{}
	reduceTaskOngoing *map[int]interface{}

	mapfidxs []string

	mtdmutex *sync.RWMutex
	mtomutex *sync.RWMutex
	rtdmutex *sync.RWMutex
	rtomutex *sync.RWMutex

	mtmutex    *sync.Mutex
	mtaskid    int
	rtmutex    *sync.Mutex
	rtaskid    int
	mapfidx    int
	reducefidx int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetMapFileIdx() int {
	defer func() { c.mapfidx++ }()
	return c.mapfidx
}
func (c *Coordinator) GetReduceFileIdx() int {
	defer func() { c.reducefidx++ }()
	return c.reducefidx
}

func (c *Coordinator) GetMapTaskId() int {
	c.mtmutex.Lock()

	tid := c.mtaskid
	c.mtaskid -= 1
	c.mtmutex.Unlock()
	return tid
}

func (c *Coordinator) GetReduceTaskId() int {
	c.rtmutex.Lock()
	tid := c.rtaskid
	c.rtaskid += 1
	c.rtmutex.Unlock()
	return tid
}

func (c *Coordinator) HaveMapTask() bool {
	c.mtdmutex.RLock()
	r := len(*c.mapTaskTodo) > 0
	c.mtdmutex.RUnlock()
	return r
}

func (c *Coordinator) HaveMapTaskRunning() bool {
	c.mtomutex.RLock()
	r := len(*c.mapTaskOngoing) > 0
	c.mtomutex.RUnlock()
	return r
}

func (c *Coordinator) HaveReduceTask() bool {
	c.rtdmutex.RLock()
	r := len(*c.reduceTaskTodo) > 0
	c.rtdmutex.RUnlock()
	return r
}

func (c *Coordinator) HaveReduceTaskRunning() bool {
	c.rtomutex.RLock()
	r := len(*c.reduceTaskOngoing) > 0
	c.rtomutex.RUnlock()
	return r
}

func (c *Coordinator) MapTaskRunning(taskid int) bool {
	c.mtomutex.RLock()
	_, ok := (*c.mapTaskOngoing)[taskid]
	c.mtomutex.RUnlock()
	return ok
}

func (c *Coordinator) ReduceTaskRunning(taskid int) bool {
	c.rtomutex.RLock()
	_, ok := (*c.reduceTaskOngoing)[taskid]
	c.rtomutex.RUnlock()
	return ok
}

func (c *Coordinator) ListenTimeout(taskid int, taskType TaskType) {
	ticker := time.NewTicker(time.Second * 10) //10秒超时
	defer ticker.Stop()
	select {
	case <-ticker.C:
		if taskType.isMap() {
			if c.MapTaskRunning(taskid) {
				//从活跃Map任务列表中删除并加入待办列表
				c.mtdmutex.Lock()
				c.mtomutex.Lock()

				if value, stillok := (*c.mapTaskOngoing)[taskid]; stillok {
					delete(*c.mapTaskOngoing, taskid)
					(*c.mapTaskTodo)[c.GetMapTaskId()] = value
				}
				c.mtomutex.Unlock()
				c.mtdmutex.Unlock()
			}
		} else if taskType.isReduce() {
			if c.ReduceTaskRunning(taskid) {
				//从活跃Reduce任务列表中删除并加入待办列表
				c.rtdmutex.Lock()
				c.rtomutex.Lock()
				if value, stillok := (*c.reduceTaskOngoing)[taskid]; stillok {
					delete(*c.reduceTaskOngoing, taskid)
					(*c.reduceTaskTodo)[c.GetReduceTaskId()] = value
				}
				c.rtomutex.Unlock()
				c.rtdmutex.Unlock()
			}
		}
	}

}

func (c *Coordinator) AskForTask(args *ExampleArgs, reply *Task) error {
	reply.Nreduce = c.nreduce
	//Map任务列表非空，分配Map任务
	//fmt.Println("receive request")
	if c.HaveMapTask() {
		c.mtdmutex.Lock()
		if len(*c.mapTaskTodo) > 0 {

			//取出一个task
			for key, value := range *c.mapTaskTodo {
				reply.Tid = key
				reply.Mapfname = value.([]string)[0]
				reply.Mapfidx = value.([]string)[1]
				reply.Ttype = 1
				break
			}
			delete(*c.mapTaskTodo, reply.Tid)
			//fmt.Println(*c.mapTaskTodo)
			//放到活跃Map事务列表
			c.mtomutex.Lock()
			(*c.mapTaskOngoing)[reply.Tid] = []string{reply.Mapfname, reply.Mapfidx}
			c.mtomutex.Unlock()
			//fmt.Println("give map task", reply.Tid)
			go c.ListenTimeout(reply.Tid, 1)
		} else {
			//分配空任务
			reply.Ttype = 3
			//fmt.Println("give empty task 0")
		}
		c.mtdmutex.Unlock()

	} else if c.HaveMapTaskRunning() {
		//Map任务在跑，没有可分配的Map任务；分配空任务
		reply.Ttype = 3
		//fmt.Println("give empty task 1")
	} else if c.HaveReduceTask() {
		//Reduce任务列表非空，分配Reduce任务
		c.rtdmutex.Lock()
		if len(*c.reduceTaskTodo) > 0 {
			//取出一个task
			for key, value := range *c.reduceTaskTodo {
				reply.Tid = key
				reply.Ttype = 2
				reply.Reducefidxs = c.mapfidxs
				reply.Reducefidx = value.([]string)[0]
				reply.ReduceFilterId = value.([]string)[1]
				break
			}
			delete(*c.reduceTaskTodo, reply.Tid)
			//放到活跃Reduce任务列表
			c.rtomutex.Lock()
			(*c.reduceTaskOngoing)[reply.Tid] = []string{reply.Reducefidx, reply.ReduceFilterId}
			c.rtomutex.Unlock()
			//fmt.Println("give reduce task", reply.Tid)

			go c.ListenTimeout(reply.Tid, 2)
		} else {
			//分配空任务
			reply.Ttype = 3
			//fmt.Println("give empty task 2")
		}
		c.rtdmutex.Unlock()
	} else if c.HaveReduceTaskRunning() {
		//Reduce任务在跑，没有可分配的Reduce任务；分配空任务
		reply.Ttype = 3
		//fmt.Println("give empty task 3")
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

func (c *Coordinator) Success(res Response, args *ExampleReply) error {
	if res.Ttype.isMap() {
		if c.MapTaskRunning(res.Tid) {
			c.mtomutex.Lock()
			//从活跃Map任务表中删除
			delete(*c.mapTaskOngoing, res.Tid)
			c.mtomutex.Unlock()
		}
	} else if res.Ttype.isReduce() {
		if c.ReduceTaskRunning(res.Tid) {
			c.rtomutex.Lock()
			//从活跃Reduce任务表中删除
			delete(*c.reduceTaskOngoing, res.Tid)
			c.rtomutex.Unlock()
		}
	}
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) Done() bool {
	// Your code here.
	c.mtdmutex.RLock()
	c.mtomutex.RLock()
	c.rtdmutex.RLock()
	c.rtomutex.RLock()
	defer c.mtdmutex.RUnlock()
	defer c.mtomutex.RUnlock()
	defer c.rtdmutex.RUnlock()
	defer c.rtomutex.RUnlock()
	ret := len(*c.mapTaskTodo) == 0 && len(*c.mapTaskOngoing) == 0 && len(*c.reduceTaskTodo) == 0 &&
		len(*c.reduceTaskOngoing) == 0
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nreduce = nReduce
	c.mtmutex = &sync.Mutex{}
	c.rtmutex = &sync.Mutex{}
	c.mtaskid = 10000
	c.rtaskid = 10001

	c.mapTaskTodo = &map[int]interface{}{}

	var fidxs []string
	for _, file := range files {
		fidx := c.GetMapFileIdx()
		fidxs = append(fidxs, strconv.Itoa(fidx))
		(*c.mapTaskTodo)[c.GetMapTaskId()] = []string{file, strconv.Itoa(fidx)}
	}
	//fmt.Println(*c.mapTaskTodo)
	c.mapTaskOngoing = &map[int]interface{}{}
	c.reduceTaskTodo = &map[int]interface{}{}
	c.reduceTaskOngoing = &map[int]interface{}{}

	c.mapfidxs = fidxs

	for i := 0; i < nReduce; i++ {
		id := c.GetReduceTaskId()
		(*c.reduceTaskTodo)[id] = []string{strconv.Itoa(c.GetReduceFileIdx()), strconv.Itoa(i)}
	}

	c.mtdmutex = &sync.RWMutex{}
	c.mtomutex = &sync.RWMutex{}
	c.rtdmutex = &sync.RWMutex{}
	c.rtomutex = &sync.RWMutex{}

	c.server()
	return &c
}
