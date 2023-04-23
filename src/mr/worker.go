package mr

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	//"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task, success := CallAskForTask()
		var dotasksuccess bool
		if !success {
			break
		}
		if task.Ttype < 1 || task.Ttype > 2 {
			//do nothing
			continue
		}
		if task.Ttype.isMap() {
			//time.Sleep(time.Second * 3)
			dotasksuccess = doMap(task.Mapfname, task.Mapfidx, task.Nreduce, mapf)
			//fmt.Println(task.Tid, task.MapData[0])
		} else if task.Ttype.isReduce() {
			//fmt.Println("do reduce task")
			dotasksuccess = doReduce(task.Reducefidxs, task.Reducefidx, task.ReduceFilterId, reducef)
		}
		r := Response{Tid: task.Tid, Ttype: task.Ttype}
		if !dotasksuccess {
			continue
		}

		success = CallSuccess(r)
		if !success {
			break
		}
	}
}

func doMap(filename string, fileidx string, nReduce int, mapf func(string, string) []KeyValue) bool {
	intermediate := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for idx := 0; idx < nReduce; idx++ {
		//write to intermediate
		intermediateFileName := fmt.Sprintf("intermediate-%s-%d.txt", fileidx, idx)
		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFileName)
			return false
		}
		s := ""
		for num, kv := range intermediate[idx] {
			s += fmt.Sprintf("%s:%s", kv.Key, kv.Value)
			if num != len(intermediate[idx])-1 {
				s += "\n"
			}
		}
		_, err = intermediateFile.WriteString(s)
		if err != nil {
			log.Fatalf("write error")
			return false
		}
		intermediateFile.Close()
	}
	return true

}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(fileidxs []string, fileidx string, filterId string, reducef func(string, []string) string) bool {
	var intermediate []KeyValue
	for _, fidx := range fileidxs {
		intermediateFileName := fmt.Sprintf("intermediate-%s-%s.txt", fidx, filterId)
		fileHandle, err := os.OpenFile(intermediateFileName, os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalf("read file %s error", intermediateFileName)
			return false
		}
		reader := bufio.NewReader(fileHandle)
		// 按行处理
		for {
			line, _, err := reader.ReadLine()
			if err != nil {
				switch err {
				case io.EOF:
					break
				default:
					return false
				}
			}

			strs := strings.Split(string(line), ":")
			if len(strs) < 2 {
				break
			}
			intermediate = append(intermediate, KeyValue{Key: strs[0], Value: strs[1]})
		}
		fileHandle.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%s", fileidx)
	ofile, _ := os.Create(oname)

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
		//fmt.Println(output)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return false
		}

		i = j
	}

	ofile.Close()
	return true
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

func CallAskForTask() (Task, bool) {
	args := ExampleArgs{}
	reply := Task{}
	success := call("Coordinator.AskForTask", &args, &reply)
	return reply, success
}

func CallSuccess(response Response) bool {
	reply := ExampleReply{}
	success := call("Coordinator.Success", response, &reply)
	return success
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
