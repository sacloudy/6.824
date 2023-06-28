package mr

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 貌似导包是不能导main包的, 所以这里直接复制过来了
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const MAP = "MAP"
const REDUCE = "REDUCE"

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
	// 单机运行，直接使用 PID 作为 Worker ID，方便 debug
	id := strconv.Itoa(os.Getpid())
	log.Errorf("Worker %s started\n", id)

	// 进入循环，向 Coordinator 申请 Task
	var lastTaskType string
	var lastTaskIndex int
	for {
		args := ApplyForTaskArgs{
			WorkerID:      id,
			LastTaskType:  lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}
		reply := ApplyForTaskReply{}
		//log.Warningf("Worker %s ApplyForTask, (args: %v)\n", id, args)
		lastTask := args.LastTaskType + "-" + strconv.Itoa(args.LastTaskIndex)
		log.WithFields(log.Fields{
			"Worker":   id,
			"LastTask": lastTask,
		}).Warn("Worker ApplyForTask!")

		call("Coordinator.ApplyForTask", &args, &reply)
		if reply.TaskType == "" {
			// MR 作业已完成，退出
			log.Error("Received job finish signal from coordinator")
			break
		}

		// log.Warningf("Received %s task %d from coordinator(MapInputFile is %s)", reply.TaskType, reply.TaskIndex, reply.MapInputFile)
		log.WithFields(log.Fields{
			"Worker": id,
			"reply":  reply,
		}).Warn("Worker received task!")
		if reply.TaskType == MAP { // 处理 MAP Task
			doMapTask(mapf, reply)
		} else if reply.TaskType == REDUCE {
			doReduceTask(reducef, reply) // 处理 REDUCE Task
		}
		// 记录已完成 Task 的信息，在下次 RPC 调用时捎带给 Coordinator
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
		log.Printf("Finished %s task %d", reply.TaskType, reply.TaskIndex)
	}

	log.Printf("Worker %s exit\n", id)
}

// 原来 mrsequential的map 是遍历所有文件进行处理然后生成一个很大的中间结果(数组, 还不是文件)
func doMapTask(mapf func(string, string) []KeyValue, reply ApplyForTaskReply) {
	// 读取输入数据
	file, err := os.Open(reply.MapInputFile)
	if err != nil {
		log.Fatalf("Failed to open map input file %s: %e", reply.MapInputFile, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
	}
	// 传递输入数据至 MAP 函数，得到中间结果
	kva := mapf(reply.MapInputFile, string(content))
	log.Printf("size of %s : %d", reply.MapInputFile, len(content))
	// 按 Key 的 Hash 值对中间结果进行分桶
	hashedKva := make(map[int][]KeyValue) // key是reduce号, value是map完后对应的kv: <apple, 1>
	for _, kv := range kva {
		hashed := ihash(kv.Key) % reply.ReduceNum
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}
	// 写出中间结果文件(为每个Reduce Task都生成一个文件), 完全可以看成是对未排序时的intermediate数组的切割
	for i := 0; i < reply.ReduceNum; i++ {
		ofile, _ := os.Create("mr-" + strconv.Itoa(reply.TaskIndex) + "-" + strconv.Itoa(i))
		log.Println("Create file: " + ofile.Name())
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		log.Printf("size of hashedKva[i] : %d", len(hashedKva[i]))
		ofile.Close()
	}
}

// 原来mrsequential的Reduce是在很大的imitate数组上滑动处理, 然后生成一个文件mr-out-0
// 现在是一个特定的ReduceTask(编号在0-ReduceNum之间), 也要生成一个mr-out-ReduceID呢
func doReduceTask(reducef func(string, []string) string, reply ApplyForTaskReply) {
	// 每个Reduce Task都需要从每个Map Task生成的文件中找属于自己的那个文件读取
	var lines []string
	for mi := 0; mi < reply.MapNum; mi++ {
		inputFile := "mr-" + strconv.Itoa(mi) + "-" + strconv.Itoa(reply.TaskIndex)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("Failed to open map output file %s: %e", inputFile, err)
		}
		log.Printf("Read reduce input file %s\n", inputFile)
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, " ")
		//fmt.Println(parts, len(parts))
		kva = append(kva, KeyValue{
			Key:   parts[0],
			Value: parts[1],
		})
	}

	// 按 Key 对输入数据进行排序 (将8个mr-*-ReduceID文件中的内容排序)
	sort.Sort(ByKey(kva))

	ofile, _ := os.Create("mr-out-" + strconv.Itoa(reply.TaskIndex)) // tmpReduceOutFile?
	// 按 Key 对中间结果的 Value 进行归并，传递至 Reduce 函数
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// 写出至结果文件
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	log.Println("Sort and Reduce above 8 files and Write the result to file: ", ofile.Name())
	ofile.Close()
}

func finalMapOutFile(mi int, index int) interface{} {
	return nil
}

// 在Worker()中调用给你展示下怎么发出rpc请求的, 没什么实质作用
// example function to show how to make an RPC call to the coordinator.
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
