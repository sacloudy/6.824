package mr

import (
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	WorkerID     string
	Deadline     time.Time
	Type         string
	Index        int // 对于map task就是file_id, 对于reduce task就是nReduce
	MapInputFile string
}

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex // 保护共享信息，避免并发冲突
	stage          string     // 当前整体作业阶段(MAP或REDUCE, 为空代表已完成可退出)
	nMap           int
	nReduce        int
	tasks          map[string]Task
	availableTasks chan Task
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// 每个输入文件生成一个 MAP Task
	for i, file := range files {
		task := Task{
			Type:         MAP,
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task // <"MAP-0", Task-0>
		c.availableTasks <- task
	}

	// 启动 Coordinator，开始响应 Worker 请求
	log.Printf("Coordinator start\n")
	c.server()

	// 启动 Task 自动回收过程
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					// 回收并重新分配
					log.Printf(
						"Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
						task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
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

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	 reply.Y = args.X + 2
// 	return nil
// }

// RPC 的处理入口，由 Worker (通过RPC)调用, 奇怪的是这确实是Coordinator的方法
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskType != "" {
		// 记录 Worker 的上一个 Task 已经运行完成
		c.lock.Lock()

		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		// 判断该 Task 是否仍属于该 Worker，如果已经被重新分配则直接忽略，进入后续的新 Task 分配过程
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerID == args.WorkerID {
			log.Printf("Mark %s task %d as finished on worker %s\n",
				task.Type, task.Index, args.WorkerID)
			// 将该 Worker 的临时产出文件标记为最终产出文件
			if args.LastTaskType == MAP {
				for ri := 0; ri < c.nReduce; ri++ {
					//err := os.Rename(
					//	tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri),
					//	finalMapOutFile(args.LastTaskIndex, ri))
					//if err != nil {
					//	log.Fatalf(
					//		"Failed to mark map output file `%s` as final: %e",
					//		tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri), err)
					//}
				}
				log.Println("args.LastTaskType == MAP")
			} else if args.LastTaskType == REDUCE {
				//err := os.Rename(
				//	tmpReduceOutFile(args.WorkerID, args.LastTaskIndex),
				//	finalReduceOutFile(args.LastTaskIndex))
				//if err != nil {
				//	log.Fatalf(
				//		"Failed to mark reduce output file `%s` as final: %e",
				//		tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), err)
				//}
				log.Println("args.LaskTaskType == Reduce")
			}

			delete(c.tasks, lastTaskID) // 这才算一个任务的完成(chan发出去了不能算)
			if len(c.tasks) == 0 {      // 把塞进task chan的map/reduce任务都发完了,该进入下一阶段了
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	// 获取一个可用 Task 并返回
	task, ok := <-c.availableTasks
	if !ok { // Channel 关闭，代表整个 MR 作业已完成，通知 Worker 退出
		log.Error("chan closed!")
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task // 记录 Task 分配的 Worker ID 及 Deadline
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	log.Warningf("Assign %s task %d to worker %s, deadline is %s\n", task.Type, task.Index, task.WorkerID, task.Deadline)
	return nil
}

func (c *Coordinator) transit() {
	if c.stage == MAP { // MAP Task 已全部完成，进入 REDUCE 阶段
		log.Errorf("All MAP tasks finished. Transit to REDUCE stage\n")
		c.stage = REDUCE
		// 生成 Reduce Task
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  REDUCE,
				Index: i,
				// Reduce Task 当然不用填 MapInputFile
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == REDUCE { // REDUCE Task 已全部完成，MR 作业已完成，准备退出
		log.Errorf("All REDUCE tasks finished. Prepare to exit\n")
		close(c.availableTasks) // close chan, 外面receive一定要用ok或者range来检测chan的状态
		c.stage = ""            // 使用空字符串标记作业完成
	}
}

func GenTaskID(t string, index int) string {
	return t + `-` + strconv.Itoa(index)
}
