package mr

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// Master itself
type Master struct {
	// Your definitions here.
	mu       *sync.Mutex
	cond     *sync.Cond
	signal   bool
	files    []string
	nReduce  int
	nMap     int
	workers  chan WorkerInfo // map<workerID, struct>
	listener net.Listener
	wiArr    []WorkerInfo
	phase    JobPhase
	// wg       sync.WaitGroup
}

const (
	UnAssigned = 0
	Assigned   = 1
	Finished   = 2
)

type workerStat int

const (
	idle workerStat = 0
	busy workerStat = 1
	dead workerStat = 2
)

type WorkerInfo struct {
	id   int
	sock string
	stat workerStat
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) run() {
	m.InitializeRPC()
	m.schedule()
	m.phase = reducePhase
	m.schedule()
	// m.cond.L.Lock()
	// for !m.signal {
	// 	m.cond.Wait()
	// }
	files, err := filepath.Glob("mr-tmp*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
	for _, wi := range m.wiArr {
		foo := KillArgs{}
		bar := KillReply{false}
		call(wi.sock, "W.Kill", foo, &bar)
	}
	m.TerminateRPC()
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.signal
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = &sync.Mutex{}
	m.cond = sync.NewCond(m.mu)
	m.signal = false
	m.files = files
	m.nMap = len(m.files)
	m.nReduce = nReduce
	m.phase = mapPhase
	m.workers = make(chan WorkerInfo)
	m.run()
	return &m
}

// Worker call this method to register
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	// Master should record some info for every incoming worker, then assign some work to each worker.
	m.mu.Lock()
	defer m.mu.Unlock()
	wi := WorkerInfo{args.Id, args.Sock, 0}
	// m.wg.Add(1)
	m.workers <- wi
	m.wiArr = append(m.wiArr, wi)
	reply.Msg = "Reply message from master: 王今朗她自己傻!\n"
	reply.NReduce = m.nReduce
	return nil
}

func (m *Master) schedule() {
	var nTask int
	if m.phase == mapPhase {
		nTask = m.nMap
	} else {
		nTask = m.nReduce
	}

	tasks := make(chan int)
	go func() {
		for i := 0; i < nTask; i++ {
			tasks <- i
		}
	}()

loop:
	for {
		select {
		case task := <-tasks:
			go func() {
				w := <-m.workers
				doArg := DoArgs{m.phase, "", task}
				if m.phase == mapPhase {
					doArg.File = m.files[task]
				}
				doReply := DoReply{}
				call(w.sock, "W.Do", doArg, &doReply)
				fmt.Printf("phase:%s task:%d reply:%t\n", doArg.Phase, doArg.ReduceNumber, doReply.Status)
				if doReply.Status {
					// if succeed, put worker back to worker chan (idle)
					go func() { m.workers <- w }()
					nTask--

				} else {
					// otherwise redo task
					go func() { tasks <- task }()
				}
			}()
		default:
			if nTask == 0 {
				break loop
			}
		}
	}
	if m.phase == reducePhase {
		fmt.Println("setting signal to true")
		m.signal = true
	}
}
