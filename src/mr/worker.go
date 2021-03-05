package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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

type W struct {
	id      int
	sock    string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	// file    string
	nReduce int
	mu      *sync.Mutex
	cond    *sync.Cond
	signal  bool
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := W{}
	w.mu = &sync.Mutex{}
	w.cond = sync.NewCond(w.mu)
	w.signal = false
	w.id = os.Getpid()
	w.mapf = mapf
	w.reducef = reducef
	sockname := "/var/tmp/824-mr-" + strconv.Itoa(os.Getpid())
	w.sock = sockname
	rpc.Register(&w)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	w.register()
	w.run()
}

// Report to master and ready to work
func (w *W) register() {
	args := RegisterArgs{w.id, w.sock}
	reply := RegisterReply{}
	sockname := masterSock()
	if ok := call(sockname, "Master.Register", args, &reply); !ok {
		fmt.Printf("Register: RPC register error\n")
	}
	w.nReduce = reply.NReduce
	// fmt.Printf("reply: %s\n", reply.Msg)
}

func (w *W) run() {
	w.cond.L.Lock()
	for !w.signal {
		w.cond.Wait()
	}
}

func (w *W) Kill(args *KillArgs, reply *KillReply) error {
	w.signal = true
	reply.Status = true
	return nil
}

// Master will call this method to assign a task to worker
func (w *W) Do(args *DoArgs, reply *DoReply) error {
	if args.Phase == mapPhase {
		w.doMap(args, reply)
	}
	if args.Phase == reducePhase {
		w.doReduce(args, reply)
	}
	return nil
}

func (w *W) doMap(args *DoArgs, reply *DoReply) {
	// fd, err := os.Open(w.file)
	fd, err := ioutil.ReadFile(args.File)
	if err != nil {
		log.Fatal("Worker, map, File open error: ", err)
	}
	// defer fd.Close()
	// scanner := bufio.NewScanner(fd)
	// scanner.Split(bufio.ScanLines)
	// for scanner.Scan() {
	// 	w.mapf("", scanner.Text())
	// }
	outputFiles := make([]*os.File, w.nReduce)
	kvArr := w.mapf(args.File, string(fd))
	for i := 0; i < w.nReduce; i++ {
		fileName := createIntermediateFileName(w.id, args.ReduceNumber, i)
		outputFiles[i], err = os.Create(fileName)
		if err != nil {
			reply.Status = false
			log.Fatal("Error in creating file: ", fileName)
		}
	}
	for _, kv := range kvArr {
		index := ihash(kv.Key) % w.nReduce
		enc := json.NewEncoder(outputFiles[index])
		enc.Encode(kv)
	}
	for _, file := range outputFiles {
		file.Close()
	}
	reply.Status = true
	fmt.Printf("map task done by worker: %d", w.id)
}

func (w *W) doReduce(args *DoArgs, reply *DoReply) {
	fileNames, err := getIntermediateFileName(args.ReduceNumber)
	if err != nil {
		log.Fatal("Worker, reduce, File open error: ", err)
	}
	files := make([]*os.File, len(fileNames))
	for i := 0; i < len(fileNames); i++ {
		files[i], _ = os.Open(fileNames[i])
	}
	kvMap := make(map[string][]string)
	for _, file := range files {
		defer file.Close()
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	reduceArgKeys := make([]string, 0)
	for k := range kvMap {
		reduceArgKeys = append(reduceArgKeys, k)
	}
	sort.Strings(reduceArgKeys)

	output, _ := os.Create(getOutputFileName(args.ReduceNumber))
	defer output.Close()
	writer := bufio.NewWriter(output)
	for _, key := range reduceArgKeys {
		// output.WriteString(key + " " + w.reducef(key, kvMap[key]) + "\n")
		writer.WriteString(key + " " + w.reducef(key, kvMap[key]) + "\n")
	}
	// err = output.Sync()
	err = writer.Flush()
	if err != nil {
		log.Fatal("worker, reduce, write: ", err)
	}
	reply.Status = true
}

func createIntermediateFileName(workerID, reduceNumber, index int) string {
	return "mr-tmp-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(reduceNumber) + "-" + strconv.Itoa(index)
}

func getIntermediateFileName(reduceNumber int) (matches []string, err error) {
	return filepath.Glob("mr-tmp-*-" + strconv.Itoa(reduceNumber))
}

func getOutputFileName(index int) string {
	return "mr-out-" + strconv.Itoa(index)
}
