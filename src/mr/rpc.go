package mr

// RPC definitions.
// remember to capitalize all names.
import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

/** All example codes above. My codes starts here.
 *  This file (rpg.go) should define how master and worker talking to each
 *  other by structs. Then prvoid an encapsulated call method with any necessary
 *  internal rules include but not limited to retry, validation, etc.
 */

type JobPhase string

const (
	mapPhase    JobPhase = "mapPhase"
	reducePhase JobPhase = "reducePhase"
)

type RegisterArgs struct {
	Id   int
	Sock string
}

type RegisterReply struct {
	Msg     string
	NReduce int
}

type DoArgs struct {
	Phase        JobPhase
	File         string // used in map phase
	ReduceNumber int    // used in reduce phase
}

type DoReply struct {
	Status bool
}

type KillArgs struct {
}

type KillReply struct {
	Status bool
}

// This method starts a RPC server
func (m *Master) InitializeRPC() {
	// rpcServer := rpc.NewServer()
	// rpcServer.Register(m)
	// sockname := masterSock()
	// os.Remove(sockname)
	// listener, err := net.Listen("unix", sockname)
	// if err != nil {
	// 	log.Fatal("RPC server initialization failed: ", err)
	// }
	// rpc.HandleHTTP()
	// rpcServer.HandleHTTP()
	// go http.Serve(listener, nil)

	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// This method ends a RPC server
func (m *Master) TerminateRPC() {

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(sockname string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("calling:%s::%s() error: %s\n", sockname, rpcname, err)
	return false
}
