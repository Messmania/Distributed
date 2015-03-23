//Raft_test
package raft

import (
	//"log"
	//"fmt"
	//"strings"
	//"runtime/debug"
	"fmt"
	//"math/rand"
	"testing"
	"time"
)

var r0, r1, r2, r3, r4 *Raft

var hostname string = "localhost"
var port int = 9000

func Test_StartServers(t *testing.T) {
	//Just for checking threads
	//log.Println("initial thread count:", debug.SetMaxThreads(4))
	//log.Println("initial thread count:", debug.SetMaxThreads(5))

	//make ClusterConfig object, init it and set 0 as leader for start
	clustObj := &ClusterConfig{} //set values}
	clustObj.Path = "null"       //TO BE MODIFIED
	ServerObjs := []ServerConfig{ServerConfig{0}, ServerConfig{1}, ServerConfig{2}, ServerConfig{3}, ServerConfig{4}}
	clustObj.Servers = ServerObjs
	//start all the servers by calling ServerStart for each
	//Their raft object must also be collected as returned value so that appriopriate server's Append is called
	//for testing passing timeouts differently--remove this once the functionality works correctly and use randIntn for waitTime in myRaft
	r0 = ServerStart(clustObj, 0)
	r1 = ServerStart(clustObj, 1)
	r2 = ServerStart(clustObj, 2)
	r3 = ServerStart(clustObj, 3)
	r4 = ServerStart(clustObj, 4)

	fmt.Println("Raft objects are:", r0, r1, r2, r3, r4)
	//Firing server SM in parallel

	//In code, leader sends HBs every 2sec , so keep timeout of follower more than that
	w0 := 4  //rand.Intn(2)
	w1 := 3  //rand.Intn(15)
	w2 := 7  //rand.Intn(10)
	w3 := 5  //rand.Intn(10)
	w4 := 10 //rand.Intn(22)

	fmt.Println("Waits are", w0, w1, w2, w3, w4)
	//fmt.Println("Waits are", w0, w1, w2)
	go r0.ServerSM(w0)
	go r1.ServerSM(w1)
	go r2.ServerSM(w2)
	go r3.ServerSM(w3)
	go r4.ServerSM(w4)
	//var testChan = make(chan int)
	t.Log("End of test method")
	a := time.Duration(100)
	time.Sleep(time.Second * a)
}
