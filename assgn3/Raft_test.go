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

	//fmt.Println("Raft objects are:", r0, r1, r2, r3, r4)
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
	//t.Log("End of test method")
	a := time.Duration(5)
	time.Sleep(time.Second * a)

	//This makes server 1 leader
}

//Passed
func Test_ClientAppend_ToLeader(t *testing.T) {
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := true
	response, err := r1.Append([]byte(set1))
	if err != nil {
		fmt.Println("Error!!")
	}
	//response := <-r1.commitCh
	commitStatus := response.Committed()
	if expected != commitStatus {
		t.Error("Mismatch!", expected, string(response.Data()))
	}
}

//Passed
func Test_ClientAppendToFollowers(t *testing.T) {
	const n int = 4
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := false
	//response:=[]LogItem{}--typecasting error
	response := [n]LogEntry{}
	err := [n]error{nil, nil}
	r := [n]*Raft{r0, r2, r3, r4}
	for i := 0; i < n; i++ {
		response[i], err[i] = r[i].Append([]byte(set1))
	}

	//response := <-r1.commitCh
	for i := 0; i < n; i++ {
		if err[i] != nil {
			fmt.Println("Error!!")
		}
		commitStatus := response[i].Committed()
		if expected != commitStatus {
			t.Error("Mismatch!", expected, string(response[i].Data()))
		}
	}

}

func Test_MultipleClientAppends_ToLeader(t *testing.T) {
	const n int = 5
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	set2 := "set bcd 30 5\r\nefghi\r\n"
	getm1 := "getm abc\r\n"
	getm2 := "getm bcd\r\n"
	del1 := "delete bcd\r\n"
	cmd := []string{set1, set2, getm1, getm2, del1}

	response := [n]LogEntry{}
	err := [n]error{nil, nil}
	expected := true
	for i := 0; i < n; i++ {
		response[i], err[i] = r1.Append([]byte(cmd[i]))
	}
	for i := 0; i < n; i++ {
		if err[i] != nil {
			t.Error("Append failed!")
		} else {
			commitStatus := response[i].Committed()
			if expected != commitStatus {
				t.Error("Mismatch!", expected, string(response[i].Data()))
			}
		}
	}

}
func Test_LogRepair(t *testing.T) {
	//Crash one of the follower say 0 for sometime, while leader is sending AEs to other followers
	//Wake up f0, and now leader should repair the log!

}

func Test_CommitEntryFromCurrentTerm(t *testing.T) {

}

func Test_CommitEntryFromPrevTerm(t *testing.T) {
}

func Test_LeaderChanges(t *testing.T) {

}
func Test_ServerCrash(t *testing.T) {

}
