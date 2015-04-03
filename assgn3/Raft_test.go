//Raft_test
package raft

import (
	//"log"
	//"strings"
	//"runtime/debug"
	"fmt"
	"math/rand"
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

	//reset global variables
	globMutex.Lock()
	crash = false
	server_to_crash = -1
	globMutex.Unlock()

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
	w0 := rand.Intn(13)
	w1 := rand.Intn(5)
	w2 := rand.Intn(10)
	w3 := rand.Intn(10)
	w4 := rand.Intn(22)

	fmt.Println("Waits are", w0, w1, w2, w3, w4)
	go r0.ServerSM(w0)
	go r1.ServerSM(w1)
	go r2.ServerSM(w2)
	go r3.ServerSM(w3)
	go r4.ServerSM(w4)
	//var testChan = make(chan int)
	//t.Log("End of test method")

	//System settling time: Allowing time so that leader can be elected
	time.Sleep(time.Second * 1)
	//This makes server 1 leader
}

//PASSED
func Test_SingleClientAppend_ToLeader(t *testing.T) {
	fmt.Println("Testing single client append to leader")
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := true
	response, err := r1.Append([]byte(set1))
	if err != nil {
		fmt.Println("Error!!")
	}
	commitStatus := response.Committed()
	if expected != commitStatus {
		t.Error("Mismatch!", expected, string(response.Data()))
	}
	fmt.Println("Test SingleCA_Leader finished")
}

//PASSED
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
	fmt.Println("Testing MultipleCA to leader")
	for i := 0; i < n; i++ {
		response[i], err[i] = r1.Append([]byte(cmd[i]))
	}
	for i := 0; i < n; i++ {
		if err[i] != nil {
			t.Error("Error in Append func call!!")
		} else {
			commitStatus := response[i].Committed()
			if expected != commitStatus {
				t.Error("Mismatch!", expected, string(response[i].Data()))
			}
		}
	}

	fmt.Println("Test MCA_leader completed")

	a := time.Duration(2)
	time.Sleep(time.Second * a)

}

//PASSED
func Test_ClientAppendToFollowers(t *testing.T) {
	const n int = 4
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := false
	//response := [n]LogItem{} //--typecasting error
	response := [n]LogEntry{}
	err := [n]error{nil, nil}
	r := [n]*Raft{r0, r2, r3, r4}
	for i := 0; i < n; i++ {
		response[i], err[i] = r[i].Append([]byte(set1))
	}

	//response := <-r1.commitCh
	for i := 0; i < n; i++ {
		if err[i] != nil {
			t.Error("Error in Append func call!!")
		}
		commitStatus := response[i].Committed()
		if expected != commitStatus {
			t.Error("Mismatch!", expected, string(response[i].Data()))
		}
	}
}

func Test_CommitEntryFromCurrentTerm(t *testing.T) {
	//TestSCA and MCA are checking this , coz once entry is commited then only client gets the response
}

//PASSED
func Test_ServerCrash_(t *testing.T) {
	setCrash(true)
	setServerToCrash(1)
	fmt.Println("\n=========Server 1 crashed now!============\n")

}

//PASSED
//S1 is crashed so S2 becomes leader as its wait is lesser than others and it is deserving
//Since S2 is now leader, true pops up on its commit channel
func Test_LeaderChanges(t *testing.T) {
	//giving time to elect a new leader
	time.Sleep(time.Second * 1)
	//Start a timer here and verify that Append call doesn't succeed and timer times out which means S1 is partitioned--PENDING
	const n int = 4
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := []bool{false, true, false, false}
	response := [n]LogEntry{}
	err := [n]error{nil, nil}
	r := [n]*Raft{r0, r2, r3, r4}
	for i := 0; i < n; i++ {
		response[i], err[i] = r[i].Append([]byte(set1))
	}
	for i := 0; i < n; i++ {
		if err[i] != nil {
			t.Error("Error in Append func call!!")
		}
		commitStatus := response[i].Committed()
		if expected[i] != commitStatus {
			t.Error("Mismatch!", expected, string(response[i].Data()))
		}
	}
}

//PASSED
//New leader Appends bunch of entries to make server1's log stale and when Server 1 resumes, its log is repaired in successive heartbeats
func Test_LogRepair(t *testing.T) {
	//Crash one of the follower say 0 for sometime, while leader is sending AEs to other followers
	//Wake up f0, and now leader should repair the log!
	//append more entries to make log stale! 1 entry doesn't make log stale since leader is always ahead of followers by 1 entry,
	const n int = 4
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	set3 := "set abc 3 8\r\nabcdefjg\r\n"
	set4 := "set abc 6 7\r\nmonikas\r\n"
	getm3 := "getm abc\r\n"
	cmd := []string{set1, set3, set4, getm3}
	expected := true
	for i := 0; i < n; i++ {
		response, err := r2.Append([]byte(cmd[i]))
		if err != nil {
			fmt.Println("Error!!")
		}
		commitStatus := response.Committed()
		if expected != commitStatus {
			t.Error("Mismatch!", expected, response.Committed())
		}
	}
	fmt.Println("\n=========Server 1 resuming now!============\n")
	setCrash(false)
	setServerToCrash(-1)
	//now Server1's log gets repaired when it starts receiving Heartbeats during this time period--HOW TO TEST?
	time.Sleep(time.Second * 1)
}

func Test_CommitEntryFromPrevTerm(t *testing.T) {
	//not able to simulate the scenario for now
	//leader appends entries to its log and crashes, comes back up before anyone else timesout, now testing can be done
	//crash leader 2 for less than 7 msec, which is next viable leader's timeout i.e. S2
	//i.e. before its RetryTimer times out??
	//reduce retry timer to 4, so that it comes up at 5 as follower and times out and restarts the elections--check the numbers agan

}
