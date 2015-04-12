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

var r0, r1 *Raft

func Test_StartServers(t *testing.T) {
	//Just for checking threads
	//log.Println("initial thread count:", debug.SetMaxThreads(4))
	//log.Println("initial thread count:", debug.SetMaxThreads(5))

	//reset global variables
	setCrash(false)
	setServerToCrash(-1)
	//Setting cluster config
	Server0 := ServerConfig{0, "localhost", 9000, 8000}
	Server1 := ServerConfig{1, "localhost", 9001, 8001}
	ServerArr := []ServerConfig{Server0, Server1}
	clustObj := &ClusterConfig{ServerArr}
	//fmt.Println("Prepared cluster obj")

	w0 := rand.Intn(13)
	w1 := rand.Intn(5)

	fmt.Println("Waits are", w0, w1)
	//fmt.Println("Waits are", w0, w1, w2)
	go ServerStart(clustObj, 0, w0)
	go ServerStart(clustObj, 1, w1)
	//	go ServerStart(clustObj, 2, w2)
	//	go ServerStart(clustObj, 3, w3)
	//	go ServerStart(clustObj, 4, w4)

	//System settling time: Allowing time so that leader can be elected
	time.Sleep(time.Second * 11)
	//This makes server 1 leader
}

/* //With 5 servers

func Test_StartServers(t *testing.T) {
	//Just for checking threads
	//log.Println("initial thread count:", debug.SetMaxThreads(4))
	//log.Println("initial thread count:", debug.SetMaxThreads(5))

	//reset global variables
	setCrash(false)
	setServerToCrash(-1)
	//Setting cluster config
	Server0 := ServerConfig{0, "localhost", 9000, 8000}
	Server1 := ServerConfig{1, "localhost", 9001, 8001}
	Server2 := ServerConfig{2, "localhost", 9002, 8002}
	Server3 := ServerConfig{3, "localhost", 9003, 8003}
	Server4 := ServerConfig{4, "localhost", 9004, 8004}
	ServerArr := []ServerConfig{Server0, Server1, Server2, Server3, Server4}
	clustObj := &ClusterConfig{ServerArr}
	fmt.Println("Prepared cluster obj")
	//start all the servers by calling ServerStart for each
	//No need to serialize calling now as raftObj is not needed, each server is known by its port.
	//Remove this later--Right now conn error is coming--RESOLVED

	//Just to pass timeouts, serversm is launched here, otherwise it should be launched in connHandler
	w0 := rand.Intn(13)
	w1 := rand.Intn(5)
	w2 := rand.Intn(10)
	w3 := rand.Intn(10)
	w4 := rand.Intn(22)

	fmt.Println("Waits are", w0, w1, w2, w3, w4)
	//fmt.Println("Waits are", w0, w1, w2)
	go ServerStart(clustObj, 0, w0)
	go ServerStart(clustObj, 1, w1)
	go ServerStart(clustObj, 2, w2)
	go ServerStart(clustObj, 3, w3)
	go ServerStart(clustObj, 4, w4)

	//System settling time: Allowing time so that leader can be elected
	time.Sleep(time.Second * 11)
	//This makes server 1 leader
}*/

/*
//PASSED
func Test_SingleClientAppend_ToLeader(t *testing.T) {
	//fmt.Println("Testing single client append to leader")
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := true
	myChan := make(chan LogEntry)
	go r1.Client(myChan, set1)
	response := <-myChan
	commitStatus := response.Committed()
	if expected != commitStatus {
		t.Error("Mismatch!", expected, string(response.Data()))
	}
	//fmt.Println("Test SingleCA_Leader finished")
}

//PASSED
func Test_MultipleClientAppends_ToLeader(t *testing.T) {
	const n int = 5
	set1 := "CA1"
	set2 := "CA2"
	getm1 := "getm abc\r\n"
	getm2 := "getm bcd\r\n"
	del1 := "delete bcd\r\n"
	chann := make([]chan LogEntry, n)

	cmd := []string{set1, set2, getm1, getm2, del1}
	expected := true
	//fmt.Println("Testing MultipleCA to leader")

	for k := 0; k < n; k++ {
		chann[k] = make(chan LogEntry)
	}
	for i := 0; i < n; i++ {
		go r1.Client(chann[i], cmd[i])
	}
	for i := 0; i < n; i++ {
		response := <-chann[i]
		commitStatus := response.Committed()
		if expected != commitStatus {
			t.Error("Mismatch!", expected, string(response.Data()))
		}
	}

	//fmt.Println("Test MCA_leader completed")

	a := time.Duration(1)
	time.Sleep(time.Second * a)

}

//PASSED
func Test_ClientAppendToFollowers(t *testing.T) {
	const n int = 4
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := false
	chann := make([]chan LogEntry, n)

	for k := 0; k < n; k++ {
		chann[k] = make(chan LogEntry)
	}

	r := [n]*Raft{r0, r2, r3, r4}
	for i := 0; i < n; i++ {
		go r[i].Client(chann[i], set1)
	}

	//response := <-r1.commitCh
	for i := 0; i < n; i++ {
		response := <-chann[i]
		commitStatus := response.Committed()
		if expected != commitStatus {
			t.Error("Mismatch!", expected, string(response.Data()))
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
	//fmt.Println("\n=========Server 1 crashed now!============\n")

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
	chann := make([]chan LogEntry, n)
	r := [n]*Raft{r0, r2, r3, r4}
	for k := 0; k < n; k++ {
		chann[k] = make(chan LogEntry)
	}
	for i := 0; i < n; i++ {
		go r[i].Client(chann[i], set1)
	}

	for i := 0; i < n; i++ {
		response := <-chann[i]
		commitStatus := response.Committed()
		if expected[i] != commitStatus {
			t.Error("Mismatch!", expected, string(response.Data()))
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
	chann := make([]chan LogEntry, n)
	for k := 0; k < n; k++ {
		chann[k] = make(chan LogEntry)
	}

	for i := 0; i < n; i++ {
		go r2.Client(chann[i], cmd[i])
	}
	for i := 0; i < n; i++ {
		response := <-chann[i]
		commitStatus := response.Committed()
		if expected != commitStatus {
			t.Error("Mismatch!", expected, response.Committed())
		}
	}
	//fmt.Println("\n=========Server 1 resuming now!============\n")
	setCrash(false)
	setServerToCrash(-1)
	//now Server1's log gets repaired when it starts receiving Heartbeats during this time period--HOW TO TEST?
	time.Sleep(time.Second * 1)
}

func Test_CommitEntryFromPrevTerm(t *testing.T) {
	//not able to simulate the scenario for now
	//leader appends entries to its log and crashes, comes back up before anyone else timesout, now testing can be done
	//crash leader 2 for less than 2 msec, which is next viable leader's timeout i.e. S1
	//i.e. before its RetryTimer times out??
	//reduce retry timer to 4, so that it comes up at 5 as follower and times out and restarts the elections--check the numbers again

}

*/
