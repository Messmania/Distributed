//Raft_test
//package clientCh

package raft

import (
	"clientCH"
	//"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

var hostname string = "localhost"
var port int = 9001

//var msecs time.Duration = time.Millisecond * 10
var msecs time.Duration = time.Second
var process = [5]*exec.Cmd{}
var filename string = "json.json"
var w0, w1, w2, w3, w4 int
var w, id []int

func Test_StartServers(t *testing.T) {
	//exec the servers
	filename := "json.json"
	w0 = rand.Intn(13)
	w1 = rand.Intn(5)
	w2 = rand.Intn(10)
	w3 = rand.Intn(10)
	w4 = rand.Intn(22)

	id = []int{0, 1, 2, 3, 4}
	w = []int{w0, w1, w2, w3, w4}
	for i := 0; i < 5; i++ {
		process[i] = exec.Command("serverStarter", filename, strconv.Itoa(id[i]), strconv.Itoa(w[i]), strconv.Itoa(50))
		err := process[i].Start()
		if err != nil {
			panic(err)
		}
	}
	time.Sleep(time.Second * 2)
}

//======================PASSED=====================
func Test_SingleClientAppend_ToLeader(t *testing.T) {

	port = 9001
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	expected := "OK"
	myChan := make(chan string)
	go clientCH.Client(myChan, set1, hostname, port)
	//fmt.Println("Launched client, Waiting for response in Test method")
	response := <-myChan
	RLine := strings.Split(response, "\r\n")
	//fmt.Println("response", response, "Rline is:", RLine[1], RLine[0])
	R1 := strings.Fields(RLine[0])
	if R1[0] != expected {
		t.Error("Mismatch!", R1, expected)
	}

	//time.Sleep(time.Millisecond * 1000)
	//time.Sleep(time.Second * 5)

	//w := msecs * time.Duration(10)
	w := msecs * time.Duration(1) //for secs
	time.Sleep(w)
}

//====================PASSED====================
func Test_MultipleClientAppends_ToLeader(t *testing.T) {
	const n int = 4
	const nResponses int = 5

	set2 := "set bcd 30 5\r\nefghi\r\n"
	getm1 := "getm abc\r\n"
	getm2 := "getm bcd\r\n"
	del1 := "delete bcd\r\n"

	cmd := []string{set2, getm1, getm2, del1}
	//cd := []string{"set2", "getm1", "getm2", "del1"}

	//not checking remaining time field in response , since testing sleeps changes the time, this func is checked in TestCheck&Expire
	E := []string{"OK", "VALUE 8\r\nabcdefjg", "VALUE 5\r\nefghi", "ERRNOTFOUND", "DELETED"}

	chann := make([]chan string, n)
	//fmt.Println("Testing MultipleCA to leader")

	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}
	for i := 0; i < n; i++ {
		go clientCH.Client(chann[i], cmd[i], hostname, port)
	}

	for j := 0; j < n; j++ {
		//fmt.Println("Before reading", j)
		R := <-chann[j]
		//fmt.Println("After reading", j)
		Result := ""
		matched := 0
		RLine := strings.Split(R, "\r\n")
		R1 := strings.Fields(RLine[0])
		if RLine[1] == "" { //means 1 line response
			//for response: OK <ver>\r\n, or ERRNOTFOUND,DELETED
			Result = R1[0]
		} else { //two line response
			//For response: VALUE <ver> <exp> numbytes\r\n<dataBytes>\r\n
			Result = R1[0] + " " + R1[3] + "\r\n" + RLine[1]
		}
		//Checking from the list of expected responses
		for i := 0; i < nResponses; i++ {
			if Result == E[i] { //must match one of the expected responses
				matched = 1

			}
		}
		//All responses must match one of the responses in array E
		if matched != 1 {
			t.Error("Received values are:\r\n", Result)
		}
	}

	//time.Sleep(time.Second * 1)
	//time.Sleep(time.Second * 2)

	w := msecs * time.Duration(1)
	time.Sleep(w)

}

//=================PASSED======================
func Test_ClientAppendToFollowers(t *testing.T) {

	const n int = 4
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	E := "ERR_REDIRECT localhost 9001"
	chann := make([]chan string, n)

	for k := 0; k < n; k++ {
		chann[k] = make(chan string, 2)
	}
	ports := []int{9000, 9002, 9003, 9004}
	for i := 0; i < n; i++ {
		go clientCH.Client(chann[i], set1, hostname, ports[i])
	}

	//response := <-r1.commitCh
	for i := 0; i < n; i++ {
		//fmt.Println("Waiting on chan:", i)
		response := <-chann[i]
		//fmt.Println("Response came!")
		if E != response {
			t.Error("Mismatch! Expected and received values are:", E, response)
		}
	}
	//time.Sleep(time.Second * 1) //s that entries are appended to all, before server1 crashes
	//time.Sleep(time.Second * 5) //s that entries are appended to all, before server1 crashes

	w := msecs * time.Duration(1)
	time.Sleep(w)
}

//New leader Appends bunch of entries to make server1's log stale and when Server 1 resumes, its log is repaired in successive heartbeats
func Test_LogRepair(t *testing.T) {
	//Crash one of the follower say 0 for sometime, while leader is sending AEs to other followers
	//Wake up f0, and now leader should repair the log!
	//append more entries to make log stale! 1 entry doesn't make log stale since leader is always ahead of followers by 1 entry,

	const n int = 4
	const nResponses int = 3
	port = 9001
	set1 := "set mno 20 8\r\nabcdefjg\r\n"
	set3 := "set efg 3 8\r\nabcdefjg\r\n"
	set4 := "set ooo 6 7\r\nmonikas\r\n"
	getm3 := "getm mno\r\n"

	E := []string{"OK", "VALUE 8\r\nabcdefjg", "ERRNOTFOUND"}
	cmd := []string{set1, set3, set4, getm3}
	chann := make([]chan string, n)
	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}

	for i := 0; i < n; i++ {
		go clientCH.Client(chann[i], cmd[i], hostname, port)
	}
	for j := 0; j < n; j++ {

		R := <-chann[j]
		Result := ""
		matched := 0
		RLine := strings.Split(R, "\r\n")
		R1 := strings.Fields(RLine[0])
		if RLine[1] == "" { //for one line response
			//for response: OK <ver>\r\n, ERRNOTFOUND
			Result = R1[0]
		} else { //for two line response
			//For response: VALUE <ver> <exp> <numbytes>\r\n<dataBytes>\r\n
			Result = R1[0] + " " + R1[3] + "\r\n" + RLine[1]
		}
		//Checking from the list of expected responses
		for i := 0; i < nResponses; i++ {
			if Result == E[i] {
				matched = 1
			}
		}
		//All responses must match one of the responses in array E
		if matched != 1 {
			t.Error("Received values are:\r\n", Result)
		}
	}

	process[1] = exec.Command("serverStarter", filename, strconv.Itoa(id[1]), strconv.Itoa(w[1]), strconv.Itoa(50))
	//==For testing only
	filepath := "S" + strconv.Itoa(1)
	fh, _ := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND, 0777)
	process[1].Stdout = fh
	//===

	//now Server1's log gets repaired when it starts receiving Heartbeats during this time period--HOW TO TEST?--by checking the log

	w := msecs * time.Duration(1)
	time.Sleep(w)
}

//Testing kvstore-- appends to leader
//================PASSED===============
func TestErrors(t *testing.T) {

	const n int = 10
	port = 9001
	//port = 9002
	//=============For ERRNOTFOUND==========
	get1 := "get ms\r\n"
	getm1 := "getm ms\r\n"
	cas1 := "cas ms 2 2204 10\r\nmonikasaha\r\n"
	del1 := "delete ms\r\n"

	//============For ERR_CMD_ERR===========
	set1 := "SET ms 5 11 extra\r\nmonikasahai\r\n"
	cas2 := "cas a\r\nnewVal"
	get2 := "get ms ok\r\n"

	//===========For ERR_VERSION============
	set2 := "set ms 2 11\r\nmonikasahai\r\n"
	cas3 := "cas ms 2 1092 9\r\nnewMonika\r\n"

	//==========For ERR_INTERNAL============
	msg1 := "del ms"

	cmd := []string{get1, getm1, cas1, del1, get1, set1, cas2, get2, set2, cas3, msg1}
	//cd := []string{"get1", "getm1", "cas1", "del1", "get1", "set1", "cas2", "get2", "set2", "cas3", "msg1"}
	err0 := "ERRNOTFOUND\r\n"
	err1 := "ERR_CMD_ERR\r\n"
	err2 := "ERR_VERSION\r\n"
	Rset1 := "OK"
	err3 := "ERR_INTERNAL\r\n"
	E := []string{err0, err0, err0, err0, err0, err1, err1, err1, Rset1, err2, err3}

	//Declare and initialize channels
	chann := make([]chan string, n)
	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}
	// Launch clients
	for i := 0; i < n; i++ {
		go clientCH.Client(chann[i], cmd[i], hostname, port)
		//Sleep is added so that all errors can be tested explicitly
		time.Sleep(time.Millisecond * 100)
	}

	var R [n]string
	for j := 0; j < n; j++ {
		R[j] = <-chann[j]
		if j == 8 {
			r := strings.Fields(R[j])
			R[j] = r[0]
		}
		if R[j] != E[j] {
			t.Error("Expected and Received values are:\r\n", E[j], R[j])
		}
	}

}

//============PASSED==============
func TestMRSC(t *testing.T) {

	//port = 9002
	port = 9001
	chann := make(chan string)
	const n int = 4
	cmd1 := "set abc 10 5\r\ndata1\r\ngetm abc\r\ndelete abc\r\ngetm abc\r\n"
	E := []string{"OK", "VALUE 10 5\r\ndata1\r\n", "DELETED", "ERRNOTFOUND"}

	go clientCH.Client(chann, cmd1, hostname, port)

	for i := 0; i < n; i++ {
		R := <-chann
		Rline := strings.Split(R, "\r\n")
		r := strings.Fields(Rline[0])
		if i == 0 {
			R = r[0]
		} else if i == 1 {
			R = r[0] + " " + r[2] + " " + r[3] + "\r\n" + Rline[1] + "\r\n"
		} else {
			R = Rline[0]
		}
		if R != E[i] {
			t.Errorf("Expected and Received values are:\r\n%v\n%v", E, R)
		}
	}

}

//===============PASSED=================
func TestMRMC(t *testing.T) {
	//port = 9002
	port = 9001
	const n int = 4  //No. of cmds per client
	const nC int = 2 //No.of clients
	cmd1 := "set abc 10 5\r\ndata1\r\ngetm abc\r\ndelete abc\r\ngetm abc\r\n"
	cmd2 := "set bcd 2 2\r\nmn\r\ngetm bcd\r\ndelete bcd\r\ngetm bcd\r\n"
	E := []string{"OK", "VALUE 5\r\ndata1", "VALUE 2\r\nmn", "DELETED", "ERRNOTFOUND"}
	cmd := []string{cmd1, cmd2}
	//Declare and initialize channels
	chann := make([]chan string, n)
	for k := 0; k < nC; k++ {
		chann[k] = make(chan string, 2)
	}
	// Launch clients
	for j := 0; j < nC; j++ {
		go clientCH.Client(chann[j], cmd[j], hostname, port)
	}
	for j := 0; j < nC; j++ {
		for k := 0; k < n; k++ {
			R := <-chann[j]
			Result := ""
			matched := 0
			RLine := strings.Split(R, "\r\n")
			R1 := strings.Fields(RLine[0])
			if RLine[1] == "" {
				//for set,err,deleted response: OK <ver>\r\n
				Result = R1[0]
			} else if len(RLine) == 3 {
				//For getm response: VALUE <ver> <exp> <numbytes>\r\n<dataBytes>\r\n
				Result = R1[0] + " " + R1[3] + "\r\n" + RLine[1]
			}
			//Checking from the list of expected responses
			for i := 0; i < 5; i++ {
				if Result == E[i] {
					matched = 1
				}
			}
			//Every response must match one of the responses in array E
			if matched != 1 {
				t.Error("Received values are:\r\n", Result)
			}
		}
	}

}
