//Raft_test
package raft

import (
	"strings"
	"testing"
	"time"
)

var hostname string = "localhost"
var port int = 9000

func Test_StartServers() {
	//make ClusterConfig object, init it and set 0 as leader for start
	clustObj := ClusterConfig{} //set values}
	//start all the servers by calling ServerStart for each
	//Their raft object must also be collected as returned value so that appriopriate server's Append is called
	r0 := ServerStart(clustObj, 0)
	r1 := ServerStart(clustObj, 1)
	r2 := ServerStart(clustObj, 2)
	r3 := ServerStart(clustObj, 3)
	r4 := ServerStart(clustObj, 4)
}

func Test_AppendToLog() {
	//send a append request and monitor on commitCh
}

/*
//PASSED
//Checking server's affirmative responses and concurrency
func TestResponse(t *testing.T) {

	const n int = 5
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	set2 := "set bcd 30 5\r\nefghi\r\n"
	getm1 := "getm abc\r\n"
	getm2 := "getm bcd\r\n"
	del1 := "delete bcd\r\n"

	cmd := []string{set1, set2, getm1, getm2, del1}
	cd := []string{"set1", "set2", "getm1", "getm2", "del1"}
	E := []string{"OK", "VALUE 20 8\r\nabcdefjg", "VALUE 30 5\r\nefghi", "ERRNOTFOUND", "DELETED"}

	chann := make([]chan string, n)
	//for initialing the array
	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}

	//Launching Clients
	for i := 0; i < n; i++ {
		go clientCH.Client(chann[i], cmd[i], cd[i], hostname, port)
	}

	for j := 0; j < n; j++ {
		R := <-chann[j]
		Result := ""
		matched := 0
		RLine := strings.Split(R, "\r\n")
		R1 := strings.Fields(RLine[0])
		if RLine[1] == "" {
			//for response: OK <ver>\r\n
			Result = R1[0]
		} else {
			//For response: VALUE <ver> <exp> <numbytes>\r\n<dataBytes>\r\n
			Result = R1[0] + " " + R1[2] + " " + R1[3] + "\r\n" + RLine[1]
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

//For testing expiry and remaining exp in getm
func TestCheckAndExpire(t *testing.T) {
	const n int = 4
	chann := make([]chan string, n)

	//for initialing the array
	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}

	//Commands
	set1 := "set abc 3 8\r\nabcdefjg\r\n"
	set2 := "set abc 6 7\r\nmonikas\r\n"
	getm1 := "getm abc\r\n"

	//Expected values
	Eset1 := "OK"
	Egetm1 := "VALUE 2 8\r\nabcdefjg\r\n"
	Egetm2 := "VALUE 5 7\r\nmonikas\r\n"
	E := []string{Eset1, Egetm1, Eset1, Egetm2}

	go clientCH.Client(chann[0], set1, "set1", hostname, port)
	time.Sleep(time.Second * 1)
	go clientCH.Client(chann[1], getm1, "getm1", hostname, port)
	time.Sleep(time.Second * 1)
	go clientCH.Client(chann[2], set2, "set2", hostname, port)
	time.Sleep(time.Second * 1)
	go clientCH.Client(chann[3], getm1, "getm2", hostname, port)

	var R [n]string

	//Excluding hard coded version number
	Rset1 := strings.Fields(<-chann[0])
	R[0] = Rset1[0]
	RLine := strings.Split(<-chann[1], "\r\n")
	Rgetm1 := strings.Fields(RLine[0])
	R[1] = Rgetm1[0] + " " + Rgetm1[2] + " " + Rgetm1[3] + "\r\n" + RLine[1] + "\r\n"

	Rset2 := strings.Fields(<-chann[2])
	R[2] = Rset2[0]
	RLine1 := strings.Split(<-chann[3], "\r\n")
	Rgetm2 := strings.Fields(RLine1[0])
	R[3] = Rgetm2[0] + " " + Rgetm2[2] + " " + Rgetm2[3] + "\r\n" + RLine1[1] + "\r\n"

	for j := 0; j < n; j++ {
		if R[j] != E[j] {
			t.Error("Expected and Received values are:\r\n", E[j], R[j])
		}
	}

}

//PASSED
func TestErrors(t *testing.T) {
	const n int = 10
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
	cd := []string{"get1", "getm1", "cas1", "del1", "get1", "set1", "cas2", "get2", "set2", "cas3", "msg1"}
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
		go clientCH.Client(chann[i], cmd[i], cd[i], hostname, port)
		//Sleep is added so that all errors can be tested explicitly
		time.Sleep(time.Millisecond * 10)
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

//PASSED
//Checking multiple commands send by single user
func TestMultipleCmds(t *testing.T) {
	chann := make(chan string)
	const n int = 4
	cmd1 := "set abc 10 5\r\ndata1\r\ngetm abc\r\ndelete abc\r\ngetm abc\r\n"
	E := []string{"OK", "VALUE 10 5\r\ndata1\r\n", "DELETED", "ERRNOTFOUND"}

	go clientCH.Client(chann, cmd1, "cmd1", hostname, port)

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

//PASSED
//For Multiple request from multiple users and concurrency
func TestMRMC(t *testing.T) {
	const n int = 4  //No. of cmds per client
	const nC int = 2 //No.of clients
	cmd1 := "set abc 10 5\r\ndata1\r\ngetm abc\r\ndelete abc\r\ngetm abc\r\n"
	cmd2 := "set bcd 2 2\r\nmn\r\ngetm bcd\r\ndelete bcd\r\ngetm bcd\r\n"
	E := []string{"OK", "VALUE 10 5\r\ndata1", "VALUE 2 2\r\nmn", "DELETED", "ERRNOTFOUND"}
	cmd := []string{cmd1, cmd2}
	cd := []string{"cmd1", "cmd2"}

	//Declare and initialize channels
	chann := make([]chan string, n)
	for k := 0; k < nC; k++ {
		chann[k] = make(chan string)
	}
	// Launch clients
	for j := 0; j < nC; j++ {
		go clientCH.Client(chann[j], cmd[j], cd[j], hostname, port)
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
				Result = R1[0] + " " + R1[2] + " " + R1[3] + "\r\n" + RLine[1]
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

//PASSED
func TestFollowerRedirect(t *testing.T) {
	port = 9001
	chann := make(chan string)
	const n int = 1
	cmd1 := "set abc 10 5\r\ndata1\r\n"
	E := "ERR_REDIRECT localhost 9000"

	go clientCH.Client(chann, cmd1, "cmd1", hostname, port)
	R := <-chann
	if R != E {
		t.Errorf("Expected and Received values are:\r\n%v\n%v", E, R)
	}
}
*/
