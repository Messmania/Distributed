package Server

import (
	"strings"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	go Server()
}


//PASSED
//Checking server's affirmative responses
func TestResponse(t *testing.T) {
	t.Parallel()
	verStr := "7887"
	const n int = 6
	set1 := "set abc 20 8\r\nabcdefjg\r\n"
	set2 := "set bcd 30 5\r\nefghi\r\n"
	getm1 := "getm abc\r\n"
	getm2 := "getm bcd\r\n"
	cas1 := "cas bcd 30 " + verStr + " 8\r\nnewValue\r\n"
	del1 := "delete bcd\r\n"

	//Expected values
	Eset1 := "OK"
	Egetm1 := "VALUE 20 8\r\nabcdefjg\r\n"
	Egetm2 := "VALUE 30 5\r\nefghi\r\n"
	Edel1 := "DELETED\r\n"
	E := []string{Eset1, Eset1, Egetm1, Egetm2, Eset1, Edel1}

	cmd := []string{set1, set2, getm1, getm2, cas1, del1}
	cd := []string{"set1", "set2", "getm1", "getm2", "cas1", "del1"}
	chann := make([]chan string, n)

	//for initialing the array otherwise it is becoming nil
	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}

	//Launching clients
	for i := 0; i < n; i++ {
		go Client(chann[i], cmd[i], cd[i])
	}

	var R [n]string
	var ver string
	for j := 0; j < n; j++ {
		R[j] = <-chann[j]
		Rline := strings.Split(R[j], "\r\n")
		r := strings.Fields(Rline[0])
		if (j == 0) || (j == 1) || (j == 4) {
			if (j == 4) && (ver != verStr) {
				E[j] = "ERR_VERSION\r\n"
			} else {
				R[j] = r[0]
			}
		} else if (j == 2) || (j == 3) {
			if j == 3 {
				ver = r[1]
			}
			R[j] = r[0] + " " + r[2] + " " + r[3] + "\r\n" + Rline[1] + "\r\n"
		}
		if R[j] != E[j] {
			t.Error("Expected and Received values are:\r\n", E[j], R[j])
		}
	}

}

//For testing expiry and remaining exp in getm
func TestCheckAndExpire(t *testing.T) {
	t.Parallel()
	const n int = 4
	chann := make([]chan string, n)

	//for initialing the array otherwise it is becoming nil
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

	go Client(chann[0], set1, "set1")
	time.Sleep(time.Second * 1)
	go Client(chann[1], getm1, "getm1")
	time.Sleep(time.Second * 1)
	go Client(chann[2], set2, "set2")
	time.Sleep(time.Second * 1)
	go Client(chann[3], getm1, "getm2")

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
	t.Parallel()
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
		go Client(chann[i], cmd[i], cd[i])
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
	t.Parallel()
	chann := make(chan string)
	const n int = 4
	cmd1 := "set abc 10 5\r\ndata1\r\ngetm abc\r\ndelete abc\r\ngetm abc\r\n"
	E := []string{"OK", "VALUE 10 5\r\ndata1\r\n", "DELETED", "ERRNOTFOUND"}

	go Client(chann, cmd1, "cmd1")

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
//For Multiple request from multiple users
func TestMRMC(t *testing.T) {
	t.Parallel()
	const n int = 4
	const nC int = 2
	cmd1 := "set abc 10 5\r\ndata1\r\ngetm abc\r\ndelete abc\r\ngetm abc\r\n"
	cmd2 := "set bcd 2 2\r\nmn\r\ngetm bcd\r\ndelete bcd\r\ngetm bcd\r\n"
	E1 := []string{"OK", "VALUE 10 5\r\ndata1\r\n", "DELETED", "ERRNOTFOUND"}
	E2 := []string{"OK", "VALUE 2 2\r\nmn\r\n", "DELETED", "ERRNOTFOUND"}
	E := [][]string{E1, E2}
	cmd := []string{cmd1, cmd2}
	cd := []string{"cmd1", "cmd2"}

	//Declare and initialize channels
	chann := make([]chan string, n)
	for k := 0; k < n; k++ {
		chann[k] = make(chan string)
	}
	// Launch clients
	for j := 0; j < nC; j++ {
		go Client(chann[j], cmd[j], cd[j])
	}
	for i := 0; i < nC; i++ {
		for k := 0; k < n; k++ {
			R := <-chann[i]
			Rline := strings.Split(R, "\r\n")
			r := strings.Fields(Rline[0])
			if k == 0 {
				R = r[0]
			} else if k == 1 {
				R = r[0] + " " + r[2] + " " + r[3] + "\r\n" + Rline[1] + "\r\n"
			} else {
				R = Rline[0]
			}
			if R != E[i][k] {
				t.Errorf("Expected and Received values are:\r\n%v\n%v", E[i][k], R)
			}
		}
	}

}

//PASSED
//Test NO reply option
func TestNoReply(t *testing.T) {
	t.Parallel()
	const n int = 3
	cmd := "set mnp 10 8 noreply\r\nabcdefjg\r\nset bcd 15 5\r\nmonik\r\nget mnp\r\n"
	chann := make(chan string)

	//Expected outputs
	E := []string{"OK", "VALUE 8\r\nabcdefjg\r\n"}

	go Client(chann, cmd, "mc")
	for i := 0; i < n-1; i++ {
		R := <-chann
		Rline := strings.Split(R, "\r\n")
		r := strings.Fields(Rline[0])
		if i == 0 {
			R = r[0]
		} else if i == 1 {
			R = Rline[0] + "\r\n" + Rline[1] + "\r\n"
		}
		if R != E[i] {
			t.Error("Expected and Received values are:\r\n", E[i], R)
		}
	}

}


