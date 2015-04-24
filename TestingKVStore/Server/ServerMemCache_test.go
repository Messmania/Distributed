package Server

import (
	"fmt"
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
	//	var ver string
	for j := 0; j < n; j++ {
		R[j] = <-chann[j]
		fmt.Println("Response is:", R[j])

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
