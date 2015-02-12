package clientCH

import (
	"log"
	"net"
	"strings"
	//"fmt"
	"strconv"
)

func Client(ch chan string, strEcho string, c string, hostname string, port int) {
	service := hostname + ":" + strconv.Itoa(port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	checkErr(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		checkErr(err)
		return
	} else {
		cmd := SeparateCmds(strEcho)
		for i := 0; i < len(cmd); i++ {
			conn.Write([]byte(cmd[i]))
			var rep [512]byte
			n, err1 := conn.Read(rep[0:])
			checkErr(err1)
			reply := string(rep[0:n])
			ch <- reply
		}
	}
	conn.Close()
}



//For separating multiple cmds in a single string (MRMC case)
func SeparateCmds(str string) (cmd []string) {
	line := strings.Split(str, "\r\n")
	count := len(line) - 1
	args := make([][]string, count)
	for i := 0; i < count; i++ {
		args[i] = strings.Fields(line[i])
	}
	ptr := 0
	cmd = make([]string, 0)
	var newC string
	for j := 0; j < count; j++ {
		op := strings.ToLower(args[ptr][0])
		if (op == "set") || (op == "cas") {
			newC := line[ptr] + "\r\n" + line[ptr+1] + "\r\n"
			cmd = append(cmd, newC)
			ptr = ptr + 2
			j++
		} else if (op == "get") || (op == "getm") || (op == "delete") {
			newC = line[ptr] + "\r\n"
			cmd = append(cmd, newC)
			ptr++
		} else {
			newC = line[j]
			cmd = append(cmd, newC)
			ptr++
		}
	}
	return
}

func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered:", err)

	}
}
